from pathlib import Path
from datetime import datetime, timezone

from prefect import task, get_run_logger

from etl.utils import (
    get_db_connection,
    Schemas,
    sanitize_column_name,
)


def _utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _ensure_schema(con):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.BRONZE}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.META}")


def _table_exists(con, full_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        full_name.split(".", 1),
    ).fetchone()
    return row is not None


def _get_table_columns(con, full_name: str) -> list[str]:
    schema, table = full_name.split(".", 1)
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def _add_missing_columns(con, full_name: str, cols: list[str]) -> None:
    for c in cols:
        con.execute(f'ALTER TABLE {full_name} ADD COLUMN "{c}" VARCHAR')


def _source_relation(source_path: str) -> str:
    # In phase2 ingestiamo dal lake (parquet). Se capita CSV, supportiamolo comunque.
    return (
        "read_parquet(?)"
        if source_path.lower().endswith(".parquet")
        else "read_csv_auto(?, ALL_VARCHAR=TRUE)"
    )


def _get_source_cols_duckdb(con, source_path: str) -> list[str]:
    rel = _source_relation(source_path)
    rows = con.execute(
        f"DESCRIBE SELECT * FROM {rel}",
        [source_path],
    ).fetchall()

    cols = []
    for r in rows:
        colname = r[0]
        cols.append(sanitize_column_name(colname))
    return cols


def _detect_dataset_type(cols: list[str], source_path: str) -> str:
    cols_lc = {c.lower() for c in cols}
    path_lc = source_path.lower()

    has_call = "call_number" in cols_lc or "callnumber" in cols_lc
    has_inc = "incident_number" in cols_lc or "incidentnumber" in cols_lc

    if has_inc and has_call:
        if "fire_calls" in path_lc or "calls" in path_lc or "call" in path_lc:
            return "calls"
        if "fire_incidents" in path_lc or "incidents" in path_lc or "incident" in path_lc:
            return "incidents"
        return "incidents"

    if has_inc:
        return "incidents"
    if has_call:
        return "calls"

    if "incident" in path_lc:
        return "incidents"
    if "call" in path_lc:
        return "calls"

    return "incidents"


def _mark_done_and_upsert_success(run_id: str, pipeline_name: str, file_sha256: str):
    """
    - status DONE in ingestion_log per questa run_id
    - upsert in ingested_contents (così la prossima detect può fare SKIPPED)
    """
    now = _utcnow_naive()
    with get_db_connection() as con:
        con.execute(
            f"""
            UPDATE {Schemas.META}.ingestion_log
            SET status = 'DONE', error_message = NULL
            WHERE run_id = ? AND pipeline_name = ? AND file_sha256 = ?
            """,
            [run_id, pipeline_name, file_sha256],
        )

        # upsert: first_success_at resta il primo, last_success_at si aggiorna
        con.execute(
            f"""
            INSERT INTO {Schemas.META}.ingested_contents
              (pipeline_name, content_sha256, first_success_at, last_success_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (pipeline_name, content_sha256)
            DO UPDATE SET last_success_at = excluded.last_success_at
            """,
            [pipeline_name, file_sha256, now, now],
        )


@task(name="Phase2 - Bronze incremental ingest (from Data Lake if available)", retries=0)
def ingest_bronze_incremental(run_id: str, pipeline_name: str = "phase2_incremental") -> dict:
    """
    Legge i file PENDING della run_id da meta.ingestion_log e li inserisce in bronze.calls / bronze.incidents
    in modo idempotente tramite (_source_sha256, _source_row_number).

    Fonte preferita: Data Lake (lake_path -> parquet).
    Fallback: file_path (csv da Client_drop) se lake_path è NULL.

    NOTE:
    - Ingest "streaming" lato DuckDB: NON carica l'intero file in memoria.
    - inserted_* conta SOLO le righe effettivamente nuove inserite in questa run.
    """
    logger = get_run_logger()

    # Recupero i PENDING una volta, poi processo file-per-file
    with get_db_connection() as con:
        _ensure_schema(con)
        rows = con.execute(
            f"""
            SELECT file_path, file_sha256, lake_path
            FROM {Schemas.META}.ingestion_log
            WHERE run_id = ? AND pipeline_name = ? AND status = 'PENDING'
            """,
            [run_id, pipeline_name],
        ).fetchall()

    if not rows:
        logger.info("Nessun file PENDING da ingerire in bronze.")
        return {"inserted_calls": 0, "inserted_incidents": 0, "files": 0}

    inserted_calls = 0
    inserted_incidents = 0

    for file_path, file_sha256, lake_path in rows:
        source_path = str(lake_path) if lake_path else str(file_path)
        logger.info(f"Ingest source: {source_path}")

        with get_db_connection() as con:
            _ensure_schema(con)

            # 1) Colonne dalla sorgente (NO load in memoria)
            src_cols = _get_source_cols_duckdb(con, source_path)

            # 2) Detect dataset
            dataset = _detect_dataset_type(src_cols, source_path)
            target = f"{Schemas.BRONZE}.{dataset}"

            # 3) Colonne tecniche (idempotenza)
            tech_cols = [
                "_source_row_number",
                "_source_sha256",
                "_source_file_path",
                "_ingested_at_utc",
            ]

            # 4) Se tabella non esiste, la creo VUOTA ma coerente (VARCHAR) con tutte le colonne attese
            if not _table_exists(con, target):
                logger.info(f"Tabella {target} non esiste: creazione.")
                rel = _source_relation(source_path)
                select_src = ",\n".join([f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in src_cols])

                con.execute(
                    f"""
                    CREATE TABLE {target} AS
                    SELECT
                      {select_src},
                      CAST(row_number() OVER () - 1 AS VARCHAR) AS _source_row_number,
                      CAST(? AS VARCHAR) AS _source_sha256,
                      CAST(? AS VARCHAR) AS _source_file_path,
                      CAST(? AS VARCHAR) AS _ingested_at_utc
                    FROM {rel}
                    WHERE 1=0
                    """,
                    # ordine placeholder: sha256, file_path, ingested_at, source_path(for read_parquet/read_csv)
                    [file_sha256, source_path, _utcnow_naive(), source_path],
                )

            # 5) Schema evolution: aggiungo eventuali colonne nuove (sia src che tech)
            existing_cols = _get_table_columns(con, target)
            desired_cols = src_cols + tech_cols

            new_cols = [c for c in desired_cols if c not in existing_cols]
            if new_cols:
                logger.info(f"Aggiungo nuove colonne a {target}: {new_cols}")
                _add_missing_columns(con, target, new_cols)
                existing_cols = _get_table_columns(con, target)

            # 6) SELECT che produce esattamente le colonne della tabella (mappa src->VARCHAR + tech)
            rel = _source_relation(source_path)

            select_map = {c: f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in src_cols}
            select_map["_source_row_number"] = 'CAST(row_number() OVER () - 1 AS VARCHAR) AS "_source_row_number"'
            select_map["_source_sha256"] = 'CAST(? AS VARCHAR) AS "_source_sha256"'
            select_map["_source_file_path"] = 'CAST(? AS VARCHAR) AS "_source_file_path"'
            select_map["_ingested_at_utc"] = 'CAST(? AS VARCHAR) AS "_ingested_at_utc"'

            # colonne presenti in tabella ma assenti nella sorgente -> NULL
            for c in existing_cols:
                if c not in select_map:
                    select_map[c] = f'CAST(NULL AS VARCHAR) AS "{c}"'

            final_select = ",\n".join([select_map[c] for c in existing_cols])

            # 7) Count PRIMA dell'insert (solo per questo sha)
            before_for_sha = con.execute(
                f"SELECT COUNT(*) FROM {target} WHERE _source_sha256 = ?",
                [file_sha256],
            ).fetchone()[0]

            # 8) INSERT idempotente (una sola volta!)
            con.execute(
                f"""
                INSERT INTO {target}
                SELECT t.*
                FROM (
                  SELECT
                    {final_select}
                  FROM {rel}
                ) t
                WHERE NOT EXISTS (
                  SELECT 1 FROM {target} b
                  WHERE b._source_sha256 = t._source_sha256
                    AND b._source_row_number = t._source_row_number
                )
                """,
                # ordine placeholder: sha256, file_path, ingested_at, source_path
                [file_sha256, source_path, _utcnow_naive(), source_path],
            )

            # 9) Count DOPO l'insert (solo per questo sha)
            after_for_sha = con.execute(
                f"SELECT COUNT(*) FROM {target} WHERE _source_sha256 = ?",
                [file_sha256],
            ).fetchone()[0]

            inserted_now = int(after_for_sha - before_for_sha)
            total_for_sha = int(after_for_sha)

        # contatori: SOLO righe nuove effettivamente inserite
        if dataset == "calls":
            inserted_calls += inserted_now
        else:
            inserted_incidents += inserted_now

        logger.info(
            f"Bronze file done | dataset={dataset} inserted_now={inserted_now} total_for_sha={total_for_sha} sha={file_sha256}"
        )

        # Marca DONE + upsert in ingested_contents (incrementale vero)
        _mark_done_and_upsert_success(run_id=run_id, pipeline_name=pipeline_name, file_sha256=file_sha256)

    logger.info(
        f"Bronze ingest completato | inserted_calls={inserted_calls} inserted_incidents={inserted_incidents} files={len(rows)}"
    )
    return {"inserted_calls": inserted_calls, "inserted_incidents": inserted_incidents, "files": len(rows)}
