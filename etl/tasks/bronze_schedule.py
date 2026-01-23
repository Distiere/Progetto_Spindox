import polars as pl
from datetime import datetime, timezone
from prefect import task, get_run_logger

from utils import get_db_connection, Schemas, sanitize_column_name, sanitize_columns


def _utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _ensure_schema(con):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.BRONZE}")


def _table_exists(con, full_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1",
        full_name.split("."),
    ).fetchone()
    return row is not None


def _get_table_columns(con, full_name: str) -> list[str]:
    schema, table = full_name.split(".")
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


def _add_missing_columns(con, full_name: str, missing: list[str]):
    # in bronze teniamo tutto come VARCHAR per robustezza
    for c in missing:
        con.execute(f"ALTER TABLE {full_name} ADD COLUMN {c} VARCHAR")


def _read_header_sanitized(csv_path: str) -> list[str]:
    df0 = pl.read_csv(csv_path, n_rows=0)
    return sanitize_columns(df0.columns)


def _detect_dataset_type(cols: list[str]) -> str:
    cols_set = set(cols)

    # PRIORITÀ: incidents
    if "incident_number" in cols_set:
        return "incidents"

    if "call_number" in cols_set:
        return "calls"

    return "incidents"


def _load_file_polars(path: str) -> pl.DataFrame:
    """
    Carica CSV o Parquet in Polars, sanitizza colonne e cast a string.
    """
    if path.lower().endswith(".parquet"):
        df = pl.read_parquet(path)
    else:
        df = pl.read_csv(path, infer_schema_length=5000)

    df.columns = sanitize_columns(df.columns)
    df = df.with_columns([pl.all().cast(pl.Utf8)])
    return df

def _source_relation(source_path: str) -> str:
    # In phase2 ingestiamo dal lake (parquet). Se capita CSV, supportiamolo comunque.
    return "read_parquet(?)" if source_path.lower().endswith(".parquet") else "read_csv_auto(?, ALL_VARCHAR=TRUE)"

def _get_source_cols_duckdb(con, source_path: str) -> list[str]:
    rel = _source_relation(source_path)
    rows = con.execute(f"DESCRIBE SELECT * FROM {rel}", [source_path]).fetchall()
    # DESCRIBE: (column_name, column_type, null, key, default, extra)
    return [sanitize_column_name(r[0]) for r in rows]


@task(name="Phase2 - Bronze incremental ingest (from Data Lake if available)", retries=0)
def ingest_bronze_incremental(run_id: str, pipeline_name: str = "phase2_incremental") -> dict:
    """
    Legge i file PENDING della run_id da meta.ingestion_log e li inserisce in bronze.calls / bronze.incidents
    in modo idempotente tramite (_source_sha256, _source_row_number).

    Fonte preferita: Data Lake (lake_path -> parquet).
    Fallback: file_path (csv da Client_drop) se lake_path è NULL.
    """
    logger = get_run_logger()

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

        target = None
        count = 0

        with get_db_connection() as con:
            _ensure_schema(con)

            # 1) Leggo lo schema colonne dalla sorgente (NO load in memoria)
            src_cols = _get_source_cols_duckdb(con, source_path)

            # 2) Detect dataset (qui DEVE essere incidents-first se usi merged)
            dataset = _detect_dataset_type(src_cols)
            target = f"{Schemas.BRONZE}.{dataset}"

            # 3) Colonne tecniche (sempre presenti in bronze)
            tech_cols = [
                "_source_row_number",
                "_source_sha256",
                "_source_file_path",
                "_ingested_at_utc",
            ]

            # 4) Se tabella non esiste, la creo con schema coerente (tutto VARCHAR)
            if not _table_exists(con, target):
                logger.info(f"Tabella {target} non esiste: creazione.")

                rel = _source_relation(source_path)

                # Seleziono le colonne source come VARCHAR mantenendo i nomi sanitize
                # IMPORTANT: qui assumiamo che i nomi colonna nella sorgente siano già compatibili
                # con sanitize_column_name. Se non lo fossero, serve mappatura (ma nel tuo caso lo sono).
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
                    [file_sha256, source_path, _utcnow_naive(), source_path],
                )

            # 5) Schema evolution: aggiungo eventuali nuove colonne source + tech
            existing_cols = _get_table_columns(con, target)
            desired_cols = src_cols + tech_cols

            new_cols = [c for c in desired_cols if c not in existing_cols]
            if new_cols:
                logger.info(f"Aggiungo nuove colonne a {target}: {new_cols}")
                _add_missing_columns(con, target, new_cols)
                existing_cols = _get_table_columns(con, target)

            # 6) Costruisco SELECT che produce ESATTAMENTE le colonne di tabella
            # - source cols -> cast varchar
            # - tech cols -> valorizzate
            # - colonne in tabella ma non nel source -> NULL
            rel = _source_relation(source_path)

            select_map = {c: f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in src_cols}
            select_map["_source_row_number"] = 'CAST(row_number() OVER () - 1 AS VARCHAR) AS "_source_row_number"'
            select_map["_source_sha256"] = 'CAST(? AS VARCHAR) AS "_source_sha256"'
            select_map["_source_file_path"] = 'CAST(? AS VARCHAR) AS "_source_file_path"'
            select_map["_ingested_at_utc"] = 'CAST(? AS VARCHAR) AS "_ingested_at_utc"'

            for c in existing_cols:
                if c not in select_map:
                    select_map[c] = f'CAST(NULL AS VARCHAR) AS "{c}"'

            final_select = ",\n".join([select_map[c] for c in existing_cols])

            # 7) INSERT idempotente: anti-join su sha + row_number
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
                [source_path, file_sha256, source_path, _utcnow_naive()],
            )

            count = con.execute(
                f"SELECT COUNT(*) FROM {target} WHERE _source_sha256 = ?",
                [file_sha256],
            ).fetchone()[0]

        if dataset == "calls":
            inserted_calls += int(count)
        else:
            inserted_incidents += int(count)

    logger.info(
        f"Bronze ingest completato | calls={inserted_calls} incidents={inserted_incidents} files={len(rows)}"
    )
    return {"inserted_calls": inserted_calls, "inserted_incidents": inserted_incidents, "files": len(rows)}
