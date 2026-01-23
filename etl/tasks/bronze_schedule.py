import polars as pl
from datetime import datetime, timezone
from prefect import task, get_run_logger

from utils import get_db_connection, Schemas, sanitize_columns


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
    # Regola robusta: calls ha call_number
    if "call_number" in cols:
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

        # Detect dataset: se parquet, prendiamo direttamente le colonne dal df;
        # se csv, leggiamo header per evitare load completo doppio
        if source_path.lower().endswith(".parquet"):
            df = _load_file_polars(source_path)
            cols = df.columns
        else:
            cols = _read_header_sanitized(source_path)
            df = _load_file_polars(source_path)

        dataset = _detect_dataset_type(cols)

        # Colonne tecniche per idempotenza (robuste anche se già presenti nel parquet)
        if "_source_row_number" not in df.columns:
            df = df.with_row_index(name="_source_row_number")

        df = df.with_columns([
            pl.col("_source_row_number").cast(pl.Utf8).alias("_source_row_number"),
            pl.lit(file_sha256).cast(pl.Utf8).alias("_source_sha256"),
            pl.lit(source_path).cast(pl.Utf8).alias("_source_file_path"),
            pl.lit(_utcnow_naive()).cast(pl.Utf8).alias("_ingested_at_utc"),
        ])

        target = f"{Schemas.BRONZE}.{dataset}"

        with get_db_connection() as con:
            _ensure_schema(con)

            if not _table_exists(con, target):
                logger.info(f"Tabella {target} non esiste: creazione.")
                con.register("tmp_df", df.to_arrow())
                con.execute(f"CREATE TABLE {target} AS SELECT * FROM tmp_df")

                # count righe per questo sha (dopo create)
                count = con.execute(
                    f"SELECT COUNT(*) FROM {target} WHERE _source_sha256 = ?",
                    [file_sha256],
                ).fetchone()[0]

            else:
                existing_cols = _get_table_columns(con, target)
                df_cols = df.columns

                # se il df ha colonne nuove -> alter table
                new_cols = [c for c in df_cols if c not in existing_cols]
                if new_cols:
                    logger.info(f"Aggiungo nuove colonne a {target}: {new_cols}")
                    _add_missing_columns(con, target, new_cols)
                    existing_cols = _get_table_columns(con, target)

                # se mancano colonne nel df -> le aggiungo come NULL
                missing_in_df = [c for c in existing_cols if c not in df_cols]
                if missing_in_df:
                    df = df.with_columns([pl.lit(None).cast(pl.Utf8).alias(c) for c in missing_in_df])

                # reorder come la tabella
                df = df.select(existing_cols)

                con.register("tmp_df", df.to_arrow())

                # INSERT idempotente (stesso contenuto riletto -> no duplicates)
                con.execute(
                    f"""
                    INSERT INTO {target}
                    SELECT t.*
                    FROM tmp_df t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {target} b
                        WHERE b._source_sha256 = t._source_sha256
                          AND b._source_row_number = t._source_row_number
                    )
                    """
                )

                # count righe per questo sha (post-insert)
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
