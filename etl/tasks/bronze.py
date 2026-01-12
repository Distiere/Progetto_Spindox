import polars as pl
from prefect import task, get_run_logger

from etl.utils import get_db_connection, Schemas


def _sanitize_column_names(columns: list[str]) -> list[str]:
    """Converte i nomi delle colonne in snake_case (semplice)."""
    return [
        col.lower().strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "")
        for col in columns
    ]


@task(name="bronze_ingestion")
def ingest_bronze(source_path: str, table_name: str, schema_overrides: dict | None = None) -> int:
    """
    Ingestione CSV -> DuckDB (schema bronze).

    Args:
        source_path: percorso del CSV
        table_name: nome tabella DEST (senza schema), es. "calls"
        schema_overrides: dict opzionale per forzare dtypes (colonne miste)
    """
    logger = get_run_logger()
    full_table_name = f"{Schemas.BRONZE}.{table_name}"

    logger.info(f"🚀 Avvio ingestione Bronze per: {table_name}")
    logger.info(f"   Source: {source_path}")

    try:
        # 1) Scan lazy 
        lazy_df = pl.scan_csv(
            source_path,
            infer_schema_length=10000,
            schema_overrides=schema_overrides,
            null_values=["None", "NULL", ""],
            ignore_errors=True,
            truncate_ragged_lines=True,
        )

        # 2) Sanitize colonne
        old_cols = lazy_df.collect_schema().names()
        new_cols = _sanitize_column_names(old_cols)
        rename_map = dict(zip(old_cols, new_cols))
        lazy_df = lazy_df.rename(rename_map)

        df = lazy_df.collect(streaming=True)

        # 4) Scrivi su DuckDB
        with get_db_connection() as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.BRONZE}")
            con.execute(f"DROP TABLE IF EXISTS {full_table_name}")

            con.register("tmp_df", df.to_arrow())

            logger.info(f"   Scrittura in corso su {full_table_name}...")
            con.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM tmp_df")

            row_count = con.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0]

        logger.info(f"✅ Completato {table_name}. Righe: {row_count:,}")
        return int(row_count)

    except Exception as e:
        logger.error(f" Errore critico ingestione {table_name}: {e}")
        raise
