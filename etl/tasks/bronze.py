import polars as pl
from prefect import task, get_run_logger
from etl.utils import get_db_connection, Schemas

def _sanitize_column_names(columns: list[str]) -> list[str]:
    """Helper: Converte i nomi delle colonne in snake_case."""
    return [col.lower().strip().replace(" ", "_").replace("-", "_").replace(".", "") for col in columns]

@task(name="bronze_ingestion")
def ingest_bronze(source_path: str, table_name: str, schema_overrides: dict = None) -> int:
    """
    Ingestione Generica CSV -> DuckDB (Bronze Schema).
    
    Args:
        source_path (str): Percorso del file CSV.
        table_name (str): Nome della tabella di destinazione (senza schema).
        schema_overrides (dict, optional): Dizionario per forzare i tipi (es. {"Box": pl.String}).
    """
    logger = get_run_logger()
    full_table_name = f"{Schemas.BRONZE}.{table_name}"
    

    logger.info(f"🚀 Avvio ingestione Bronze per: {table_name}")
    logger.info(f"   Source: {source_path}")

    try:
        # 1. Lazy Scan
        lazy_df = pl.scan_csv(
            source_path, 
            infer_schema_length=10000,
            schema_overrides=schema_overrides,
            ignore_errors=True
        )
        
        # 2. Rename Colonne
        old_cols = lazy_df.columns
        new_cols = _sanitize_column_names(old_cols)
        rename_map = dict(zip(old_cols, new_cols))
        lazy_df = lazy_df.rename(rename_map)

        # 3. Materializzazione su DuckDB
        with get_db_connection() as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.BRONZE}")
            con.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            
            logger.info(f" Scrittura in corso su {full_table_name}...")
            
            # Scrittura
            con.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM lazy_df")
            
            # Conta righe
            row_count = con.execute(f"SELECT count(*) FROM {full_table_name}").fetchone()[0]
            
        logger.info(f"Completato {table_name}. Righe: {row_count:,}")
        return row_count

    except Exception as e:
        logger.error(f" Errore critico ingestione {table_name}: {e}")
        raise e