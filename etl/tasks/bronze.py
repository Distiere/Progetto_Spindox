import polars as pl
from prefect import task, get_run_logger
from etl.utils import get_db_connection, Schemas

def _sanitize_column_names(columns: list[str]) -> list[str]:
    """Helper function: Converte una lista di stringhe in snake_case."""
    return [col.lower().strip().replace(" ", "_").replace("-", "_") for col in columns]

@task(name="bronze_ingestion")
def ingest_bronze(source_path: str) -> int:
    """
    Ingestione Raw Data: CSV -> DuckDB (Bronze Schema).
    
    Args:
        source_path (str): Percorso del file CSV sorgente.
        
    Returns:
        int: Numero di righe ingerite.
    """
    logger = get_run_logger()
    table_name = f"{Schemas.BRONZE}.calls"
    
    logger.info(f" Avvio ingestione Bronze. Source: {source_path}")

    try:
        # 1. Lazy Scan: Definiamo il piano di lettura senza caricare in memoria
        lazy_df = pl.scan_csv(source_path, infer_schema_length=10000)
        
        # 2. Rename Colonne (Sanitizzazione)
        old_cols = lazy_df.columns
        new_cols = _sanitize_column_names(old_cols)
        
        # Creiamo la mappatura {vecchio_nome: nuovo_nome}
        rename_map = dict(zip(old_cols, new_cols))
        lazy_df = lazy_df.rename(rename_map)

        # 3. Materializzazione su DuckDB
        with get_db_connection() as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.BRONZE}")
            
            # Full Refresh pattern: Drop & Re-create
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            logger.info(f"💾 Scrittura in corso su {table_name}...")
            
            # Eseguiamo la query Polars direttamente dentro DuckDB
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM lazy_df")
            
            # Verifica e metriche
            row_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            
        logger.info(f"Ingestione completata. Righe processate: {row_count:,}")
        return row_count

    except Exception as e:
        logger.error(f"Errore critico durante l'ingestione Bronze: {e}")
        raise e