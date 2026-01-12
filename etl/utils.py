import duckdb
from contextlib import contextmanager

# Costanti di progetto
DB_PATH = "data/warehouse.duckdb"

class Schemas:
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

@contextmanager
def get_db_connection(read_only: bool = False):
    """
    Context manager per gestire la connessione e la chiusura sicura di DuckDB.
    Usage:
        with get_db_connection() as con:
            con.execute(...)
    """
    con = duckdb.connect(DB_PATH, read_only=read_only)
    try:
        yield con
    finally:
        con.close()