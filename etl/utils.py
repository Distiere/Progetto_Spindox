from __future__ import annotations

import os
import re
from contextlib import contextmanager

import duckdb


# -------------------------
# Project constants
# -------------------------
# Warehouse path can be overridden for safe test runs:
#   Windows (PowerShell): $env:WAREHOUSE_DB_PATH="data/warehouse_test.duckdb"
DB_PATH = os.getenv("WAREHOUSE_DB_PATH", "data/warehouse.duckdb")

# Client drop zone (Phase 2)
CLIENT_DROP_DIR = os.getenv("CLIENT_DROP_DIR", "data/Client_drop")


class Schemas:
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    META = "meta"


@contextmanager
def get_db_connection(read_only: bool = False):
    """Context manager per DuckDB."""
    con = duckdb.connect(DB_PATH, read_only=read_only)
    try:
        yield con
    finally:
        con.close()


# -------------------------
# Column name sanitization
# -------------------------
_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")
_ACRONYM = re.compile(r"([A-Z]+)([A-Z][a-z])")


def sanitize_column_name(col: str) -> str:
    """Converte un nome colonna in snake_case in modo robusto.

    Gestisce:
    - spazi / '-' / '.' -> '_'
    - CamelCase -> snake_case
    - acronimi tipo 'DtTm' -> 'dt_tm'
    """
    c = col.strip()

    # normalize separators first
    c = c.replace(" ", "_").replace("-", "_").replace(".", "_")

    # deal with acronyms boundaries: 'DT' + 'Tm' -> 'DT_Tm'
    c = _ACRONYM.sub(r"\1_\2", c)

    # camelCase / PascalCase boundaries
    c = _CAMEL_1.sub(r"\1_\2", c)
    c = _CAMEL_2.sub(r"\1_\2", c)

    # collapse multiple underscores
    c = re.sub(r"__+", "_", c)

    return c.lower().strip("_")


def sanitize_columns(columns: list[str]) -> list[str]:
    return [sanitize_column_name(c) for c in columns]
