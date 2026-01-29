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
    con.execute("PRAGMA temp_directory='data/tmp_duckdb'")
    con.execute("PRAGMA memory_limit='8GB'")
    con.execute("PRAGMA threads=4")

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
    c = col.strip()
    keep_leading_underscore = c.startswith("_")

    c = c.replace(" ", "_").replace("-", "_").replace(".", "_")
    c = _ACRONYM.sub(r"\1_\2", c)
    c = _CAMEL_1.sub(r"\1_\2", c)
    c = _CAMEL_2.sub(r"\1_\2", c)
    c = re.sub(r"__+", "_", c)

    c = c.lower().strip("_")

    if keep_leading_underscore:
        c = "_" + c  # forza underscore iniziale

    return c



def sanitize_columns(columns: list[str]) -> list[str]:
    return [sanitize_column_name(c) for c in columns]
