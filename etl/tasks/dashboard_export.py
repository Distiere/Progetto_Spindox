from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import duckdb

DEFAULT_WAREHOUSE_DB = Path("data/warehouse.duckdb")
DEFAULT_EXPORT_DB = Path("dashboard_exports/dashboard.duckdb")

KPI_VIEWS = [
    "v_kpi_incident_volume_month",
    "v_kpi_response_time_month",
    "v_kpi_top_incident_type",
]

DIM_TABLES = [
    "dim_date",
    "dim_incident_type",
    "dim_location",
]


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _sql_quote_path(p: Path) -> str:
    s = str(p)
    return "'" + s.replace("'", "''") + "'"


def _exists_in_src(dst: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    """
    Verifica esistenza oggetto (table/view) dentro al DB attached "src".
    In DuckDB l'information_schema è globale, quindi si filtra per table_catalog='src'.
    """
    row = dst.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_catalog = 'src'
          AND table_schema = ?
          AND table_name = ?
        LIMIT 1
        """,
        [schema, name],
    ).fetchone()
    return row is not None


def export_dashboard_db(
    output_path: str | Path = DEFAULT_EXPORT_DB,
    warehouse_db: str | Path = DEFAULT_WAREHOUSE_DB,
) -> Path:
    """
    Export serving DB cloud-friendly (dashboard.duckdb) con:
      - KPI (gold.v_kpi_*)
      - Dimensioni (gold.dim_*)
      - Metadata (meta.dashboard_metadata)

    Non esporta fact_incident (troppo grande).
    """
    warehouse_db = Path(warehouse_db)
    output_path = Path(output_path)

    if not warehouse_db.exists():
        raise FileNotFoundError(f"Warehouse DB non trovato: {warehouse_db}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # rigenera sempre
    if output_path.exists():
        output_path.unlink()

    dst = duckdb.connect(str(output_path))

    # ATTACH sorgente in read-only
    attached = _sql_quote_path(warehouse_db.resolve())
    dst.execute(f"ATTACH {attached} AS src (READ_ONLY)")

    # Schemi target nel DB export
    dst.execute("CREATE SCHEMA IF NOT EXISTS gold")
    dst.execute("CREATE SCHEMA IF NOT EXISTS meta")

    # Metadata nello schema meta
    dst.execute(
        """
        CREATE TABLE meta.dashboard_metadata (
            exported_at TIMESTAMP,
            source_db   VARCHAR,
            note        VARCHAR
        )
        """
    )
    dst.execute(
        "INSERT INTO meta.dashboard_metadata VALUES (?, ?, ?)",
        [
            _utcnow_naive(),
            str(warehouse_db),
            "Serving DB per Streamlit Cloud: KPI + dimensioni (no fact)",
        ],
    )

    # Export KPI -> gold.*
    for view in KPI_VIEWS:
        if not _exists_in_src(dst, "gold", view):
            raise RuntimeError(
                f"Nel warehouse mancano oggetti richiesti: src.gold.{view}. "
                f"Esegui la pipeline fino al GOLD prima dell'export."
            )
        dst.execute(f"DROP TABLE IF EXISTS gold.{view}")
        dst.execute(
            f"""
            CREATE TABLE gold.{view} AS
            SELECT * FROM src.gold.{view}
            """
        )

    # Export dimensions -> gold.*
    for dim in DIM_TABLES:
        if not _exists_in_src(dst, "gold", dim):
            raise RuntimeError(
                f"Nel warehouse mancano oggetti richiesti: src.gold.{dim}. "
                f"Esegui la pipeline fino al GOLD prima dell'export."
            )
        dst.execute(f"DROP TABLE IF EXISTS gold.{dim}")
        dst.execute(
            f"""
            CREATE TABLE gold.{dim} AS
            SELECT * FROM src.gold.{dim}
            """
        )

    dst.execute("DETACH src")
    dst.close()

    print("Export completato:", output_path.resolve())
    return output_path


if __name__ == "__main__":
    export_dashboard_db()
