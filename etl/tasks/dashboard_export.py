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


def _utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _sql_quote_path(p: Path) -> str:
    """
    DuckDB vuole stringhe con apici singoli.
    Per sicurezza raddoppiamo eventuali apici singoli nel path.
    """
    s = str(p)
    return "'" + s.replace("'", "''") + "'"


def export_dashboard_db(
    output_path: str | Path = DEFAULT_EXPORT_DB,
    warehouse_db: str | Path = DEFAULT_WAREHOUSE_DB,
) -> Path:
    """
    Export serving DB cloud-friendly (dashboard.duckdb) con:
      - KPI (gold.v_kpi_*)
      - Dimensioni (gold.dim_*)
    Non esporta fact_incident (troppo grande).

    Compatibile con Prefect flow che passa output_path=...
    """
    warehouse_db = Path(warehouse_db)
    output_path = Path(output_path)

    if not warehouse_db.exists():
        raise FileNotFoundError(f"Warehouse DB non trovato: {warehouse_db}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        output_path.unlink()

    dst = duckdb.connect(str(output_path))

    # ATTACH non supporta placeholder "?"
    attached = _sql_quote_path(warehouse_db.resolve())
    dst.execute(f"ATTACH {attached} AS src (READ_ONLY)")

    # Metadata
    dst.execute(
        """
        CREATE TABLE dashboard_metadata (
            exported_at TIMESTAMP,
            source_db   VARCHAR,
            note        VARCHAR
        )
        """
    )
    dst.execute(
        "INSERT INTO dashboard_metadata VALUES (?, ?, ?)",
        [
            _utcnow_naive(),
            str(warehouse_db),
            "Serving DB per Streamlit Cloud: KPI + dimensioni (no fact)",
        ],
    )

    # Export KPI views
    for view in KPI_VIEWS:
        dst.execute(
            f"""
            CREATE TABLE {view} AS
            SELECT *
            FROM src.gold.{view}
            """
        )

    # Export dimensions
    for dim in DIM_TABLES:
        dst.execute(
            f"""
            CREATE TABLE {dim} AS
            SELECT *
            FROM src.gold.{dim}
            """
        )

    dst.execute("DETACH src")
    dst.close()

    print("Export completato:", output_path.resolve())
    return output_path


if __name__ == "__main__":
    export_dashboard_db()
