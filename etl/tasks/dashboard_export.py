from pathlib import Path
from datetime import datetime, timezone
import duckdb

# =========================
# Config
# =========================

WAREHOUSE_DB = Path("data/warehouse.duckdb")
EXPORT_DIR = Path("dashboard_exports")
EXPORT_DB = EXPORT_DIR / "dashboard.duckdb"

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


# =========================
# Helpers
# =========================

def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


# =========================
# Main export logic
# =========================

def export_dashboard_db():
    if not WAREHOUSE_DB.exists():
        raise FileNotFoundError(f"Warehouse DB non trovato: {WAREHOUSE_DB}")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Rimuovo export precedente se esiste
    if EXPORT_DB.exists():
        EXPORT_DB.unlink()

    # Connessione DB sorgente
    src = duckdb.connect(str(WAREHOUSE_DB), read_only=True)

    # Connessione DB destinazione (dashboard)
    dst = duckdb.connect(str(EXPORT_DB))

    # =========================
    # Metadata
    # =========================
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
        """
        INSERT INTO dashboard_metadata
        VALUES (?, ?, ?)
        """,
        [
            utcnow(),
            str(WAREHOUSE_DB),
            "Serving DB per Streamlit Cloud: KPI + dimensioni (no fact)"
        ],
    )

    # =========================
    # Export KPI views
    # =========================
    for view in KPI_VIEWS:
        print(f"Export KPI view: gold.{view}")
        dst.execute(
            f"""
            CREATE TABLE {view} AS
            SELECT *
            FROM src.gold.{view}
            """
        )

    # =========================
    # Export dimension tables
    # =========================
    for dim in DIM_TABLES:
        print(f"Export dimension: gold.{dim}")
        dst.execute(
            f"""
            CREATE TABLE {dim} AS
            SELECT *
            FROM src.gold.{dim}
            """
        )

    # =========================
    # Cleanup
    # =========================
    src.close()
    dst.close()

    print("Export completato:", EXPORT_DB.resolve())


if __name__ == "__main__":
    export_dashboard_db()
