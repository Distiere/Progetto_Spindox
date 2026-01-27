from prefect import task, get_run_logger
from etl.utils import get_db_connection, Schemas

@task(name="gold_create_kpi_views")
def create_kpi_views() -> None:
    logger = get_run_logger()

    with get_db_connection() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.GOLD}")

        # Q1 - Avg response time (trend mensile)
        con.execute("DROP VIEW IF EXISTS gold.v_kpi_response_time_month")
        con.execute("""
        CREATE VIEW gold.v_kpi_response_time_month AS
        SELECT
            d.year,
            d.month,
            AVG(f.response_time_sec) AS avg_response_time_sec,
            COUNT(*) AS n_incidents
        FROM gold.fact_incident f
        JOIN gold.dim_date d ON d.date_id = f.date_id
        WHERE f.response_time_sec IS NOT NULL
        GROUP BY 1,2
        ORDER BY 1,2
        """)

        # Q2 - Incident count per mese
        con.execute("DROP VIEW IF EXISTS gold.v_kpi_incident_volume_month")
        con.execute("""
        CREATE VIEW gold.v_kpi_incident_volume_month AS
        SELECT
            d.year,
            d.month,
            COUNT(*) AS incident_count
        FROM gold.fact_incident f
        JOIN gold.dim_date d ON d.date_id = f.date_id
        GROUP BY 1,2
        ORDER BY 1,2
        """)

        # Q3 - Top incident types
        con.execute("DROP VIEW IF EXISTS gold.v_kpi_top_incident_type")
        con.execute("""
        CREATE VIEW gold.v_kpi_top_incident_type AS
        SELECT
            it.call_type_group,
            it.call_type,
            COUNT(*) AS incident_count,
            AVG(f.response_time_sec) AS avg_response_time_sec
        FROM gold.fact_incident f
        JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
        GROUP BY 1,2
        ORDER BY incident_count DESC
        """)

        logger.info("Create KPI views in gold.*")
