from prefect import task, get_run_logger
from etl.utils import get_db_connection

def _assert_sql(con, sql: str, msg: str):
    val = con.execute(sql).fetchone()[0]
    if val and int(val) != 0:
        raise ValueError(f"{msg} (count={val})")

@task(name="validate_gold_quality")
def validate_gold_quality() -> None:
    """
    Quality gate GOLD: blocca la pipeline se la fact non è 'joinabile' o ha chiavi/misure incoerenti.
    Check SQL su tutta la tabella (veloci e affidabili).
    """
    logger = get_run_logger()

    with get_db_connection(read_only=True) as con:
        logger.info("Avvio quality gate su GOLD")

        # 1) fact non vuota
        _assert_sql(
            con,
            "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM gold.fact_incident;",
            "FAIL: gold.fact_incident è vuota",
        )

        # 2) chiavi fondamentali non nulle
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM gold.fact_incident WHERE incident_number IS NULL;",
            "FAIL: incident_number NULL in gold.fact_incident",
        )
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM gold.fact_incident WHERE date_id IS NULL;",
            "FAIL: date_id NULL in gold.fact_incident",
        )

        # 3) FK verso dimensioni (copertura join) — di solito vuoi che siano quasi sempre valorizzate
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM gold.fact_incident WHERE location_id IS NULL;",
            "FAIL: location_id NULL (join con dim_location non coperto)",
        )
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM gold.fact_incident WHERE incident_type_id IS NULL;",
            "FAIL: incident_type_id NULL (join con dim_incident_type non coperto)",
        )

        # 4) coerenza misure temporali (non negative)
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM gold.fact_incident WHERE response_time_sec < 0;",
            "FAIL: response_time_sec negativo in gold.fact_incident",
        )
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM gold.fact_incident WHERE dispatch_delay_sec < 0;",
            "FAIL: dispatch_delay_sec negativo in gold.fact_incident",
        )
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM gold.fact_incident WHERE travel_time_sec < 0;",
            "FAIL: travel_time_sec negativo in gold.fact_incident",
        )
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM gold.fact_incident WHERE incident_duration_sec < 0;",
            "FAIL: incident_duration_sec negativo in gold.fact_incident",
        )

        logger.info("Quality gate GOLD superato.")
