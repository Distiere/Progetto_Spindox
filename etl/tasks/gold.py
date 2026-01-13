from prefect import task, get_run_logger
from etl.utils import get_db_connection, Schemas

@task(name="gold_dim_date")
def build_dim_date() -> None:
    logger = get_run_logger()

    with get_db_connection() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.GOLD}")

        # Ricostruisco dim_date prendendo TUTTE le date presenti (calls + incidents)
        con.execute("DROP TABLE IF EXISTS gold.dim_date")

        con.execute("""
        CREATE TABLE gold.dim_date AS
        WITH all_dates AS (
            SELECT DISTINCT call_date AS d
            FROM silver.calls_clean
            WHERE call_date IS NOT NULL

            UNION

            SELECT DISTINCT incident_date AS d
            FROM silver.incidents_clean
            WHERE incident_date IS NOT NULL
        )
        SELECT
            -- chiave surrogate semplice: YYYYMMDD
            CAST(strftime(d, '%Y%m%d') AS BIGINT) AS date_id,
            d AS date,
            EXTRACT(year FROM d)::INT  AS year,
            EXTRACT(month FROM d)::INT AS month,
            EXTRACT(day FROM d)::INT   AS day,
            EXTRACT(dow FROM d)::INT   AS weekday,          -- 0=Sunday
            EXTRACT(week FROM d)::INT  AS week_of_year,
            CASE WHEN EXTRACT(dow FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
        FROM all_dates
        ORDER BY d
        """)

        n = con.execute("SELECT COUNT(*) FROM gold.dim_date").fetchone()[0]
        logger.info(f"Creata gold.dim_date (righe: {n:,})")


@task(name="gold_dim_incident_type")
def build_dim_incident_type() -> None:
    logger = get_run_logger()

    with get_db_connection() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.GOLD}")
        con.execute("DROP TABLE IF EXISTS gold.dim_incident_type")

        con.execute("""
        CREATE TABLE gold.dim_incident_type AS
        WITH base AS (
            SELECT
                c.call_type,
                c.call_type_group,
                c.final_priority,
                i.primary_situation
            FROM silver.calls_clean c
            LEFT JOIN silver.incidents_clean i
              ON i.incident_number = c.incident_number
            WHERE c.incident_number IS NOT NULL
        ),
        cleaned AS (
            SELECT DISTINCT
                NULLIF(TRIM(call_type), '') AS call_type,
                NULLIF(TRIM(call_type_group), '') AS call_type_group,
                final_priority,
                NULLIF(TRIM(primary_situation), '') AS primary_situation
            FROM base
            WHERE
                call_type IS NOT NULL
                OR call_type_group IS NOT NULL
                OR final_priority IS NOT NULL
                OR primary_situation IS NOT NULL
        )
        SELECT
            row_number() OVER (ORDER BY
                call_type, call_type_group, primary_situation, final_priority
            ) AS incident_type_id,
            call_type,
            call_type_group,
            primary_situation,
            final_priority
        FROM cleaned
        ;
        """)

        n = con.execute("SELECT COUNT(*) FROM gold.dim_incident_type").fetchone()[0]
        logger.info(f"Creata gold.dim_incident_type (righe: {n:,})")


@task(name="gold_dim_location")
def build_dim_location() -> None:
    logger = get_run_logger()

    with get_db_connection() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.GOLD}")
        con.execute("DROP TABLE IF EXISTS gold.dim_location")

        con.execute("""
        CREATE TABLE gold.dim_location AS
        WITH base AS (
            SELECT
                c.address AS c_address,
                c.city AS c_city,
                c.zipcode_of_incident AS c_zipcode,
                c.neighborhoods_analysis_boundaries AS c_neighborhood,
                c.battalion AS c_battalion,
                c.station_area AS c_station_area,
                c.supervisor_district AS c_supervisor_district,
                c.fire_prevention_district AS c_fire_prevention_district,
                c.box AS c_box,
                c.location AS c_location_point,

                i.address AS i_address,
                i.city AS i_city,
                i.zipcode AS i_zipcode,
                i.neighborhood_district AS i_neighborhood,
                i.battalion AS i_battalion,
                i.station_area AS i_station_area,
                i.supervisor_district AS i_supervisor_district,
                i.box AS i_box,
                i.location AS i_location_point
            FROM silver.calls_clean c
            LEFT JOIN silver.incidents_clean i
              ON i.incident_number = c.incident_number
        ),
        keys AS (
            SELECT DISTINCT
                NULLIF(TRIM(COALESCE(c_address, i_address)), '') AS address,
                NULLIF(TRIM(COALESCE(c_city, i_city)), '') AS city,
                COALESCE(c_zipcode, i_zipcode) AS zipcode,
                NULLIF(TRIM(COALESCE(c_neighborhood, i_neighborhood)), '') AS neighborhood,
                NULLIF(TRIM(COALESCE(c_battalion, i_battalion)), '') AS battalion,
                NULLIF(TRIM(COALESCE(c_station_area, i_station_area)), '') AS station_area,
                COALESCE(c_supervisor_district, i_supervisor_district) AS supervisor_district,
                NULLIF(TRIM(c_fire_prevention_district), '') AS fire_prevention_district,

                -- box: mismatch risolto forzando VARCHAR
                NULLIF(TRIM(COALESCE(CAST(c_box AS VARCHAR), CAST(i_box AS VARCHAR))), '') AS box,

                NULLIF(TRIM(COALESCE(c_location_point, i_location_point)), '') AS location_point
            FROM base
        ),
        keyed AS (
            SELECT
                -- Chiave stabile per join fact↔dim (evita mismatch su molte colonne)
                md5(
                    COALESCE(address,'') || '|' ||
                    COALESCE(city,'') || '|' ||
                    COALESCE(CAST(zipcode AS VARCHAR),'') || '|' ||
                    COALESCE(neighborhood,'') || '|' ||
                    COALESCE(battalion,'') || '|' ||
                    COALESCE(station_area,'') || '|' ||
                    COALESCE(CAST(supervisor_district AS VARCHAR),'') || '|' ||
                    COALESCE(fire_prevention_district,'') || '|' ||
                    COALESCE(box,'') || '|' ||
                    COALESCE(location_point,'')
                ) AS location_key,
                *
            FROM keys
        )
        SELECT
            row_number() OVER (ORDER BY location_key) AS location_id,
            location_key,
            address, city, zipcode, neighborhood, battalion, station_area, supervisor_district,
            fire_prevention_district, box, location_point
        FROM keyed
        ;
        """)

        logger.info("Creato gold.dim_location")

@task(name="gold_fact_incident")
def build_fact_incident() -> None:
    logger = get_run_logger()

    with get_db_connection() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.GOLD}")
        con.execute("DROP TABLE IF EXISTS gold.fact_incident")

        con.execute("""
        CREATE TABLE gold.fact_incident AS
        WITH base AS (
            SELECT
                c.call_number,
                c.incident_number,

                -- timestamps principali (calls)
                c.received_ts,
                c.dispatch_ts,
                c.response_ts,
                c.on_scene_ts,

                -- misure già calcolate in silver
                c.response_time_sec,
                c.dispatch_delay_sec,
                c.travel_time_sec,

                -- severity / risorse (mix calls+incidents)
                c.number_of_alarms AS c_number_of_alarms,
                i.number_of_alarms AS i_number_of_alarms,
                i.suppression_units,
                i.suppression_personnel,
                i.ems_units,
                i.ems_personnel,
                i.other_units,
                i.other_personnel,
                i.estimated_property_loss,
                i.estimated_contents_loss,

                -- tipo incidente
                c.call_type,
                c.call_type_group,
                i.primary_situation,
                c.final_priority,

                -- date (per dim_date)
                c.call_date,
                i.incident_date,

                -- close (per duration)
                i.close_ts,

                -- location fields per hash
                c.address AS c_address,
                c.city AS c_city,
                c.zipcode_of_incident AS c_zipcode,
                c.neighborhoods_analysis_boundaries AS c_neighborhood,
                c.battalion AS c_battalion,
                c.station_area AS c_station_area,
                c.supervisor_district AS c_supervisor_district,
                c.fire_prevention_district AS c_fire_prevention_district,
                c.box AS c_box,
                c.location AS c_location_point,

                i.address AS i_address,
                i.city AS i_city,
                i.zipcode AS i_zipcode,
                i.neighborhood_district AS i_neighborhood,
                i.battalion AS i_battalion,
                i.station_area AS i_station_area,
                i.supervisor_district AS i_supervisor_district,
                i.box AS i_box,
                i.location AS i_location_point
            FROM silver.calls_clean c
            LEFT JOIN silver.incidents_clean i
              ON i.incident_number = c.incident_number
            WHERE c.incident_number IS NOT NULL
        ),
        loc_keys AS (
            SELECT
                *,
                -- normalizzazione IDENTICA a dim_location
                NULLIF(TRIM(COALESCE(c_address, i_address)), '') AS address,
                NULLIF(TRIM(COALESCE(c_city, i_city)), '') AS city,
                COALESCE(c_zipcode, i_zipcode) AS zipcode,
                NULLIF(TRIM(COALESCE(c_neighborhood, i_neighborhood)), '') AS neighborhood,
                NULLIF(TRIM(COALESCE(c_battalion, i_battalion)), '') AS battalion,
                NULLIF(TRIM(COALESCE(c_station_area, i_station_area)), '') AS station_area,
                COALESCE(c_supervisor_district, i_supervisor_district) AS supervisor_district,
                NULLIF(TRIM(c_fire_prevention_district), '') AS fire_prevention_district,
                NULLIF(TRIM(COALESCE(CAST(c_box AS VARCHAR), CAST(i_box AS VARCHAR))), '') AS box,
                NULLIF(TRIM(COALESCE(c_location_point, i_location_point)), '') AS location_point
            FROM base
        ),
        keyed AS (
            SELECT
                *,
                md5(
                    COALESCE(address,'') || '|' ||
                    COALESCE(city,'') || '|' ||
                    COALESCE(CAST(zipcode AS VARCHAR),'') || '|' ||
                    COALESCE(neighborhood,'') || '|' ||
                    COALESCE(battalion,'') || '|' ||
                    COALESCE(station_area,'') || '|' ||
                    COALESCE(CAST(supervisor_district AS VARCHAR),'') || '|' ||
                    COALESCE(fire_prevention_district,'') || '|' ||
                    COALESCE(box,'') || '|' ||
                    COALESCE(location_point,'')
                ) AS location_key
            FROM loc_keys
        ),
        d AS (
            SELECT
                k.*,
                COALESCE(k.incident_date, k.call_date) AS event_date
            FROM keyed k
            WHERE COALESCE(k.incident_date, k.call_date) IS NOT NULL
        )
        SELECT
            row_number() OVER () AS incident_id,

            d.incident_number,
            d.call_number,

            dd.date_id,

            -- Join su chiave hash => coverage piena
            dl.location_id,

            dit.incident_type_id,

            d.received_ts,
            d.dispatch_ts,
            d.response_ts,
            d.on_scene_ts,
            d.close_ts,

            d.response_time_sec,
            d.dispatch_delay_sec,
            d.travel_time_sec,

            CASE
              WHEN d.received_ts IS NOT NULL AND d.close_ts IS NOT NULL
               AND datediff('second', d.received_ts, d.close_ts) >= 0
              THEN datediff('second', d.received_ts, d.close_ts)
              ELSE NULL
            END AS incident_duration_sec,

            COALESCE(d.c_number_of_alarms, d.i_number_of_alarms) AS number_of_alarms,
            d.suppression_units,
            d.suppression_personnel,
            d.ems_units,
            d.ems_personnel,
            d.other_units,
            d.other_personnel,
            d.estimated_property_loss,
            d.estimated_contents_loss,

            d.final_priority
        FROM d
        LEFT JOIN gold.dim_date dd
          ON dd.date = CAST(d.event_date AS DATE)
        LEFT JOIN gold.dim_location dl
          ON dl.location_key = d.location_key
        LEFT JOIN gold.dim_incident_type dit
          ON dit.call_type IS NOT DISTINCT FROM NULLIF(TRIM(d.call_type), '')
         AND dit.call_type_group IS NOT DISTINCT FROM NULLIF(TRIM(d.call_type_group), '')
         AND dit.primary_situation IS NOT DISTINCT FROM NULLIF(TRIM(d.primary_situation), '')
         AND dit.final_priority IS NOT DISTINCT FROM d.final_priority
        ;
        """)

        logger.info("Creato gold.fact_incident")