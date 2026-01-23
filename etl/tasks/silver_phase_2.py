from prefect import task, get_run_logger
from utils import get_db_connection, Schemas

# riuso gli helper già presenti in silver.py (stesso progetto)
from tasks.silver import _parse_date_sql, _parse_ts_sql


@task(name="Clean Silver (PHASE 2)", retries=0)
def clean_silver_phase2(
    memory_limit: str = "8GB",
    temp_directory: str = "data/tmp_duckdb",
    threads: int = 4,
) -> None:
    """
    Silver PHASE 2 (compatibile con DB creato dalla Phase 1):
    - calls: colonne con underscore (call_number, incident_number, ...)
    - incidents: colonne senza underscore (incidentnumber, callnumber, ...)
    - dedup con QUALIFY ROW_NUMBER()
    - PRAGMA per evitare OutOfMemory (spill su disco)
    """
    logger = get_run_logger()

    with get_db_connection() as con:
        # ---- PRAGMA anti-OOM ----
        con.execute(f"PRAGMA temp_directory='{temp_directory}';")
        con.execute(f"PRAGMA memory_limit='{memory_limit}';")
        con.execute(f"PRAGMA threads={int(threads)};")

        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.SILVER}")

        # =========================
        # SILVER: CALLS  (underscore)
        # =========================
        con.execute("DROP TABLE IF EXISTS silver.calls_clean")

        calls_sql = f"""
        CREATE TABLE silver.calls_clean AS
        SELECT
            -- Keys
            try_cast(call_number AS BIGINT) AS call_number,
            try_cast(incident_number AS BIGINT) AS incident_number,
            unit_id,

            -- Type / Priority
            call_type,
            call_type_group,
            try_cast(original_priority AS INTEGER) AS original_priority,
            try_cast(priority AS INTEGER) AS priority,
            try_cast(final_priority AS INTEGER) AS final_priority,
            als_unit,

            -- Dates / Timestamps
            {_parse_date_sql("call_date")}  AS call_date,
            {_parse_date_sql("watch_date")} AS watch_date,

            {_parse_ts_sql("received_dttm")}   AS received_ts,
            {_parse_ts_sql("entry_dttm")}      AS entry_ts,
            {_parse_ts_sql("dispatch_dttm")}   AS dispatch_ts,
            {_parse_ts_sql("response_dttm")}   AS response_ts,
            {_parse_ts_sql("on_scene_dttm")}   AS on_scene_ts,
            {_parse_ts_sql("transport_dttm")}  AS transport_ts,
            {_parse_ts_sql("hospital_dttm")}   AS hospital_ts,
            {_parse_ts_sql("available_dttm")}  AS available_ts,

            -- Derived metrics (seconds)
            CASE
              WHEN {_parse_ts_sql("received_dttm")} IS NOT NULL
               AND {_parse_ts_sql("on_scene_dttm")} IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', {_parse_ts_sql("received_dttm")}, {_parse_ts_sql("on_scene_dttm")}) >= 0
                  THEN datediff('second', {_parse_ts_sql("received_dttm")}, {_parse_ts_sql("on_scene_dttm")})
                  ELSE NULL
                END
              ELSE NULL
            END AS response_time_sec,

            CASE
              WHEN {_parse_ts_sql("received_dttm")} IS NOT NULL
               AND {_parse_ts_sql("dispatch_dttm")} IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', {_parse_ts_sql("received_dttm")}, {_parse_ts_sql("dispatch_dttm")}) >= 0
                  THEN datediff('second', {_parse_ts_sql("received_dttm")}, {_parse_ts_sql("dispatch_dttm")})
                  ELSE NULL
                END
              ELSE NULL
            END AS dispatch_delay_sec,

            CASE
              WHEN {_parse_ts_sql("dispatch_dttm")} IS NOT NULL
               AND {_parse_ts_sql("on_scene_dttm")} IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', {_parse_ts_sql("dispatch_dttm")}, {_parse_ts_sql("on_scene_dttm")}) >= 0
                  THEN datediff('second', {_parse_ts_sql("dispatch_dttm")}, {_parse_ts_sql("on_scene_dttm")})
                  ELSE NULL
                END
              ELSE NULL
            END AS travel_time_sec,

            -- Location
            address,
            city,
            try_cast(zipcode_of_incident AS INTEGER) AS zipcode_of_incident,
            battalion,
            station_area,
            box,
            try_cast(supervisor_district AS INTEGER) AS supervisor_district,

            -- Fix “brutto” da sanitize bronze
            neighborhooods___analysis_boundaries AS neighborhoods_analysis_boundaries,
            location,

            -- Other useful fields
            call_final_disposition,
            try_cast(number_of_alarms AS INTEGER) AS number_of_alarms,
            unit_type,
            try_cast(unit_sequence_in_call_dispatch AS INTEGER) AS unit_sequence_in_call_dispatch,
            fire_prevention_district,
            rowid

        FROM bronze.calls
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY try_cast(call_number AS BIGINT)
          ORDER BY
            COALESCE({_parse_ts_sql("received_dttm")}, TIMESTAMP '1900-01-01') DESC,
            try_cast(_ingested_at_utc AS TIMESTAMP) DESC,
            rowid DESC
        ) = 1;
        """
        logger.info("Creating silver.calls_clean (PHASE2) ...")
        con.execute(calls_sql)
        logger.info("silver.calls_clean created.")

        # =========================
        # SILVER: INCIDENTS (no underscore, come silver.py)
        # =========================
        con.execute("DROP TABLE IF EXISTS silver.incidents_clean")

        incidents_sql = f"""
        CREATE TABLE silver.incidents_clean AS
        SELECT
            -- Keys (standard)
            try_cast(incidentnumber AS BIGINT) AS incident_number,
            try_cast(callnumber AS BIGINT)     AS call_number,
            try_cast(exposurenumber AS INTEGER) AS exposure_number,

            -- Date / timestamps
            {_parse_date_sql("incidentdate")} AS incident_date,
            {_parse_ts_sql("alarmdttm")}      AS alarm_ts,
            {_parse_ts_sql("arrivaldttm")}    AS arrival_ts,
            {_parse_ts_sql("closedttm")}      AS close_ts,

            -- Location (standard)
            address,
            city,
            try_cast(zipcode AS INTEGER) AS zipcode,
            battalion,
            stationarea AS station_area,
            box,
            try_cast(supervisordistrict AS INTEGER) AS supervisor_district,
            neighborhooddistrict AS neighborhood_district,
            location,

            -- Measures / severity
            try_cast(numberofalarms AS INTEGER) AS number_of_alarms,
            try_cast(suppressionunits AS INTEGER) AS suppression_units,
            try_cast(suppressionpersonnel AS INTEGER) AS suppression_personnel,
            try_cast(emsunits AS INTEGER) AS ems_units,
            try_cast(emspersonnel AS INTEGER) AS ems_personnel,
            try_cast(otherunits AS INTEGER) AS other_units,
            try_cast(otherpersonnel AS INTEGER) AS other_personnel,

            CASE
              WHEN try_cast(estimatedpropertyloss AS BIGINT) < 0 THEN NULL
              ELSE try_cast(estimatedpropertyloss AS BIGINT)
            END AS estimated_property_loss,
            CASE
              WHEN try_cast(estimatedcontentsloss AS BIGINT) < 0 THEN NULL
              ELSE try_cast(estimatedcontentsloss AS BIGINT)
            END AS estimated_contents_loss,

            try_cast(firefatalities AS BIGINT) AS fire_fatalities,
            try_cast(fireinjuries AS BIGINT) AS fire_injuries,
            try_cast(civilianfatalities AS BIGINT) AS civilian_fatalities,
            try_cast(civilianinjuries AS BIGINT) AS civilian_injuries,

            -- Type / situation
            primarysituation AS primary_situation,
            mutualaid AS mutual_aid,

            -- Optional descriptive attributes
            actiontakenprimary AS action_taken_primary,
            actiontakensecondary AS action_taken_secondary,
            actiontakenother AS action_taken_other,
            detectoralertedoccupants AS detector_alerted_occupants,
            propertyuse AS property_use,
            areaoffireorigin AS area_of_fire_origin,
            ignitioncause AS ignition_cause,
            ignitionfactorprimary AS ignition_factor_primary,
            ignitionfactorsecondary AS ignition_factor_secondary,
            heatsource AS heat_source,
            itemfirstignited AS item_first_ignited,
            humanfactorsassociatedwithignition AS human_factors_associated_with_ignition,
            structuretype AS structure_type,
            structurestatus AS structure_status,
            flooroffireorigin AS floor_of_fire_origin,
            firespread AS fire_spread,
            noflamespead AS no_flame_spead,

            try_cast(numberoffloorswithminimumdamage AS INTEGER) AS floors_min_damage,
            try_cast(numberoffloorswithsignificantdamage AS INTEGER) AS floors_significant_damage,
            try_cast(numberoffloorswithheavydamage AS INTEGER) AS floors_heavy_damage,
            try_cast(numberoffloorswithextremedamage AS INTEGER) AS floors_extreme_damage,

            detectorspresent AS detectors_present,
            detectortype AS detector_type,
            detectoroperation AS detector_operation,
            detectoreffectiveness AS detector_effectiveness,
            detectorfailurereason AS detector_failure_reason,

            automaticextinguishingsystempresent AS automatic_extinguishing_system_present,
            automaticextinguishingsytemtype AS automatic_extinguishing_system_type,
            automaticextinguishingsytemperfomance AS automatic_extinguishing_system_performance,
            automaticextinguishingsytemfailurereason AS automatic_extinguishing_system_failure_reason,
            try_cast(numberofsprinklerheadsoperating AS INTEGER) AS sprinkler_heads_operating,

            firstunitonscene AS first_unit_on_scene,
            rowid

        FROM bronze.incidents
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY try_cast(incidentnumber AS BIGINT)
          ORDER BY
            COALESCE({_parse_ts_sql("alarmdttm")}, TIMESTAMP '1900-01-01') DESC,
            try_cast(_ingested_at_utc AS TIMESTAMP) DESC,
            rowid DESC
        ) = 1;
        """
        logger.info("Creating silver.incidents_clean (PHASE2) ...")
        con.execute(incidents_sql)
        logger.info("silver.incidents_clean created.")

    logger.info("Silver PHASE2 clean + dedup completato.")
