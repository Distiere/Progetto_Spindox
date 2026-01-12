from prefect import task, get_run_logger
from etl.utils import get_db_connection, Schemas


def _parse_ts_sql(col: str) -> str:
    """
    Parsing timestamp robusto.
    Prova più formati e poi fallback a TRY_CAST.
    """
    return f"""
    COALESCE(
        try_strptime({col}, '%m/%d/%Y %I:%M:%S %p'),
        try_strptime({col}, '%m/%d/%Y %H:%M:%S'),
        try_strptime({col}, '%Y-%m-%d %H:%M:%S'),
        try_cast({col} AS TIMESTAMP)
    )
    """


def _parse_date_sql(col: str) -> str:
    """
    Parsing date robusto.
    """
    return f"""
    COALESCE(
        try_strptime({col}, '%m/%d/%Y'),
        try_strptime({col}, '%Y-%m-%d'),
        try_cast({col} AS DATE)
    )
    """


@task(name="silver_cleaning")
def clean_silver() -> None:
    logger = get_run_logger()

    with get_db_connection() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.SILVER}")

        # =========================
        # SILVER: CALLS
        # dalla analisi delle colonne in explore_Schema.py e conoscenza dominio vado a creare la tabella pulita
        # qui posso fare parsing date/timestamp, cast vari, calcolo metriche derivate
        # obiettivo: avere tabella “pulita” pronta per analisi e gold
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
        ;
        """
        con.execute(calls_sql)
        logger.info("✅ Creato silver.calls_clean")

        # =========================
        # SILVER: INCIDENTS
        # dalla analisi delle colonne in explore_Schema.py e conoscenza dominio vado a creare la tabella pulita
        # qui posso fare parsing date/timestamp, cast vari, calcolo metriche derivate
        # obiettivo: avere tabella “pulita” pronta per analisi e gold
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

            try_cast(estimatedpropertyloss AS BIGINT) AS estimated_property_loss,
            try_cast(estimatedcontentsloss AS BIGINT) AS estimated_contents_loss,

            try_cast(firefatalities AS INTEGER) AS fire_fatalities,
            try_cast(fireinjuries AS INTEGER) AS fire_injuries,
            try_cast(civilianfatalities AS INTEGER) AS civilian_fatalities,
            try_cast(civilianinjuries AS INTEGER) AS civilian_injuries,

            -- Type / situation
            primarysituation AS primary_situation,
            mutualaid AS mutual_aid,

            -- Optional descriptive attributes (teniamo metriche forse utili in gold per futura valutazione o scalabilità)
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

            firstunitonscene AS first_unit_on_scene

        FROM bronze.incidents
        ;
        """
        con.execute(incidents_sql)
        logger.info("✅ Creato silver.incidents_clean")
