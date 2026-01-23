from prefect import task, get_run_logger

from utils import get_db_connection, Schemas


def _parse_ts_sql(col: str) -> str:
    """Parsing timestamp robusto (DuckDB).

    `col` deve essere un identificatore SQL (nome colonna già sanitizzato).
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
    """Parsing date robusto (DuckDB)."""
    return f"""
    COALESCE(
        try_strptime({col}, '%m/%d/%Y'),
        try_strptime({col}, '%Y-%m-%d'),
        try_cast({col} AS DATE)
    )
    """


def _table_columns(con, schema: str, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info('{schema}.{table}')").fetchall()
    return {r[1] for r in rows}


def _pick_col(existing: set[str], *candidates: str) -> str:
    for c in candidates:
        if c in existing:
            return c
    raise ValueError(f"Nessuna delle colonne candidate esiste: {candidates}")


@task(name="silver_cleaning")
def clean_silver() -> None:
    """Crea/rigenera SILVER (calls_clean, incidents_clean) a partire dal BRONZE.

    Patch anti-BinderError:
    - legge lo schema reale di bronze.calls / bronze.incidents
    - risolve automaticamente i nomi colonna tra candidati (es. *_dttm vs *_dt_tm)
    - usa _ingested_at_utc SOLO se esiste (Phase2), altrimenti ignora (Phase1)
    """
    logger = get_run_logger()

    with get_db_connection() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.SILVER}")

        # Read actual schemas
        calls_cols = _table_columns(con, Schemas.BRONZE, "calls")
        inc_cols = _table_columns(con, Schemas.BRONZE, "incidents")

        # Optional Phase2 ordering column
        has_calls_ingested = "_ingested_at_utc" in calls_cols
        order_calls_ingested = "try_cast(_ingested_at_utc AS TIMESTAMP) DESC," if has_calls_ingested else ""

        # -------------------------
        # Resolve bronze.calls names (based on your PRAGMA)
        # -------------------------
        call_number_col = _pick_col(calls_cols, "call_number", "callnumber")
        incident_number_col = _pick_col(calls_cols, "incident_number", "incidentnumber")

        received_col = _pick_col(calls_cols, "received_dt_tm", "received_dttm")
        entry_col = _pick_col(calls_cols, "entry_dt_tm", "entry_dttm")
        dispatch_col = _pick_col(calls_cols, "dispatch_dt_tm", "dispatch_dttm")
        response_col = _pick_col(calls_cols, "response_dt_tm", "response_dttm")
        on_scene_col = _pick_col(calls_cols, "on_scene_dt_tm", "on_scene_dttm")
        transport_col = _pick_col(calls_cols, "transport_dt_tm", "transport_dttm")
        hospital_col = _pick_col(calls_cols, "hospital_dt_tm", "hospital_dttm")
        available_col = _pick_col(calls_cols, "available_dt_tm", "available_dttm")

        zipcode_inc_col = _pick_col(calls_cols, "zipcode_of_incident", "zipcodeofincident", "zipcode_ofincident")
        supervisor_col = _pick_col(calls_cols, "supervisor_district", "supervisordistrict")

        num_alarms_col = _pick_col(calls_cols, "number_of_alarms", "numberof_alarms", "numberofalarms")
        unit_seq_col = _pick_col(
            calls_cols,
            "unit_sequence_in_call_dispatch",
            "unitsequence_in_call_dispatch",
            "unitsequenceincalldispatch",
        )

        # This one is present in your bronze.calls as: neighborhooods_analysis_boundaries
        neigh_col = _pick_col(
            calls_cols,
            "neighborhooods_analysis_boundaries",
            "neighborhoods_analysis_boundaries",
        )

        # Calls has row_id in your PRAGMA
        calls_rowid_col = _pick_col(calls_cols, "row_id", "rowid")

        # -------------------------
        # Resolve bronze.incidents names (based on your PRAGMA)
        # -------------------------
        inc_incident_col = _pick_col(inc_cols, "incident_number", "incidentnumber")
        inc_call_col = _pick_col(inc_cols, "call_number", "callnumber")
        inc_exposure_col = _pick_col(inc_cols, "exposure_number", "exposurenumber")

        inc_incident_date_col = _pick_col(inc_cols, "incident_date", "incidentdate")

        alarm_col = _pick_col(inc_cols, "alarm_dt_tm", "alarm_dttm", "alarmdttm")
        arrival_col = _pick_col(inc_cols, "arrival_dt_tm", "arrival_dttm", "arrivaldttm")
        close_col = _pick_col(inc_cols, "close_dt_tm", "close_dttm", "closedttm")

        inc_zip_col = _pick_col(inc_cols, "zipcode", "zip_code", "zip")
        inc_supervisor_col = _pick_col(inc_cols, "supervisor_district", "supervisordistrict")
        inc_neigh_col = _pick_col(inc_cols, "neighborhood_district", "neighborhooddistrict")

        inc_numberof_alarms_col = _pick_col(inc_cols, "numberof_alarms", "number_of_alarms", "numberofalarms")
        inc_ems_units_col = _pick_col(inc_cols, "ems_units", "emsunits")
        inc_ems_personnel_col = _pick_col(inc_cols, "ems_personnel", "emspersonnel")

        inc_area_fire_origin_col = _pick_col(inc_cols, "areaof_fire_origin", "area_of_fire_origin", "areaoffireorigin")
        inc_human_factors_col = _pick_col(
            inc_cols,
            "human_factors_associatedwith_ignition",
            "human_factors_associated_with_ignition",
            "humanfactorsassociatedwithignition",
        )
        inc_floor_fire_origin_col = _pick_col(
            inc_cols,
            "floorof_fire_origin",
            "floor_of_fire_origin",
            "flooroffireorigin",
        )

        inc_floors_min_col = _pick_col(inc_cols, "numberoffloorswithminimumdamage", "number_of_floors_with_minimum_damage")
        inc_floors_sig_col = _pick_col(inc_cols, "numberoffloorswithsignificantdamage", "number_of_floors_with_significant_damage")
        inc_floors_heavy_col = _pick_col(inc_cols, "numberoffloorswithheavydamage", "number_of_floors_with_heavy_damage")
        inc_floors_ext_col = _pick_col(inc_cols, "numberoffloorswithextremedamage", "number_of_floors_with_extreme_damage")

        inc_sprinkler_heads_col = _pick_col(
            inc_cols,
            "numberof_sprinkler_heads_operating",
            "number_of_sprinkler_heads_operating",
            "numberofsprinklerheadsoperating",
        )

        aes_present_col = _pick_col(inc_cols, "automatic_extinguishing_system_present", "automaticextinguishingsystempresent")
        aes_type_col = _pick_col(inc_cols, "automatic_extinguishing_sytem_type", "automatic_extinguishing_system_type", "automaticextinguishingsytemtype")
        aes_perf_col = _pick_col(inc_cols, "automatic_extinguishing_sytem_perfomance", "automatic_extinguishing_system_performance", "automaticextinguishingsytemperfomance")
        aes_fail_col = _pick_col(inc_cols, "automatic_extinguishing_sytem_failure_reason", "automatic_extinguishing_system_failure_reason", "automaticextinguishingsytemfailurereason")

        # -------------------------
        # SILVER: CALLS
        # -------------------------
        con.execute("DROP TABLE IF EXISTS silver.calls_clean")

        calls_sql = f"""
        CREATE TABLE silver.calls_clean AS
        SELECT
            -- Keys
            try_cast({call_number_col} AS BIGINT) AS call_number,
            try_cast({incident_number_col} AS BIGINT) AS incident_number,
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

            {_parse_ts_sql(received_col)}   AS received_ts,
            {_parse_ts_sql(entry_col)}      AS entry_ts,
            {_parse_ts_sql(dispatch_col)}   AS dispatch_ts,
            {_parse_ts_sql(response_col)}   AS response_ts,
            {_parse_ts_sql(on_scene_col)}   AS on_scene_ts,
            {_parse_ts_sql(transport_col)}  AS transport_ts,
            {_parse_ts_sql(hospital_col)}   AS hospital_ts,
            {_parse_ts_sql(available_col)}  AS available_ts,

            -- Derived metrics (seconds)
            CASE
              WHEN {_parse_ts_sql(received_col)} IS NOT NULL
               AND {_parse_ts_sql(on_scene_col)} IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', {_parse_ts_sql(received_col)}, {_parse_ts_sql(on_scene_col)}) >= 0
                  THEN datediff('second', {_parse_ts_sql(received_col)}, {_parse_ts_sql(on_scene_col)})
                  ELSE NULL
                END
              ELSE NULL
            END AS response_time_sec,

            CASE
              WHEN {_parse_ts_sql(received_col)} IS NOT NULL
               AND {_parse_ts_sql(dispatch_col)} IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', {_parse_ts_sql(received_col)}, {_parse_ts_sql(dispatch_col)}) >= 0
                  THEN datediff('second', {_parse_ts_sql(received_col)}, {_parse_ts_sql(dispatch_col)})
                  ELSE NULL
                END
              ELSE NULL
            END AS dispatch_delay_sec,

            CASE
              WHEN {_parse_ts_sql(dispatch_col)} IS NOT NULL
               AND {_parse_ts_sql(on_scene_col)} IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', {_parse_ts_sql(dispatch_col)}, {_parse_ts_sql(on_scene_col)}) >= 0
                  THEN datediff('second', {_parse_ts_sql(dispatch_col)}, {_parse_ts_sql(on_scene_col)})
                  ELSE NULL
                END
              ELSE NULL
            END AS travel_time_sec,

            -- Location
            address,
            city,
            try_cast({zipcode_inc_col} AS INTEGER) AS zipcode_of_incident,
            battalion,
            station_area,
            box,
            try_cast({supervisor_col} AS INTEGER) AS supervisor_district,

            -- Fix da sanitize/typo
            {neigh_col} AS neighborhoods_analysis_boundaries,
            location,

            -- Other useful fields
            call_final_disposition,
            try_cast({num_alarms_col} AS INTEGER) AS number_of_alarms,
            unit_type,
            try_cast({unit_seq_col} AS INTEGER) AS unit_sequence_in_call_dispatch,
            fire_prevention_district,
            {calls_rowid_col} AS row_id
        FROM bronze.calls
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY try_cast({call_number_col} AS BIGINT)
          ORDER BY
            COALESCE({_parse_ts_sql(received_col)}, TIMESTAMP '1900-01-01') DESC,
            {order_calls_ingested}
            {calls_rowid_col} DESC
        ) = 1
        ;
        """
        logger.info("Creo silver.calls_clean ...")
        con.execute(calls_sql)
        logger.info("silver.calls_clean creato.")

        # -------------------------
        # SILVER: INCIDENTS
        # -------------------------
        con.execute("DROP TABLE IF EXISTS silver.incidents_clean")

        incidents_sql = f"""
        CREATE TABLE silver.incidents_clean AS
        SELECT
            -- Keys
            try_cast({inc_incident_col} AS BIGINT)  AS incident_number,
            try_cast({inc_call_col} AS BIGINT)      AS call_number,
            try_cast({inc_exposure_col} AS INTEGER) AS exposure_number,

            -- Date / timestamps
            {_parse_date_sql(inc_incident_date_col)} AS incident_date,
            {_parse_ts_sql(alarm_col)}               AS alarm_ts,
            {_parse_ts_sql(arrival_col)}             AS arrival_ts,
            {_parse_ts_sql(close_col)}               AS close_ts,

            -- Location
            address,
            city,
            try_cast({inc_zip_col} AS INTEGER) AS zipcode,
            battalion,
            station_area,
            box,
            try_cast({inc_supervisor_col} AS INTEGER) AS supervisor_district,
            {inc_neigh_col} AS neighborhood_district,
            location,

            -- Measures / severity
            try_cast({inc_numberof_alarms_col} AS INTEGER) AS number_of_alarms,
            try_cast(suppression_units AS INTEGER) AS suppression_units,
            try_cast(suppression_personnel AS INTEGER) AS suppression_personnel,
            try_cast({inc_ems_units_col} AS INTEGER) AS ems_units,
            try_cast({inc_ems_personnel_col} AS INTEGER) AS ems_personnel,
            try_cast(other_units AS INTEGER) AS other_units,
            try_cast(other_personnel AS INTEGER) AS other_personnel,

            CASE
              WHEN try_cast(estimated_property_loss AS BIGINT) < 0 THEN NULL
              ELSE try_cast(estimated_property_loss AS BIGINT)
            END AS estimated_property_loss,
            CASE
              WHEN try_cast(estimated_contents_loss AS BIGINT) < 0 THEN NULL
              ELSE try_cast(estimated_contents_loss AS BIGINT)
            END AS estimated_contents_loss,

            try_cast(fire_fatalities AS BIGINT) AS fire_fatalities,
            try_cast(fire_injuries AS BIGINT) AS fire_injuries,
            try_cast(civilian_fatalities AS BIGINT) AS civilian_fatalities,
            try_cast(civilian_injuries AS BIGINT) AS civilian_injuries,

            -- Situation / attributes
            primary_situation,
            mutual_aid,
            action_taken_primary,
            action_taken_secondary,
            action_taken_other,
            detector_alerted_occupants,
            property_use,
            {inc_area_fire_origin_col} AS area_of_fire_origin,
            ignition_cause,
            ignition_factor_primary,
            ignition_factor_secondary,
            heat_source,
            item_first_ignited,
            {inc_human_factors_col} AS human_factors_associated_with_ignition,
            structure_type,
            structure_status,
            {inc_floor_fire_origin_col} AS floor_of_fire_origin,
            fire_spread,
            no_flame_spead,

            try_cast({inc_floors_min_col} AS INTEGER) AS floors_min_damage,
            try_cast({inc_floors_sig_col} AS INTEGER) AS floors_significant_damage,
            try_cast({inc_floors_heavy_col} AS INTEGER) AS floors_heavy_damage,
            try_cast({inc_floors_ext_col} AS INTEGER) AS floors_extreme_damage,

            detectors_present,
            detector_type,
            detector_operation,
            detector_effectiveness,
            detector_failure_reason,

            {aes_present_col} AS automatic_extinguishing_system_present,
            {aes_type_col} AS automatic_extinguishing_system_type,
            {aes_perf_col} AS automatic_extinguishing_system_performance,
            {aes_fail_col} AS automatic_extinguishing_system_failure_reason,
            try_cast({inc_sprinkler_heads_col} AS INTEGER) AS sprinkler_heads_operating,

            first_unit_on_scene
        FROM bronze.incidents
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY try_cast({inc_incident_col} AS BIGINT)
          ORDER BY
            COALESCE({_parse_ts_sql(alarm_col)}, TIMESTAMP '1900-01-01') DESC
        ) = 1
        ;
        """
        logger.info("Creo silver.incidents_clean ...")
        con.execute(incidents_sql)
        logger.info("silver.incidents_clean creato.")
