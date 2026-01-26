from prefect import task, get_run_logger
from utils import get_db_connection, Schemas

# riuso gli helper già presenti in silver.py (stesso progetto)
from tasks.silver import _parse_date_sql, _parse_ts_sql


def _table_cols(con, schema: str, table: str) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchall()
    return {r[0] for r in rows}


def _pick(cols: set[str], *candidates: str) -> str | None:
    """Ritorna il primo candidato presente in cols, altrimenti None."""
    for c in candidates:
        if c in cols:
            return c
    return None


def _pick_or_null(cols: set[str], *candidates: str) -> str:
    """Ritorna un identificatore colonna se esiste, altrimenti NULL."""
    c = _pick(cols, *candidates)
    return c if c else "NULL"


@task(name="Clean Silver (PHASE 2)", retries=0)
def clean_silver_phase2(
    memory_limit: str = "8GB",
    temp_directory: str = "data/tmp_duckdb",
    threads: int = 4,
) -> None:
    """
    Silver PHASE 2:
    - Robust a differenze naming tra Phase1/Phase2 (dttm vs dt_tm, rowid vs row_id, ecc.)
    - Dedup con QUALIFY ROW_NUMBER()
    - PRAGMA anti-OOM (spill su disco)
    """
    logger = get_run_logger()

    with get_db_connection() as con:
        # ---- PRAGMA anti-OOM ----
        con.execute(f"PRAGMA temp_directory='{temp_directory}';")
        con.execute(f"PRAGMA memory_limit='{memory_limit}';")
        con.execute(f"PRAGMA threads={int(threads)};")
        con.execute("PRAGMA preserve_insertion_order=false;")
        con.execute("PRAGMA threads=1;")


        con.execute(f"CREATE SCHEMA IF NOT EXISTS {Schemas.SILVER}")

        # =====================================================================
        # SILVER: CALLS
        # =====================================================================
        con.execute("DROP TABLE IF EXISTS silver.calls_clean")

        calls_cols = _table_cols(con, "bronze", "calls")

        # timestamp columns: Phase1 usa *_dttm, Phase2 usa *_dt_tm
        received_col = _pick(calls_cols, "received_dt_tm", "received_dttm")
        entry_col = _pick(calls_cols, "entry_dt_tm", "entry_dttm")
        dispatch_col = _pick(calls_cols, "dispatch_dt_tm", "dispatch_dttm")
        response_col = _pick(calls_cols, "response_dt_tm", "response_dttm")
        on_scene_col = _pick(calls_cols, "on_scene_dt_tm", "on_scene_dttm")
        transport_col = _pick(calls_cols, "transport_dt_tm", "transport_dttm")
        hospital_col = _pick(calls_cols, "hospital_dt_tm", "hospital_dttm")
        available_col = _pick(calls_cols, "available_dt_tm", "available_dttm")

        if not received_col:
            # senza received non possiamo deduplicare correttamente => errore esplicito
            raise RuntimeError(
                "bronze.calls non contiene né received_dt_tm né received_dttm. "
                "Controlla naming/ingest Bronze."
            )

        # altre colonne con varianti frequenti
        neighborhoods_col = _pick_or_null(
            calls_cols,
            "neighborhoods_analysis_boundaries",
            "neighborhooods_analysis_boundaries",
            "neighborhooods___analysis_boundaries",
        )
        rowid_col = _pick_or_null(calls_cols, "rowid", "row_id")

        calls_sql = f"""
        CREATE TABLE silver.calls_clean AS
        WITH base AS (
            SELECT
                -- Keys
                try_cast(call_number AS BIGINT) AS call_number,
                try_cast(incident_number AS BIGINT) AS incident_number,
                {_pick_or_null(calls_cols, "unit_id")} AS unit_id,
        
                -- Type / Priority
                {_pick_or_null(calls_cols, "call_type")} AS call_type,
                {_pick_or_null(calls_cols, "call_type_group")} AS call_type_group,
                try_cast({_pick_or_null(calls_cols, "original_priority")} AS INTEGER) AS original_priority,
                try_cast({_pick_or_null(calls_cols, "priority")} AS INTEGER) AS priority,
                try_cast({_pick_or_null(calls_cols, "final_priority")} AS INTEGER) AS final_priority,
                {_pick_or_null(calls_cols, "als_unit")} AS als_unit,
        
                -- Dates
                {_parse_date_sql("call_date")}  AS call_date,
                {_parse_date_sql("watch_date")} AS watch_date,
        
                -- Parsed timestamps (calcolati UNA volta)
                {_parse_ts_sql(received_col)}    AS received_ts,
                {_parse_ts_sql(entry_col)}       AS entry_ts,
                {_parse_ts_sql(dispatch_col)}    AS dispatch_ts,
                {_parse_ts_sql(response_col)}    AS response_ts,
                {_parse_ts_sql(on_scene_col)}    AS on_scene_ts,
                {_parse_ts_sql(transport_col)}   AS transport_ts,
                {_parse_ts_sql(hospital_col)}    AS hospital_ts,
                {_parse_ts_sql(available_col)}   AS available_ts,
        
                -- Location
                {_pick_or_null(calls_cols, "address")} AS address,
                {_pick_or_null(calls_cols, "city")} AS city,
                try_cast({_pick_or_null(calls_cols, "zipcode_of_incident")} AS INTEGER) AS zipcode_of_incident,
                {_pick_or_null(calls_cols, "battalion")} AS battalion,
                {_pick_or_null(calls_cols, "station_area")} AS station_area,
                {_pick_or_null(calls_cols, "box")} AS box,
                try_cast({_pick_or_null(calls_cols, "supervisor_district")} AS INTEGER) AS supervisor_district,
                {neighborhoods_col} AS neighborhoods_analysis_boundaries,
                {_pick_or_null(calls_cols, "location")} AS location,
        
                -- Other useful fields
                {_pick_or_null(calls_cols, "call_final_disposition")} AS call_final_disposition,
                try_cast({_pick_or_null(calls_cols, "number_of_alarms")} AS INTEGER) AS number_of_alarms,
                {_pick_or_null(calls_cols, "unit_type")} AS unit_type,
                try_cast({_pick_or_null(calls_cols, "unit_sequence_in_call_dispatch")} AS INTEGER) AS unit_sequence_in_call_dispatch,
                {_pick_or_null(calls_cols, "fire_prevention_district")} AS fire_prevention_district,
                {rowid_col} AS rowid,
        
                -- Ingest ts (una volta)
                try_cast(_ingested_at_utc AS TIMESTAMP) AS ingested_ts
        
            FROM bronze.calls
        )
        SELECT
            *,
            -- Derived metrics (seconds) usando le colonne già parse
            CASE
              WHEN received_ts IS NOT NULL AND on_scene_ts IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', received_ts, on_scene_ts) >= 0
                  THEN datediff('second', received_ts, on_scene_ts)
                  ELSE NULL
                END
              ELSE NULL
            END AS response_time_sec,
        
            CASE
              WHEN received_ts IS NOT NULL AND dispatch_ts IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', received_ts, dispatch_ts) >= 0
                  THEN datediff('second', received_ts, dispatch_ts)
                  ELSE NULL
                END
              ELSE NULL
            END AS dispatch_delay_sec,
        
            CASE
              WHEN dispatch_ts IS NOT NULL AND on_scene_ts IS NOT NULL
              THEN
                CASE
                  WHEN datediff('second', dispatch_ts, on_scene_ts) >= 0
                  THEN datediff('second', dispatch_ts, on_scene_ts)
                  ELSE NULL
                END
              ELSE NULL
            END AS travel_time_sec
        
        FROM base
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY call_number
          ORDER BY
            COALESCE(received_ts, TIMESTAMP '1900-01-01') DESC,
            COALESCE(ingested_ts, TIMESTAMP '1900-01-01') DESC,
            rowid DESC
        ) = 1;
        """
        

        logger.info("Creating silver.calls_clean (PHASE2) ...")
        con.execute(calls_sql)
        logger.info("silver.calls_clean created.")

        # =====================================================================
        # SILVER: INCIDENTS
        # =====================================================================
        con.execute("DROP TABLE IF EXISTS silver.incidents_clean")

        inc_cols = _table_cols(con, "bronze", "incidents")

        # keys / timestamps: supporto naming con e senza underscore
        inc_incident = _pick(inc_cols, "incident_number", "incidentnumber")
        inc_call = _pick(inc_cols, "call_number", "callnumber")
        inc_exposure = _pick(inc_cols, "exposure_number", "exposurenumber")

        inc_incident_date = _pick(inc_cols, "incident_date", "incidentdate")
        inc_alarm = _pick(inc_cols, "alarm_dt_tm", "alarmdttm", "alarm_dt_tm")
        inc_arrival = _pick(inc_cols, "arrival_dt_tm", "arrivaldttm")
        inc_close = _pick(inc_cols, "close_dt_tm", "closedttm")

        incidents_sql = f"""
        CREATE TABLE silver.incidents_clean AS
        SELECT
            -- Keys
            try_cast({inc_incident} AS BIGINT) AS incident_number,
            try_cast({inc_call} AS BIGINT)     AS call_number,
            try_cast({_pick_or_null(inc_cols, inc_exposure)} AS INTEGER) AS exposure_number,

            -- Date / timestamps
            {_parse_date_sql(inc_incident_date)} AS incident_date,
            {_parse_ts_sql(inc_alarm)}           AS alarm_ts,
            {_parse_ts_sql(inc_arrival)}         AS arrival_ts,
            {_parse_ts_sql(inc_close)}           AS close_ts,

            -- Location
            {_pick_or_null(inc_cols, "address")} AS address,
            {_pick_or_null(inc_cols, "city")} AS city,
            try_cast({_pick_or_null(inc_cols, "zipcode", "zip_code")} AS INTEGER) AS zipcode,
            {_pick_or_null(inc_cols, "battalion")} AS battalion,
            {_pick_or_null(inc_cols, "station_area", "stationarea")} AS station_area,
            {_pick_or_null(inc_cols, "box")} AS box,
            try_cast({_pick_or_null(inc_cols, "supervisor_district", "supervisordistrict")} AS INTEGER) AS supervisor_district,
            {_pick_or_null(inc_cols, "neighborhood_district", "neighborhooddistrict")} AS neighborhood_district,
            {_pick_or_null(inc_cols, "location")} AS location,

            -- Measures / severity
            try_cast({_pick_or_null(inc_cols, "number_of_alarms", "numberofalarms")} AS INTEGER) AS number_of_alarms,
            try_cast({_pick_or_null(inc_cols, "suppression_units", "suppressionunits")} AS INTEGER) AS suppression_units,
            try_cast({_pick_or_null(inc_cols, "suppression_personnel", "suppressionpersonnel")} AS INTEGER) AS suppression_personnel,
            try_cast({_pick_or_null(inc_cols, "ems_units", "emsunits")} AS INTEGER) AS ems_units,
            try_cast({_pick_or_null(inc_cols, "ems_personnel", "emspersonnel")} AS INTEGER) AS ems_personnel,
            try_cast({_pick_or_null(inc_cols, "other_units", "otherunits")} AS INTEGER) AS other_units,
            try_cast({_pick_or_null(inc_cols, "other_personnel", "otherpersonnel")} AS INTEGER) AS other_personnel,

            CASE
              WHEN try_cast({_pick_or_null(inc_cols, "estimated_property_loss", "estimatedpropertyloss")} AS BIGINT) < 0 THEN NULL
              ELSE try_cast({_pick_or_null(inc_cols, "estimated_property_loss", "estimatedpropertyloss")} AS BIGINT)
            END AS estimated_property_loss,

            CASE
              WHEN try_cast({_pick_or_null(inc_cols, "estimated_contents_loss", "estimatedcontentsloss")} AS BIGINT) < 0 THEN NULL
              ELSE try_cast({_pick_or_null(inc_cols, "estimated_contents_loss", "estimatedcontentsloss")} AS BIGINT)
            END AS estimated_contents_loss,

            try_cast({_pick_or_null(inc_cols, "fire_fatalities", "firefatalities")} AS BIGINT) AS fire_fatalities,
            try_cast({_pick_or_null(inc_cols, "fire_injuries", "fireinjuries")} AS BIGINT) AS fire_injuries,
            try_cast({_pick_or_null(inc_cols, "civilian_fatalities", "civilianfatalities")} AS BIGINT) AS civilian_fatalities,
            try_cast({_pick_or_null(inc_cols, "civilian_injuries", "civilianinjuries")} AS BIGINT) AS civilian_injuries,

            -- Type / situation
            {_pick_or_null(inc_cols, "primary_situation", "primarysituation")} AS primary_situation,
            {_pick_or_null(inc_cols, "mutual_aid", "mutualaid")} AS mutual_aid

        FROM bronze.incidents
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY try_cast({inc_incident} AS BIGINT), try_cast({inc_call} AS BIGINT)
          ORDER BY
            COALESCE({_parse_ts_sql(inc_alarm)}, TIMESTAMP '1900-01-01') DESC,
            try_cast(_ingested_at_utc AS TIMESTAMP) DESC
        ) = 1;
        """

        logger.info("Creating silver.incidents_clean (PHASE2) ...")
        con.execute(incidents_sql)
        logger.info("silver.incidents_clean created.")

        logger.info("Silver PHASE2 clean + dedup completato.")
