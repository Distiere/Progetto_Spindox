import pandera as pa
from pandera import Column, Check
from prefect import task, get_run_logger

from etl.utils import get_db_connection

# -----------------------------
# Pandera Schemas
# -----------------------------
CallsSchema = pa.DataFrameSchema(
    {
        "call_number": Column(pa.Int64, nullable=False, checks=Check.gt(0)),

        # nullable integer (pandas Int64)
        "incident_number": Column("Int64", nullable=True),

        "received_ts": Column("datetime64[ns]", nullable=False),
        "dispatch_ts": Column("datetime64[ns]", nullable=False),
        "on_scene_ts": Column("datetime64[ns]", nullable=True),

        # nullable integer -> NON fare coerce a int64 puro
        "response_time_sec": Column("Int64", nullable=True, checks=Check.ge(0)),
        "dispatch_delay_sec": Column("Int64", nullable=True, checks=Check.ge(0)),
        "travel_time_sec": Column("Int64", nullable=True, checks=Check.ge(0)),

        "city": Column(object, nullable=True),
        "battalion": Column(object, nullable=True),
    },
    strict=False,
)

IncidentsSchema = pa.DataFrameSchema(
    {
        "incident_number": Column(pa.Int64, nullable=False, checks=Check.gt(0)),
        "incident_date": Column("datetime64[ns]", nullable=True),

        # possono arrivare come Int64 o int64: lasciamo nullable Int64
        "estimated_property_loss": Column("Int64", nullable=True, checks=Check.ge(0)),
        "estimated_contents_loss": Column("Int64", nullable=True, checks=Check.ge(0)),
        "fire_injuries": Column("Int64", nullable=True, checks=Check.ge(0)),
        "fire_fatalities": Column("Int64", nullable=True, checks=Check.ge(0)),
        "civilian_injuries": Column("Int64", nullable=True, checks=Check.ge(0)),
        "civilian_fatalities": Column("Int64", nullable=True, checks=Check.ge(0)),
    },
    strict=False,
)

def _assert_sql(con, sql: str, msg: str):
    val = con.execute(sql).fetchone()[0]
    if val and int(val) != 0:
        raise ValueError(f"{msg} (count={val})")

@task(name="validate_silver_quality")
def validate_silver_quality(sample_n: int = 200_000) -> None:
    logger = get_run_logger()

    with get_db_connection(read_only=True) as con:
        logger.info("Avvio quality gate su SILVER")

        # ---- Hard checks via SQL ----
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM silver.calls_clean WHERE received_ts IS NULL",
            "FAIL: received_ts contiene NULL",
        )
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM silver.calls_clean WHERE dispatch_ts IS NULL",
            "FAIL: dispatch_ts contiene NULL",
        )
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM silver.calls_clean WHERE response_time_sec < 0",
            "FAIL: response_time_sec negativo",
        )
        _assert_sql(
            con,
            "SELECT COUNT(*) FROM silver.incidents_clean WHERE incident_number IS NULL",
            "FAIL: incident_number NULL in incidents_clean",
        )

        # ---- Pandera validation su campione ----
        calls_df = con.execute(f"""
            SELECT
              call_number, incident_number, received_ts, dispatch_ts, on_scene_ts,
              response_time_sec, dispatch_delay_sec, travel_time_sec,
              city, battalion
            FROM silver.calls_clean
            USING SAMPLE {sample_n}
        """).df()

        incidents_df = con.execute(f"""
            SELECT
              incident_number, incident_date,
              estimated_property_loss, estimated_contents_loss,
              fire_injuries, fire_fatalities, civilian_injuries, civilian_fatalities
            FROM silver.incidents_clean
            USING SAMPLE {min(sample_n, 100_000)}
        """).df()

        CallsSchema.validate(calls_df, lazy=True)
        IncidentsSchema.validate(incidents_df, lazy=True)

        logger.info("Quality gate SILVER superato (SQL checks + Pandera sample).")
