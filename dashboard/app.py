import os
import re
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Text-to-SQL (opzionale)
try:
    from google import genai
except Exception:
    genai = None


# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(
    page_title="SF Fire Dept — KPI Dashboard",
    layout="wide",
)


# =====================================================
# PATHS
# =====================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_WAREHOUSE = PROJECT_ROOT / "data" / "warehouse.duckdb"
DEFAULT_SERVING   = PROJECT_ROOT / "dashboard_exports" / "dashboard.duckdb"

ENV_DB = os.getenv("DASHBOARD_DB_PATH")

if ENV_DB:
    DB_PATH = Path(ENV_DB)
elif DEFAULT_WAREHOUSE.exists():
    DB_PATH = DEFAULT_WAREHOUSE
else:
    DB_PATH = DEFAULT_SERVING

DB_PATH = DB_PATH.resolve()


# =====================================================
# SIDEBAR — MODE TOGGLE
# =====================================================
st.sidebar.header("Modalità DB")

choice = st.sidebar.radio(
    "Sorgente dati",
    ["AUTO", "FULL (warehouse)", "SERVING (dashboard export)"],
    index=0,
    help=(
        "AUTO: usa warehouse se esiste, altrimenti serving.\n"
        "FULL: forza data/warehouse.duckdb (dataset completo).\n"
        "SERVING: forza dashboard_exports/dashboard.duckdb (cloud-friendly)."
    ),
)

if choice == "FULL (warehouse)":
    DB_PATH = DEFAULT_WAREHOUSE
elif choice == "SERVING (dashboard export)":
    DB_PATH = DEFAULT_SERVING

DB_PATH = DB_PATH.resolve()


# =====================================================
# HELPERS
# =====================================================
def _safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    FIX DEFINITIVO Streamlit Cloud:
    elimina Arrow LargeUtf8 convertendo tutto a Python str/object.
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    for c in out.columns:
        out[c] = out[c].apply(lambda x: None if pd.isna(x) else str(x))
    return out


def _find_schema_for(con: duckdb.DuckDBPyConnection, name: str) -> str | None:
    q = """
    SELECT table_schema
    FROM information_schema.tables
    WHERE table_name = ?
    UNION ALL
    SELECT table_schema
    FROM information_schema.views
    WHERE table_name = ?
    """
    rows = con.execute(q, [name, name]).fetchall()
    if not rows:
        return None

    schemas = [r[0] for r in rows if r and r[0]]
    return "gold" if "gold" in schemas else schemas[0]


def detect_mode_and_schema():
    if not DB_PATH.exists():
        return "missing", None

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        schema_fact = _find_schema_for(con, "fact_incident")
        if schema_fact:
            return "full", schema_fact

        schema_kpi = _find_schema_for(con, "v_kpi_incident_volume_month")
        if schema_kpi:
            return "serving", schema_kpi

        return "unknown", None
    finally:
        con.close()


MODE, DATA_SCHEMA = detect_mode_and_schema()


# =====================================================
# HEADER
# =====================================================
st.title("🚒 San Francisco Fire Dept — KPI Dashboard")
st.caption(f"DB: `{DB_PATH}` | mode: **{MODE.upper()}** | schema: `{DATA_SCHEMA or '-'}`")

if MODE in ("missing", "unknown"):
    st.error(
        "Database non valido o non popolato.\n\n"
        "Esegui la pipeline oppure genera `dashboard_exports/dashboard.duckdb`."
    )
    st.stop()


# =====================================================
# SERVING MODE (Streamlit Cloud)
# =====================================================
if MODE == "serving":
    st.info("Modalità **SERVING** — KPI aggregati (cloud-ready, DB leggero)")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        vol = con.execute(
            f"SELECT * FROM {DATA_SCHEMA}.v_kpi_incident_volume_month ORDER BY year, month"
        ).fetchdf()

        rt = con.execute(
            f"SELECT * FROM {DATA_SCHEMA}.v_kpi_response_time_month ORDER BY year, month"
        ).fetchdf()

        top = con.execute(
            f"""
            SELECT
              CAST(call_type_group AS VARCHAR) AS call_type_group,
              CAST(call_type AS VARCHAR)       AS call_type,
              incident_count,
              avg_response_time_sec
            FROM {DATA_SCHEMA}.v_kpi_top_incident_type
            ORDER BY incident_count DESC
            LIMIT 20
            """
        ).fetchdf()

        meta_schema = _find_schema_for(con, "dashboard_metadata")
        meta = (
            con.execute(f"SELECT * FROM {meta_schema}.dashboard_metadata").fetchdf()
            if meta_schema else None
        )
    finally:
        con.close()

    vol = _safe_df(vol)
    rt  = _safe_df(rt)
    top = _safe_df(top)
    if meta is not None:
        meta = _safe_df(meta)

    if meta is not None and "exported_at_utc" in meta.columns:
        st.caption(f"Exported at (UTC): {meta.loc[0, 'exported_at_utc']}")

    st.subheader("📈 Incident volume (monthly)")
    st.dataframe(vol, use_container_width=True)

    st.subheader("⏱️ Avg response time (monthly)")
    st.dataframe(rt, use_container_width=True)

    st.subheader("🏷️ Top incident types")
    st.dataframe(top, use_container_width=True)

    st.stop()


# =====================================================
# FULL MODE (warehouse — locale)
# =====================================================
st.info("Modalità **FULL** — dataset completo (warehouse locale)")

@st.cache_data(ttl=60)
def read_df(sql: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


@st.cache_data(ttl=300)
def get_filters(schema: str):
    years = read_df(f"""
        SELECT DISTINCT d.year
        FROM {schema}.fact_incident f
        JOIN {schema}.dim_date d ON d.date_id = f.date_id
        ORDER BY 1
    """)["year"].tolist()

    months = list(range(1, 13))

    call_type_groups = read_df(f"""
        SELECT DISTINCT it.call_type_group
        FROM {schema}.fact_incident f
        JOIN {schema}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
        WHERE it.call_type_group IS NOT NULL
        ORDER BY 1
    """)["call_type_group"].tolist()

    neighborhoods = read_df(f"""
        SELECT DISTINCT l.neighborhood
        FROM {schema}.fact_incident f
        JOIN {schema}.dim_location l ON l.location_id = f.location_id
        WHERE l.neighborhood IS NOT NULL
        ORDER BY 1
    """)["neighborhood"].tolist()

    return years, months, call_type_groups, neighborhoods


def esc(x: str) -> str:
    return x.replace("'", "''")


def build_where(y, m, c, n):
    clauses = []
    if y != "Tutti":
        clauses.append(f"d.year = {int(y)}")
    if m != "Tutti":
        clauses.append(f"d.month = {int(m)}")
    if c != "Tutti":
        clauses.append(f"it.call_type_group = '{esc(c)}'")
    if n != "Tutti":
        clauses.append(f"l.neighborhood = '{esc(n)}'")
    return "WHERE " + " AND ".join(clauses) if clauses else ""


# ---------------- Filters
years, months, ctgs, neighs = get_filters(DATA_SCHEMA)

st.sidebar.header("Filtri (FULL)")
y = st.sidebar.selectbox("Anno", ["Tutti"] + years)
m = st.sidebar.selectbox("Mese", ["Tutti"] + months)
c = st.sidebar.selectbox("Call Type Group", ["Tutti"] + ctgs)
n = st.sidebar.selectbox("Neighborhood", ["Tutti"] + neighs)

where = build_where(y, m, c, n)

# ---------------- KPIs
st.subheader("KPI principali")

kpi = read_df(f"""
SELECT
  COUNT(*) AS total_incidents,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM {DATA_SCHEMA}.fact_incident f
JOIN {DATA_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {DATA_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {DATA_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where}
""").iloc[0]

c1, c2 = st.columns(2)
c1.metric("Incidenti", f"{int(kpi['total_incidents']):,}".replace(",", "."))
c2.metric("Avg response time (sec)", f"{(kpi['avg_response_time_sec'] or 0):.1f}")

# ---------------- Tables
st.subheader("Trend mensile incidenti")

trend = _safe_df(read_df(f"""
SELECT d.year, d.month, COUNT(*) AS incident_count
FROM {DATA_SCHEMA}.fact_incident f
JOIN {DATA_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {DATA_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {DATA_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where}
GROUP BY 1,2
ORDER BY 1,2
"""))

st.dataframe(trend, use_container_width=True)

st.subheader("Top incident types")

top = _safe_df(read_df(f"""
SELECT
  it.call_type_group,
  it.call_type,
  COUNT(*) AS incident_count,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM {DATA_SCHEMA}.fact_incident f
JOIN {DATA_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {DATA_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {DATA_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where}
GROUP BY 1,2
ORDER BY incident_count DESC
LIMIT 20
"""))

st.dataframe(top, use_container_width=True)
