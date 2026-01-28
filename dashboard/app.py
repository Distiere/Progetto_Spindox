import streamlit as st
import duckdb
import pandas as pd
import os
import re


from dotenv import load_dotenv
load_dotenv()
from pathlib import Path

# Text-to-SQL (Gemini)
try:
    from google import genai
except Exception:
    genai = None

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="SF Fire Dept — KPI Dashboard (Gold)",
    layout="wide",
)

# -----------------------------
# DB PATH (dual-mode)
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WAREHOUSE = PROJECT_ROOT / "data" / "warehouse.duckdb"
DEFAULT_SERVING = PROJECT_ROOT / "dashboard_exports" / "dashboard.duckdb"

# allow override
ENV_DB = os.getenv("DASHBOARD_DB_PATH")

if ENV_DB:
    DB_PATH = Path(ENV_DB)
elif DEFAULT_WAREHOUSE.exists():
    DB_PATH = DEFAULT_WAREHOUSE
else:
    DB_PATH = DEFAULT_SERVING

DB_PATH = DB_PATH.resolve()


def _table_exists(_con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ?
    LIMIT 1
    """
    return _con.execute(q, [schema, name]).fetchone() is not None


def _view_exists(_con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.views
    WHERE table_schema = ? AND table_name = ?
    LIMIT 1
    """
    return _con.execute(q, [schema, name]).fetchone() is not None

def _streamlit_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit Cloud a volte non digerisce Arrow LargeUtf8 (large_string).
    Forziamo le colonne testuali a stringhe Python (dtype=object).
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    for c in out.columns:
        # string dtype (include string[pyarrow], string[python], object con stringhe)
        if pd.api.types.is_string_dtype(out[c]) or out[c].dtype == "object":
            # astype(str) -> object, evita Arrow LargeUtf8
            out[c] = out[c].astype(str)
            # opzionale: ripristina NULL leggibili (se ti interessa)
            out.loc[out[c].isin(["None", "nan", "NaT"]), c] = None
    return out

# Detect mode: FULL (locale) vs SERVING (cloud)
_con0 = duckdb.connect(str(DB_PATH), read_only=True)
try:
    has_fact = _table_exists(_con0, "gold", "fact_incident")
    has_kpi = (
        _table_exists(_con0, "gold", "v_kpi_incident_volume_month")
        or _view_exists(_con0, "gold", "v_kpi_incident_volume_month")
    )
finally:
    _con0.close()

MODE = "full" if has_fact else "serving" if has_kpi else "unknown"

st.title(" San Francisco Fire Dept — KPI Dashboard")
st.caption(f"DB in uso: {DB_PATH} | mode: {MODE}")

if MODE == "unknown":
    st.error(
        "DB non riconosciuto: non trovo gold.fact_incident (full) né gold.v_kpi_* (serving).\n"
        "Esegui la pipeline oppure genera dashboard_exports/dashboard.duckdb."
    )
    st.stop()

# -----------------------------------------------------------------
# SERVING MODE (Streamlit Cloud): usa solo KPI già aggregati
# -----------------------------------------------------------------
if MODE == "serving":
    st.info(
        "Modalità SERVING: dashboard pubblicabile (DB leggero) con KPI aggregati dal layer GOLD."
    )

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        vol = con.execute(
            "SELECT * FROM gold.v_kpi_incident_volume_month ORDER BY year, month"
        ).df()
        rt = con.execute(
            "SELECT * FROM gold.v_kpi_response_time_month ORDER BY year, month"
        ).df()
        top = con.execute(
            "SELECT * FROM gold.v_kpi_top_incident_type ORDER BY incident_count DESC LIMIT 20"
        ).df()

        meta_exists = _table_exists(con, "meta", "dashboard_metadata")
        meta = con.execute("SELECT * FROM meta.dashboard_metadata").df() if meta_exists else None
    finally:
        con.close()
    vol = _streamlit_safe_df(vol)
    rt  = _streamlit_safe_df(rt)
    top = _streamlit_safe_df(top)
    if meta is not None:
        meta = _streamlit_safe_df(meta)


    if meta is not None and not meta.empty and "exported_at_utc" in meta.columns:
        st.caption(f"Exported at (UTC): {meta.loc[0, 'exported_at_utc']}")

    st.subheader("📈 Incident volume (monthly)")
    st.dataframe(vol, use_container_width=True)

    st.subheader("⏱️ Avg response time (monthly)")
    st.dataframe(rt, use_container_width=True)

    st.subheader("🏷️ Top incident types")
    st.dataframe(top, use_container_width=True)

    st.stop()



# -----------------------------
# DB HELPERS
# -----------------------------
@st.cache_data(ttl=60)
def read_df(sql: str) -> pd.DataFrame:
    """Esegue una query e ritorna un pandas DataFrame (cache 60s)."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


@st.cache_data(ttl=300)
def get_filter_options():
    """Preleva opzioni per i filtri (cache 5 min)."""
    years = read_df("""
        SELECT DISTINCT d.year
        FROM gold.fact_incident f
        JOIN gold.dim_date d ON d.date_id = f.date_id
        ORDER BY 1
    """)["year"].tolist()

    months = list(range(1, 13))

    call_type_groups = read_df("""
        SELECT DISTINCT it.call_type_group
        FROM gold.fact_incident f
        JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
        WHERE it.call_type_group IS NOT NULL AND TRIM(it.call_type_group) <> ''
        ORDER BY 1
    """)["call_type_group"].tolist()

    neighborhoods = read_df("""
        SELECT DISTINCT l.neighborhood
        FROM gold.fact_incident f
        JOIN gold.dim_location l ON l.location_id = f.location_id
        WHERE l.neighborhood IS NOT NULL AND TRIM(l.neighborhood) <> ''
        ORDER BY 1
    """)["neighborhood"].tolist()

    return years, months, call_type_groups, neighborhoods


def escape_sql_literal(value: str) -> str:
    """Escapa apici singoli per sicurezza nelle stringhe SQL."""
    return value.replace("'", "''")


def build_where(year_sel, month_sel, ctg_sel, neigh_sel) -> str:
    """
    Costruisce una WHERE dinamica.
    Nota: usa alias d/it/l che sono coerenti con le query sotto.
    """
    clauses = []
    if year_sel != "Tutti":
        clauses.append(f"d.year = {int(year_sel)}")
    if month_sel != "Tutti":
        clauses.append(f"d.month = {int(month_sel)}")
    if ctg_sel != "Tutti":
        clauses.append(f"it.call_type_group = '{escape_sql_literal(ctg_sel)}'")
    if neigh_sel != "Tutti":
        clauses.append(f"l.neighborhood = '{escape_sql_literal(neigh_sel)}'")

    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)


# -----------------------------
# SIDEBAR FILTERS
# -----------------------------
years, months, call_type_groups, neighborhoods = get_filter_options()

st.sidebar.header("Filtri")
year_sel = st.sidebar.selectbox("Anno", ["Tutti"] + years)
month_sel = st.sidebar.selectbox("Mese", ["Tutti"] + months)
ctg_sel = st.sidebar.selectbox("Call Type Group", ["Tutti"] + call_type_groups)
neigh_sel = st.sidebar.selectbox("Neighborhood", ["Tutti"] + neighborhoods)

where_sql = build_where(year_sel, month_sel, ctg_sel, neigh_sel)

# -----------------------------
# KPIs
# -----------------------------
st.subheader("KPI principali")
kpi_sql = f"""
SELECT
  COUNT(*) AS total_incidents,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM gold.fact_incident f
JOIN gold.dim_date d ON d.date_id = f.date_id
JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN gold.dim_location l ON l.location_id = f.location_id
{where_sql}
"""
kpis = read_df(kpi_sql).iloc[0]

col1, col2 = st.columns(2)
col1.metric("Incidenti", f"{int(kpis['total_incidents']):,}".replace(",", "."))
col2.metric("Avg response time (sec)", f"{(kpis['avg_response_time_sec'] or 0):.1f}")

# -----------------------------
# CHARTS
# -----------------------------
st.subheader("Trend mensile: numero incidenti")
trend_sql = f"""
SELECT
  d.year,
  d.month,
  COUNT(*) AS incident_count
FROM gold.fact_incident f
JOIN gold.dim_date d ON d.date_id = f.date_id
JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN gold.dim_location l ON l.location_id = f.location_id
{where_sql}
GROUP BY 1,2
ORDER BY 1,2
"""
trend = read_df(trend_sql)
st.dataframe(trend, use_container_width=True)

st.subheader("Top incident types")
top_sql = f"""
SELECT
  it.call_type_group,
  it.call_type,
  COUNT(*) AS incident_count,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM gold.fact_incident f
JOIN gold.dim_date d ON d.date_id = f.date_id
JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN gold.dim_location l ON l.location_id = f.location_id
{where_sql}
GROUP BY 1,2
ORDER BY incident_count DESC
LIMIT 20
"""
top = read_df(top_sql)
st.dataframe(top, use_container_width=True)

# -----------------------------
# OPTIONAL: Text-to-SQL (Gemini)
# -----------------------------
st.divider()
st.subheader("Text-to-SQL (opzionale)")

if genai is None:
    st.info("Modulo google-genai non disponibile. (Opzionale)")
else:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        st.warning("Imposta GEMINI_API_KEY per usare Text-to-SQL.")
    else:
        client = genai.Client(api_key=api_key)
        user_q = st.text_input("Domanda (in linguaggio naturale)", "")
        if st.button("Genera SQL") and user_q.strip():
            schema_hint = """
Tabelle disponibili:
- gold.fact_incident(incident_id, incident_number, call_number, date_id, location_id, incident_type_id,
                     response_time_sec, dispatch_delay_sec, travel_time_sec, ...)
- gold.dim_date(date_id, date, year, month, day)
- gold.dim_incident_type(incident_type_id, call_type_group, call_type)
- gold.dim_location(location_id, neighborhood, city, zipcode_of_incident, supervisor_district, ...)
"""
            prompt = f"""
Sei un assistente SQL. Genera una query DuckDB SQL corretta.
{schema_hint}

Domanda: {user_q}

Regole:
- Usa solo queste tabelle e colonne.
- Non fare UPDATE/DELETE, solo SELECT.
- Limita a 200 righe se la query non è aggregata.
"""
            resp = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            sql = resp.text.strip()

            # pulizia eventuale markdown ```sql
            sql = re.sub(r"^```sql", "", sql, flags=re.I).strip()
            sql = re.sub(r"```$", "", sql).strip()

            st.code(sql, language="sql")

            if st.button("Esegui SQL"):
                try:
                    df = read_df(sql)
                    st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error(str(e))
