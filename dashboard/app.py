import os
import re
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Text-to-SQL (Gemini) - opzionale
try:
    from google import genai
except Exception:
    genai = None


# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="SF Fire Dept — KPI Dashboard",
    layout="wide",
)

# -----------------------------
# DB PATH (dual-mode)
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WAREHOUSE = PROJECT_ROOT / "data" / "warehouse.duckdb"
DEFAULT_SERVING = PROJECT_ROOT / "dashboard_exports" / "dashboard.duckdb"

ENV_DB = os.getenv("DASHBOARD_DB_PATH")

if ENV_DB:
    DB_PATH = Path(ENV_DB)
elif DEFAULT_WAREHOUSE.exists():
    DB_PATH = DEFAULT_WAREHOUSE
else:
    DB_PATH = DEFAULT_SERVING

DB_PATH = DB_PATH.resolve()


# -----------------------------
# HELPERS
# -----------------------------
def _streamlit_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit Cloud può crashare con Arrow LargeUtf8 (large_string).
    Qui forziamo le colonne testuali a dtype 'object' (stringhe Python pure).
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    for c in out.columns:
        s = out[c]
        if pd.api.types.is_string_dtype(s) or s.dtype == "object":
            try:
                out[c] = s.astype("object").map(lambda x: None if pd.isna(x) else str(x))
            except Exception:
                out[c] = [None if pd.isna(x) else str(x) for x in s.tolist()]
    return out


def _relation_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str, kind: str) -> bool:
    """
    kind: 'tables' oppure 'views'
    """
    q = f"""
    SELECT 1
    FROM information_schema.{kind}
    WHERE table_schema = ? AND table_name = ?
    LIMIT 1
    """
    return con.execute(q, [schema, name]).fetchone() is not None


def _find_schema_for(con: duckdb.DuckDBPyConnection, name: str) -> str | None:
    """
    Trova lo schema (es: 'gold' o 'main') dove esiste una table/view con quel nome.
    """
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

    # preferisci gold se c'è, altrimenti il primo trovato (tipicamente main nel serving db)
    schemas = [r[0] for r in rows if r and r[0]]
    if "gold" in schemas:
        return "gold"
    return schemas[0]


def _detect_mode_and_schema() -> tuple[str, str | None]:
    """
    MODE:
      - 'full'    : presente fact_incident
      - 'serving' : presenti le view KPI (esportate)
      - 'unknown' : nessuno dei due
    Inoltre ritorna lo schema dove stanno i dati (gold o main).
    """
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        schema_fact = _find_schema_for(con, "fact_incident")
        if schema_fact:
            return "full", schema_fact

        # serving: cerchiamo una view KPI
        schema_kpi = _find_schema_for(con, "v_kpi_incident_volume_month")
        if schema_kpi:
            return "serving", schema_kpi

        return "unknown", None
    finally:
        con.close()


MODE, DATA_SCHEMA = _detect_mode_and_schema()

st.title("San Francisco Fire Dept — KPI Dashboard")
st.caption(f"DB in uso: {DB_PATH} | mode: {MODE} | schema: {DATA_SCHEMA or '-'}")

if MODE == "unknown" or DATA_SCHEMA is None:
    st.error(
        "DB non riconosciuto: non trovo fact_incident né v_kpi_*.\n"
        "Esegui la pipeline oppure genera dashboard_exports/dashboard.duckdb."
    )
    st.stop()


# -----------------------------
# SERVING MODE (Streamlit Cloud)
# -----------------------------
if MODE == "serving":
    st.info("Modalità SERVING: dashboard pubblicabile (DB leggero) con KPI aggregati + dimensioni.")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        # KPI
        vol = con.execute(
            f"SELECT * FROM {DATA_SCHEMA}.v_kpi_incident_volume_month ORDER BY year, month"
        ).fetchdf()

        rt = con.execute(
            f"SELECT * FROM {DATA_SCHEMA}.v_kpi_response_time_month ORDER BY year, month"
        ).fetchdf()

        # Cast forte a VARCHAR per evitare LargeUtf8 su Streamlit Cloud
        top = con.execute(
            f"""
            SELECT
              CAST(call_type_group AS VARCHAR) AS call_type_group,
              CAST(call_type AS VARCHAR) AS call_type,
              incident_count,
              avg_response_time_sec
            FROM {DATA_SCHEMA}.v_kpi_top_incident_type
            ORDER BY incident_count DESC
            LIMIT 20
            """
        ).fetchdf()

        # metadata: nel tuo export spesso è in main (es: main.dashboard_metadata)
        meta_schema = _find_schema_for(con, "dashboard_metadata")
        if meta_schema:
            meta = con.execute(f"SELECT * FROM {meta_schema}.dashboard_metadata").fetchdf()
        else:
            meta = None

        # (opzionale) dimensioni esportate
        dim_date_schema = _find_schema_for(con, "dim_date")
        dim_it_schema = _find_schema_for(con, "dim_incident_type")
        dim_loc_schema = _find_schema_for(con, "dim_location")

        dim_date = con.execute(f"SELECT * FROM {dim_date_schema}.dim_date").fetchdf() if dim_date_schema else None
        dim_it = con.execute(f"SELECT * FROM {dim_it_schema}.dim_incident_type").fetchdf() if dim_it_schema else None
        dim_loc = con.execute(f"SELECT * FROM {dim_loc_schema}.dim_location").fetchdf() if dim_loc_schema else None

    finally:
        con.close()

    vol = _streamlit_safe_df(vol)
    rt = _streamlit_safe_df(rt)
    top = _streamlit_safe_df(top)
    if meta is not None:
        meta = _streamlit_safe_df(meta)
    if dim_date is not None:
        dim_date = _streamlit_safe_df(dim_date)
    if dim_it is not None:
        dim_it = _streamlit_safe_df(dim_it)
    if dim_loc is not None:
        dim_loc = _streamlit_safe_df(dim_loc)

    if meta is not None and not meta.empty and "exported_at_utc" in meta.columns:
        st.caption(f"Exported at (UTC): {meta.loc[0, 'exported_at_utc']}")

    st.subheader("📈 Incident volume (monthly)")
    st.dataframe(vol, use_container_width=True)

    st.subheader("⏱️ Avg response time (monthly)")
    st.dataframe(rt, use_container_width=True)

    st.subheader("🏷️ Top incident types")
    st.dataframe(top, use_container_width=True)

    with st.expander("🔎 Dimensioni (opzionale)"):
        if dim_date is not None:
            st.write("dim_date")
            st.dataframe(dim_date.head(200), use_container_width=True)
        if dim_it is not None:
            st.write("dim_incident_type")
            st.dataframe(dim_it.head(200), use_container_width=True)
        if dim_loc is not None:
            st.write("dim_location")
            st.dataframe(dim_loc.head(200), use_container_width=True)

    st.stop()


# -----------------------------
# FULL MODE (locale - warehouse)
# -----------------------------
@st.cache_data(ttl=60)
def read_df(sql: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


@st.cache_data(ttl=300)
def get_filter_options(schema: str):
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
        WHERE it.call_type_group IS NOT NULL AND TRIM(it.call_type_group) <> ''
        ORDER BY 1
    """)["call_type_group"].tolist()

    neighborhoods = read_df(f"""
        SELECT DISTINCT l.neighborhood
        FROM {schema}.fact_incident f
        JOIN {schema}.dim_location l ON l.location_id = f.location_id
        WHERE l.neighborhood IS NOT NULL AND TRIM(l.neighborhood) <> ''
        ORDER BY 1
    """)["neighborhood"].tolist()

    return years, months, call_type_groups, neighborhoods


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def build_where(year_sel, month_sel, ctg_sel, neigh_sel) -> str:
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


st.sidebar.header("Filtri")

years, months, call_type_groups, neighborhoods = get_filter_options(DATA_SCHEMA)

year_sel = st.sidebar.selectbox("Anno", ["Tutti"] + years)
month_sel = st.sidebar.selectbox("Mese", ["Tutti"] + months)
ctg_sel = st.sidebar.selectbox("Call Type Group", ["Tutti"] + call_type_groups)
neigh_sel = st.sidebar.selectbox("Neighborhood", ["Tutti"] + neighborhoods)

where_sql = build_where(year_sel, month_sel, ctg_sel, neigh_sel)

st.subheader("KPI principali")

kpi_sql = f"""
SELECT
  COUNT(*) AS total_incidents,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM {DATA_SCHEMA}.fact_incident f
JOIN {DATA_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {DATA_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {DATA_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where_sql}
"""
kpis = read_df(kpi_sql).iloc[0]

col1, col2 = st.columns(2)
col1.metric("Incidenti", f"{int(kpis['total_incidents']):,}".replace(",", "."))
col2.metric("Avg response time (sec)", f"{(kpis['avg_response_time_sec'] or 0):.1f}")

st.subheader("Trend mensile: numero incidenti")

trend_sql = f"""
SELECT
  d.year,
  d.month,
  COUNT(*) AS incident_count
FROM {DATA_SCHEMA}.fact_incident f
JOIN {DATA_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {DATA_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {DATA_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where_sql}
GROUP BY 1,2
ORDER BY 1,2
"""
trend = _streamlit_safe_df(read_df(trend_sql))
st.dataframe(trend, use_container_width=True)

st.subheader("Top incident types")

top_sql = f"""
SELECT
  it.call_type_group,
  it.call_type,
  COUNT(*) AS incident_count,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM {DATA_SCHEMA}.fact_incident f
JOIN {DATA_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {DATA_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {DATA_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where_sql}
GROUP BY 1,2
ORDER BY incident_count DESC
LIMIT 20
"""
top = _streamlit_safe_df(read_df(top_sql))
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
            schema_hint = f"""
Tabelle disponibili (schema: {DATA_SCHEMA}):
- {DATA_SCHEMA}.fact_incident(...)
- {DATA_SCHEMA}.dim_date(date_id, date, year, month, day)
- {DATA_SCHEMA}.dim_incident_type(incident_type_id, call_type_group, call_type)
- {DATA_SCHEMA}.dim_location(location_id, neighborhood, city, zipcode_of_incident, supervisor_district, ...)
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

            sql = re.sub(r"^```sql", "", sql, flags=re.I).strip()
            sql = re.sub(r"```$", "", sql).strip()

            st.code(sql, language="sql")

            if st.button("Esegui SQL"):
                try:
                    df = _streamlit_safe_df(read_df(sql))
                    st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error(str(e))
