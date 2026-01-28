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

# Arrow (per fix LargeUtf8)
try:
    import pyarrow as pa
except Exception:
    pa = None


# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="SF Fire Dept — KPI Dashboard",
    layout="wide",
)

st.title("San Francisco Fire Dept — KPI Dashboard")


# -----------------------------
# PATHS (dual-mode)
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
st.caption(f"DB in uso: {DB_PATH}")


# -----------------------------
# DB helpers
# -----------------------------
def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ?
    LIMIT 1
    """
    return con.execute(q, [schema, name]).fetchone() is not None


def _view_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.views
    WHERE table_schema = ? AND table_name = ?
    LIMIT 1
    """
    return con.execute(q, [schema, name]).fetchone() is not None


def _find_schema_for(con: duckdb.DuckDBPyConnection, table_or_view: str) -> str | None:
    """
    In alcuni export, le tabelle/vista sono in schema `main` e non `gold`.
    Cerchiamo in information_schema.
    """
    row = con.execute(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name = ?
        UNION ALL
        SELECT table_schema
        FROM information_schema.views
        WHERE table_name = ?
        LIMIT 1
        """,
        [table_or_view, table_or_view],
    ).fetchone()
    return row[0] if row else None


def _df_to_arrow_safe(df: pd.DataFrame):
    """
    FIX definitivo per Streamlit Cloud:
    Streamlit serializza via Arrow e può esplodere su LargeUtf8 (large_string).
    Qui forziamo tutte le colonne testuali a pa.string() (Utf8 “normale”).
    """
    if pa is None:
        return df  # fallback: useremo st.table più sotto se serve

    if df is None:
        return df

    if df.empty:
        # Arrow Table vuota con schema “safe”
        return pa.Table.from_pandas(df, preserve_index=False)

    arrays = {}
    fields = []

    for c in df.columns:
        s = df[c]

        # Colonne testuali -> pa.string()
        if pd.api.types.is_string_dtype(s) or s.dtype == "object":
            # mantieni None, converti il resto a string
            py_vals = [None if pd.isna(v) else str(v) for v in s.tolist()]
            arr = pa.array(py_vals, type=pa.string())
            arrays[c] = arr
            fields.append(pa.field(c, pa.string()))
        else:
            # numeric / datetime -> lascia che Arrow inferisca
            arr = pa.array(s.tolist())
            arrays[c] = arr
            fields.append(pa.field(c, arr.type))

    schema = pa.schema(fields)
    return pa.Table.from_pydict(arrays, schema=schema)


def _show_df(label: str, df: pd.DataFrame):
    """
    Render robusto: prova st.dataframe con Arrow-safe; se pyarrow non c'è,
    fallback su st.table.
    """
    st.subheader(label)
    try:
        safe = _df_to_arrow_safe(df)
        st.dataframe(safe, use_container_width=True)
    except Exception:
        # fallback ultra-safe (HTML-ish)
        st.table(df)


# -----------------------------
# Detect MODE (FULL vs SERVING)
# -----------------------------
with duckdb.connect(str(DB_PATH), read_only=True) as con0:
    has_full_fact = _table_exists(con0, "gold", "fact_incident")
    # serving: KPI views/tables potrebbero stare in gold o in main
    kpi_schema = _find_schema_for(con0, "v_kpi_incident_volume_month")
    has_serving_kpi = kpi_schema is not None

if has_full_fact:
    MODE = "full"
elif has_serving_kpi:
    MODE = "serving"
else:
    MODE = "unknown"

st.caption(f"mode: {MODE}")

if MODE == "unknown":
    st.error(
        "DB non riconosciuto: non trovo gold.fact_incident (FULL) "
        "né v_kpi_* (SERVING).\n"
        "Esegui la pipeline oppure genera dashboard_exports/dashboard.duckdb."
    )
    st.stop()


# ============================================================
# SERVING MODE (Streamlit Cloud): KPI aggregati + dimensioni
# ============================================================
if MODE == "serving":
    st.info("Modalità SERVING: dashboard pubblicabile (DB leggero) con KPI aggregati dal layer GOLD.")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        DATA_SCHEMA = _find_schema_for(con, "v_kpi_incident_volume_month") or "main"

        vol = con.execute(
            f"SELECT * FROM {DATA_SCHEMA}.v_kpi_incident_volume_month ORDER BY year, month"
        ).fetchdf()

        rt = con.execute(
            f"SELECT * FROM {DATA_SCHEMA}.v_kpi_response_time_month ORDER BY year, month"
        ).fetchdf()

        # forza esplicita CAST + Arrow-safe (doppiamente robusto)
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

        meta_schema = _find_schema_for(con, "dashboard_metadata")
        meta = con.execute(f"SELECT * FROM {meta_schema}.dashboard_metadata").fetchdf() if meta_schema else None

        # dimensioni (opzionali) se esportate
        dim_date_schema = _find_schema_for(con, "dim_date")
        dim_it_schema = _find_schema_for(con, "dim_incident_type")
        dim_loc_schema = _find_schema_for(con, "dim_location")

        dim_date = con.execute(f"SELECT * FROM {dim_date_schema}.dim_date").fetchdf() if dim_date_schema else None
        dim_it = con.execute(f"SELECT * FROM {dim_it_schema}.dim_incident_type").fetchdf() if dim_it_schema else None
        dim_loc = con.execute(f"SELECT * FROM {dim_loc_schema}.dim_location").fetchdf() if dim_loc_schema else None

    if meta is not None and not meta.empty and "exported_at_utc" in meta.columns:
        st.caption(f"Exported at (UTC): {meta.loc[0, 'exported_at_utc']}")

    _show_df("📈 Incident volume (monthly)", vol)
    _show_df("⏱️ Avg response time (monthly)", rt)
    _show_df("🏷️ Top incident types", top)

    with st.expander("🔎 Dimensioni (opzionale)"):
        if dim_date is not None:
            _show_df("dim_date (head)", dim_date.head(200))
        if dim_it is not None:
            _show_df("dim_incident_type (head)", dim_it.head(200))
        if dim_loc is not None:
            _show_df("dim_location (head)", dim_loc.head(200))

    st.stop()


# ============================================================
# FULL MODE (locale - warehouse): fact + filtri
# ============================================================
@st.cache_data(ttl=60)
def read_df(sql: str) -> pd.DataFrame:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        return con.execute(sql).fetchdf()


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

    return "" if not clauses else "WHERE " + " AND ".join(clauses)


FULL_SCHEMA = "gold"

st.sidebar.header("Filtri (FULL mode)")
years, months, call_type_groups, neighborhoods = get_filter_options(FULL_SCHEMA)

year_sel = st.sidebar.selectbox("Anno", ["Tutti"] + years)
month_sel = st.sidebar.selectbox("Mese", ["Tutti"] + months)
ctg_sel = st.sidebar.selectbox("Call Type Group", ["Tutti"] + call_type_groups)
neigh_sel = st.sidebar.selectbox("Neighborhood", ["Tutti"] + neighborhoods)

where_sql = build_where(year_sel, month_sel, ctg_sel, neigh_sel)

st.subheader("KPI principali (FULL)")
kpi_sql = f"""
SELECT
  COUNT(*) AS total_incidents,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM {FULL_SCHEMA}.fact_incident f
JOIN {FULL_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {FULL_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {FULL_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where_sql}
"""
kpis = read_df(kpi_sql).iloc[0]
col1, col2 = st.columns(2)
col1.metric("Incidenti", f"{int(kpis['total_incidents']):,}".replace(",", "."))
col2.metric("Avg response time (sec)", f"{(kpis['avg_response_time_sec'] or 0):.1f}")

trend_sql = f"""
SELECT
  d.year,
  d.month,
  COUNT(*) AS incident_count
FROM {FULL_SCHEMA}.fact_incident f
JOIN {FULL_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {FULL_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {FULL_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where_sql}
GROUP BY 1,2
ORDER BY 1,2
"""
trend = read_df(trend_sql)
_show_df("Trend mensile: numero incidenti", trend)

top_sql = f"""
SELECT
  CAST(it.call_type_group AS VARCHAR) AS call_type_group,
  CAST(it.call_type AS VARCHAR) AS call_type,
  COUNT(*) AS incident_count,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM {FULL_SCHEMA}.fact_incident f
JOIN {FULL_SCHEMA}.dim_date d ON d.date_id = f.date_id
JOIN {FULL_SCHEMA}.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN {FULL_SCHEMA}.dim_location l ON l.location_id = f.location_id
{where_sql}
GROUP BY 1,2
ORDER BY incident_count DESC
LIMIT 20
"""
top = read_df(top_sql)
_show_df("Top incident types", top)

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
Tabelle disponibili (FULL):
- {FULL_SCHEMA}.fact_incident(incident_id, incident_number, call_number, date_id, location_id, incident_type_id,
                     response_time_sec, dispatch_delay_sec, travel_time_sec, ...)
- {FULL_SCHEMA}.dim_date(date_id, date, year, month, day)
- {FULL_SCHEMA}.dim_incident_type(incident_type_id, call_type_group, call_type)
- {FULL_SCHEMA}.dim_location(location_id, neighborhood, city, zipcode_of_incident, supervisor_district, ...)
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
                    df = read_df(sql)
                    _show_df("Risultato", df)
                except Exception as e:
                    st.error(str(e))
