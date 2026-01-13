import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="SF Fire Dept — KPI Dashboard (Gold)",
    layout="wide",
)

# -----------------------------
# DB PATH
# (assume: /data/warehouse.duckdb)
# -----------------------------
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"


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

    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


# -----------------------------
# UI: HEADER + SIDEBAR E FILTRI
# -----------------------------
st.title("🚒 San Francisco Fire Dept — KPI Dashboard (Gold)")
st.caption("KPI e grafici calcolati sul Gold layer (fact + dimensions).")

years, months, call_type_groups, neighborhoods = get_filter_options()

st.sidebar.header("Filtri")
year_sel = st.sidebar.selectbox("Anno", options=["Tutti"] + years, index=0)
month_sel = st.sidebar.selectbox("Mese", options=["Tutti"] + months, index=0)
ctg_sel = st.sidebar.selectbox("Call Type Group", options=["Tutti"] + call_type_groups, index=0)
neigh_sel = st.sidebar.selectbox("Neighborhood", options=["Tutti"] + neighborhoods, index=0)

where = build_where(year_sel, month_sel, ctg_sel, neigh_sel)

# -----------------------------
# KPI NUMERICHE
# -----------------------------
kpi_sql = f"""
SELECT
  COUNT(*) AS total_incidents,
  AVG(f.response_time_sec) FILTER (WHERE f.response_time_sec IS NOT NULL) AS avg_resp,
  AVG(f.dispatch_delay_sec) FILTER (WHERE f.dispatch_delay_sec IS NOT NULL) AS avg_dispatch,
  AVG(f.travel_time_sec) FILTER (WHERE f.travel_time_sec IS NOT NULL) AS avg_travel,
  quantile_cont(f.response_time_sec, 0.9) FILTER (WHERE f.response_time_sec IS NOT NULL) AS p90_resp
FROM gold.fact_incident f
JOIN gold.dim_date d ON d.date_id = f.date_id
JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN gold.dim_location l ON l.location_id = f.location_id
{where};
"""

k = read_df(kpi_sql).iloc[0]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Incidenti totali", f"{int(k['total_incidents']):,}")
c2.metric("Avg Response (sec)", "n/a" if pd.isna(k["avg_resp"]) else f"{k['avg_resp']:.1f}")
c3.metric("Avg Dispatch Delay (sec)", "n/a" if pd.isna(k["avg_dispatch"]) else f"{k['avg_dispatch']:.1f}")
c4.metric("Avg Travel Time (sec)", "n/a" if pd.isna(k["avg_travel"]) else f"{k['avg_travel']:.1f}")
c5.metric("P90 Response (sec)", "n/a" if pd.isna(k["p90_resp"]) else f"{k['p90_resp']:.1f}")

st.divider()

# -----------------------------
# GRAFICO 1: Trend response time mensile (linechart)
# -----------------------------
st.subheader("Grafico 1 — Avg Response Time (Monthly)")
df_rt = read_df(f"""
SELECT
  d.year,
  d.month,
  AVG(f.response_time_sec) AS avg_response_time_sec
FROM gold.fact_incident f
JOIN gold.dim_date d ON d.date_id = f.date_id
JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN gold.dim_location l ON l.location_id = f.location_id
{where}
AND f.response_time_sec IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2
""")

if df_rt.empty:
    st.info("Nessun dato per i filtri selezionati.")
else:
    df_rt["ym"] = df_rt["year"].astype(str) + "-" + df_rt["month"].astype(str).str.zfill(2)
    st.line_chart(df_rt.set_index("ym")[["avg_response_time_sec"]], height=260)

# -----------------------------
# GRAFICO 2: Volume incidenti mensile (barchart)
# -----------------------------
st.subheader("Grafico 2 — Incident Volume (Monthly)")
df_vol = read_df(f"""
SELECT
  d.year,
  d.month,
  COUNT(*) AS incident_count
FROM gold.fact_incident f
JOIN gold.dim_date d ON d.date_id = f.date_id
JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN gold.dim_location l ON l.location_id = f.location_id
{where}
GROUP BY 1,2
ORDER BY 1,2
""")

if df_vol.empty:
    st.info("Nessun dato per i filtri selezionati.")
else:
    df_vol["ym"] = df_vol["year"].astype(str) + "-" + df_vol["month"].astype(str).str.zfill(2)
    st.bar_chart(df_vol.set_index("ym")[["incident_count"]], height=260)

# -----------------------------
# GRAFICO 3: Top incident type (barchart)
# -----------------------------
st.subheader("Grafico 3 — Top Incident Types (count)")
df_top = read_df(f"""
SELECT
  it.call_type_group,
  it.call_type,
  COUNT(*) AS incident_count
FROM gold.fact_incident f
JOIN gold.dim_date d ON d.date_id = f.date_id
JOIN gold.dim_incident_type it ON it.incident_type_id = f.incident_type_id
JOIN gold.dim_location l ON l.location_id = f.location_id
{where}
GROUP BY 1,2
ORDER BY incident_count DESC
LIMIT 15
""")

if df_top.empty:
    st.info("Nessun dato per i filtri selezionati.")
else:
    df_top["label"] = df_top["call_type_group"].fillna("N/A") + " | " + df_top["call_type"].fillna("N/A")
    st.bar_chart(df_top.set_index("label")[["incident_count"]], height=320)

# -----------------------------
# tabelle di supporto
# -----------------------------
with st.expander("Mostra tabelle di supporto"):
     st.dataframe(df_rt, width='stretch')
     st.dataframe(df_vol, width='stretch')
     st.dataframe(df_top, width='stretch')
