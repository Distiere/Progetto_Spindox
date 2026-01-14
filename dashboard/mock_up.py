import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(page_title="SFFD Dashboard (Mock-up)", layout="wide")

DB_PATH = Path("data/warehouse.duckdb")
BASE_CALLS = "silver.calls_clean"
BASE_INC = "silver.incidents_clean"

# -----------------------------
# DB HELPER
# -----------------------------
@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()

def _make_in_clause(values):
    """Crea una clausola IN sicura per stringhe SQL (escape ' -> '')."""
    if not values:
        return None
    escaped = [v.replace("'", "''") for v in values]
    return ",".join(f"'{v}'" for v in escaped)

def make_where_calls(date_from, date_to, selected_groups, selected_cities, selected_battalions):
    clauses = [f"call_date BETWEEN '{date_from}' AND '{date_to}'"]

    in_groups = _make_in_clause(selected_groups)
    if in_groups:
        clauses.append(f"call_type_group IN ({in_groups})")

    in_cities = _make_in_clause(selected_cities)
    if in_cities:
        clauses.append(f"city IN ({in_cities})")

    in_bats = _make_in_clause(selected_battalions)
    if in_bats:
        clauses.append(f"battalion IN ({in_bats})")

    return "WHERE " + " AND ".join(clauses)

# -----------------------------
# UI: HEADER
# -----------------------------
st.title("San Francisco Fire Department — Mock-up Dashboard")

if not DB_PATH.exists():
    st.error(f"Database non trovato: {DB_PATH.resolve()}")
    st.stop()

# -----------------------------
# UI: SIDEBAR FILTERS
# -----------------------------
with st.sidebar:
    st.header("Filtri (Calls)")

    dmin = q(f"SELECT min(call_date) AS dmin FROM {BASE_CALLS}")["dmin"][0]
    dmax = q(f"SELECT max(call_date) AS dmax FROM {BASE_CALLS}")["dmax"][0]

    date_range = st.date_input(
        "Intervallo date (call_date)",
        value=(dmin, dmax),
        min_value=dmin,
        max_value=dmax,
    )
    date_from, date_to = date_range[0], date_range[1]

    ctg = q(f"""
        SELECT DISTINCT call_type_group
        FROM {BASE_CALLS}
        WHERE call_type_group IS NOT NULL
        ORDER BY 1
    """)["call_type_group"].tolist()

    selected_groups = st.multiselect(
        "Call Type Group",
        ctg,
        default=ctg[:3] if len(ctg) >= 3 else ctg
    )

    cities = q(f"""
        SELECT DISTINCT city
        FROM {BASE_CALLS}
        WHERE city IS NOT NULL
        ORDER BY 1
    """)["city"].tolist()

    selected_cities = st.multiselect(
        "City",
        cities,
        default=["San Francisco"] if "San Francisco" in cities else []
    )

    battalions = q(f"""
        SELECT DISTINCT battalion
        FROM {BASE_CALLS}
        WHERE battalion IS NOT NULL
        ORDER BY 1
    """)["battalion"].tolist()

    selected_battalions = st.multiselect(
        "Battalion",
        battalions,
        default=[]
    )

    st.caption("Nota: in Severity (Incidents) applichiamo gli stessi filtri via join su incident_number.")

where_calls = make_where_calls(date_from, date_to, selected_groups, selected_cities, selected_battalions)

# -----------------------------
# TABS
# -----------------------------
tab_overview, tab_time, tab_geo, tab_sev = st.tabs(["Overview", "Time", "Geography", "Severity"])

# ============================================================
# TAB: OVERVIEW
# ============================================================
with tab_overview:
    kpi = q(f"""
        SELECT
          COUNT(*) AS total_calls,
          AVG(response_time_sec) AS avg_response_sec,
          quantile_cont(response_time_sec, 0.5) AS median_response_sec,
          quantile_cont(response_time_sec, 0.9) AS p90_response_sec,
          SUM(CASE WHEN response_time_sec IS NULL THEN 1 ELSE 0 END) AS null_response_time
        FROM {BASE_CALLS}
        {where_calls}
    """).iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Calls", f"{int(kpi['total_calls']):,}")
    col2.metric("Avg response (sec)", f"{(kpi['avg_response_sec'] or 0):.1f}")
    col3.metric("Median response (sec)", f"{(kpi['median_response_sec'] or 0):.1f}")
    col4.metric("P90 response (sec)", f"{(kpi['p90_response_sec'] or 0):.1f}")
    col5.metric("Response time NULL", f"{int(kpi['null_response_time']):,}")

    with st.expander("Data Quality (calls)"):
        dq = q(f"""
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN response_time_sec IS NULL THEN 1 ELSE 0 END) AS null_resp,
              SUM(CASE WHEN dispatch_delay_sec IS NULL THEN 1 ELSE 0 END) AS null_dispatch_delay,
              SUM(CASE WHEN travel_time_sec IS NULL THEN 1 ELSE 0 END) AS null_travel
            FROM {BASE_CALLS}
            {where_calls}
        """).iloc[0]
        pct_null = (dq["null_resp"] / dq["total"] * 100.0) if dq["total"] else 0
        st.write(f"- Records nel filtro: **{int(dq['total']):,}**")
        st.write(f"- Response time NULL: **{int(dq['null_resp']):,}** (**{pct_null:.2f}%**)")

    st.divider()

    st.subheader("Volume chiamate nel tempo (mensile)")
    df_month = q(f"""
        SELECT
          date_trunc('month', call_date) AS month,
          COUNT(*) AS calls
        FROM {BASE_CALLS}
        {where_calls}
        GROUP BY 1
        ORDER BY 1
    """)

    fig = plt.figure()
    plt.plot(df_month["month"], df_month["calls"])
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    st.pyplot(fig)

# ============================================================
# TAB: TIME
# ============================================================
with tab_time:
    st.subheader("Distribuzione response time (campione)")

    df_hist = q(f"""
        SELECT response_time_sec
        FROM {BASE_CALLS}
        {where_calls}
        AND response_time_sec IS NOT NULL
        USING SAMPLE 20000
    """)

    fig = plt.figure()
    plt.hist(df_hist["response_time_sec"], bins=60)
    plt.tight_layout()
    st.pyplot(fig)

    st.subheader("Response time medio per mese")
    df_rt_month = q(f"""
        SELECT
          date_trunc('month', call_date) AS month,
          AVG(response_time_sec) AS avg_resp
        FROM {BASE_CALLS}
        {where_calls}
        AND response_time_sec IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """)

    fig2 = plt.figure()
    plt.plot(df_rt_month["month"], df_rt_month["avg_resp"])
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    st.pyplot(fig2)

# ============================================================
# TAB: GEOGRAPHY
# ============================================================
with tab_geo:
    st.subheader("Top Call Type (15)")
    df_top = q(f"""
        SELECT call_type, COUNT(*) AS n
        FROM {BASE_CALLS}
        {where_calls}
        AND call_type IS NOT NULL
        GROUP BY 1
        ORDER BY n DESC
        LIMIT 15
    """)

    fig = plt.figure()
    plt.barh(df_top["call_type"][::-1], df_top["n"][::-1])
    plt.tight_layout()
    st.pyplot(fig)

    st.subheader("Quartieri con response time medio più alto (Top 15)")
    df_neigh = q(f"""
        SELECT
          COALESCE(neighborhoods_analysis_boundaries, 'Unknown') AS neighborhood,
          AVG(response_time_sec) AS avg_resp
        FROM {BASE_CALLS}
        {where_calls}
        GROUP BY 1
        HAVING avg_resp IS NOT NULL
        ORDER BY avg_resp DESC
        LIMIT 15
    """)

    fig2 = plt.figure()
    plt.barh(df_neigh["neighborhood"][::-1], df_neigh["avg_resp"][::-1])
    plt.tight_layout()
    st.pyplot(fig2)

# ============================================================
# TAB: SEVERITY (INCIDENTS) con filtri CALLS
# ============================================================
with tab_sev:
    st.subheader("Severity (Incidents) — metriche di danno e vittime")

    # Subquery: incidenti distinti dalle CALLS con gli stessi filtri
    sev = q(f"""
        WITH calls_filtered AS (
          SELECT DISTINCT
            incident_number
          FROM {BASE_CALLS}
          {where_calls}
          AND incident_number IS NOT NULL
        )
        SELECT
          COUNT(*) AS total_incidents,
          SUM(COALESCE(i.estimated_property_loss, 0)) AS prop_loss,
          SUM(COALESCE(i.estimated_contents_loss, 0)) AS cont_loss,
          SUM(COALESCE(i.fire_injuries, 0)) AS fire_injuries,
          SUM(COALESCE(i.fire_fatalities, 0)) AS fire_fatalities,
          SUM(COALESCE(i.civilian_injuries, 0)) AS civ_injuries,
          SUM(COALESCE(i.civilian_fatalities, 0)) AS civ_fatalities
        FROM {BASE_INC} i
        INNER JOIN calls_filtered c
          ON i.incident_number = c.incident_number
        WHERE i.incident_date BETWEEN '{date_from}' AND '{date_to}'
    """).iloc[0]

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Incidents", f"{int(sev['total_incidents']):,}")
    c2.metric("Property loss", f"{int(sev['prop_loss']):,}")
    c3.metric("Contents loss", f"{int(sev['cont_loss']):,}")
    c4.metric("Fire injuries", f"{int(sev['fire_injuries']):,}")
    c5.metric("Fire fatalities", f"{int(sev['fire_fatalities']):,}")
    c6.metric("Civilian injuries", f"{int(sev['civ_injuries']):,}")
    c7.metric("Civilian fatalities", f"{int(sev['civ_fatalities']):,}")

    st.subheader("Top Primary Situation (15) — con filtri calls")
    df_ps = q(f"""
        WITH calls_filtered AS (
          SELECT DISTINCT
            incident_number
          FROM {BASE_CALLS}
          {where_calls}
          AND incident_number IS NOT NULL
        )
        SELECT
          COALESCE(i.primary_situation, 'Unknown') AS primary_situation,
          COUNT(*) AS n
        FROM {BASE_INC} i
        INNER JOIN calls_filtered c
          ON i.incident_number = c.incident_number
        WHERE i.incident_date BETWEEN '{date_from}' AND '{date_to}'
        GROUP BY 1
        ORDER BY n DESC
        LIMIT 15
    """)

    fig = plt.figure()
    plt.barh(df_ps["primary_situation"][::-1], df_ps["n"][::-1])
    plt.tight_layout()
    st.pyplot(fig)

    st.subheader("Loss nel tempo (mensile) — con filtri calls")
    df_loss_month = q(f"""
        WITH calls_filtered AS (
          SELECT DISTINCT
            incident_number
          FROM {BASE_CALLS}
          {where_calls}
          AND incident_number IS NOT NULL
        )
        SELECT
          date_trunc('month', i.incident_date) AS month,
          SUM(COALESCE(i.estimated_property_loss, 0) + COALESCE(i.estimated_contents_loss, 0)) AS total_loss
        FROM {BASE_INC} i
        INNER JOIN calls_filtered c
          ON i.incident_number = c.incident_number
        WHERE i.incident_date BETWEEN '{date_from}' AND '{date_to}'
        GROUP BY 1
        ORDER BY 1
    """)

    fig2 = plt.figure()
    plt.plot(df_loss_month["month"], df_loss_month["total_loss"])
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    st.pyplot(fig2)

