# dashboard/streamlit_cloud.py
import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="SF Fire Dept — KPI Dashboard (Serving)",
    layout="wide",
)

# -----------------------------
# PATHS (Streamlit Cloud friendly)
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SERVING = PROJECT_ROOT / "dashboard_exports" / "dashboard.duckdb"

# Allow override from Streamlit secrets/env
ENV_DB = os.getenv("DASHBOARD_DB_PATH")
DB_PATH = Path(ENV_DB) if ENV_DB else DEFAULT_SERVING
DB_PATH = DB_PATH.resolve()


# -----------------------------
# HELPERS
# -----------------------------
def _streamlit_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit Cloud può crashare con Arrow LargeUtf8 (large_string).
    Forziamo tutte le colonne testuali a stringhe Python (dtype object).
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    for c in out.columns:
        s = out[c]
        if pd.api.types.is_string_dtype(s) or s.dtype == "object":
            # converte in stringhe python (evita LargeUtf8 in Arrow)
            out[c] = s.map(lambda x: None if pd.isna(x) else str(x))
    return out


def _find_schema_for(con: duckdb.DuckDBPyConnection, name: str) -> str | None:
    """
    Trova lo schema (es: 'main' o 'gold') dove esiste una table/view con quel nome.
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
    schemas = [r[0] for r in rows if r and r[0]]
    # preferisci gold se esiste, altrimenti main/altro
    if "gold" in schemas:
        return "gold"
    return schemas[0]


def _require_db_and_schema() -> tuple[duckdb.DuckDBPyConnection, str]:
    if not DB_PATH.exists():
        st.error(
            f"Non trovo il DB serving.\n"
            f"Percorso atteso: {DB_PATH}\n\n"
            f"Soluzione: genera dashboard_exports/dashboard.duckdb con la pipeline (task export) "
            f"oppure imposta DASHBOARD_DB_PATH."
        )
        st.stop()

    con = duckdb.connect(str(DB_PATH), read_only=True)
    schema_kpi = _find_schema_for(con, "v_kpi_incident_volume_month")
    if not schema_kpi:
        con.close()
        st.error(
            "DB non riconosciuto: non trovo v_kpi_*.\n"
            "Soluzione: rigenera dashboard_exports/dashboard.duckdb (export) e riprova."
        )
        st.stop()
    return con, schema_kpi


@st.cache_data(ttl=60)
def read_df(sql: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


# -----------------------------
# UI HEADER
# -----------------------------
st.title("San Francisco Fire Dept — KPI Dashboard (Serving)")
st.caption(f"DB in uso: {DB_PATH}")

# -----------------------------
# LOAD KPI + DIMS
# -----------------------------
con, DATA_SCHEMA = _require_db_and_schema()
con.close()

# KPI
vol = read_df(
    f"""
    SELECT
      year,
      month,
      incident_count
    FROM {DATA_SCHEMA}.v_kpi_incident_volume_month
    ORDER BY year, month
    """
)

rt = read_df(
    f"""
    SELECT
      year,
      month,
      avg_response_time_sec
    FROM {DATA_SCHEMA}.v_kpi_response_time_month
    ORDER BY year, month
    """
)

# Qui facciamo CAST forte a VARCHAR per evitare LargeUtf8
top = read_df(
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
)

# metadata (se presente)
con2 = duckdb.connect(str(DB_PATH), read_only=True)
try:
    meta_schema = _find_schema_for(con2, "dashboard_metadata")
finally:
    con2.close()

meta = read_df(f"SELECT * FROM {meta_schema}.dashboard_metadata") if meta_schema else None

# dimensioni (se presenti nel serving db)
con3 = duckdb.connect(str(DB_PATH), read_only=True)
try:
    dim_date_schema = _find_schema_for(con3, "dim_date")
    dim_it_schema = _find_schema_for(con3, "dim_incident_type")
    dim_loc_schema = _find_schema_for(con3, "dim_location")
finally:
    con3.close()

dim_date = read_df(f"SELECT * FROM {dim_date_schema}.dim_date") if dim_date_schema else None
dim_it = read_df(f"SELECT * FROM {dim_it_schema}.dim_incident_type") if dim_it_schema else None
dim_loc = read_df(f"SELECT * FROM {dim_loc_schema}.dim_location") if dim_loc_schema else None

# Safe conversion (anti LargeUtf8)
vol = _streamlit_safe_df(vol)
rt = _streamlit_safe_df(rt)
top = _streamlit_safe_df(top)
meta = _streamlit_safe_df(meta) if meta is not None else None
dim_date = _streamlit_safe_df(dim_date) if dim_date is not None else None
dim_it = _streamlit_safe_df(dim_it) if dim_it is not None else None
dim_loc = _streamlit_safe_df(dim_loc) if dim_loc is not None else None


# -----------------------------
# RENDER
# -----------------------------
if meta is not None and not meta.empty and "exported_at_utc" in meta.columns:
    st.caption(f"Exported at (UTC): {meta.loc[0, 'exported_at_utc']}")

c1, c2 = st.columns(2)

with c1:
    st.subheader("📈 Incident volume (monthly)")
    st.dataframe(vol, use_container_width=True)

with c2:
    st.subheader("⏱️ Avg response time (monthly)")
    st.dataframe(rt, use_container_width=True)

st.subheader("🏷️ Top incident types")
st.dataframe(top, use_container_width=True)

with st.expander("🔎 Dimensioni (opzionale)"):
    if dim_date is not None:
        st.write("dim_date (preview)")
        st.dataframe(dim_date.head(200), use_container_width=True)
    if dim_it is not None:
        st.write("dim_incident_type (preview)")
        st.dataframe(dim_it.head(200), use_container_width=True)
    if dim_loc is not None:
        st.write("dim_location (preview)")
        st.dataframe(dim_loc.head(200), use_container_width=True)

st.caption("Modalità SERVING: usa solo KPI aggregati + dimensioni esportate (cloud-friendly).")
