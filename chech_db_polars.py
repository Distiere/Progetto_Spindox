import duckdb
import polars as pl

DB_PATH = "data/warehouse.duckdb"


def pl_q(con: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    """
    Esegue una query su DuckDB e ritorna un DataFrame Polars (via Arrow).
    """
    return pl.from_arrow(con.execute(sql).arrow())


def main():
    con = duckdb.connect(DB_PATH, read_only=True)

    # --- SCHEMAS ---
    print("SCHEMAS:")
    schemas = pl_q(con, "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;")
    print(schemas)

    # --- TABLES IN BRONZE ---
    print("\nTABLES IN BRONZE:")
    tables_bronze = pl_q(
        con,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze' ORDER BY table_name;",
    )
    print(tables_bronze)

    # --- ROW COUNTS ---
    print("\nROW COUNTS:")
    counts = pl_q(
        con,
        """
        SELECT 'bronze.calls' AS tbl, COUNT(*) AS n FROM bronze.calls
        UNION ALL
        SELECT 'bronze.incidents' AS tbl, COUNT(*) AS n FROM bronze.incidents
        UNION ALL
        SELECT 'silver.calls_clean' AS tbl, COUNT(*) AS n FROM silver.calls_clean
        UNION ALL
        SELECT 'silver.incidents_clean' AS tbl, COUNT(*) AS n FROM silver.incidents_clean
        ORDER BY tbl
        """,
    )
    print(counts)

    # null response_time_sec
    null_resp = pl_q(
        con,
        "SELECT COUNT(*) AS null_response_time_sec FROM silver.calls_clean WHERE response_time_sec IS NULL;",
    )
    print("\nnull response_time_sec:")
    print(null_resp)

    # -- 1) Null timestamps
    null_timestamps = pl_q(
        con,
        """
        SELECT
            SUM(CASE WHEN received_ts IS NULL THEN 1 ELSE 0 END) AS null_received,
            SUM(CASE WHEN on_scene_ts IS NULL THEN 1 ELSE 0 END) AS null_on_scene,
            SUM(CASE WHEN dispatch_ts IS NULL THEN 1 ELSE 0 END) AS null_dispatch
        FROM silver.calls_clean;
        """,
    )
    print("\nNull Timestamps:")
    print(null_timestamps)

    # -- 2) Negative response_time_sec
    neg_response = pl_q(
        con,
        """
        SELECT COUNT(*) AS negative_resp
        FROM silver.calls_clean
        WHERE response_time_sec < 0;
        """,
    )
    print("\nNegative response_time_sec:")
    print(neg_response)

    # --- GOLD checks ---
    gold_dim_date = pl_q(con, "SELECT MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS n FROM gold.dim_date;")
    print("\nGold dim_date data:")
    print(gold_dim_date)

    gold_incident_type_count = pl_q(con, "SELECT COUNT(*) AS n FROM gold.dim_incident_type;")
    gold_incident_type_sample = pl_q(con, "SELECT * FROM gold.dim_incident_type LIMIT 10;")
    print("\nGold dim_incident_type count:")
    print(gold_incident_type_count)
    print("\nGold dim_incident_type sample (10):")
    print(gold_incident_type_sample)

    gold_location_count = pl_q(con, "SELECT COUNT(*) AS n FROM gold.dim_location;")
    gold_location_sample = pl_q(con, "SELECT * FROM gold.dim_location LIMIT 10;")
    print("\nGold dim_location count:")
    print(gold_location_count)
    print("\nGold dim_location sample (10):")
    print(gold_location_sample)


    print(pl_q(con, "SELECT COUNT(*) AS n FROM gold.fact_incident;"))
    print(pl_q(con, "SELECT COUNT(*) AS null_loc FROM gold.fact_incident WHERE location_id IS NULL;"))
    print(pl_q(con, "SELECT COUNT(*) AS null_type FROM gold.fact_incident WHERE incident_type_id IS NULL;"))
    print(pl_q(con, "SELECT COUNT(*) AS null_date FROM gold.fact_incident WHERE date_id IS NULL;"))
    print(pl_q(con, "SELECT MIN(response_time_sec) AS min_rt, MAX(response_time_sec) AS max_rt FROM gold.fact_incident;"))


    con.close()


if __name__ == "__main__":
    main()
