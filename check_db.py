import duckdb

con = duckdb.connect("data/warehouse.duckdb")

print("SCHEMAS:")
print(
    con.execute(
        "SELECT schema_name FROM information_schema.schemata"
    ).fetchall()
)

print("\nTABLES IN BRONZE:")
print(
    con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'"
    ).fetchall()
)

print("\nROW COUNTS:")
print("calls:", con.execute("SELECT COUNT(*) FROM bronze.calls").fetchone()[0])
print("incidents:", con.execute("SELECT COUNT(*) FROM bronze.incidents").fetchone()[0])

print("silver.calls_clean:", con.execute("SELECT COUNT(*) FROM silver.calls_clean").fetchone()[0])
print("silver.incidents_clean:", con.execute("SELECT COUNT(*) FROM silver.incidents_clean").fetchone()[0])
print("null response_time_sec:",
      con.execute("SELECT COUNT(*) FROM silver.calls_clean WHERE response_time_sec IS NULL").fetchone()[0])


#-- 1) Quanti casi hanno timestamp mancanti?
null_timestamps = con.execute("""
            SELECT
                SUM(CASE WHEN received_ts IS NULL THEN 1 ELSE 0 END) AS null_received,
                SUM(CASE WHEN on_scene_ts IS NULL THEN 1 ELSE 0 END) AS null_on_scene,
                SUM(CASE WHEN dispatch_ts IS NULL THEN 1 ELSE 0 END) AS null_dispatch
            FROM silver.calls_clean;""")
print("Null Timestamps:", null_timestamps.fetchall())

#-- 2) quanti response_time negativi
neg_response = con.execute("""
SELECT COUNT(*) AS negative_resp
FROM silver.calls_clean
WHERE response_time_sec < 0;
""")
print("Negative response_time_sec:", neg_response.fetchall())

check_data_gold = con.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM gold.dim_date;")
print("Gold dim_date data:", check_data_gold.fetchall())


check_dim_incident_type = con.execute("""SELECT COUNT(*) FROM gold.dim_incident_type;
SELECT * FROM gold.dim_incident_type LIMIT 10;
""")
print("Gold dim_incident_type count and sample:", check_dim_incident_type.fetchall())

check_dim_location = con.execute("""SELECT COUNT(*) FROM gold.dim_location;
SELECT * FROM gold.dim_location LIMIT 10;
""")
print("Gold dim_location count and sample:", check_dim_location.fetchall())


check_fact_incidents = con.execute("""SELECT COUNT(*) FROM gold.fact_incident;
SELECT * FROM gold.fact_incident LIMIT 10;
                                   """)
print("Gold fact_incident count and sample:", check_fact_incidents.fetchall())
con.close()
