import duckdb

con = duckdb.connect("warehouse.duckdb")

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

con.close()
