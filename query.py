import duckdb

con = duckdb.connect("data/warehouse.duckdb", read_only=True)

sql = r"""SELECT * FROM gold.v_kpi_response_time_month LIMIT 5;"""

print(con.execute(sql).fetchdf())
con.close()
