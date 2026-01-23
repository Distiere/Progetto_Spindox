import duckdb
import pandas as pd

df = pd.read_csv("data\Client_drop\Fire_Department_Calls_for_Service_merged.csv")
df_2 = pd.read_csv("data\Client_drop\Fire_Incidents_merged.csv")
#voglio avere i primi 20 record di df e df_2
print(df.head(20))
print(df_2.head(20))

con = duckdb.connect("data/warehouse.duckdb", read_only=True)

## bisogno di calcolare il numero di chiamate totali, la data della prima e dell'ultima chiamata
#query = con.execute("""SELECT MIN(received_ts) AS min_ts, MAX(received_ts) AS max_ts, COUNT(*) AS n
#FROM silver.calls_clean;
#""")
#
#query_2 = con.execute("""WITH daily AS (
#  SELECT
#    CAST(received_ts AS DATE) AS day,
#    EXTRACT('dow' FROM received_ts) AS dow,
#    COUNT(*) AS n_calls
#  FROM silver.calls_clean
#  WHERE received_ts >= (SELECT MAX(received_ts) - INTERVAL '60 days' FROM silver.calls_clean)
#  GROUP BY 1,2
#)
#SELECT
#  dow,
#  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n_calls) AS median_calls
#FROM daily
#GROUP BY 1
#ORDER BY 1;
#
#""")
#
#query_3 = con.execute("""SELECT
#  EXTRACT('hour' FROM received_ts) AS hour,
#  COUNT(*) AS n
#FROM silver.calls_clean
#WHERE received_ts >= (SELECT MAX(received_ts) - INTERVAL '60 days' FROM silver.calls_clean)
#GROUP BY 1
#ORDER BY 1;
#""")

#rows = con.execute("PRAGMA table_info('bronze.incidents')").fetchall()
#for r in rows:
#    print(r[1])
#
#print(query.df())
#print(query_2.df())
#print(query_3.df())


#import polars as pl
#from etl.utils import sanitize_columns  # o la funzione nuova
#
#CSV_PATH = "data/raw/Fire_Incidents.csv"
#
#df0 = pl.read_csv(CSV_PATH, n_rows=0)
#print(sanitize_columns(df0.columns))

con.close()
