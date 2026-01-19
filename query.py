import duckdb
import pandas as pd
con = duckdb.connect("data/warehouse.duckdb", read_only=True)

# bisogno di calcolare il numero di chiamate totali, la data della prima e dell'ultima chiamata
query = con.execute("""SELECT MIN(received_ts) AS min_ts, MAX(received_ts) AS max_ts, COUNT(*) AS n
FROM silver.calls_clean;
""")

query_2 = con.execute("""WITH daily AS (
  SELECT
    CAST(received_ts AS DATE) AS day,
    EXTRACT('dow' FROM received_ts) AS dow,
    COUNT(*) AS n_calls
  FROM silver.calls_clean
  WHERE received_ts >= (SELECT MAX(received_ts) - INTERVAL '60 days' FROM silver.calls_clean)
  GROUP BY 1,2
)
SELECT
  dow,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n_calls) AS median_calls
FROM daily
GROUP BY 1
ORDER BY 1;

""")

query_3 = con.execute("""SELECT
  EXTRACT('hour' FROM received_ts) AS hour,
  COUNT(*) AS n
FROM silver.calls_clean
WHERE received_ts >= (SELECT MAX(received_ts) - INTERVAL '60 days' FROM silver.calls_clean)
GROUP BY 1
ORDER BY 1;
""")
print(query.df())
print(query_2.df())
print(query_3.df())
con.close()
