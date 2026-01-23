import polars as pl

calls_path = "data\Client_drop\Fire_Department_Calls_for_Service_merged.csv"

lf = pl.scan_csv(
    calls_path,
    infer_schema=False,
)

cols = lf.collect_schema().names()

print(f"Numero colonne: {len(cols)}\n")
for c in cols:
    print(c)

with open("columns_calls_Calls_merged.txt", "w") as f:
    for c in lf.columns:
        f.write(c + "\n")

calls_path_2 = "data\Client_drop\Fire_Incidents_merged.csv"

lf_2 = pl.scan_csv(
    calls_path_2,
    infer_schema=False,
)

cols_2 = lf_2.collect_schema().names()

print(f"Numero colonne: {len(cols_2)}\n")
for c in cols_2:
    print(c)

with open("columns_calls_Incidents_merged.txt", "w") as f:
    for c in lf_2.columns:
        f.write(c + "\n")

