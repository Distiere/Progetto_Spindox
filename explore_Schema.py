import polars as pl

calls_path = "data/raw/Fire_Department_Calls_for_Service.csv"

lf = pl.scan_csv(
    calls_path,
    infer_schema=False,
)

cols = lf.collect_schema().names()

print(f"Numero colonne: {len(cols)}\n")
for c in cols:
    print(c)

with open("columns_calls_CSV_1.txt", "w") as f:
    for c in lf.columns:
        f.write(c + "\n")

calls_path_2 = "data/raw/Fire_Incidents.csv"

lf_2 = pl.scan_csv(
    calls_path_2,
    infer_schema=False,
)

cols_2 = lf_2.collect_schema().names()

print(f"Numero colonne: {len(cols_2)}\n")
for c in cols_2:
    print(c)

with open("columns_calls_CSV_2.txt", "w") as f:
    for c in lf_2.columns:
        f.write(c + "\n")

