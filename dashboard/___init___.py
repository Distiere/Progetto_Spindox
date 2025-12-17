import polars as pl

calls = pl.read_csv(
    "data/raw/Fire_Department_Calls_for_Service.csv",
    infer_schema_length=20000,
    null_values=["None", ""],
    ignore_errors=True
)
incidents = pl.read_csv(
    "data/raw/Fire_Incidents.csv",
    infer_schema_length=20000,
    null_values=["None", ""],
    ignore_errors=True
)

print(calls.shape, incidents.shape)
print(calls.columns)
print(incidents.columns)

