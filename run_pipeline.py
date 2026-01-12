from prefect import flow
import polars as pl

from etl.tasks.bronze import ingest_bronze
from etl.tasks.silver import clean_silver


CALLS_CSV = "data/raw/Fire_Department_Calls_for_Service.csv"
INCIDENTS_CSV = "data/raw/Fire_Incidents.csv"

@flow(name="San Francisco Fire Dept Pipeline", log_prints=True)
def main_flow():
    print("🚦 Avvio della Pipeline Dati...")

    # Overrides utili per colonne “miste” (alfanumeriche)
    calls_overrides = {
        "Box": pl.Utf8,
        "Battalion": pl.Utf8,
        "Station Area": pl.Utf8,
        "Supervisor District": pl.Utf8,
        "Fire Prevention District": pl.Utf8,
    }

    # 1) Bronze - Calls
    ingest_bronze(
        source_path=CALLS_CSV,
        table_name="calls",
        schema_overrides=calls_overrides
    )

    # 2) Bronze - Incidents 
    ingest_bronze(
        source_path=INCIDENTS_CSV,
        table_name="incidents",
        schema_overrides=None
    )

    # 3) Silver - Cleaning
    clean_silver()
    print("Pipeline completata con successo!")

if __name__ == "__main__":
    main_flow()
