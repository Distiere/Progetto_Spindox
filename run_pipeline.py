from prefect import flow
import polars as pl

from etl.tasks.bronze import ingest_bronze
from etl.tasks.silver import clean_silver
from etl.test.gate import validate_silver_quality
from etl.tasks.gold import build_dim_date, build_dim_incident_type, build_dim_location, build_fact_incident
from etl.test.gate_gold import validate_gold_quality


CALLS_CSV = "data/raw/Fire_Department_Calls_for_Service.csv"
INCIDENTS_CSV = "data/raw/Fire_Incidents.csv"

@flow(name="San Francisco Fire Dept Pipeline", log_prints=True)
def main_flow():
    print("Avvio della Pipeline Dati")

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

    # 4) Quality Gate su Silver
    validate_silver_quality(sample_n=200_000)
    print("Pipeline completata con successo.")  

    # 5) GOLD - dims
    build_dim_date()
    build_dim_incident_type()
    build_dim_location()
    
    # 6) GOLD - fact
    build_fact_incident()
    
    # 7) GOLD - quality gate
    validate_gold_quality()
 

if __name__ == "__main__":
    main_flow()
