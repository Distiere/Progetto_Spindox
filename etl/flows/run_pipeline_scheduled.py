from prefect import flow

from tasks.Lake_writer import write_pending_to_lake
from tasks.silver_phase_2 import clean_silver_phase2
from tasks.ingestion_meta import detect_and_log_client_drop
from tasks.bronze_schedule import ingest_bronze_incremental
from tasks.silver import clean_silver
from test.gate import validate_silver_quality
from tasks.gold import build_dim_date, build_dim_incident_type, build_dim_location, build_fact_incident
from tasks.kpi_gold_view import create_kpi_views
from test.gate_gold import validate_gold_quality

@flow(name="San Francisco Fire Dept Pipeline - PHASE 2", log_prints=True)
def phase2_flow():
    # STEP1 deve tornare anche run_id (ti faccio cambiare return)
    info = detect_and_log_client_drop()
    run_id = info["run_id"]

    if info["pending"] == 0:
        print("Nessun nuovo contenuto: no-op.")
        return info

    write_pending_to_lake(run_id=run_id)
    ingest_bronze_incremental(run_id=run_id)
    
    clean_silver_phase2()
    validate_silver_quality()

    build_dim_date()
    build_dim_incident_type()
    build_dim_location()
    build_fact_incident()

    create_kpi_views()
    validate_gold_quality()

    # STEP5: solo se tutto ok
    #mark_run_contents_success(run_id=run_id)

    print("PHASE 2 completata con successo.")

if __name__ == "__main__":
    phase2_flow()
