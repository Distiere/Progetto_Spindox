from prefect import flow

from tasks.Lake_writer import write_pending_to_lake
from tasks.silver_phase_2 import clean_silver_phase2
from tasks.ingestion_meta import detect_and_log_client_drop
from tasks.bronze_schedule import ingest_bronze_incremental
from test.gate import validate_silver_quality
from tasks.gold import build_dim_date, build_dim_incident_type, build_dim_location, build_fact_incident
from tasks.kpi_gold_view import create_kpi_views
from test.gate_gold import validate_gold_quality


@flow(name="San Francisco Fire Dept Pipeline - PHASE 2", log_prints=True)
def phase2_flow():
    # STEP1: detect + log
    info = detect_and_log_client_drop()
    run_id = info["run_id"]

    # Se non ci sono file PENDING già a monte, no-op completo
    if info.get("pending", 0) == 0:
        print("Nessun nuovo contenuto (PENDING=0): no-op.")
        return info

    # STEP2: lake write (ora già skippa se parquet esiste)
    write_pending_to_lake(run_id=run_id)

    # STEP3: bronze ingest incrementale (idempotente)
    bronze_res = ingest_bronze_incremental(run_id=run_id)

    inserted_calls = int(bronze_res.get("inserted_calls", 0))
    inserted_incidents = int(bronze_res.get("inserted_incidents", 0))
    inserted_total = inserted_calls + inserted_incidents

    print(
        f"Bronze summary | inserted_calls={inserted_calls} "
        f"inserted_incidents={inserted_incidents} inserted_total={inserted_total}"
    )

    # Se non è entrato nulla di nuovo in bronze, non ha senso rifare silver+gold
    if inserted_total == 0:
        print("Nessun nuovo dato inserito in BRONZE: skip SILVER + GOLD (no-op veloce).")
        return {**info, **bronze_res, "skipped_downstream": True}

    # STEP4: silver + gate
    clean_silver_phase2()
    validate_silver_quality()

    # STEP5: gold + views + gate
    build_dim_date()
    build_dim_incident_type()
    build_dim_location()
    build_fact_incident()

    create_kpi_views()
    validate_gold_quality()

    print("PHASE 2 completata con successo.")
    return {**info, **bronze_res, "skipped_downstream": False}


if __name__ == "__main__":
    phase2_flow()
