from prefect import flow
from etl.tasks.bronze import ingest_bronze
#from etl.tasks.silver import clean_silver
#from etl.tasks.gold import create_gold_layer

PERCORSO_CSV = "data/raw/Fire_Department_Calls_for_Service.csv"

@flow(name="San Francisco Fire Dept Pipeline", log_prints=True)
def main_flow():
    """
    Questo è il FLUSSO Principale (Orchestratore).
    Decide l'ordine di esecuzione dei task.
    """
    print("🚦 Avvio della Pipeline Dati...")
    
    # 1. Esegui Bronze
    
    ingest_bronze(source_path=PERCORSO_CSV)
    
    # 2. Esegui Silver
    # Parte solo se il Bronze ha finito con successo
    #clean_silver()
    
    # 3. Esegui Gold
    # Parte solo se il Silver ha finito con successo
    #create_gold_layer()
    
    print(" Pipeline completata con successo!")

if __name__ == "__main__":
    # Avvia il flusso
    main_flow()