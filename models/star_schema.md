# Star Schema – San Francisco Fire Department Calls for Service

## 1. Domande di business

Questo modello dati è progettato per rispondere a un insieme di domande di business basate sulle chiamate e sugli interventi del dipartimento dei Vigili del Fuoco di San Francisco.

### Q1 – Tempo di risposta
**Qual è il tempo medio di risposta tra la chiamata e l'arrivo dei vigili sul luogo dell'incidente?**  
- Vogliamo misurare quanto tempo passa da quando la chiamata viene ricevuta a quando la prima unità arriva “On Scene”.

### Q2 – Volume di incidenti nel tempo
**Quanti incidenti avvengono ogni anno? Esiste una stagionalità (mesi più critici)?**  
- Vogliamo vedere se esistono pattern temporali (per anno, mese, giorno della settimana, fascia oraria).

### Q3 – Tipologia di incidenti
**Esistono tipi di incidenti che ricorrono maggiormente? Quali? È possibile identificarli per eventuale prevenzione?**  
- Analisi per `Call Type`, `Call Type Group` e `Primary Situation`.

### Q4 – Performance per area geografica
**Ci sono quartieri o zone della città con tempi di risposta medi più alti o con più incidenti?**  
- Analisi per `Neighborhood`, `Battalion`, `Station Area`.

### Q5 – Gravità/complessità degli incidenti
**Come varia il volume e/o il carico di lavoro in base al numero di allarmi e al personale coinvolto?**  
- Analisi per `Number of Alarms`, `SuppressionPersonnel`, `EMSPersonnel`, ecc.


---

## 2. KPI principali

Dal modello deriviamo le seguenti KPI numeriche principali:

1. **Avg Response Time**  
   - Definizione: tempo medio tra `Received DtTm` e `On Scene DtTm` / `ArrivalDtTm`.
   - KPI globale e per: anno, mese, quartiere, tipo di incidente.

2. **Incident Count**  
   - Numero totale di incidenti in un periodo (giorno/mese/anno).

3. **Incident Count by Month / Year**  
   - Conteggio incidenti per mese e anno → base per analisi di stagionalità.

4. **Incident Count by Incident Type**  
   - Numero di incidenti per `Call Type` / `Call Type Group` / `PrimarySituation`.

5. **Avg Number of Alarms / Incident**  
   - Media del `Number of Alarms` per incidente, opzionalmente filtrato per tipo o area.

6. *(Opzionale)* **Avg Dispatch Delay / Travel Time**  
   - Dispatch Delay: `Dispatch DtTm – Received DtTm`  
   - Travel Time: `On Scene/ArrivalDtTm – Dispatch DtTm`  


---

## 3. Grain (granularità) della fact table

**Grain della fact principale:**

> Una riga di `fact_incident` rappresenta una singola chiamata/incident gestito dal dipartimento dei Vigili del Fuoco (identificato da `IncidentNumber` / `CallNumber`).

- Ogni riga contiene:
  - identificativi dell’incidente
  - gli orari principali (ricezione, dispatch, arrivo, chiusura)
  - le misure derivate (tempi, risorse, allarmi)
  - chiavi esterne verso dimensioni (tempo, luogo, tipo incidente)

---

## 4. Tabelle del modello Star

### 4.1 `fact_incident`

**Descrizione:**  
Tabella centrale che contiene un record per ogni incidente/chiamata gestita.

**Chiave primaria candidata:**
- `incident_id` (surrogate key, generata)  
- chiavi naturali utili: `IncidentNumber`, `CallNumber`

**Colonne (proposta):**

- **Chiavi / identificativi**
  - `incident_id` *(PK, surrogate)*  
  - `incident_number` ← `IncidentNumber`  
  - `call_number` ← `Call Number` / `CallNumber`  
  - `date_id` *(FK → dim_date, derivata da `IncidentDate` oppure `Call Date`)*  
  - `location_id` *(FK → dim_location)*  
  - `incident_type_id` *(FK → dim_incident_type)*  

- **Timestamp grezzi (per derivare le misure)**
  - `received_dttm` ← `Received DtTm`  
  - `dispatch_dttm` ← `Dispatch DtTm`  
  - `response_dttm` ← `Response DtTm`  
  - `on_scene_dttm` / `arrival_dttm` ← `On Scene DtTm` / `ArrivalDtTm`  
  - `close_dttm` ← `CloseDtTm`  
  - *(opzionale)* `alarm_dttm` ← `AlarmDtTm`  

- **Misure derivate (numeriche)**
  - `response_time` = `on_scene_dttm – received_dttm`  
  - `dispatch_delay` = `dispatch_dttm – received_dttm`  
  - `travel_time` = `on_scene_dttm – dispatch_dttm`  
  - `incident_duration` = `close_dttm – received_dttm`  

- **Misure di gravità / risorse**
  - `number_of_alarms` ← `Number of Alarms` / `NumberofAlarms`  
  - `suppression_units` ← `SuppressionUnits`  
  - `suppression_personnel` ← `SuppressionPersonnel`  
  - `ems_units` ← `EMSUnits`  
  - `ems_personnel` ← `EMSPersonnel`  
  - `other_units` ← `OtherUnits`  
  - `other_personnel` ← `OtherPersonnel`  
  - `estimated_property_loss` ← `EstimatedPropertyLoss`  
  - `estimated_contents_loss` ← `EstimatedContentsLoss`  

- **Altri attributi direttamente utili**
  - `als_unit_flag` ← `ALS Unit` (booleano)  
  - `call_final_disposition` ← `Call Final Disposition`  

---

### 4.2 `dim_date`

**Descrizione:**  
Tabella dimensionale per l’analisi temporale.

**Grain:**
> una riga per ogni giorno (si può scendere a livello di data+ora se necessario).

**Colonne:**

- `date_id` *(PK)*  
- `date` (data completa)  
- `year`  
- `month`  
- `day`  
- `weekday` (nome o numero)  
- `week_of_year`  
- `is_weekend` (boolean)  

> Nota: da `dim_date` si può collegare la fact usando `date_id` derivato da `IncidentDate` o `Call Date`. Per semplificare il modello, manteniano una sola relazione principale (es. data incidente); i timestamp completi restano nella fact.

---

### 4.3 `dim_incident_type`

**Descrizione:**  
Categoria dell’incidente, utile per analisi di frequenza e prevenzione.

**Colonne:**

- `incident_type_id` *(PK, surrogate)*  
- `call_type` ← `Call Type`  
- `call_type_group` ← `Call Type Group`  
- `primary_situation` ← `PrimarySituation`  
- *(opzionale)* `final_priority` ← `Final Priority`  

> Questa dimensione permette grafici e KPI come:  
> - incidenti per tipo  
> - tempi medi di risposta per tipo  
> - incidenza di specifiche situazioni primarie.

---

### 4.4 `dim_location`

**Descrizione:**  
Localizzazione geografica / amministrativa dell’incidente.

**Colonne:**

- `location_id` *(PK, surrogate)*  
- `address` ← `Address`  
- `city` ← `City`  
- `zipcode` ← `Zipcode of Incident` / `Zipcode`  
- `neighborhood` ← `NeighborhoodDistrict` / `Neighborhooods - Analysis Boundaries`  
- `battalion` ← `Battalion`  
- `station_area` ← `Station Area` / `StationArea`  
- `supervisor_district` ← `Supervisor District`  
- `fire_prevention_district` ← `Fire Prevention District`  
- `box` ← `Box`  
- `location_point` ← `Location` (coordinate come stringa/testo)  

> Questa dimensione supporta analisi di distribuzione incidenti / tempi di risposta per quartiere, battaglione, stazione, ecc.

---


## 5. Mappatura Domande → KPI → Tabelle

### Q1 – Tempo medio di risposta
- **Tabelle:** `fact_incident`, `dim_incident_type`, `dim_location`, `dim_date`  
- **KPI:** `avg(response_time)`  
- **Possibili grafici:**
  - tempo medio di risposta per tipo di incidente  
  - tempo medio di risposta per quartiere  

---

### Q2 – Incidenti per anno/mese (stagionalità)
- **Tabelle:** `fact_incident`, `dim_date`  
- **KPI:** `incident_count`, `incident_count_by_month`  
- **Possibili grafici:**
  - line chart incidenti per mese  
  - bar chart incidenti per anno  

---

### Q3 – Tipi di incidenti ricorrenti
- **Tabelle:** `fact_incident`, `dim_incident_type`  
- **KPI:** `incident_count_by_type`  
- **Possibili grafici:**
  - bar chart dei `Call Type` più frequenti  
  - donut/pie per `Call Type Group` (se utile)  

---

### Q4 – Performance per area
- **Tabelle:** `fact_incident`, `dim_location`, `dim_date`  
- **KPI:**  
  - `incident_count_by_neighborhood`  
  - `avg_response_time_by_neighborhood`  
- **Possibili grafici:**
  - bar chart incidenti per quartiere  
  - bar/heatmap tempi medi di risposta per quartiere  

---

### Q5 – Gravità e risorse
- **Tabelle:** `fact_incident`  
- **KPI:**  
  - `avg(number_of_alarms)`  
  - `avg(suppression_personnel)` / `avg(ems_personnel)`  
- **Possibili grafici:**
  - scatter plot response time vs number of alarms  
  - bar chart numero medio di allarmi per tipo di incidente  

---

## 6. Note finali

- Lo schema è **iterativo**: potrà essere raffinato durante l’implementazione dei layer bronze/silver/gold.
- Alcune colonne del dataset originale **non vengono riportate nel gold**, perché:
  - non sono direttamente utili alle analisi
  - aumenterebbero solo la complessità del modello
