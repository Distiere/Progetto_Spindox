from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timedelta, date

import numpy as np
import polars as pl
import duckdb
import joblib


# -------------------------
# PERCORSI
# -------------------------
DB_PATH = Path("data/warehouse.duckdb")
ARTIFACT_DIR = Path("synthetic/artifacts")

RAW_DIR = Path("data/raw")
RAW_CALLS = RAW_DIR / "Fire_Department_Calls_for_Service.csv"
RAW_INCS  = RAW_DIR / "Fire_Incidents.csv"

# output sintetici (separati, così posso mostrarli come deliverable)
SYN_CALLS = RAW_DIR / "Fire_Department_Calls_for_Service_synth.csv"
SYN_INCS  = RAW_DIR / "Fire_Incidents_synth.csv"

# output merged (questi sono quelli che potrai usare per run di pipeline senza toccare gli originali)
MERGED_CALLS = RAW_DIR / "Fire_Department_Calls_for_Service_merged.csv"
MERGED_INCS  = RAW_DIR / "Fire_Incidents_merged.csv"

# header raw (identici agli originali: per me è fondamentale per non rompere la pipeline)
COLS_CALLS_TXT = Path("columns_calls_CSV_1.txt")
COLS_INCS_TXT  = Path("columns_calls_CSV_2.txt")

# formato timestamp raw (verificato sul dataset: MM/DD/YYYY HH:MM:SS AM/PM)
RAW_TS_FMT = "%m/%d/%Y %I:%M:%S %p"


def read_header_list(path: Path) -> list[str]:
    # Io uso questi file per assicurarmi che il CSV sintetico abbia esattamente
    # le stesse colonne dei raw originali.
    return [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def weighted_choice(values, probs, size):
    values = np.array(values, dtype=object)
    probs = np.array(probs, dtype=float)
    probs = probs / probs.sum()
    idx = np.random.choice(len(values), size=size, p=probs)
    return values[idx]


def sample_hour(hour_probs: dict, n: int):
    hours = list(map(int, hour_probs.keys()))
    probs = [hour_probs[str(h)] for h in hours]
    return weighted_choice(hours, probs, n).astype(int)


def main():
    # Io fisso il seed così la generazione è riproducibile: se rifaccio girare lo script,
    # ottengo gli stessi dati sintetici (utile per debug e presentazione).
    np.random.seed(42)

    # Carico il modello e le distribuzioni salvate in training.
    rf = joblib.load(ARTIFACT_DIR / "calls_rf.joblib")
    meta = json.loads((ARTIFACT_DIR / "meta.json").read_text(encoding="utf-8"))

    y_cols = meta["y_cols"]
    cat_cols = meta["cat_cols"]
    hour_probs = meta["hour_probs"]
    # dow: Mon=0..Sun=6
    med_calls = {int(k): float(v) for k, v in meta["median_calls_per_dow"].items()}
    cat_dist = meta["cat_distributions_by_dow"]

    calls_header = read_header_list(COLS_CALLS_TXT)
    incs_header = read_header_list(COLS_INCS_TXT)

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Io prendo l'ultima data reale e genero dal giorno successivo fino a oggi.
    max_ts = con.execute("SELECT MAX(received_ts) FROM silver.calls_clean").fetchone()[0]
    if max_ts is None:
        raise RuntimeError("received_ts max è NULL: non posso determinare da dove partire.")
    max_date = pl.Series([max_ts]).cast(pl.Datetime).dt.date().item()

    start_date = max_date + timedelta(days=1)
    end_date = date.today()

    if start_date > end_date:
        print(" Nessuna data da generare (sei già up-to-date).")
        return

    print(f" Genero dati sintetici da {start_date} a {end_date} (incluso).")

    # Io genero ID sequenziali partendo dagli ID massimi già presenti.
    max_call_number = con.execute("SELECT MAX(call_number) FROM silver.calls_clean").fetchone()[0] or 0
    max_inc_number  = con.execute("SELECT MAX(incident_number) FROM silver.calls_clean").fetchone()[0] or 0

    next_call = int(max_call_number) + 1
    next_inc  = int(max_inc_number) + 1

    rows_calls: list[dict] = []

    cur = start_date
    while cur <= end_date:
        # weekday: Mon=0..Sun=6
        dow = int(datetime(cur.year, cur.month, cur.day).weekday())

        # Io uso la mediana per weekday per decidere quante righe genero: è robusta agli outlier.
        n_calls = int(round(med_calls.get(dow, 850)))

        # 1) Creo received_ts realistici campionando l'ora con la distribuzione osservata.
        hours = sample_hour(hour_probs, n_calls)
        minutes = np.random.randint(0, 60, size=n_calls)
        seconds = np.random.randint(0, 60, size=n_calls)

        received_dt = [
            datetime(cur.year, cur.month, cur.day, int(h), int(m), int(s))
            for h, m, s in zip(hours, minutes, seconds)
        ]

        # 2) Feature X (minime, solo tempo): hour, dow, month, is_weekend
        X = np.column_stack([
            hours,
            np.full(n_calls, dow, dtype=int),
            np.full(n_calls, cur.month, dtype=int),
            np.array([1 if dow in (5, 6) else 0] * n_calls, dtype=int),
        ])

        # 3) Il modello predice le metriche numeriche (multi-output regression)
        Y_pred = rf.predict(X)
        Y_pred = np.maximum(Y_pred, 0)  # clip a 0 per evitare durate negative

        y = {c: Y_pred[:, i] for i, c in enumerate(y_cols)}

        dispatch_delay = np.round(y.get("dispatch_delay_sec", np.zeros(n_calls))).astype(int)
        travel_time = np.round(y.get("travel_time_sec", np.zeros(n_calls))).astype(int)
        response_time = np.round(y.get("response_time_sec", np.zeros(n_calls))).astype(int)

        # 4) Ricostruisco timestamp coerenti usando le durate.
        dispatch_dt = [rd + timedelta(seconds=int(dd)) for rd, dd in zip(received_dt, dispatch_delay)]
        on_scene_dt = [ddt + timedelta(seconds=int(tt)) for ddt, tt in zip(dispatch_dt, travel_time)]
        response_dt = [rd + timedelta(seconds=int(rt)) for rd, rt in zip(received_dt, response_time)]

        # available_ts: semplifico con una finestra plausibile dopo response (10-40 min)
        avail_add = np.random.randint(10, 40, size=n_calls)
        available_dt = [r + timedelta(minutes=int(m)) for r, m in zip(response_dt, avail_add)]

        # 5) Campiono categoriche condizionate al weekday (non è un secondo modello)
        cats = {}
        for col in cat_cols:
            spec = cat_dist.get(col, {}).get(str(dow)) or cat_dist.get(col, {}).get("0")
            if spec:
                cats[col] = weighted_choice(spec["values"], spec["probs"], n_calls)
            else:
                cats[col] = np.array([None] * n_calls, dtype=object)

        # 6) Incident number: raggruppo 1–3 calls per incident per creare una relazione coerente.
        group_sizes = np.random.choice([1, 2, 3], size=n_calls, p=[0.55, 0.30, 0.15])
        inc_nums = []
        i = 0
        while i < n_calls:
            size = int(group_sizes[i])
            inc = next_inc
            next_inc += 1
            for _ in range(min(size, n_calls - i)):
                inc_nums.append(inc)
                i += 1
        inc_nums = inc_nums[:n_calls]

        # Call number sequenziale
        call_nums = list(range(next_call, next_call + n_calls))
        next_call += n_calls

        # 7) Creo righe in schema RAW (header identico ai CSV originali)
        for i in range(n_calls):
            row = {h: None for h in calls_header}

            # ID
            row["Call Number"] = call_nums[i]
            row["Incident Number"] = inc_nums[i]

            # Timestamp RAW (stringhe AM/PM)
            row["Received DtTm"] = received_dt[i].strftime(RAW_TS_FMT)
            row["Dispatch DtTm"] = dispatch_dt[i].strftime(RAW_TS_FMT)
            row["Response DtTm"] = response_dt[i].strftime(RAW_TS_FMT)
            row["On Scene DtTm"] = on_scene_dt[i].strftime(RAW_TS_FMT)
            row["Available DtTm"] = available_dt[i].strftime(RAW_TS_FMT)

            # Campi “core”
            row["Call Type"] = cats["call_type"][i]
            row["Call Type Group"] = cats["call_type_group"][i]
            row["Final Priority"] = int(round(y.get("final_priority", np.full(n_calls, 3.0))[i]))
            row["Number of Alarms"] = int(round(y.get("number_of_alarms", np.full(n_calls, 1.0))[i]))

            row["Battalion"] = cats["battalion"][i]
            row["Station Area"] = cats["station_area"][i]
            row["Unit Type"] = cats["unit_type"][i]

            # Nota: questo nome è ESATTAMENTE quello dell’header (tripla 'o')
            row["Neighborhooods - Analysis Boundaries"] = cats["neighborhoods_analysis_boundaries"][i]

            rows_calls.append(row)

        cur += timedelta(days=1)

    # -------------------------
    # SCRITTURA CALLS SYNTH
    # -------------------------
    calls_synth = pl.DataFrame(rows_calls, schema=calls_header)
    calls_synth.write_csv(SYN_CALLS)
    print("Scritto synth calls:", SYN_CALLS.name)

    # -------------------------
    # INCIDENTS SYNTH DERIVATO
    # -------------------------
    # Io genero incidents dal calls synth: così sono coerenti su IncidentNumber/CallNumber e sulle date.
    inc_grp = (
        calls_synth
        .select(["Incident Number", "Call Number", "Received DtTm"])
        .with_columns([
            pl.col("Received DtTm").str.strptime(pl.Datetime, RAW_TS_FMT, strict=False).alias("_recv_dt")
        ])
        .group_by("Incident Number")
        .agg([
            pl.col("Call Number").min().alias("CallNumber"),
            pl.col("_recv_dt").min().alias("_alarm_dt"),
        ])
        .with_columns([
            pl.col("_alarm_dt").dt.strftime(RAW_TS_FMT).alias("AlarmDtTm"),
            pl.col("_alarm_dt").dt.strftime("%m/%d/%Y").alias("IncidentDate"),
            pl.col("Incident Number").alias("IncidentNumber"),
        ])
        .drop(["_alarm_dt"])
    )

    # Creo DF con header incidents completo, poi sovrascrivo solo le colonne che ho.
    inc_base = pl.DataFrame({h: [None] * inc_grp.height for h in incs_header}, schema=incs_header)

    for col in ["IncidentNumber", "CallNumber", "IncidentDate", "AlarmDtTm"]:
        if col in inc_base.columns:
            inc_base = inc_base.with_columns(pl.Series(col, inc_grp[col].to_list()))

    inc_base.write_csv(SYN_INCS)
    print("Scritto synth incidents:", SYN_INCS.name)

    # -------------------------
    # CREO I MERGED (senza toccare originali)
    # -------------------------
    # Io creo due file merged per far vedere chiaramente il contributo del sintetico,
    # senza sovrascrivere i file originali del dataset.
    orig_calls = pl.read_csv(RAW_CALLS, ignore_errors=True)
    merged_calls = pl.concat([orig_calls, calls_synth], how="vertical_relaxed")
    merged_calls.write_csv(MERGED_CALLS)
    print(" Creato merged calls:", MERGED_CALLS.name)

    orig_incs = pl.read_csv(RAW_INCS, ignore_errors=True)
    merged_incs = pl.concat([orig_incs, inc_base], how="vertical_relaxed")
    merged_incs.write_csv(MERGED_INCS)
    print("Creato merged incidents:", MERGED_INCS.name)

    print("\nOutput finale:")
    print(" - synth:", SYN_CALLS.name, "|", SYN_INCS.name)
    print(" - merged:", MERGED_CALLS.name, "|", MERGED_INCS.name)
    print("\n👉 Se vuoi far girare la pipeline sui merged, basta puntare la flow a *_merged.csv (2 path).")


if __name__ == "__main__":
    main()
