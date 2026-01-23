from __future__ import annotations

import os
import json
from pathlib import Path
from datetime import datetime

import numpy as np
import polars as pl
import duckdb
from sklearn.ensemble import RandomForestRegressor
import joblib


DB_PATH = Path("data/warehouse.duckdb")
ARTIFACT_DIR = Path("synthetic/artifacts")
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

# Target numerici che useremo per generare record "plausibili"
Y_COLS = [
    "dispatch_delay_sec",
    "travel_time_sec",
    "response_time_sec",
    "number_of_alarms",
    "final_priority",
]

# Colonne categoriche da campionare condizionate al weekday (non è un secondo modello)
CAT_COLS = [
    "call_type_group",
    "call_type",
    "battalion",
    "station_area",
    "unit_type",
    "neighborhoods_analysis_boundaries",
]

def main():
    sample_n = int(os.getenv("TRAIN_SAMPLE_N", "200000"))

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # campionamento: evita di usare 4.8M righe in train
    df = con.execute(f"""
        SELECT
          received_ts,
          {", ".join(Y_COLS)},
          {", ".join(CAT_COLS)}
        FROM silver.calls_clean
        WHERE received_ts IS NOT NULL
        USING SAMPLE {sample_n} ROWS;
    """).df()

    # Convertiamo a polars
    pl_df = pl.from_pandas(df)

    # Drop null su target + received_ts
    pl_df = pl_df.drop_nulls(["received_ts"] + Y_COLS)

    # Feature temporali (Polars)
    pl_df = pl_df.with_columns([
        pl.col("received_ts").dt.hour().cast(pl.Int16).alias("hour"),
        pl.col("received_ts").dt.weekday().cast(pl.Int16).alias("dow"),  # 1=Mon..7=Sun in Polars
        pl.col("received_ts").dt.month().cast(pl.Int16).alias("month"),
    ]).with_columns([
        (pl.col("dow").is_in([6, 7])).cast(pl.Int8).alias("is_weekend")
    ])

    # Normalizziamo dow su scala 0..6 (Mon=0..Sun=6) per semplicità
    pl_df = pl_df.with_columns([(pl.col("dow") - 1).alias("dow")])

    # Pulizia target numerici
    for c in ["dispatch_delay_sec", "travel_time_sec", "response_time_sec", "number_of_alarms"]:
        if c in pl_df.columns:
            pl_df = pl_df.with_columns([
                pl.col(c).cast(pl.Float64, strict=False).fill_null(0).clip(lower_bound=0).alias(c)
            ])

    pl_df = pl_df.with_columns([
        pl.col("final_priority").cast(pl.Float64, strict=False).fill_null(3).round(0).clip(0, 10).alias("final_priority")
    ])

    X = pl_df.select(["hour", "dow", "month", "is_weekend"]).to_numpy()
    Y = pl_df.select(Y_COLS).to_numpy()

    rf = RandomForestRegressor(
        n_estimators=250,
        random_state=33,
        n_jobs=-1,
        min_samples_leaf=3,
    )
    rf.fit(X, Y)

    # Distribuzione ore (globale)
    hour_counts = (
        pl_df.group_by("hour")
        .len()
        .sort("hour")
    )
    hours = hour_counts["hour"].to_list()
    probs = (hour_counts["len"] / hour_counts["len"].sum()).to_list()
    hour_probs = {str(int(h)): float(p) for h, p in zip(hours, probs)}

    # Distribuzioni categoriche condizionate a dow (Mon=0..Sun=6)
    cat_distributions = {}
    for col in CAT_COLS:
        cat_distributions[col] = {}
        for dow in range(7):
            sub = pl_df.filter(pl.col("dow") == dow).select(pl.col(col)).drop_nulls()
            if sub.height == 0:
                continue
            vc = sub.group_by(col).len().sort("len", descending=True)
            values = vc[col].to_list()
            p = (vc["len"] / vc["len"].sum()).to_list()
            cat_distributions[col][str(dow)] = {"values": values, "probs": [float(x) for x in p]}

    # Mediane per weekday (useremo quelle che hai calcolato; le incolliamo qui “hard” per coerenza)
    # DuckDB: 0=Sun..6=Sat (tuo output)
    duckdb_medians = {0:850.0,1:860.0,2:846.0,3:881.0,4:885.5,5:901.0,6:836.0}
    # Convertite a Polars/Python dow (Mon=0..Sun=6)
    medians_pd = {
        0: duckdb_medians[1],  # Mon
        1: duckdb_medians[2],  # Tue
        2: duckdb_medians[3],  # Wed
        3: duckdb_medians[4],  # Thu
        4: duckdb_medians[5],  # Fri
        5: duckdb_medians[6],  # Sat
        6: duckdb_medians[0],  # Sun
    }

    joblib.dump(rf, ARTIFACT_DIR / "calls_rf.joblib")
    meta = {
        "trained_at": datetime.utcnow().isoformat() + "Z",
        "y_cols": Y_COLS,
        "cat_cols": CAT_COLS,
        "hour_probs": hour_probs,
        "median_calls_per_dow": {str(k): float(v) for k, v in medians_pd.items()},
        "cat_distributions_by_dow": cat_distributions,
    }
    (ARTIFACT_DIR / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print("Training completato.")
    print("Artifacts:", ARTIFACT_DIR / "calls_rf.joblib", "and", ARTIFACT_DIR / "meta.json")

if __name__ == "__main__":
    main()
