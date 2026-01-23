import os
from pathlib import Path
from datetime import datetime, timezone

import polars as pl
from prefect import task, get_run_logger

from utils import get_db_connection, Schemas, sanitize_columns

# Root del Data Lake (open-source file-based + parquet colonnare)
LAKE_ROOT_DIR = os.getenv("LAKE_ROOT_DIR", "data/Data_Lake")


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _today_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _read_header_sanitized(csv_path: str) -> list[str]:
    # leggiamo solo header (zero righe) per non caricare file intero
    df0 = pl.read_csv(csv_path, n_rows=0)
    return sanitize_columns(df0.columns)


def _detect_dataset_type(cols: list[str], file_path: str = "") -> str:
    cols_set = set(cols)

    # PRIORITÀ: incidents (i merged possono avere anche call_number)
    if "incident_number" in cols_set:
        return "fire_incidents"

    if "call_number" in cols_set:
        return "fire_calls"

    # fallback sul nome file
    name = Path(file_path).name.lower()
    if "incident" in name:
        return "fire_incidents"
    if "call" in name:
        return "fire_calls"

    return "fire_incidents"



def _load_csv_polars(csv_path: str) -> pl.DataFrame:
    # 0) leggo solo header per conoscere i nomi colonna originali
    df0 = pl.read_csv(csv_path, n_rows=0)
    old_cols = df0.columns

    # 1) forzo tutte le colonne a Utf8 già in parsing (zero inferenza numerica)
    schema_overrides = {c: pl.Utf8 for c in old_cols}

    # 2) scan lazy (come in bronze.py)
    lazy_df = pl.scan_csv(
        csv_path,
        infer_schema_length=10000,
        schema_overrides=schema_overrides,
        null_values=["None", "NULL", ""],
        ignore_errors=True,
        truncate_ragged_lines=True,
    )

    # 3) sanitize colonne (stesso identico pattern del bronze.py)
    new_cols = sanitize_columns(old_cols)
    rename_map = dict(zip(old_cols, new_cols))
    lazy_df = lazy_df.rename(rename_map)

    # 4) materializza
    df = lazy_df.collect(streaming=True)

    # 5) cast “di sicurezza” (tutto string)
    df = df.with_columns([pl.all().cast(pl.Utf8)])
    return df



def _lake_target_path(dataset: str, ingest_date: str, file_sha256: str) -> Path:
    # Partizione per ingest_date + cartella per contenuto (sha)
    # data/lake/fire_calls/ingest_date=YYYY-MM-DD/sha256=<sha>/data.parquet
    return Path(LAKE_ROOT_DIR) / dataset / f"ingest_date={ingest_date}" / f"sha256={file_sha256}" / "data.parquet"


@task(name="Phase2 - Write PENDING to Data Lake (Parquet)", retries=0)
def write_pending_to_lake(run_id: str, pipeline_name: str = "phase2_incremental") -> dict:
    """
    Data Lake:
    - Legge i file PENDING (CSV) rilevati in meta.ingestion_log
    - Scrive Parquet colonnare in data/lake/<dataset>/ingest_date=YYYY-MM-DD/sha256=<sha>/
    - Aggiorna meta.ingestion_log con lake_path e lake_written_at

    Non cambia status (resta PENDING) per permettere debug/test end-to-end.
    """
    logger = get_run_logger()
    ingest_date = _today_utc_str()

    # prendo i file PENDING + sha
    with get_db_connection() as con:
        rows = con.execute(
            f"""
            SELECT file_path, file_sha256
            FROM {Schemas.META}.ingestion_log
            WHERE run_id = ? AND pipeline_name = ? AND status = 'PENDING'
            """,
            [run_id, pipeline_name],
        ).fetchall()

    if not rows:
        logger.info("Nessun file PENDING da scrivere nel Data Lake.")
        return {"written": 0, "calls": 0, "incidents": 0}

    written = 0
    calls_n = 0
    incidents_n = 0

    for file_path, file_sha256 in rows:
        csv_path = str(file_path)

        # detect dataset senza fidarsi del nome file
        cols = _read_header_sanitized(csv_path)
        dataset = _detect_dataset_type(cols, csv_path)

        # leggi tutto e scrivi parquet
        df = _load_csv_polars(csv_path)

        # metadati utili nel lake (non rompe nulla e aiuta audit)
        df = df.with_row_index(name="_source_row_number")
        df = df.with_columns([
            pl.lit(file_sha256).cast(pl.Utf8).alias("_source_sha256"),
            pl.lit(csv_path).cast(pl.Utf8).alias("_source_file_path"),
            pl.lit(_utcnow_naive()).cast(pl.Utf8).alias("_lake_written_at_utc"),
        ])
        df = df.with_columns([pl.col("_source_row_number").cast(pl.Utf8)])

        target = _lake_target_path(dataset, ingest_date, file_sha256)
        target.parent.mkdir(parents=True, exist_ok=True)

        # idempotenza lake-write: se già esiste quel parquet per quello sha, riscrivere è ok.
        # Se vuoi “hard idempotent”, puoi skipparlo se target.exists().
        logger.info(f"Writing parquet to Data Lake: {target}")
        df.write_parquet(str(target))

        # aggiorna meta.ingestion_log (assumiamo che lake_path e lake_written_at siano nel META_DDL)
        with get_db_connection() as con:
            con.execute(
                f"""
                UPDATE {Schemas.META}.ingestion_log
                SET lake_path = ?, lake_written_at = ?
                WHERE run_id = ? AND pipeline_name = ? AND file_sha256 = ?
                """,
                [str(target), _utcnow_naive(), run_id, pipeline_name, file_sha256],
            )

        written += 1
        if dataset == "fire_calls":
            calls_n += 1
        else:
            incidents_n += 1

    logger.info(f"Lake write done | written={written} calls={calls_n} incidents={incidents_n}")
    return {"written": written, "calls": calls_n, "incidents": incidents_n}
