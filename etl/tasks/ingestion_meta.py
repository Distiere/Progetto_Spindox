import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict

from prefect import task, get_run_logger

from etl.utils import get_db_connection, CLIENT_DROP_DIR, Schemas


META_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {Schemas.META};

CREATE TABLE IF NOT EXISTS {Schemas.META}.ingestion_runs (
  run_id            UUID PRIMARY KEY,
  pipeline_name     VARCHAR NOT NULL,
  started_at        TIMESTAMP NOT NULL,
  finished_at       TIMESTAMP,
  status            VARCHAR NOT NULL,   -- RUNNING / SUCCESS / FAILED
  trigger_type      VARCHAR NOT NULL,   -- SCHEDULE / MANUAL
  notes             VARCHAR
);

CREATE TABLE IF NOT EXISTS {Schemas.META}.ingestion_log (
  log_id            UUID PRIMARY KEY,
  run_id            UUID NOT NULL,
  pipeline_name     VARCHAR NOT NULL,

  drop_dir          VARCHAR NOT NULL,
  file_name         VARCHAR NOT NULL,
  file_path         VARCHAR NOT NULL,

  file_size_bytes   BIGINT NOT NULL,
  file_mtime_utc    TIMESTAMP NOT NULL,
  file_sha256       VARCHAR NOT NULL,

  detected_at       TIMESTAMP NOT NULL,

  -- Data Lake integration
  lake_path         VARCHAR,
  lake_written_at   TIMESTAMP,

  status            VARCHAR NOT NULL,   -- PENDING / SKIPPED / DONE / FAILED
  error_message     VARCHAR,

  CONSTRAINT fk_run FOREIGN KEY(run_id) REFERENCES {Schemas.META}.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS {Schemas.META}.ingested_contents (
  pipeline_name     VARCHAR NOT NULL,
  content_sha256    VARCHAR NOT NULL,
  first_success_at  TIMESTAMP NOT NULL,
  last_success_at   TIMESTAMP NOT NULL,
  PRIMARY KEY (pipeline_name, content_sha256)
);

CREATE INDEX IF NOT EXISTS ix_ingestion_log_run ON {Schemas.META}.ingestion_log(run_id);
CREATE INDEX IF NOT EXISTS ix_ingestion_log_sha ON {Schemas.META}.ingestion_log(pipeline_name, file_sha256);
"""


@dataclass(frozen=True)
class FileFingerprint:
    file_name: str
    file_path: str
    file_size_bytes: int
    file_mtime_utc: datetime
    sha256: str


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _ensure_meta_tables() -> None:
    # crea schema+tabelle se non esistono
    with get_db_connection() as con:
        con.execute(META_DDL)


def _sha256_of_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _fingerprint_file(path: Path) -> FileFingerprint:
    st = path.stat()
    mtime_utc = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).replace(tzinfo=None)
    return FileFingerprint(
        file_name=path.name,
        file_path=str(path.resolve()),
        file_size_bytes=st.st_size,
        file_mtime_utc=mtime_utc,
        sha256=_sha256_of_file(path),
    )


def _scan_drop_zone(drop_dir: Path) -> List[Path]:
    if not drop_dir.exists():
        return []
    return sorted([p for p in drop_dir.glob("*.csv") if p.is_file()])


def _start_run(pipeline_name: str, trigger_type: str, notes: Optional[str]) -> uuid.UUID:
    run_id = uuid.uuid4()
    _ensure_meta_tables()
    with get_db_connection() as con:
        con.execute(
            f"""
            INSERT INTO {Schemas.META}.ingestion_runs(
              run_id, pipeline_name, started_at, status, trigger_type, notes
            )
            VALUES (?, ?, ?, 'RUNNING', ?, ?)
            """,
            [str(run_id), pipeline_name, _utcnow_naive(), trigger_type, notes],
        )
    return run_id


def _finish_run(run_id: uuid.UUID, status: str) -> None:
    with get_db_connection() as con:
        con.execute(
            f"""
            UPDATE {Schemas.META}.ingestion_runs
            SET finished_at = ?, status = ?
            WHERE run_id = ?
            """,
            [_utcnow_naive(), status, str(run_id)],
        )


def _is_already_successfully_ingested(pipeline_name: str, sha256: str) -> bool:
    """
    True se quel contenuto (sha) è già stato processato con successo in passato
    (presente in meta.ingested_contents).
    """
    _ensure_meta_tables()
    with get_db_connection(read_only=True) as con:
        row = con.execute(
            f"""
            SELECT 1
            FROM {Schemas.META}.ingested_contents
            WHERE pipeline_name = ? AND content_sha256 = ?
            LIMIT 1
            """,
            [pipeline_name, sha256],
        ).fetchone()
    return row is not None


def _insert_log(
    run_id: uuid.UUID,
    pipeline_name: str,
    drop_dir: str,
    fp: FileFingerprint,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    with get_db_connection() as con:
        con.execute(
            f"""
            INSERT INTO {Schemas.META}.ingestion_log(
              log_id, run_id, pipeline_name,
              drop_dir, file_name, file_path,
              file_size_bytes, file_mtime_utc, file_sha256,
              detected_at, status, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                str(run_id),
                pipeline_name,
                drop_dir,
                fp.file_name,
                fp.file_path,
                int(fp.file_size_bytes),
                fp.file_mtime_utc,
                fp.sha256,
                _utcnow_naive(),
                status,
                error_message,
            ],
        )


@task(name="Phase2 - Detect & log client drop", retries=0)
def detect_and_log_client_drop(
    pipeline_name: str = "phase2_incremental",
    drop_dir: str = CLIENT_DROP_DIR,
    trigger_type: str = "SCHEDULE",
    notes: Optional[str] = None,
) -> Dict[str, int]:
    """
    STEP 1:
    - crea meta tables se non esistono
    - crea una run
    - scansiona drop zone
    - logga ogni file come:
        - SKIPPED se già presente in ingested_contents (già processato con successo in passato)
        - PENDING se nuovo contenuto (da processare negli step 2/3/4)
    Ritorna summary counts per flow control.
    """
    logger = get_run_logger()
    run_id = _start_run(pipeline_name=pipeline_name, trigger_type=trigger_type, notes=notes)

    pending = 0
    skipped = 0
    failed = 0

    drop_path = Path(drop_dir)
    resolved_drop = str(drop_path.resolve())

    try:
        files = _scan_drop_zone(drop_path)
        logger.info(f"Drop zone: {resolved_drop} | CSV trovati: {len(files)}")

        for f in files:
            try:
                fp = _fingerprint_file(f)

                if fp.sha256 and _is_already_successfully_ingested(pipeline_name, fp.sha256):
                    _insert_log(
                        run_id=run_id,
                        pipeline_name=pipeline_name,
                        drop_dir=resolved_drop,
                        fp=fp,
                        status="SKIPPED",
                        error_message="already_ingested",
                    )
                    skipped += 1
                else:
                    _insert_log(
                        run_id=run_id,
                        pipeline_name=pipeline_name,
                        drop_dir=resolved_drop,
                        fp=fp,
                        status="PENDING",
                        error_message=None,
                    )
                    pending += 1

            except Exception as e:
                # fingerprint/log fallito su questo file
                dummy = FileFingerprint(
                    file_name=f.name,
                    file_path=str(f.resolve()),
                    file_size_bytes=0,
                    file_mtime_utc=_utcnow_naive(),
                    sha256="",
                )
                _insert_log(
                    run_id=run_id,
                    pipeline_name=pipeline_name,
                    drop_dir=resolved_drop,
                    fp=dummy,
                    status="FAILED",
                    error_message=str(e),
                )
                failed += 1

        # Run success anche se pending=0: è un no-op voluto
        _finish_run(run_id, status="SUCCESS")

    except Exception:
        _finish_run(run_id, status="FAILED")
        raise

    logger.info(f"STEP1 summary | PENDING={pending} SKIPPED={skipped} FAILED={failed}")
    return {"run_id": str(run_id), "pending": pending, "skipped": skipped, "failed": failed, "files": len(files)}
