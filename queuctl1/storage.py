"""
persistence + lifecycle utilities for queuectl
sqlite only for now — simple + good enough.
"""

from __future__ import annotations
import sqlite3
from typing import Optional, Dict, List
from datetime import datetime, timezone
import os
import time

# NOTE: keep DB path overridable via env for quick experiments
DEFAULT_DB = os.environ.get("QUEUECTL_DB", os.path.join(os.getcwd(), "queuectl.db"))

# schema (kept fairly small; evolve later if needed)
SCHEMA_STMTS = [
    "PRAGMA journal_mode=WAL",       # decent perf for concurrent readers
    """CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        next_attempt_at TEXT,
        last_error TEXT,
    )""",
    """CREATE TABLE IF NOT EXISTS dlq (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        attempts INTEGER NOT NULL,
        max_retries INTEGER NOT NULL,
        failed_at TEXT NOT NULL,
        last_error TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS workers (
        pid INTEGER PRIMARY KEY,
        started_at TEXT NOT NULL,
        last_heartbeat TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )"""
]


def _stamp() -> str:
    """UTC iso string with Z — I prefer this over naive datetimes."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or DEFAULT_DB
    # autocommit-ish mode (isolation_level=None)
    db = sqlite3.connect(path, timeout=10, isolation_level=None)
    db.execute("PRAGMA foreign_keys=ON")
    return db


def init(db_path: Optional[str] = None) -> None:
    db = connect(db_path)
    with db:
        for s in SCHEMA_STMTS:
            db.execute(s)
        # defaults; can be changed via CLI config
        if db.execute("SELECT 1 FROM config WHERE key='backoff_base'").fetchone() is None:
            db.execute("INSERT INTO config(key, value) VALUES('backoff_base','2')")
        if db.execute("SELECT 1 FROM config WHERE key='default_max_retries'").fetchone() is None:
            db.execute("INSERT INTO config(key, value) VALUES('default_max_retries','3')")


# ---- config helpers ---------------------------------------------------------

def get_config(db: sqlite3.Connection, key: str) -> Optional[str]:
    row = db.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_config(db: sqlite3.Connection, key: str, value: str) -> None:
    # slight convenience; sqlite UPSERT
    db.execute(
        "INSERT INTO config(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )


# ---- job operations ---------------------------------------------------------

def enqueue(db: sqlite3.Connection, job: Dict) -> None:
    now = _stamp()
    # if not provided, pick default from config   (small convenience)
    max_retries = job.get("max_retries")
    if max_retries is None:
        max_retries = int(get_config(db, "default_max_retries") or 3)

    db.execute(
        "INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (job["id"], job["command"], "pending", 0, int(max_retries), now, now)
    )


def list_jobs(db: sqlite3.Connection, state: Optional[str] = None) -> List[Dict]:
    if state:
        cur = db.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at", (state,))
    else:
        cur = db.execute("SELECT * FROM jobs ORDER BY created_at")
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def claim_next_job(db: sqlite3.Connection, worker_pid: int) -> Optional[Dict]:
    """
    find + claim one eligible job atomically.
    uses BEGIN IMMEDIATE to avoid duplicate processing across workers.
    """
    now = _stamp()
    db.execute("BEGIN IMMEDIATE")
    try:
        cur = db.execute(
            """
            SELECT id FROM jobs
             WHERE state='pending'
                OR (state='failed' AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
             ORDER BY created_at
             LIMIT 1
            """,
            (now,)
        )
        row = cur.fetchone()
        if not row:
            db.execute("COMMIT")
            return None

        job_id = row[0]
        db.execute(
            "UPDATE jobs SET state='processing', updated_at=? "
            "WHERE id=? AND state!='processing'",
            (now, job_id)
        )
        db.execute("COMMIT")

        cur = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        cols = [c[0] for c in cur.description]
        rec = cur.fetchone()
        return dict(zip(cols, rec)) if rec else None

    except Exception:
        db.execute("ROLLBACK")
        raise


def update_job_success(db: sqlite3.Connection, job_id: str) -> None:
    db.execute("UPDATE jobs SET state='completed', updated_at=? WHERE id=?", (_stamp(), job_id))


def update_job_failure(
    db: sqlite3.Connection,
    job_id: str,
    attempts: int,
    max_retries: int,
    backoff_base: int,
    error: str
) -> str:
    """
    bump attempts; if exceeded -> move to DLQ, else schedule next_attempt_at with exponential backoff.
      quick formula: delay = base ^ attempts
    """
    now = _stamp()
    attempts += 1

    if attempts > max_retries:
        row = db.execute("SELECT id, command FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row:
            db.execute("DELETE FROM jobs WHERE id=?", (job_id,))
            db.execute(
                "INSERT INTO dlq(id, command, attempts, max_retries, failed_at, last_error) "
                "VALUES(?,?,?,?,?,?)",
                (row[0], row[1], attempts - 1, max_retries, now, (error or "")[:500])
            )
        return "dead"

    # schedule next run (keep it simple for assignment)
    delay_secs = backoff_base ** attempts
    next_ts = int(time.time() + delay_secs)
    next_iso = datetime.fromtimestamp(next_ts, tz=timezone.utc).replace(microsecond=0)\
        .isoformat().replace("+00:00", "Z")

    db.execute(
        "UPDATE jobs SET state='failed', attempts=?, next_attempt_at=?, last_error=?, updated_at=? WHERE id=?",
        (attempts, next_iso, (error or "")[:500], now, job_id)
    )
    return "failed"


# ---- dlq / status / workers -------------------------------------------------

def dlq_list(db: sqlite3.Connection) -> List[Dict]:
    cur = db.execute("SELECT * FROM dlq ORDER BY failed_at DESC")
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def dlq_retry(db: sqlite3.Connection, job_id: str) -> None:
    cur = db.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError("Job not found in DLQ")

    cols = [c[0] for c in cur.description]
    rec = dict(zip(cols, row))

    now = _stamp()
    db.execute("DELETE FROM dlq WHERE id=?", (job_id,))
    db.execute(
        "INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, next_attempt_at, last_error) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (rec["id"], rec["command"], "pending", 0, rec["max_retries"], now, now, None, None)
    )


def status(db: sqlite3.Connection) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for st in ("pending", "processing", "completed", "failed"):
        out[st] = db.execute("SELECT COUNT(*) FROM jobs WHERE state=?", (st,)).fetchone()[0]
    out["dead"] = db.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
    out["workers"] = db.execute("SELECT COUNT(*) FROM workers").fetchone()[0]
    return out


def workers_register(db: sqlite3.Connection, pid: int) -> None:
    now = _stamp()
    db.execute(
        "INSERT OR REPLACE INTO workers(pid, started_at, last_heartbeat) VALUES(?,?,?)",
        (pid, now, now)
    )


def workers_heartbeat(db: sqlite3.Connection, pid: int) -> None:
    db.execute("UPDATE workers SET last_heartbeat=? WHERE pid=?", (_stamp(), pid))


