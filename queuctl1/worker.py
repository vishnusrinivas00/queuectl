"""
worker loop: picks jobs, executes, handles retries.
"""

from __future__ import annotations
import os
import signal
import subprocess
import time
from typing import Optional

from .storage import (
    connect, init, claim_next_job, update_job_success, update_job_failure,
    get_config, workers_register, workers_heartbeat
)

# terminate signal flag — keep it simple; graceful enough for this scope
_SHOULD_EXIT = False


def _trap(signum, _frame):
    # small: just flip a flag; let current job finish
    global _SHOULD_EXIT
    _SHOULD_EXIT = True


def run_worker(db_path: Optional[str] = None, heartbeat_sec: int = 2):
    # install signal handlers (Ctrl+C / kill) for graceful shutdown
    signal.signal(signal.SIGTERM, _trap)
    signal.signal(signal.SIGINT, _trap)

    init(db_path)
    db = connect(db_path)

    pid = os.getpid()
    workers_register(db, pid)

    # very small delay to avoid tight loop when idle
    idle_sleep = 0.5

    while True:
        if _SHOULD_EXIT:      # exit point (graceful-ish)
            break

        workers_heartbeat(db, pid)

        job = claim_next_job(db, pid)
        if not job:
            time.sleep(idle_sleep)
            continue

        # actually run the command (shell=True because assignment uses echo/sleep)
        try:
            run_cmd = subprocess.run(job["command"], shell=True, capture_output=True, text=True)
            if run_cmd.returncode == 0:
                update_job_success(db, job["id"])
            else:
                base = int(get_config(db, "backoff_base") or 2)     # tweakable via CLI
                err = (run_cmd.stderr or run_cmd.stdout or "").strip()[:500]
                update_job_failure(db, job["id"], job["attempts"], job["max_retries"], base, err or f"exit={run_cmd.returncode}")
        except Exception as boom:
            # unexpected execution error
            base = int(get_config(db, "backoff_base") or 2)
            update_job_failure(db, job["id"], job["attempts"], job["max_retries"], base, repr(boom))

        # slight breather between jobs  ( no need to be hyper-aggressive )
        time.sleep(0.10)
