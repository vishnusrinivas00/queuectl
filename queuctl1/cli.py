"""
thin CLI wrapping storage+worker modules.
ex: queuectl enqueue '{"id":"j1","command":"echo hi"}'
"""

from __future__ import annotations
import argparse
import json
import time
from multiprocessing import Process
from typing import Optional

from .storage import (
    init, connect, enqueue, list_jobs, status,
    dlq_list, dlq_retry, set_config, get_config
)
from .worker import run_worker


# ---- commands ---------------------------------------------------------------

def _cmd_enqueue(args):
    init(args.db)
    db = connect(args.db)

    # job spec is passed as a raw JSON string; keep the interface identical to the prompt
    payload = json.loads(args.json)
    if "id" not in payload or "command" not in payload:
        raise SystemExit("Job JSON must contain 'id' and 'command'")

    enqueue(db, payload)
    print(f"Enqueued job {payload['id']}")


def _cmd_worker_start(args):
    init(args.db)
    procs = []

    for _ in range(args.count):
        p = Process(target=run_worker, kwargs={"db_path": args.db})
        p.daemon = False   # keep it explicit; we want normal lifecycle
        p.start()
        procs.append(p)
        print(f"Started worker pid={p.pid}")

    print("Workers running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping workers ...")
        for p in procs: p.terminate()
        for p in procs: p.join()


def _cmd_worker_stop(_args):
    # best-effort: since we start in-foreground, stopping is Ctrl+C.
    print("Workers started in-foreground: use Ctrl+C to stop them.")


def _cmd_status(args):
    init(args.db)
    db = connect(args.db)
    print(json.dumps(status(db), indent=2))


def _cmd_list(args):
    init(args.db)
    db = connect(args.db)
    print(json.dumps(list_jobs(db, state=args.state), indent=2))


def _cmd_dlq_list(args):
    init(args.db)
    db = connect(args.db)
    print(json.dumps(dlq_list(db), indent=2))


def _cmd_dlq_retry(args):
    init(args.db)
    db = connect(args.db)
    dlq_retry(db, args.id)
    print(f"Moved {args.id} from DLQ to pending.")


def _cmd_config_set(args):
    init(args.db)
    db = connect(args.db)
    set_config(db, args.key, args.value)
    print(f"Set {args.key}={args.value}")


def _cmd_config_get(args):
    init(args.db)
    db = connect(args.db)
    val = get_config(db, args.key)
    print(val if val is not None else "")


# ---- parser -----------------------------------------------------------------

def _make_parser():
    p = argparse.ArgumentParser(prog="queuectl", description="CLI job queue with retries & DLQ")
    p.add_argument("--db", help="Path to SQLite DB (default: ./queuectl.db)", default=None)

    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("enqueue", help="Enqueue a job from JSON string")
    sp.add_argument("json", help="e.g. '{\"id\":\"job1\",\"command\":\"echo hi\"}'")
    sp.set_defaults(func=_cmd_enqueue)

    wp = sub.add_parser("worker", help="Worker management")
    wsub = wp.add_subparsers(dest="wcmd", required=True)

    wstart = wsub.add_parser("start", help="Start one or more workers (foreground)")
    wstart.add_argument("--count", type=int, default=1, help="number of workers")
    wstart.set_defaults(func=_cmd_worker_start)

    wstop = wsub.add_parser("stop", help="Stop workers (if supervised elsewhere)")
    wstop.set_defaults(func=_cmd_worker_stop)

    sp2 = sub.add_parser("status", help="Show summary of job states & active workers")
    sp2.set_defaults(func=_cmd_status)

    sp3 = sub.add_parser("list", help="List jobs (optionally by state)")
    sp3.add_argument("--state", choices=["pending","processing","completed","failed"], help="Filter by state")
    sp3.set_defaults(func=_cmd_list)

    dp = sub.add_parser("dlq", help="Dead Letter Queue operations")
    dsub = dp.add_subparsers(dest="dcmd", required=True)

    dlist = dsub.add_parser("list", help="List DLQ jobs")
    dlist.set_defaults(func=_cmd_dlq_list)

    dretry = dsub.add_parser("retry", help="Retry a DLQ job by id")
    dretry.add_argument("id")
    dretry.set_defaults(func=_cmd_dlq_retry)

    cp = sub.add_parser("config", help="Configuration")
    csub = cp.add_subparsers(dest="ccmd", required=True)

    cset = csub.add_parser("set", help="Set a config key")
    cset.add_argument("key")
    cset.add_argument("value")
    cset.set_defaults(func=_cmd_config_set)

    cget = csub.add_parser("get", help="Get a config key")
    cget.add_argument("key")
    cget.set_defaults(func=_cmd_config_get)

    return p


def main(argv: Optional[list[str]] = None):
    parser = _make_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
