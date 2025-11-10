# QueueCTL

Small CLI job queue with worker processes, retries (exponential backoff), and a dead-letter queue (DLQ).
Kept intentionally compact; good enough for an internship assignment and easy to read.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
