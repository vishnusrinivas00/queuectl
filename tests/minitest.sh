#!/usr/bin/env bash
set -euo pipefail

# quick smoke; assumes 'python -m queuectl.cli' works in your env
rm -f queuectl.db

python -m queuectl.cli worker start --count 2 &
WPID=$!
sleep 1

python -m queuectl.cli enqueue '{"id":"ok1","command":"echo hello"}'
python -m queuectl.cli enqueue '{"id":"bad1","command":"bash -c \"exit 1\""}'
python -m queuectl.cli enqueue '{"id":"slow1","command":"sleep 1 && echo done"}'

# give it a few seconds to churn, including retries
sleep 6

python -m queuectl.cli status
python -m queuectl.cli dlq list || true
python -m queuectl.cli dlq retry bad1 || true

kill $WPID || true
wait $WPID || true
echo "mini test done"
