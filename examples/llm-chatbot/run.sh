#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

python3 -m venv .venv
# shellcheck disable=SC1091
. .venv/bin/activate
pip install -q -r requirements.txt
python -m unittest discover -s tests -v

python api.py --port 8000 &
api_pid=$!
trap 'kill "$api_pid" 2>/dev/null || true' EXIT

for _ in 1 2 3 4 5 6 7 8 9 10; do
  if python -c "from urllib.request import urlopen; urlopen('http://127.0.0.1:8000/health', timeout=1)"; then
    break
  fi
  sleep 0.3
done

echo
echo "=== c1 maya (travel) ==="
python chat.py --customer-id c1 --base-url http://127.0.0.1:8000
echo
echo "=== c2 omar (groceries) ==="
python chat.py --customer-id c2 --base-url http://127.0.0.1:8000
echo
echo "=== c5 ana (student) ==="
python chat.py --customer-id c5 --base-url http://127.0.0.1:8000
