#!/usr/bin/env bash
# Short demo for reviewers. Run from examples/llm-banking.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m venv .venv >/dev/null 2>&1 || true
# shellcheck disable=SC1091
. .venv/bin/activate
pip install -q -r requirements.txt

echo "== unittest =="
python -m unittest discover -s tests -v

echo
echo "== cust_002 (travel) =="
python app.py --customer-id cust_002 --no-vector

echo
echo "== cust_003 (starter) =="
python app.py --customer-id cust_003 --no-vector
