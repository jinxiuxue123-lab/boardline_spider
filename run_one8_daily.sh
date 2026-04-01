#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"

cd "$PROJECT_ROOT"
export PLAYWRIGHT_HEADLESS="${PLAYWRIGHT_HEADLESS:-1}"

bash "$PROJECT_ROOT/run_daily.sh"

"$PYTHON_BIN" spider_one8_list_db.py
"$PYTHON_BIN" scripts/repair_one8_failures.py
"$PYTHON_BIN" refresh_one8_discount_pricing.py
"$PYTHON_BIN" scripts/sync_xianyu_stock_changes.py --source one8
