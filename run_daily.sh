#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"

cd "$PROJECT_ROOT"
export PLAYWRIGHT_HEADLESS="${PLAYWRIGHT_HEADLESS:-1}"

echo "[1/4] 开始日常同步..."
"$PYTHON_BIN" daily_sync.py

echo "[2/4] 刷新折扣与人民币价格..."
"$PYTHON_BIN" refresh_discount_pricing.py

echo "[3/4] 导出全量库存表..."
"$PYTHON_BIN" export_all_stock.py

echo "[4/4] 刷新网页展示数据..."
"$PYTHON_BIN" generate_catalog_site.py

echo "全部完成"
