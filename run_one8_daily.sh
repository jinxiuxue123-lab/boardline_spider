#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"

cd "$PROJECT_ROOT"
export PLAYWRIGHT_HEADLESS="${PLAYWRIGHT_HEADLESS:-1}"

echo "[1/5] 先执行 Boardline 日更..."
bash "$PROJECT_ROOT/run_daily.sh"

echo "[2/5] 抓取 One8 商品列表..."
"$PYTHON_BIN" spider_one8_list_db.py

echo "[3/5] 修复 One8 失败尾巴..."
"$PYTHON_BIN" scripts/repair_one8_failures.py

echo "[4/5] 刷新 One8 折扣与人民币价格..."
"$PYTHON_BIN" refresh_one8_discount_pricing.py

echo "[5/5] 同步 One8 闲鱼库存变化..."
"$PYTHON_BIN" scripts/sync_xianyu_stock_changes.py --source one8

echo "One8 全部完成"
