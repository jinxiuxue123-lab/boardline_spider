#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN=""

if [ -x "$PROJECT_ROOT/.venv/bin/python" ]; then
  PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
elif [ -x "$PROJECT_ROOT/venv/bin/python" ]; then
  PYTHON_BIN="$PROJECT_ROOT/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
else
  echo "未找到可用 Python，请先创建虚拟环境或安装 python3"
  exit 1
fi

cd "$PROJECT_ROOT"
export PLAYWRIGHT_HEADLESS="${PLAYWRIGHT_HEADLESS:-1}"
"$PYTHON_BIN" scripts/daily_run_cli.py ensure >/dev/null

OWN_RUN=0
if [ -z "${DAILY_RUN_ID:-}" ]; then
  export DAILY_RUN_ID="$("$PYTHON_BIN" scripts/daily_run_cli.py start-run --run-type full --trigger-mode shell)"
  OWN_RUN=1
fi

run_step() {
  local step_key="$1"
  local step_name="$2"
  shift 2
  "$PYTHON_BIN" scripts/daily_run_cli.py start-step --step-key "$step_key" --step-name "$step_name"
  if "$@"; then
    "$PYTHON_BIN" scripts/daily_run_cli.py finish-step --step-key "$step_key" --status success --message "${step_name}完成"
  else
    "$PYTHON_BIN" scripts/daily_run_cli.py finish-step --step-key "$step_key" --status failed --message "${step_name}失败"
    if [ "$OWN_RUN" -eq 1 ]; then
      "$PYTHON_BIN" scripts/daily_run_cli.py finish-run --status failed --note "${step_name}失败"
    fi
    exit 1
  fi
}

echo "[1/7] 先执行 Boardline 日更..."
run_step "boardline_full" "Boardline 全量日更" bash "$PROJECT_ROOT/run_daily.sh"

echo "[2/7] 抓取 One8 商品列表..."
run_step "one8_list_sync" "抓取 One8 商品列表" "$PYTHON_BIN" spider_one8_list_db.py

echo "[3/7] 修复 One8 失败尾巴..."
run_step "one8_repair" "修复 One8 失败尾巴" "$PYTHON_BIN" scripts/repair_one8_failures.py

echo "[4/7] 刷新 One8 折扣与人民币价格..."
run_step "one8_pricing" "刷新 One8 折扣与人民币价格" "$PYTHON_BIN" refresh_one8_discount_pricing.py

echo "[5/7] 刷新 One8 商品组..."
run_step "one8_group_refresh" "刷新 One8 商品组" "$PYTHON_BIN" scripts/refresh_one8_product_groups.py

echo "[6/7] 重新导出 One8 库存表..."
run_step "one8_export" "重新导出 One8 库存表" "$PYTHON_BIN" -c "from spider_one8_list_db import export_one8_inventory_excel; count = export_one8_inventory_excel(); print(f'已重新导出 one8_products.xlsx | 条数: {count}')"

echo "[7/7] 同步 One8 闲鱼库存变化..."
run_step "one8_xianyu_sync" "同步 One8 闲鱼库存变化" "$PYTHON_BIN" scripts/sync_xianyu_stock_changes.py --source one8

echo "One8 全部完成"
if [ "$OWN_RUN" -eq 1 ]; then
  "$PYTHON_BIN" scripts/daily_run_cli.py finish-run --status success --note "Boardline + One8 日更完成"
fi
