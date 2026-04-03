#!/bin/bash
set -e

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

echo "[1/5] 先执行 Boardline 日更..."
bash "$PROJECT_ROOT/run_daily.sh"

echo "[2/5] 抓取 One8 商品列表..."
"$PYTHON_BIN" spider_one8_list_db.py

echo "[3/5] 修复 One8 失败尾巴..."
"$PYTHON_BIN" scripts/repair_one8_failures.py

echo "[4/5] 刷新 One8 折扣与人民币价格..."
"$PYTHON_BIN" refresh_one8_discount_pricing.py

echo "[5/6] 重新导出 One8 库存表..."
"$PYTHON_BIN" -c "from spider_one8_list_db import export_one8_inventory_excel; count = export_one8_inventory_excel(); print(f'已重新导出 one8_products.xlsx | 条数: {count}')"

echo "[6/6] 同步 One8 闲鱼库存变化..."
"$PYTHON_BIN" scripts/sync_xianyu_stock_changes.py --source one8

echo "One8 全部完成"
