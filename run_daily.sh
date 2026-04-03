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

echo "[1/4] 开始日常同步..."
"$PYTHON_BIN" daily_sync.py

echo "[2/4] 刷新折扣与人民币价格..."
"$PYTHON_BIN" refresh_discount_pricing.py

echo "[3/4] 导出全量库存表..."
"$PYTHON_BIN" export_all_stock.py

echo "[4/4] 刷新网页展示数据..."
"$PYTHON_BIN" generate_catalog_site.py

echo "全部完成"
