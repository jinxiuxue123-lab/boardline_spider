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
  export DAILY_RUN_ID="$("$PYTHON_BIN" scripts/daily_run_cli.py start-run --run-type boardline --trigger-mode shell)"
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

echo "[1/4] 开始日常同步..."
run_step "boardline_run_daily" "Boardline 日常同步" "$PYTHON_BIN" daily_sync.py

echo "[2/4] 刷新折扣与人民币价格..."
run_step "boardline_pricing" "刷新折扣与人民币价格" "$PYTHON_BIN" refresh_discount_pricing.py

echo "[3/4] 导出全量库存表..."
run_step "boardline_export" "导出全量库存表" "$PYTHON_BIN" export_all_stock.py

echo "[4/4] 刷新网页展示数据..."
run_step "boardline_catalog" "刷新网页展示数据" "$PYTHON_BIN" generate_catalog_site.py

echo "全部完成"
if [ "$OWN_RUN" -eq 1 ]; then
  "$PYTHON_BIN" scripts/daily_run_cli.py finish-run --status success --note "Boardline 日更完成"
fi
