#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -x "$PROJECT_ROOT/.venv/bin/python" ]; then
  PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
elif [ -x "$PROJECT_ROOT/venv/bin/python" ]; then
  PYTHON_BIN="$PROJECT_ROOT/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
else
  echo "未找到可用的 Python 解释器"
  exit 1
fi

REMOTE_USER="${AI_SYNC_REMOTE_USER:-root}"
REMOTE_HOST="${AI_SYNC_REMOTE_HOST:-47.80.63.228}"
REMOTE_DIR="${AI_SYNC_REMOTE_DIR:-/root/boardline_spider}"
REMOTE_PYTHON="${AI_SYNC_REMOTE_PYTHON:-$REMOTE_DIR/.venv/bin/python}"
LOCAL_DB="${AI_SYNC_LOCAL_DB:-$PROJECT_ROOT/products.db}"
EXPORT_DIR="${AI_SYNC_EXPORT_DIR:-$PROJECT_ROOT/data/exports}"
EXPORT_FILE="$EXPORT_DIR/ai_image_records_sync.json"
REMOTE_IMPORT_DIR="$REMOTE_DIR/data/imports"
REMOTE_IMPORT_FILE="$REMOTE_IMPORT_DIR/ai_image_records_sync.json"

ACCOUNT_NAME=""
SHARED_ONLY=0
OVERWRITE_SELECTED=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --account-name)
      ACCOUNT_NAME="${2:-}"
      shift 2
      ;;
    --shared-only)
      SHARED_ONLY=1
      shift
      ;;
    --overwrite-selected)
      OVERWRITE_SELECTED=1
      shift
      ;;
    *)
      echo "未知参数: $1"
      echo "用法: bash scripts/push_ai_image_records_to_server.sh [--account-name 名称] [--shared-only] [--overwrite-selected]"
      exit 1
      ;;
  esac
done

mkdir -p "$EXPORT_DIR"

EXPORT_CMD=("$PYTHON_BIN" "$PROJECT_ROOT/scripts/export_ai_image_records.py" "--db" "$LOCAL_DB" "--output" "$EXPORT_FILE")
if [[ -n "$ACCOUNT_NAME" ]]; then
  EXPORT_CMD+=("--account-name" "$ACCOUNT_NAME")
fi
if [[ "$SHARED_ONLY" -eq 1 ]]; then
  EXPORT_CMD+=("--shared-only")
fi

echo "导出本地 AI 图元数据..."
"${EXPORT_CMD[@]}"

echo "上传元数据到服务器..."
ssh "$REMOTE_USER@$REMOTE_HOST" "mkdir -p '$REMOTE_IMPORT_DIR'"
scp "$EXPORT_FILE" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_IMPORT_FILE"

IMPORT_CMD="cd '$REMOTE_DIR' && '$REMOTE_PYTHON' scripts/import_ai_image_records.py --input '$REMOTE_IMPORT_FILE'"
if [[ "$OVERWRITE_SELECTED" -eq 1 ]]; then
  IMPORT_CMD="$IMPORT_CMD --overwrite-selected"
fi

echo "在服务器导入元数据..."
ssh "$REMOTE_USER@$REMOTE_HOST" "$IMPORT_CMD"

echo "完成：已推送 AI 图元数据到 $REMOTE_USER@$REMOTE_HOST"
