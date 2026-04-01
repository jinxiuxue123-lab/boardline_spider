#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_PATH="${1:-$PROJECT_ROOT/boardline_spider_deploy.tar.gz}"

cd "$(dirname "$PROJECT_ROOT")"

tar \
  --exclude='boardline_spider/.git' \
  --exclude='boardline_spider/.venv' \
  --exclude='boardline_spider/__pycache__' \
  --exclude='boardline_spider/scripts/__pycache__' \
  --exclude='boardline_spider/services/__pycache__' \
  --exclude='boardline_spider/xianyu_open/__pycache__' \
  --exclude='boardline_spider/taobao_browser/__pycache__' \
  --exclude='boardline_spider/logs' \
  --exclude='boardline_spider/data' \
  --exclude='boardline_spider/*.db' \
  --exclude='boardline_spider/*.tar.gz' \
  --exclude='boardline_spider/*.log' \
  --exclude='boardline_spider/*.xlsx' \
  -czf "$OUTPUT_PATH" \
  boardline_spider

echo "部署包已生成: $OUTPUT_PATH"
