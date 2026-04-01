#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$SCRIPT_DIR/callback_env.sh" ]; then
  source "$SCRIPT_DIR/callback_env.sh"
elif [ -f "$SCRIPT_DIR/admin_env.sh" ]; then
  source "$SCRIPT_DIR/admin_env.sh"
fi

cd "$PROJECT_ROOT"
python3 scripts/run_xianyu_callback_server.py --host 0.0.0.0 --port 8787
