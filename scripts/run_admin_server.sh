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
  echo "未找到可用的 Python 解释器，请先安装 python3 或创建 .venv"
  exit 1
fi

if [ -f "$SCRIPT_DIR/admin_env.sh" ]; then
  source "$SCRIPT_DIR/admin_env.sh"
elif [ -f "$SCRIPT_DIR/admin_env.example.sh" ]; then
  echo "未找到 scripts/admin_env.sh，请先复制并填写："
  echo "cp scripts/admin_env.example.sh scripts/admin_env.sh"
  exit 1
fi

NO_PROXY_HOSTS="127.0.0.1,localhost"
if [ -n "${ALIYUN_OSS_ENDPOINT:-}" ]; then
  NO_PROXY_HOSTS="$NO_PROXY_HOSTS,${ALIYUN_OSS_ENDPOINT}"
fi
if [ -n "${XIANYU_IMAGE_CDN_BASE_URL:-}" ]; then
  CDN_HOST="$("$PYTHON_BIN" - <<'PY'
from urllib.parse import urlparse
import os
value = os.getenv("XIANYU_IMAGE_CDN_BASE_URL", "").strip()
print(urlparse(value).hostname or "")
PY
)"
  if [ -n "$CDN_HOST" ]; then
    NO_PROXY_HOSTS="$NO_PROXY_HOSTS,$CDN_HOST"
  fi
fi

if [ -n "${NO_PROXY:-}" ]; then
  export NO_PROXY="${NO_PROXY},${NO_PROXY_HOSTS}"
else
  export NO_PROXY="${NO_PROXY_HOSTS}"
fi
if [ -n "${no_proxy:-}" ]; then
  export no_proxy="${no_proxy},${NO_PROXY_HOSTS}"
else
  export no_proxy="${NO_PROXY_HOSTS}"
fi

cd "$PROJECT_ROOT"
"$PYTHON_BIN" scripts/run_xianyu_admin_server.py --host 0.0.0.0 --port 8790
