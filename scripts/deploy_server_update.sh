#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVICE_NAME="boardline-admin"

echo "[1/5] 进入项目目录"
cd "$PROJECT_ROOT"

echo "[2/5] 拉取最新代码"
git pull --ff-only

echo "[3/5] 同步 systemd 配置"
cp "$PROJECT_ROOT/deploy/systemd/boardline-admin.service" /etc/systemd/system/
cp "$PROJECT_ROOT/deploy/systemd/boardline-daily.service" /etc/systemd/system/
cp "$PROJECT_ROOT/deploy/systemd/boardline-daily.timer" /etc/systemd/system/
cp "$PROJECT_ROOT/deploy/systemd/boardline-one8-daily.service" /etc/systemd/system/
cp "$PROJECT_ROOT/deploy/systemd/boardline-one8-daily.timer" /etc/systemd/system/
systemctl daemon-reload

echo "[4/5] 重启后台"
systemctl restart "$SERVICE_NAME"

echo "[5/5] 健康检查"
sleep 2
systemctl status "$SERVICE_NAME" --no-pager -l
curl -i http://127.0.0.1:8790/ | head -20

echo
echo "发布完成。"
