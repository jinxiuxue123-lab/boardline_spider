#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVICE_NAME="boardline-admin"
DB_PATH="$PROJECT_ROOT/products.db"
BACKUP_DIR="$PROJECT_ROOT/backups"

echo "[1/6] 进入项目目录"
cd "$PROJECT_ROOT"

echo "[2/6] 备份数据库"
if [ -f "$DB_PATH" ]; then
  mkdir -p "$BACKUP_DIR"
  BACKUP_PATH="$BACKUP_DIR/products.db.$(date +%Y%m%d_%H%M%S).bak"
  cp "$DB_PATH" "$BACKUP_PATH"
  echo "已备份到: $BACKUP_PATH"
else
  echo "未找到 products.db，跳过备份"
fi

echo "[3/6] 拉取最新代码"
git pull --ff-only

echo "[4/6] 同步 systemd 配置"
cp "$PROJECT_ROOT/deploy/systemd/boardline-admin.service" /etc/systemd/system/
cp "$PROJECT_ROOT/deploy/systemd/boardline-daily.service" /etc/systemd/system/
cp "$PROJECT_ROOT/deploy/systemd/boardline-daily.timer" /etc/systemd/system/
cp "$PROJECT_ROOT/deploy/systemd/boardline-one8-daily.service" /etc/systemd/system/
cp "$PROJECT_ROOT/deploy/systemd/boardline-one8-daily.timer" /etc/systemd/system/
systemctl daemon-reload

echo "[5/6] 重启后台"
systemctl restart "$SERVICE_NAME"

echo "[6/6] 健康检查"
sleep 2
systemctl status "$SERVICE_NAME" --no-pager -l
curl -i http://127.0.0.1:8790/ | head -20

echo
echo "发布完成。"
