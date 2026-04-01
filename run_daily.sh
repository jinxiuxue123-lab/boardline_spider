#!/bin/zsh
set -euo pipefail

cd "$(dirname "$0")"

echo "[1/4] 开始日常同步..."
python3 daily_sync.py

echo "[2/4] 刷新折扣与人民币价格..."
python3 refresh_discount_pricing.py

echo "[3/4] 导出全量库存表..."
python3 export_all_stock.py

echo "[4/4] 刷新网页展示数据..."
python3 generate_catalog_site.py

echo "全部完成"
