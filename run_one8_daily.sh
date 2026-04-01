#!/bin/zsh
set -e

cd /Users/jinxiuxue/Desktop/boardline_spider

./run_daily.sh

python3 spider_one8_list_db.py
python3 scripts/repair_one8_failures.py
python3 refresh_one8_discount_pricing.py
python3 scripts/sync_xianyu_stock_changes.py --source one8
