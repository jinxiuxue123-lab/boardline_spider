import subprocess
import sys
import shlex
from datetime import datetime
import sqlite3
from pathlib import Path

from db_utils import (
    increment_missing_days_for_not_seen_today,
    reset_missing_days_for_seen_today,
    mark_inactive_products,
)


SOURCE_NAME = "boardline"
INACTIVE_DAYS = 3
DB_FILE = "products.db"
LOG_DIR = Path("logs")


def log(message: str, log_file=None):
    print(message)
    if log_file:
        log_file.write(message + "\n")
        log_file.flush()


def log_step(step_no: str, title: str, log_file=None):
    log(f"\n[{step_no}] {title}", log_file)


def count_non_empty_lines(path: str) -> int:
    file_path = Path(path)
    if not file_path.exists():
        return 0
    with open(file_path, "r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def run_script(script_name, log_file=None, continue_on_error: bool = False):
    log(f"\n开始执行: {script_name}", log_file)
    cmd = shlex.split(script_name)
    process = subprocess.Popen(
        [sys.executable, "-u", *cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    for line in process.stdout:
        print(line, end="")
        if log_file:
            log_file.write(line)

    process.wait()
    if log_file:
        log_file.flush()

    if process.returncode != 0:
        if continue_on_error:
            log(f"警告: {script_name} 执行失败，继续后续流程", log_file)
            return
        raise RuntimeError(f"{script_name} 执行失败")
    log(f"执行完成: {script_name}", log_file)


def get_daily_summary(today: str) -> dict:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM products
        WHERE source = ?
          AND first_seen = ?
    """, (SOURCE_NAME, today))
    new_products_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM change_logs
        WHERE date(change_time) = ?
          AND field_name = 'price'
    """, (today,))
    price_change_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM change_logs
        WHERE date(change_time) = ?
          AND field_name = 'stock'
    """, (today,))
    stock_change_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM change_logs
        WHERE date(change_time) = ?
          AND field_name = 'latest_discount_price'
    """, (today,))
    latest_discount_change_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM xianyu_publish_tasks
        WHERE off_shelved_at IS NOT NULL
          AND date(off_shelved_at) = ?
    """, (today,))
    off_shelved_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM xianyu_publish_tasks
        WHERE status = 'submitted'
          AND publish_status = 'submitted'
          AND task_result LIKE '%republish_resp%'
          AND date(updated_at) = ?
    """, (today,))
    reshelved_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM xianyu_publish_tasks
        WHERE task_result LIKE '%stock_edit_resp%'
          AND date(updated_at) = ?
    """, (today,))
    stock_synced_count = cur.fetchone()[0]

    conn.close()

    return {
        "new_products_count": new_products_count,
        "price_change_count": price_change_count,
        "stock_change_count": stock_change_count,
        "latest_discount_change_count": latest_discount_change_count,
        "off_shelved_count": off_shelved_count,
        "reshelved_count": reshelved_count,
        "stock_synced_count": stock_synced_count,
        "failed_list_pages_count": count_non_empty_lines("failed_list_pages.txt"),
        "failed_stock_urls_count": count_non_empty_lines("failed_stock_urls.txt"),
    }


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"daily_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    with open(log_path, "w", encoding="utf-8") as log_file:
        log("开始每日同步流程...", log_file)
        log(f"今天日期: {today}", log_file)
        log(f"日志文件: {log_path}", log_file)

        log_step("1/8", "巡检新增商品", log_file)
        run_script("check_new_products.py", log_file, continue_on_error=True)

        log_step("2/8", "同步 Boardline 商品列表", log_file)
        run_script("spider_list_db.py", log_file)

        log_step("3/8", "处理商品上下架状态", log_file)

        reset_missing_days_for_seen_today(SOURCE_NAME, today)
        increment_missing_days_for_not_seen_today(SOURCE_NAME, today)
        mark_inactive_products(days_threshold=INACTIVE_DAYS, source=SOURCE_NAME)
        log("商品上下架状态处理完成", log_file)

        log_step("4/8", "抓取库存和价格变动", log_file)
        run_script("update_stock_db_concurrent.py", log_file)

        log_step("5/8", "同步闲鱼上下架与库存", log_file)
        run_script("scripts/auto_downshelf_zero_stock.py", log_file)
        run_script("scripts/sync_xianyu_stock_changes.py --source boardline", log_file)
        run_script("scripts/auto_reshelf_recovered_stock.py", log_file)

        log_step("6/8", "导出今日变化表", log_file)
        run_script("export_changes.py", log_file)

        log_step("7/8", "导出今日新增商品", log_file)
        run_script("export_new_products.py", log_file)

        log_step("8/8", "汇总统计", log_file)
        summary = get_daily_summary(today)

        log("\n每日同步完成", log_file)
        log("今日汇总:", log_file)
        log(f"新增商品数: {summary['new_products_count']}", log_file)
        log(f"价格变化数: {summary['price_change_count']}", log_file)
        log(f"库存变化数: {summary['stock_change_count']}", log_file)
        log(f"最新折扣价变化数: {summary['latest_discount_change_count']}", log_file)
        log(f"闲鱼自动下架数: {summary['off_shelved_count']}", log_file)
        log(f"闲鱼库存编辑同步数: {summary['stock_synced_count']}", log_file)
        log(f"闲鱼自动重新上架数: {summary['reshelved_count']}", log_file)
        log(f"失败列表分页数: {summary['failed_list_pages_count']}", log_file)
        log(f"失败详情页数: {summary['failed_stock_urls_count']}", log_file)


if __name__ == "__main__":
    main()
