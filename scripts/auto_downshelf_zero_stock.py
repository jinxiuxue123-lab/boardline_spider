import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open import XianyuOpenClient
from xianyu_open.payload_builder import build_downshelf_payload, get_publish_task
from xianyu_open.stock_utils import parse_total_stock
from product_grouping import ensure_xianyu_group_task_support


DB_FILE = "products.db"


def get_candidates():
    ensure_xianyu_group_task_support()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT
            t.id AS task_id,
            t.third_product_id,
            COALESCE(t.publish_mode, 'single') AS publish_mode,
            t.status,
            t.publish_status,
            a.app_key,
            a.app_secret,
            a.user_name,
            d.key_value AS callback_url,
            p.id AS product_id,
            p.name,
            p.status AS product_status,
            u.stock
        FROM xianyu_publish_tasks t
        JOIN xianyu_accounts a
          ON a.id = t.account_id
        JOIN products p
          ON p.id = t.product_id
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        LEFT JOIN xianyu_publish_defaults d
          ON d.key_name = 'callback_url'
        WHERE t.third_product_id IS NOT NULL
          AND TRIM(t.third_product_id) != ''
          AND t.status IN ('submitted', 'published')
          AND a.enabled = 1
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def update_task(task_id: int, **kwargs):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    assignments = ", ".join(f"{key} = ?" for key in kwargs)
    values = list(kwargs.values()) + [task_id]
    cur.execute(
        f"UPDATE xianyu_publish_tasks SET {assignments}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        values,
    )
    conn.commit()
    conn.close()


def main():
    rows = get_candidates()
    down_count = 0
    skip_count = 0
    fail_count = 0

    for row in rows:
        task_stock = row["stock"] or ""
        if str(row["publish_mode"] or "single").strip() == "group":
            try:
                task_stock = get_publish_task(int(row["task_id"])).get("stock") or ""
            except Exception:
                task_stock = ""
        total_stock = parse_total_stock(task_stock)
        should_down = total_stock <= 0 or (
            str(row["publish_mode"] or "single").strip() != "group"
            and row["product_status"] == "inactive"
        )
        if not should_down:
            skip_count += 1
            continue

        try:
            client = XianyuOpenClient(
                app_key=(row["app_key"] or "").strip() or None,
                app_secret=(row["app_secret"] or "").strip() or None,
            )
            payload = build_downshelf_payload(
                row["third_product_id"],
                row["user_name"] or "",
                row["callback_url"] or "",
            )
            resp = client.post("/api/open/product/downShelf", payload)
            update_task(
                row["task_id"],
                status="off_shelved",
                publish_status="off_shelved",
                off_shelved_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                task_result=json.dumps({"down_shelf_resp": resp}, ensure_ascii=False),
                last_error="",
                err_code="",
                err_msg="",
            )
            print(f"已下架: task={row['task_id']} | product={row['product_id']} | stock={task_stock}")
            down_count += 1
        except Exception as e:
            update_task(
                row["task_id"],
                status="off_shelf_failed",
                publish_status="off_shelf_failed",
                last_error=str(e),
                err_msg=str(e),
            )
            print(f"下架失败: task={row['task_id']} | {e}")
            fail_count += 1

    print("自动下架检查完成")
    print(f"已下架数: {down_count}")
    print(f"跳过数: {skip_count}")
    print(f"失败数: {fail_count}")


if __name__ == "__main__":
    main()
