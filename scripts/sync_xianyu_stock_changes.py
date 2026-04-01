import json
import sqlite3
import sys
import argparse
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open import XianyuOpenClient
from xianyu_open.payload_builder import build_edit_payload, get_publish_task, load_publish_defaults


DB_FILE = "products.db"


def get_changed_task_ids(today: str, source: str = "") -> list[int]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    sql = """
        SELECT DISTINCT t.id AS task_id
        FROM change_logs c
        JOIN xianyu_publish_tasks t
          ON t.product_id = c.product_id
        JOIN products p
          ON p.id = t.product_id
        WHERE c.field_name = 'stock'
          AND date(c.change_time) = ?
          AND COALESCE(t.third_product_id, '') != ''
          AND (
            t.status IN ('submitted', 'published')
            OR t.publish_status IN ('submitted', 'published')
            OR t.callback_status IN ('published', 'success')
          )
    """
    params = [today]
    if source:
        sql += " AND p.source = ?"
        params.append(source)
    sql += " ORDER BY t.id"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [int(row["task_id"]) for row in rows]


def update_task_after_edit(task_id: int, task_result: str = "", err_msg: str = "", failed: bool = False):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    if failed:
        cur.execute(
            """
            UPDATE xianyu_publish_tasks
            SET last_error = ?,
                err_msg = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (err_msg, err_msg, task_id),
        )
    else:
        cur.execute(
            """
            UPDATE xianyu_publish_tasks
            SET task_result = ?,
                last_error = '',
                err_code = '',
                err_msg = '',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (task_result, task_id),
        )
    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["boardline", "one8"], default="", help="仅同步指定来源商品")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    defaults = load_publish_defaults()
    task_ids = get_changed_task_ids(today, source=args.source)
    success_count = 0
    skip_count = 0
    fail_count = 0

    source_label = args.source or "all"
    print(f"待同步库存变更任务数[{source_label}]: {len(task_ids)}")

    for task_id in task_ids:
        try:
            task = get_publish_task(task_id)
            payload = build_edit_payload(task, notify_url=defaults.get("callback_url", ""))
            if int(payload.get("stock") or 0) <= 0:
                skip_count += 1
                print(f"跳过库存编辑(总库存<=0): task={task_id} | product={task.get('product_id')}")
                continue
            client = XianyuOpenClient(
                app_key=(task.get("app_key") or "").strip() or None,
                app_secret=(task.get("app_secret") or "").strip() or None,
            )
            resp = client.post("/api/open/product/edit", payload)
            update_task_after_edit(
                task_id,
                task_result=json.dumps(
                    {
                        "stock_edit_request": payload,
                        "stock_edit_resp": resp,
                    },
                    ensure_ascii=False,
                ),
            )
            print(f"已同步库存: task={task_id} | third_product_id={task.get('third_product_id')} | stock={task.get('stock')}")
            success_count += 1
        except Exception as e:
            update_task_after_edit(task_id, err_msg=str(e), failed=True)
            print(f"库存同步失败: task={task_id} | {e}")
            fail_count += 1

    print(f"闲鱼库存编辑同步完成[{source_label}]")
    print(f"成功数: {success_count}")
    print(f"跳过数: {skip_count}")
    print(f"失败数: {fail_count}")


if __name__ == "__main__":
    main()
