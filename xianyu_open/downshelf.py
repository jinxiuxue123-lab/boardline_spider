import json
import sqlite3
from datetime import datetime

from .client import XianyuOpenClient
from .payload_builder import build_downshelf_payload


DB_FILE = "products.db"


def execute_task_downshelf(task_id: int) -> dict:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    row = cur.execute("""
        SELECT
            t.id AS task_id,
            t.third_product_id,
            a.app_key,
            a.app_secret,
            a.user_name,
            d.key_value AS callback_url
        FROM xianyu_publish_tasks t
        JOIN xianyu_accounts a
          ON a.id = t.account_id
        LEFT JOIN xianyu_publish_defaults d
          ON d.key_name = 'callback_url'
        WHERE t.id = ?
    """, (task_id,)).fetchone()
    conn.close()

    if not row:
        raise ValueError(f"找不到任务: {task_id}")
    if not (row["third_product_id"] or "").strip():
        raise ValueError("该任务还没有 third_product_id，无法下架")

    client = XianyuOpenClient(
        app_key=(row["app_key"] or "").strip() or None,
        app_secret=(row["app_secret"] or "").strip() or None,
    )
    resp = client.post(
        "/api/open/product/downShelf",
        build_downshelf_payload(row["third_product_id"], row["user_name"] or "", row["callback_url"] or ""),
    )

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE xianyu_publish_tasks
        SET status = 'off_shelved',
            publish_status = 'off_shelved',
            off_shelved_at = ?,
            task_result = ?,
            last_error = '',
            err_code = '',
            err_msg = '',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        json.dumps({"down_shelf_resp": resp}, ensure_ascii=False),
        task_id,
    ))
    conn.commit()
    conn.close()
    return {"task_id": task_id, "response": resp}
