import json
import sqlite3
from datetime import datetime

from .client import XianyuOpenClient


DB_FILE = "products.db"


def build_delete_payload(third_product_id) -> dict:
    text = str(third_product_id or "").strip()
    if not text.isdigit():
        raise ValueError(f"product_id 无效: {third_product_id}")
    return {"product_id": int(text)}


def execute_task_delete_product(task_id: int) -> dict:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT
            t.id AS task_id,
            t.third_product_id,
            t.status,
            t.publish_status,
            a.app_key,
            a.app_secret
        FROM xianyu_publish_tasks t
        JOIN xianyu_accounts a
          ON a.id = t.account_id
        WHERE t.id = ?
        """,
        (task_id,),
    ).fetchone()
    conn.close()

    if not row:
        raise ValueError(f"找不到任务: {task_id}")
    if not (row["third_product_id"] or "").strip():
        raise ValueError("该任务还没有 third_product_id，无法删除")

    client = XianyuOpenClient(
        app_key=(row["app_key"] or "").strip() or None,
        app_secret=(row["app_secret"] or "").strip() or None,
    )
    resp = client.post(
        "/api/open/product/delete",
        build_delete_payload(row["third_product_id"]),
    )

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    columns = {row[1] for row in cur.execute("PRAGMA table_info(xianyu_publish_tasks)").fetchall()}
    has_deleted_at = "deleted_at" in columns
    update_sql = """
        UPDATE xianyu_publish_tasks
        SET status = 'deleted',
            publish_status = 'deleted',
            task_result = ?,
            last_error = '',
            err_code = '',
            err_msg = '',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """
    params = [
        json.dumps({"delete_resp": resp}, ensure_ascii=False),
        task_id,
    ]
    if has_deleted_at:
        update_sql = """
            UPDATE xianyu_publish_tasks
            SET status = 'deleted',
                publish_status = 'deleted',
                deleted_at = ?,
                task_result = ?,
                last_error = '',
                err_code = '',
                err_msg = '',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """
        params = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            json.dumps({"delete_resp": resp}, ensure_ascii=False),
            task_id,
        ]
    cur.execute(update_sql, params)
    conn.commit()
    conn.close()
    return {"task_id": task_id, "response": resp}
