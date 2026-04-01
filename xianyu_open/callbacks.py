import json
import sqlite3
from datetime import datetime


DB_FILE = "products.db"


SUCCESS_TOKENS = {"1", "ok", "success", "succeeded", "done", "published"}
FAIL_TOKENS = {"0", "fail", "failed", "error", "rejected"}


def get_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_token(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value).strip().lower()


def infer_callback_status(payload: dict) -> tuple[str, str, str]:
    err_code = str(payload.get("err_code") or payload.get("errCode") or "").strip()
    err_msg = str(payload.get("err_msg") or payload.get("errMsg") or "").strip()

    if err_code and err_code not in ("0",):
        return "publish_failed", err_code, err_msg

    candidate_keys = ["task_result", "publish_status", "status", "result"]
    for key in candidate_keys:
        token = _normalize_token(payload.get(key))
        if not token:
            continue
        if token in SUCCESS_TOKENS:
            return "published", err_code, err_msg
        if token in FAIL_TOKENS:
            return "publish_failed", err_code, err_msg

    return "callback_received", err_code, err_msg


def process_callback(payload: dict, callback_type: str = "publish") -> dict:
    third_product_id = str(
        payload.get("product_id")
        or payload.get("productId")
        or ""
    ).strip()
    if not third_product_id:
        raise ValueError("回调缺少 product_id")

    callback_status, err_code, err_msg = infer_callback_status(payload)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id
        FROM xianyu_publish_tasks
        WHERE third_product_id = ?
        ORDER BY id DESC
        LIMIT 1
    """, (third_product_id,))
    task_row = cur.fetchone()
    task_id = task_row["id"] if task_row else None

    cur.execute("""
        INSERT INTO xianyu_callbacks (
            task_id,
            third_product_id,
            callback_type,
            callback_payload,
            callback_status,
            err_code,
            err_msg,
            processed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, (
        task_id,
        third_product_id,
        callback_type,
        json.dumps(payload, ensure_ascii=False),
        callback_status,
        err_code,
        err_msg,
    ))

    if task_id:
        update_fields = {
            "callback_raw": json.dumps(payload, ensure_ascii=False),
            "callback_status": callback_status,
            "last_callback_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "err_code": err_code,
            "err_msg": err_msg,
        }
        if callback_status == "published":
            update_fields["status"] = "published"
            update_fields["publish_status"] = "published"
            update_fields["published_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_fields["last_error"] = ""
        elif callback_status == "publish_failed":
            update_fields["status"] = "publish_failed"
            update_fields["publish_status"] = "publish_failed"
            update_fields["last_error"] = err_msg or err_code
        else:
            update_fields["publish_status"] = callback_status

        assignments = ", ".join(f"{key} = ?" for key in update_fields)
        values = list(update_fields.values()) + [task_id]
        cur.execute(
            f"UPDATE xianyu_publish_tasks SET {assignments}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            values,
        )

        cur.execute("""
            UPDATE xianyu_publish_batches
            SET success_count = (
                SELECT COUNT(*) FROM xianyu_publish_tasks
                WHERE batch_id = xianyu_publish_batches.id
                  AND status IN ('created', 'submitted', 'published', 'deleted')
            ),
                failed_count = (
                SELECT COUNT(*) FROM xianyu_publish_tasks
                WHERE batch_id = xianyu_publish_batches.id
                  AND status IN ('failed', 'publish_failed')
            ),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = (
                SELECT batch_id FROM xianyu_publish_tasks WHERE id = ?
            )
        """, (task_id,))

    conn.commit()
    conn.close()

    return {
        "task_id": task_id,
        "third_product_id": third_product_id,
        "callback_status": callback_status,
        "err_code": err_code,
        "err_msg": err_msg,
    }
