import sqlite3


DB_FILE = "products.db"


def get_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def update_batch_counts(batch_id: int) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN status IN ('created', 'submitted', 'success', 'deleted') THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count
        FROM xianyu_publish_tasks
        WHERE batch_id = ?
    """, (batch_id,))
    row = cur.fetchone()

    total_count = row["total_count"] or 0
    success_count = row["success_count"] or 0
    failed_count = row["failed_count"] or 0

    status = "pending"
    if total_count > 0 and success_count + failed_count == total_count:
        status = "completed" if failed_count == 0 else "partial_failed"
    elif success_count > 0 or failed_count > 0:
        status = "running"

    cur.execute("""
        UPDATE xianyu_publish_batches
        SET total_count = ?,
            success_count = ?,
            failed_count = ?,
            status = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (total_count, success_count, failed_count, status, batch_id))

    conn.commit()
    conn.close()


def update_task_meta(task_id: int, **kwargs) -> None:
    if not kwargs:
        return

    conn = get_connection()
    cur = conn.cursor()
    assignments = ", ".join(f"{key} = ?" for key in kwargs)
    values = list(kwargs.values())
    values.append(task_id)
    cur.execute(
        f"UPDATE xianyu_publish_tasks SET {assignments}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        values,
    )
    conn.commit()
    conn.close()
