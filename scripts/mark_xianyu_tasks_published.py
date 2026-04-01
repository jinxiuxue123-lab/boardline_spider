import argparse
import sqlite3


DB_FILE = "products.db"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", type=int, default=0, help="批次ID")
    parser.add_argument("--task-ids", default="", help="逗号分隔的任务ID")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    task_ids = []
    if args.batch_id:
        cur.execute("SELECT id FROM xianyu_publish_tasks WHERE batch_id = ?", (args.batch_id,))
        task_ids.extend([row[0] for row in cur.fetchall()])

    if args.task_ids.strip():
        task_ids.extend([int(x) for x in args.task_ids.split(",") if x.strip().isdigit()])

    task_ids = sorted(set(task_ids))
    if not task_ids:
        raise ValueError("请提供 --batch-id 或 --task-ids")

    placeholders = ",".join("?" for _ in task_ids)
    cur.execute(f"""
        UPDATE xianyu_publish_tasks
        SET status = 'published',
            publish_status = 'published',
            callback_status = 'manual_confirmed',
            published_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
    """, task_ids)

    if args.batch_id:
        cur.execute("""
            UPDATE xianyu_publish_batches
            SET success_count = (
                SELECT COUNT(*) FROM xianyu_publish_tasks WHERE batch_id = ? AND status = 'published'
            ),
                failed_count = (
                SELECT COUNT(*) FROM xianyu_publish_tasks WHERE batch_id = ? AND status IN ('failed', 'publish_failed')
            ),
                status = 'completed',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (args.batch_id, args.batch_id, args.batch_id))

    conn.commit()
    conn.close()

    print(f"已标记 published 的任务数: {len(task_ids)}")


if __name__ == "__main__":
    main()
