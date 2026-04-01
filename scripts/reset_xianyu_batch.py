import argparse
import sqlite3


DB_FILE = "products.db"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", type=int, required=True, help="要重置的 xianyu_publish_batches.id")
    parser.add_argument("--delete-batch", action="store_true", help="同时删除批次记录")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM xianyu_publish_tasks WHERE batch_id = ?", (args.batch_id,))
    task_count = cur.fetchone()[0]

    cur.execute("DELETE FROM xianyu_publish_tasks WHERE batch_id = ?", (args.batch_id,))

    if args.delete_batch:
        cur.execute("DELETE FROM xianyu_publish_batches WHERE id = ?", (args.batch_id,))
    else:
        cur.execute("""
            UPDATE xianyu_publish_batches
            SET status = 'pending',
                total_count = 0,
                success_count = 0,
                failed_count = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (args.batch_id,))

    conn.commit()
    conn.close()

    print(f"已删除任务数: {task_count}")
    if args.delete_batch:
        print(f"已删除批次: {args.batch_id}")
    else:
        print(f"已重置批次: {args.batch_id}")


if __name__ == "__main__":
    main()
