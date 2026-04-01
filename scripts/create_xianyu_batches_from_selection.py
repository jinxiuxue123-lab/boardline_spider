import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


DB_FILE = "products.db"
SELECTION_FILE = "xianyu_batch_selection.xlsx"


def clean_cell(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def get_product_id(cur, product_id_text: str, branduid: str) -> int | None:
    if product_id_text:
        cur.execute("SELECT id FROM products WHERE id = ?", (product_id_text,))
        row = cur.fetchone()
        if row:
            return row[0]

    if branduid:
        cur.execute("SELECT id FROM products WHERE branduid = ? LIMIT 1", (branduid,))
        row = cur.fetchone()
        if row:
            return row[0]

    return None


def get_or_create_batch(cur, account_id: int, batch_name: str, note: str) -> int:
    cur.execute("""
        INSERT INTO xianyu_publish_batches (account_id, batch_name, status, note)
        VALUES (?, ?, 'pending', ?)
        ON CONFLICT(account_id, batch_name) DO UPDATE SET
            note = excluded.note,
            updated_at = CURRENT_TIMESTAMP
    """, (account_id, batch_name, note))

    cur.execute("""
        SELECT id
        FROM xianyu_publish_batches
        WHERE account_id = ? AND batch_name = ?
    """, (account_id, batch_name))
    return cur.fetchone()[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file",
        default=SELECTION_FILE,
        help="选品 Excel 文件，默认 xianyu_batch_selection.xlsx",
    )
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        raise FileNotFoundError(f"找不到选品文件: {path}")

    df = pd.read_excel(path)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    created_count = 0
    skipped_count = 0
    missing_count = 0

    for _, row in df.iterrows():
        enabled = int(row.get("enabled", 1) or 1)
        if not enabled:
            continue

        batch_name = clean_cell(row.get("batch_name"))
        account_name = clean_cell(row.get("account_name"))
        product_id_text = clean_cell(row.get("product_id"))
        branduid = clean_cell(row.get("branduid"))
        note = clean_cell(row.get("note"))

        if not batch_name or not account_name:
            skipped_count += 1
            continue

        cur.execute("""
            SELECT id
            FROM xianyu_accounts
            WHERE account_name = ? AND enabled = 1
            LIMIT 1
        """, (account_name,))
        account_row = cur.fetchone()
        if not account_row:
            print(f"跳过，账号不存在或未启用: {account_name}")
            skipped_count += 1
            continue

        product_id = get_product_id(cur, product_id_text, branduid)
        if not product_id:
            print(f"未找到商品: batch={batch_name} account={account_name} product_id={product_id_text} branduid={branduid}")
            missing_count += 1
            continue

        batch_id = get_or_create_batch(cur, account_row[0], batch_name, note)

        cur.execute("""
            INSERT INTO xianyu_publish_tasks (
                account_id,
                batch_id,
                product_id,
                status,
                publish_status
            )
            VALUES (?, ?, ?, 'pending', 'pending')
        """, (account_row[0], batch_id, product_id))
        created_count += 1

    cur.execute("""
        UPDATE xianyu_publish_batches
        SET total_count = (
            SELECT COUNT(*)
            FROM xianyu_publish_tasks t
            WHERE t.batch_id = xianyu_publish_batches.id
        ),
            updated_at = CURRENT_TIMESTAMP
    """)

    conn.commit()
    conn.close()

    print("已根据选品表创建批次任务")
    print(f"新增任务数: {created_count}")
    print(f"跳过数: {skipped_count}")
    print(f"未找到商品数: {missing_count}")


if __name__ == "__main__":
    main()
