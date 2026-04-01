import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open.auto_attributes import apply_auto_attributes_for_product


DB_FILE = "products.db"


def main():
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute("""
        SELECT
            p.id,
            p.category,
            p.name,
            COALESCE(u.stock, '')
        FROM products p
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        WHERE p.source = 'boardline'
          AND p.status = 'active'
        ORDER BY p.id
    """).fetchall()
    conn.close()

    updated = 0
    skipped = 0
    for product_id, category, name, stock in rows:
        if apply_auto_attributes_for_product(product_id, category or "", name or "", stock or ""):
            updated += 1
        else:
            skipped += 1

    print("自动属性回填完成")
    print(f"更新数: {updated}")
    print(f"跳过数: {skipped}")


if __name__ == "__main__":
    main()
