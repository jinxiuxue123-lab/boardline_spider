import re
import sqlite3


DB_PATH = "products.db"


def normalize_name(name: str) -> str:
    text = (name or "").strip()
    if not text:
        return ""

    season_match = re.search(r"\d{2}/\d{2}", text)
    if season_match:
        text = text[season_match.start():]

    text = re.sub(r"\s+", " ", text).strip()
    return text


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT id, name
        FROM products
        WHERE COALESCE(name, '') != ''
        ORDER BY id
    """).fetchall()

    updated = 0
    skipped = 0
    samples = []

    for row in rows:
        product_id = int(row["id"])
        old_name = str(row["name"] or "").strip()
        new_name = normalize_name(old_name)

        if not new_name or new_name == old_name:
            skipped += 1
            continue

        cur.execute("""
            UPDATE products
            SET name = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (new_name, product_id))
        updated += 1
        if len(samples) < 20:
            samples.append((product_id, old_name, new_name))

    conn.commit()
    conn.close()

    print(f"标题修复完成")
    print(f"更新数: {updated}")
    print(f"跳过数: {skipped}")
    if samples:
        print("示例:")
        for product_id, old_name, new_name in samples:
            print(f"- {product_id}: {old_name} -> {new_name}")


if __name__ == "__main__":
    main()
