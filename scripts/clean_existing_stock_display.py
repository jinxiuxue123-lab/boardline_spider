import re
import sqlite3


DB_FILE = "products.db"


def parse_positive_qty(text: str) -> int | None:
    match = re.search(r":\s*(\d+)", text or "")
    if not match:
        return None
    qty = int(match.group(1))
    return qty if qty > 0 else None


def normalize_sku_label(text: str, preserve_numeric_parens: bool = False) -> str:
    text = (text or "").strip()
    if not preserve_numeric_parens:
        text = re.sub(r"\(\s*[+\-]?\d[\d,\s]*\)", "", text)
    text = re.sub(r"\(\)", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_stock_text(stock_text: str, category: str = "") -> str:
    stock_text = (stock_text or "").strip()
    if not stock_text:
        return ""
    preserve_numeric_parens = "滑雪鞋" in (category or "")

    cleaned_parts = []
    for part in stock_text.split("|"):
        part = part.strip()
        if not part:
            continue

        qty = parse_positive_qty(part)
        if qty is not None and part.count(":") == 1:
            label = normalize_sku_label(part.rsplit(":", 1)[0], preserve_numeric_parens=preserve_numeric_parens)
            cleaned_parts.append(f"{label}:{qty}")
            continue

        nested_match = re.match(r"^(.*?)\((.*)\)$", part)
        if not nested_match:
            continue

        prefix = nested_match.group(1).strip()
        inner = nested_match.group(2).strip()
        kept_inner = []
        for inner_part in inner.split(","):
            inner_part = inner_part.strip()
            inner_qty = parse_positive_qty(inner_part)
            if inner_qty is None:
                continue
            kept_inner.append(re.sub(r":\s*\d+\s*$", f":{inner_qty}", inner_part))

        if kept_inner:
            cleaned_parts.append(f"{normalize_sku_label(prefix, preserve_numeric_parens=preserve_numeric_parens)}({','.join(kept_inner)})")

    return " | ".join(cleaned_parts)


def main():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT u.product_id, u.stock, p.category
        FROM product_updates u
        JOIN products p
          ON p.id = u.product_id
        WHERE TRIM(COALESCE(u.stock, '')) != ''
        ORDER BY u.product_id
    """).fetchall()

    updated = 0
    unchanged = 0

    for row in rows:
        old_stock = row["stock"] or ""
        new_stock = clean_stock_text(old_stock, row["category"] or "")
        if new_stock == old_stock:
            unchanged += 1
            continue

        cur.execute("""
            UPDATE product_updates
            SET stock = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE product_id = ?
        """, (new_stock, row["product_id"]))
        updated += 1

    conn.commit()
    conn.close()

    print("库存展示清洗完成")
    print(f"更新数: {updated}")
    print(f"未变化数: {unchanged}")


if __name__ == "__main__":
    main()
