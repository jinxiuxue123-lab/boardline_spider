import sqlite3

from pricing_rules import calculate_cny_pricing, load_pricing_rules


DB_FILE = "products.db"
RULES_FILE = "one8_pricing_rules.xlsx"
SOURCE_NAME = "one8"


def main():
    rules = load_pricing_rules(RULES_FILE)
    print("one8 人民币定价规则数:", len(rules))

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            p.id,
            p.category,
            u.price,
            u.original_price,
            u.latest_discount_price
        FROM products p
        JOIN product_updates u
          ON p.id = u.product_id
        WHERE p.source = ?
        ORDER BY p.id
    """, (SOURCE_NAME,))

    rows = cur.fetchall()
    updated_count = 0

    for product_id, category, price, original_price, latest_discount_price in rows:
        pricing = calculate_cny_pricing(
            category,
            price or "",
            original_price or "",
            latest_discount_price or "",
            rules,
        )

        cur.execute("""
            UPDATE product_updates
            SET price_cny = ?,
                original_price_cny = ?,
                shipping_fee_cny = ?,
                final_price_cny = ?,
                exchange_rate = ?,
                profit_rate = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE product_id = ?
        """, (
            pricing["price_cny"],
            pricing["original_price_cny"],
            pricing["shipping_fee_cny"],
            pricing["final_price_cny"],
            pricing["exchange_rate"],
            pricing["profit_rate"],
            product_id,
        ))
        updated_count += 1

    conn.commit()
    conn.close()

    print("one8 已刷新人民币价格商品数:", updated_count)


if __name__ == "__main__":
    main()
