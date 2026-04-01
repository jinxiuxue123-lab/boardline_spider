import sqlite3

from discount_rules import calculate_latest_discount_price, load_discount_rules
from pricing_rules import calculate_cny_pricing, load_pricing_rules


DB_FILE = "products.db"
DISCOUNT_RULES_FILE = "one8_discount_rules.xlsx"
PRICING_RULES_FILE = "one8_pricing_rules.xlsx"
SOURCE_NAME = "one8"


def main():
    discount_rules = load_discount_rules(DISCOUNT_RULES_FILE)
    pricing_rules = load_pricing_rules(PRICING_RULES_FILE)

    print("one8 折扣规则数:", len(discount_rules))
    print("one8 人民币定价规则数:", len(pricing_rules))

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            p.id,
            p.name,
            p.category,
            u.price,
            u.original_price
        FROM products p
        JOIN product_updates u
          ON p.id = u.product_id
        WHERE p.source = ?
        ORDER BY p.id
    """, (SOURCE_NAME,))

    rows = cur.fetchall()
    updated_count = 0

    for product_id, name, category, price, original_price in rows:
        latest_discount_price = calculate_latest_discount_price(
            original_price or "",
            name or "",
            category or "",
            discount_rules,
        )

        pricing = calculate_cny_pricing(
            category or "",
            price or "",
            original_price or "",
            latest_discount_price or "",
            pricing_rules,
        )

        cur.execute("""
            UPDATE product_updates
            SET latest_discount_price = ?,
                price_cny = ?,
                original_price_cny = ?,
                shipping_fee_cny = ?,
                final_price_cny = ?,
                exchange_rate = ?,
                profit_rate = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE product_id = ?
        """, (
            latest_discount_price,
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

    print("one8 已刷新折扣与人民币价格商品数:", updated_count)


if __name__ == "__main__":
    main()
