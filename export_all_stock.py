import sqlite3
import pandas as pd

DB_FILE = "products.db"
OUTPUT_FILE = "all_products_stock.xlsx"

def main():
    conn = sqlite3.connect(DB_FILE)

    sql = """
    SELECT
        p.branduid,
        p.category,
        p.name,
        p.url,
        p.image_url,
        p.status,
        u.price,
        u.original_price,
        u.latest_discount_price,
        u.price_cny,
        u.original_price_cny,
        u.shipping_fee_cny,
        u.final_price_cny,
        u.exchange_rate,
        u.profit_rate,
        u.stock,
        u.updated_at
    FROM products p
    LEFT JOIN product_updates u
        ON p.id = u.product_id
    WHERE p.source = 'boardline'
    ORDER BY p.id
    """

    df = pd.read_sql_query(sql, conn)
    conn.close()

    df.to_excel(OUTPUT_FILE, index=False)

    print("导出完成")
    print("文件名:", OUTPUT_FILE)
    print("商品数量:", len(df))

if __name__ == "__main__":
    main()
