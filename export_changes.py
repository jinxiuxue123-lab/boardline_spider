import sqlite3
import pandas as pd
from datetime import datetime

DB_FILE = "products.db"


def main():

    conn = sqlite3.connect(DB_FILE)

    today = datetime.now().strftime("%Y-%m-%d")

    sql = """
    SELECT
        p.branduid,
        p.name,
        p.url,
        c.field_name,
        c.old_value,
        c.new_value,
        c.change_time
    FROM change_logs c
    JOIN products p
    ON p.id = c.product_id
    WHERE date(c.change_time) = date('now')
    ORDER BY c.change_time DESC
    """

    df = pd.read_sql_query(sql, conn)

    conn.close()

    if df.empty:
        print("今天没有库存或价格变化")
        return

    filename = f"stock_changes_{today}.xlsx"

    df.to_excel(filename, index=False)

    print("变化商品数量:", len(df))
    print("已导出文件:", filename)


if __name__ == "__main__":
    main()