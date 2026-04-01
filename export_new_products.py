import sqlite3
from datetime import datetime

import pandas as pd


DB_FILE = "products.db"


def main():
    conn = sqlite3.connect(DB_FILE)

    today = datetime.now().strftime("%Y-%m-%d")

    sql = """
    SELECT
        branduid,
        category,
        name,
        url,
        image_url,
        local_image_path,
        status,
        first_seen,
        last_seen
    FROM products
    WHERE source = 'boardline'
      AND first_seen = ?
    ORDER BY id
    """

    df = pd.read_sql_query(sql, conn, params=(today,))
    conn.close()

    if df.empty:
        print("今天没有新增商品")
        return

    filename = f"new_products_{today}.xlsx"
    df.to_excel(filename, index=False)

    print("新增商品数量:", len(df))
    print("已导出文件:", filename)


if __name__ == "__main__":
    main()
