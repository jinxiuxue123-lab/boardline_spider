import argparse
import sqlite3
from pathlib import Path

import pandas as pd


DB_FILE = "products.db"


def clean_cell(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        raise FileNotFoundError(f"找不到文件: {path}")

    df = pd.read_excel(path)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    touched_meta = set()
    touched_values = set()

    for _, row in df.iterrows():
        product_id = clean_cell(row.get("product_id"))
        property_id = clean_cell(row.get("property_id"))
        if not product_id.isdigit() or not property_id:
            continue

        product_id_int = int(product_id)
        stuff_status = clean_cell(row.get("stuff_status"))
        note = clean_cell(row.get("note"))
        if stuff_status or note:
            cur.execute("""
                INSERT INTO xianyu_product_publish_meta (product_id, stuff_status, note, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(product_id) DO UPDATE SET
                    stuff_status = excluded.stuff_status,
                    note = excluded.note,
                    updated_at = CURRENT_TIMESTAMP
            """, (product_id_int, stuff_status, note))
            touched_meta.add(product_id_int)

        value_id = clean_cell(row.get("value_id"))
        value_name = clean_cell(row.get("value_name"))
        property_name = clean_cell(row.get("property_name"))
        source_category = clean_cell(row.get("category"))

        cur.execute("""
            INSERT INTO xianyu_product_property_values (
                product_id, source_category, property_id, property_name, value_id, value_name, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(product_id, property_id) DO UPDATE SET
                source_category = excluded.source_category,
                property_name = excluded.property_name,
                value_id = excluded.value_id,
                value_name = excluded.value_name,
                updated_at = CURRENT_TIMESTAMP
        """, (
            product_id_int,
            source_category,
            property_id,
            property_name,
            value_id,
            value_name,
        ))
        touched_values.add((product_id_int, property_id))

    conn.commit()
    conn.close()
    print(f"已同步商品属性文件: {path.name}")
    print(f"成色/备注商品数: {len(touched_meta)}")
    print(f"属性值条数: {len(touched_values)}")


if __name__ == "__main__":
    main()
