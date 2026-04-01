import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd


DB_FILE = "products.db"


def clean_options(options_json: str) -> str:
    try:
        options = json.loads(options_json or "[]")
    except Exception:
        return ""
    labels = []
    for option in options:
        if not isinstance(option, dict):
            continue
        value_id = str(option.get("value_id") or "").strip()
        value_name = str(option.get("value_name") or "").strip()
        if value_name:
            labels.append(f"{value_name} ({value_id})" if value_id else value_name)
    return " | ".join(labels)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", required=True)
    args = parser.parse_args()

    conn = sqlite3.connect(DB_FILE)
    products = pd.read_sql_query("""
        SELECT id AS product_id, branduid, category, name
        FROM products
        WHERE source = 'boardline'
          AND category = ?
          AND status = 'active'
        ORDER BY id
    """, conn, params=[args.category])

    props = pd.read_sql_query("""
        SELECT property_id, property_name, required, input_mode, options_json
        FROM xianyu_category_properties
        WHERE source = 'boardline'
          AND source_category = ?
        ORDER BY sort_order, id
    """, conn, params=[args.category])

    meta = pd.read_sql_query("""
        SELECT product_id, stuff_status, note
        FROM xianyu_product_publish_meta
    """, conn)

    values = pd.read_sql_query("""
        SELECT product_id, property_id, value_id, value_name
        FROM xianyu_product_property_values
    """, conn)
    conn.close()

    if products.empty:
        raise ValueError(f"分类下没有商品: {args.category}")
    if props.empty:
        raise ValueError(f"分类下没有属性定义，请先运行 query_xianyu_category_properties.py")

    meta_map = {
        int(row["product_id"]): {
            "stuff_status": row["stuff_status"] if pd.notna(row["stuff_status"]) else "",
            "note": row["note"] if pd.notna(row["note"]) else "",
        }
        for _, row in meta.iterrows()
    }
    value_map = {
        (int(row["product_id"]), str(row["property_id"])): {
            "value_id": row["value_id"] if pd.notna(row["value_id"]) else "",
            "value_name": row["value_name"] if pd.notna(row["value_name"]) else "",
        }
        for _, row in values.iterrows()
    }

    rows = []
    for _, product in products.iterrows():
        product_id = int(product["product_id"])
        publish_meta = meta_map.get(product_id, {})
        for _, prop in props.iterrows():
            key = (product_id, str(prop["property_id"]))
            current = value_map.get(key, {})
            rows.append({
                "product_id": product_id,
                "branduid": product["branduid"],
                "category": product["category"],
                "name": product["name"],
                "stuff_status": publish_meta.get("stuff_status", ""),
                "property_id": prop["property_id"],
                "property_name": prop["property_name"],
                "required": prop["required"],
                "input_mode": prop["input_mode"],
                "value_id": current.get("value_id", ""),
                "value_name": current.get("value_name", ""),
                "options_hint": clean_options(prop["options_json"]),
                "note": publish_meta.get("note", ""),
            })

    out_path = Path(f"xianyu_product_attributes_{args.category}.xlsx")
    pd.DataFrame(rows).to_excel(out_path, index=False)
    print(f"已导出属性模板: {out_path.name}")
    print(f"商品数量: {len(products)}")
    print(f"属性行数: {len(rows)}")


if __name__ == "__main__":
    main()
