import argparse
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open.client import XianyuOpenClient


DB_FILE = "products.db"


def get_mapping(source_category: str) -> dict:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    row = cur.execute("""
        SELECT source, source_category, item_biz_type, sp_biz_type, channel_cat_id, channel_cat_name
        FROM xianyu_category_mapping
        WHERE source = 'boardline'
          AND source_category = ?
        LIMIT 1
    """, (source_category,)).fetchone()
    conn.close()
    if not row:
        raise ValueError(f"找不到分类映射: {source_category}")
    return dict(row)


def get_default_credentials() -> tuple[str, str]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    row = cur.execute("""
        SELECT app_key, app_secret
        FROM xianyu_accounts
        WHERE enabled = 1
          AND COALESCE(app_key, '') <> ''
          AND COALESCE(app_secret, '') <> ''
        ORDER BY id
        LIMIT 1
    """).fetchone()
    conn.close()
    if not row:
        return "", ""
    return clean_text(row["app_key"]), clean_text(row["app_secret"])


def clean_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_property(item: dict, sort_order: int) -> dict:
    property_id = clean_text(item.get("property_id") or item.get("id") or item.get("pid"))
    property_name = clean_text(item.get("property_name") or item.get("name") or item.get("property"))
    required = item.get("required")
    required_value = 1 if str(required).lower() in ("1", "true", "yes") else 0
    input_mode = clean_text(item.get("input_mode") or item.get("value_type") or item.get("type"))

    options = item.get("values") or item.get("options") or item.get("value_list") or []
    normalized_options = []
    if isinstance(options, list):
        for option in options:
            if not isinstance(option, dict):
                continue
            normalized_options.append({
                "value_id": clean_text(option.get("value_id") or option.get("id") or option.get("vid")),
                "value_name": clean_text(option.get("value_name") or option.get("name") or option.get("value")),
            })

    return {
        "property_id": property_id,
        "property_name": property_name,
        "required": required_value,
        "input_mode": input_mode,
        "options_json": json.dumps(normalized_options, ensure_ascii=False),
        "raw_json": json.dumps(item, ensure_ascii=False),
        "sort_order": sort_order,
    }


def save_properties(source_category: str, mapping: dict, properties: list[dict]) -> None:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM xianyu_category_properties
        WHERE source = 'boardline' AND source_category = ?
    """, (source_category,))
    for item in properties:
        cur.execute("""
            INSERT INTO xianyu_category_properties (
                source,
                source_category,
                channel_cat_id,
                property_id,
                property_name,
                required,
                input_mode,
                options_json,
                raw_json,
                sort_order,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            "boardline",
            source_category,
            mapping.get("channel_cat_id") or "",
            item["property_id"],
            item["property_name"],
            item["required"],
            item["input_mode"],
            item["options_json"],
            item["raw_json"],
            item["sort_order"],
        ))
    conn.commit()
    conn.close()


def export_properties(source_category: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("""
        SELECT
            source_category,
            channel_cat_id,
            property_id,
            property_name,
            required,
            input_mode,
            options_json,
            raw_json,
            sort_order
        FROM xianyu_category_properties
        WHERE source = 'boardline' AND source_category = ?
        ORDER BY sort_order, id
    """, conn, params=[source_category])
    conn.close()
    file_name = f"xianyu_category_properties_{source_category}.xlsx"
    df.to_excel(file_name, index=False)
    return file_name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-category", required=True)
    args = parser.parse_args()

    mapping = get_mapping(args.source_category)
    channel_cat_id = clean_text(mapping.get("channel_cat_id"))
    if not channel_cat_id:
        raise ValueError(f"{args.source_category} 缺少 channel_cat_id，请先补齐映射表")

    payload = {
        "channel_cat_id": channel_cat_id,
    }
    item_biz_type = clean_text(mapping.get("item_biz_type"))
    sp_biz_type = clean_text(mapping.get("sp_biz_type"))
    if item_biz_type:
        payload["item_biz_type"] = int(item_biz_type)
    if sp_biz_type:
        payload["sp_biz_type"] = int(sp_biz_type)

    app_key, app_secret = get_default_credentials()
    client = XianyuOpenClient(app_key=app_key or None, app_secret=app_secret or None)
    response = client.post("/api/open/product/pv/list", payload)
    raw_list = (response.get("data") or {}).get("list") or []
    properties = [
        normalize_property(item, index + 1)
        for index, item in enumerate(raw_list)
        if isinstance(item, dict)
    ]
    save_properties(args.source_category, mapping, properties)
    file_name = export_properties(args.source_category)
    print(f"已导出属性结果: {file_name}")
    print(f"属性数量: {len(properties)}")


if __name__ == "__main__":
    main()
