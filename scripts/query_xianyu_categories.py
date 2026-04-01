import argparse
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open import XianyuOpenClient


DB_FILE = "products.db"


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
    return str(row["app_key"] or "").strip(), str(row["app_secret"] or "").strip()


def extract_category_rows(response: dict) -> list[dict]:
    data = response.get("data")

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in ("list", "records", "items", "rows"):
            value = data.get(key)
            if isinstance(value, list):
                items = value
                break
        else:
            items = [data]
    else:
        items = []

    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "channel_cat_id": str(
                    item.get("channel_cat_id")
                    or item.get("channelCatId")
                    or item.get("id")
                    or ""
                ).strip(),
                "channel_cat_name": str(
                    item.get("channel_cat_name")
                    or item.get("channelCatName")
                    or item.get("name")
                    or ""
                ).strip(),
                "raw_json": json.dumps(item, ensure_ascii=False),
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--item-biz-type", type=int, default=1, help="默认 1")
    parser.add_argument("--sp-biz-type", type=int, default=0, help="默认 0")
    parser.add_argument(
        "--output",
        default="xianyu_categories_result.xlsx",
        help="导出文件名，默认 xianyu_categories_result.xlsx",
    )
    args = parser.parse_args()

    app_key, app_secret = get_default_credentials()
    client = XianyuOpenClient(app_key=app_key or None, app_secret=app_secret or None)
    payload = {
        "item_biz_type": args.item_biz_type,
        "sp_biz_type": args.sp_biz_type,
    }

    response = client.post("/api/open/product/category/list", payload)
    print("接口返回：")
    print(json.dumps(response, ensure_ascii=False, indent=2))

    rows = extract_category_rows(response)
    if not rows:
        print("未从返回结果中解析出类目列表。")
        return

    df = pd.DataFrame(rows)
    output_path = Path(args.output)
    df.to_excel(output_path, index=False)

    print(f"\n已导出类目结果: {output_path}")
    print(f"类目数量: {len(df)}")


if __name__ == "__main__":
    main()
