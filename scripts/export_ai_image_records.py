#!/usr/bin/env python3
import argparse
import json
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="导出 AI 图片元数据，便于跨环境同步 OSS 图记录。")
    parser.add_argument("--db", default="products.db", help="SQLite 数据库路径")
    parser.add_argument("--output", required=True, help="导出 JSON 文件路径")
    parser.add_argument("--account-name", default="", help="只导出指定账号名的记录；为空时导出全部")
    parser.add_argument("--shared-only", action="store_true", help="只导出共享记录（account_name=''）")
    parser.add_argument("--with-empty-oss", action="store_true", help="默认只导出有 oss_url 的记录；开启后允许导出无 oss_url 记录")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    where = []
    params = []
    if not args.with_empty_oss:
        where.append("COALESCE(oss_url, '') <> ''")
    if args.shared_only:
        where.append("COALESCE(account_name, '') = ''")
    elif str(args.account_name or "").strip():
        where.append("COALESCE(account_name, '') = ?")
        params.append(str(args.account_name).strip())
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""
        SELECT
            product_id,
            COALESCE(account_name, '') AS account_name,
            COALESCE(asset_type, 'main') AS asset_type,
            COALESCE(ai_main_image_path, '') AS ai_main_image_path,
            COALESCE(oss_url, '') AS oss_url,
            COALESCE(source_image_path, '') AS source_image_path,
            COALESCE(provider, '') AS provider,
            COALESCE(model_name, '') AS model_name,
            COALESCE(prompt_text, '') AS prompt_text,
            COALESCE(is_selected, 0) AS is_selected,
            COALESCE(created_at, '') AS created_at,
            COALESCE(updated_at, '') AS updated_at
        FROM xianyu_product_ai_images
        {where_sql}
        ORDER BY id ASC
        """,
        params,
    ).fetchall()
    conn.close()

    output_path = Path(args.output).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "count": len(rows),
        "items": [dict(row) for row in rows],
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已导出 AI 图片记录: {output_path} | 条数: {len(rows)}")


if __name__ == "__main__":
    main()
