#!/usr/bin/env python3
import argparse
import json
import sqlite3


def ensure_ai_image_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS xianyu_product_ai_images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            account_name TEXT DEFAULT '',
            asset_type TEXT DEFAULT 'main',
            ai_main_image_path TEXT,
            oss_url TEXT,
            source_image_path TEXT,
            provider TEXT,
            model_name TEXT,
            prompt_text TEXT,
            is_selected INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_xianyu_product_ai_images_product_account
        ON xianyu_product_ai_images(product_id, account_name)
        """
    )


def parse_args():
    parser = argparse.ArgumentParser(description="导入 AI 图片元数据，用于跨环境同步 OSS 图记录。")
    parser.add_argument("--db", default="products.db", help="SQLite 数据库路径")
    parser.add_argument("--input", required=True, help="导入 JSON 文件路径")
    parser.add_argument("--overwrite-selected", action="store_true", help="导入时覆盖同一记录的 is_selected 状态")
    return parser.parse_args()


def main():
    args = parse_args()
    with open(args.input, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    items = payload.get("items") or []

    conn = sqlite3.connect(args.db)
    ensure_ai_image_table(conn)
    inserted = 0
    updated = 0
    skipped = 0
    for item in items:
        product_id = int(item.get("product_id") or 0)
        account_name = str(item.get("account_name") or "").strip()
        asset_type = str(item.get("asset_type") or "main").strip() or "main"
        ai_main_image_path = str(item.get("ai_main_image_path") or "").strip()
        oss_url = str(item.get("oss_url") or "").strip()
        source_image_path = str(item.get("source_image_path") or "").strip()
        provider = str(item.get("provider") or "").strip()
        model_name = str(item.get("model_name") or "").strip()
        prompt_text = str(item.get("prompt_text") or "").strip()
        is_selected = int(item.get("is_selected") or 0)
        if product_id <= 0:
            skipped += 1
            continue
        existing = conn.execute(
            """
            SELECT id, COALESCE(is_selected, 0) AS is_selected
            FROM xianyu_product_ai_images
            WHERE product_id = ?
              AND COALESCE(account_name, '') = ?
              AND COALESCE(asset_type, 'main') = ?
              AND COALESCE(oss_url, '') = ?
              AND COALESCE(ai_main_image_path, '') = ?
            LIMIT 1
            """,
            (product_id, account_name, asset_type, oss_url, ai_main_image_path),
        ).fetchone()
        if existing:
            if args.overwrite_selected and int(existing[1] or 0) != is_selected:
                conn.execute(
                    """
                    UPDATE xianyu_product_ai_images
                    SET is_selected = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (is_selected, int(existing[0])),
                )
                updated += 1
            else:
                skipped += 1
            continue
        conn.execute(
            """
            INSERT INTO xianyu_product_ai_images (
                product_id, account_name, asset_type, ai_main_image_path, oss_url, source_image_path, provider, model_name, prompt_text, is_selected, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (product_id, account_name, asset_type, ai_main_image_path, oss_url, source_image_path, provider, model_name, prompt_text, is_selected),
        )
        inserted += 1
    conn.commit()
    conn.close()
    print(f"导入完成 | 新增: {inserted} | 更新: {updated} | 跳过: {skipped}")


if __name__ == "__main__":
    main()
