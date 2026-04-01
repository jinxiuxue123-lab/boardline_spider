import pandas as pd
import sqlite3
from pathlib import Path


DB_FILE = "products.db"


def clean_cell(value, default: str = "") -> str:
    if pd.isna(value):
        return default
    return str(value).strip()


def sync_category_mapping(conn):
    path = Path("xianyu_category_mapping.xlsx")
    if not path.exists():
        return

    df = pd.read_excel(path)
    cur = conn.cursor()
    for _, row in df.iterrows():
        source = clean_cell(row.get("source", "boardline"), "boardline") or "boardline"
        source_category = clean_cell(row.get("source_category", ""))
        if not source_category:
            continue
        item_biz_type = clean_cell(row.get("item_biz_type", ""))
        sp_biz_type = clean_cell(row.get("sp_biz_type", ""))
        channel_cat_id = clean_cell(row.get("channel_cat_id", ""))
        channel_cat_name = clean_cell(row.get("channel_cat_name", ""))
        enabled_raw = row.get("enabled", 1)
        enabled = int(enabled_raw if not pd.isna(enabled_raw) else 1)
        note = clean_cell(row.get("note", ""))

        cur.execute("""
            INSERT INTO xianyu_category_mapping
            (source, source_category, item_biz_type, sp_biz_type, channel_cat_id, channel_cat_name, enabled, note, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(source, source_category) DO UPDATE SET
                item_biz_type = excluded.item_biz_type,
                sp_biz_type = excluded.sp_biz_type,
                channel_cat_id = excluded.channel_cat_id,
                channel_cat_name = excluded.channel_cat_name,
                enabled = excluded.enabled,
                note = excluded.note,
                updated_at = CURRENT_TIMESTAMP
        """, (source, source_category, item_biz_type, sp_biz_type, channel_cat_id, channel_cat_name, enabled, note))


def sync_publish_defaults(conn):
    path = Path("xianyu_publish_defaults.xlsx")
    if not path.exists():
        return

    df = pd.read_excel(path)
    cur = conn.cursor()
    for _, row in df.iterrows():
        key_name = clean_cell(row.get("key_name", ""))
        if not key_name:
            continue
        key_value = clean_cell(row.get("key_value", ""))
        note = clean_cell(row.get("note", ""))

        cur.execute("""
            INSERT INTO xianyu_publish_defaults
            (key_name, key_value, note, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key_name) DO UPDATE SET
                key_value = excluded.key_value,
                note = excluded.note,
                updated_at = CURRENT_TIMESTAMP
        """, (key_name, key_value, note))


def sync_accounts(conn):
    path = Path("xianyu_accounts.xlsx")
    if not path.exists():
        return

    df = pd.read_excel(path)
    cur = conn.cursor()
    for _, row in df.iterrows():
        account_name = clean_cell(row.get("account_name", ""))
        if not account_name:
            continue

        enabled_raw = row.get("enabled", 1)
        enabled = int(enabled_raw if not pd.isna(enabled_raw) else 1)

        cur.execute("""
            INSERT INTO xianyu_accounts (
                account_name,
                app_key,
                app_secret,
                merchant_id,
                user_name,
                province,
                city,
                district,
                item_biz_type,
                sp_biz_type,
                stuff_status,
                channel_pv_json,
                enabled,
                note,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(account_name) DO UPDATE SET
                app_key = excluded.app_key,
                app_secret = excluded.app_secret,
                merchant_id = excluded.merchant_id,
                user_name = excluded.user_name,
                province = excluded.province,
                city = excluded.city,
                district = excluded.district,
                item_biz_type = excluded.item_biz_type,
                sp_biz_type = excluded.sp_biz_type,
                stuff_status = excluded.stuff_status,
                channel_pv_json = excluded.channel_pv_json,
                enabled = excluded.enabled,
                note = excluded.note,
                updated_at = CURRENT_TIMESTAMP
        """, (
            account_name,
            clean_cell(row.get("app_key", "")),
            clean_cell(row.get("app_secret", "")),
            clean_cell(row.get("merchant_id", "")),
            clean_cell(row.get("user_name", "")),
            clean_cell(row.get("province", "")),
            clean_cell(row.get("city", "")),
            clean_cell(row.get("district", "")),
            clean_cell(row.get("item_biz_type", ""), "1") or "1",
            clean_cell(row.get("sp_biz_type", ""), "0") or "0",
            clean_cell(row.get("stuff_status", ""), "1") or "1",
            clean_cell(row.get("channel_pv_json", ""), "[]") or "[]",
            enabled,
            clean_cell(row.get("note", "")),
        ))


def main():
    conn = sqlite3.connect(DB_FILE)
    sync_category_mapping(conn)
    sync_publish_defaults(conn)
    sync_accounts(conn)
    conn.commit()
    conn.close()
    print("已同步闲鱼开放平台配置到数据库")


if __name__ == "__main__":
    main()
