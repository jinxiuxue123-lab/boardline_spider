import sqlite3
from pathlib import Path

import pandas as pd


DB_FILE = "products.db"


def ensure_tables():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_category_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_category TEXT NOT NULL,
            item_biz_type TEXT,
            sp_biz_type TEXT,
            channel_cat_id TEXT,
            channel_cat_name TEXT,
            enabled INTEGER DEFAULT 1,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, source_category)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_publish_defaults (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_name TEXT NOT NULL UNIQUE,
            key_value TEXT,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_name TEXT NOT NULL UNIQUE,
            app_key TEXT,
            app_secret TEXT,
            merchant_id TEXT,
            user_name TEXT,
            province TEXT,
            city TEXT,
            district TEXT,
            item_biz_type TEXT DEFAULT '1',
            sp_biz_type TEXT DEFAULT '0',
            stuff_status TEXT DEFAULT '1',
            channel_pv_json TEXT DEFAULT '[]',
            enabled INTEGER DEFAULT 1,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_publish_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            batch_name TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            total_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(account_id, batch_name),
            FOREIGN KEY(account_id) REFERENCES xianyu_accounts(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_callbacks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            third_product_id TEXT,
            callback_type TEXT,
            callback_payload TEXT NOT NULL,
            callback_status TEXT,
            err_code TEXT,
            err_msg TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            processed_at TEXT,
            FOREIGN KEY(task_id) REFERENCES xianyu_publish_tasks(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_category_properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_category TEXT NOT NULL,
            channel_cat_id TEXT,
            property_id TEXT NOT NULL,
            property_name TEXT,
            required INTEGER DEFAULT 0,
            input_mode TEXT,
            options_json TEXT DEFAULT '[]',
            raw_json TEXT,
            sort_order INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, source_category, property_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_product_property_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            source_category TEXT,
            property_id TEXT NOT NULL,
            property_name TEXT,
            value_id TEXT,
            value_name TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_id, property_id),
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_product_publish_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL UNIQUE,
            stuff_status TEXT,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
    """)

    cur.execute("PRAGMA table_info(xianyu_category_mapping)")
    mapping_columns = {row[1] for row in cur.fetchall()}
    if "item_biz_type" not in mapping_columns:
        cur.execute("ALTER TABLE xianyu_category_mapping ADD COLUMN item_biz_type TEXT")
    if "sp_biz_type" not in mapping_columns:
        cur.execute("ALTER TABLE xianyu_category_mapping ADD COLUMN sp_biz_type TEXT")

    extra_columns = {
        "account_id": "INTEGER",
        "batch_id": "INTEGER",
        "channel_cat_id": "TEXT",
        "channel_cat_name": "TEXT",
        "channel_pv_json": "TEXT",
        "publish_payload_json": "TEXT",
        "third_product_id": "TEXT",
        "publish_status": "TEXT",
        "task_result": "TEXT",
        "err_code": "TEXT",
        "err_msg": "TEXT",
        "callback_raw": "TEXT",
        "callback_status": "TEXT",
        "published_at": "TEXT",
        "last_callback_time": "TEXT",
        "off_shelved_at": "TEXT",
    }
    cur.execute("DROP INDEX IF EXISTS idx_xianyu_tasks_account_product")

    cur.execute("PRAGMA table_info(xianyu_publish_tasks)")
    existing = {row[1] for row in cur.fetchall()}
    for name, col_type in extra_columns.items():
        if name not in existing:
            cur.execute(f"ALTER TABLE xianyu_publish_tasks ADD COLUMN {name} {col_type}")

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_xianyu_tasks_account_product
        ON xianyu_publish_tasks(account_id, product_id)
    """)

    cur.execute("SELECT DISTINCT category FROM products WHERE source='boardline' ORDER BY category")
    categories = [row[0] for row in cur.fetchall() if row[0]]
    for category in categories:
        cur.execute("""
            INSERT OR IGNORE INTO xianyu_category_mapping
            (source, source_category, enabled)
            VALUES ('boardline', ?, 1)
        """, (category,))

    defaults = [
        ("item_biz_type", "1", "闲鱼商品业务类型"),
        ("sp_biz_type", "0", "闲鱼扩展业务类型"),
        ("stuff_status", "1", "成色，1 表示全新"),
        ("user_name", "", "卖家昵称"),
        ("province", "", "省份"),
        ("city", "", "城市"),
        ("district", "", "区县"),
        ("channel_pv_json", "[]", "默认商品属性 JSON 数组"),
        ("service_support", "NFR", "商品服务项，多个时用英文逗号分隔，例如 SDR,NFR"),
        ("callback_url", "", "异步回调地址，当前仅留作记录"),
    ]
    for key, value, note in defaults:
        cur.execute("""
            INSERT OR IGNORE INTO xianyu_publish_defaults
            (key_name, key_value, note)
            VALUES (?, ?, ?)
        """, (key, value, note))

    conn.commit()
    conn.close()


def export_templates():
    conn = sqlite3.connect(DB_FILE)

    mapping_df = pd.read_sql_query("""
        SELECT
            source,
            source_category,
            item_biz_type,
            sp_biz_type,
            channel_cat_id,
            channel_cat_name,
            enabled,
            note
        FROM xianyu_category_mapping
        ORDER BY source, source_category
    """, conn)

    defaults_df = pd.read_sql_query("""
        SELECT
            key_name,
            key_value,
            note
        FROM xianyu_publish_defaults
        ORDER BY id
    """, conn)

    accounts_df = pd.read_sql_query("""
        SELECT
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
            note
        FROM xianyu_accounts
        ORDER BY id
    """, conn)
    conn.close()

    mapping_df.to_excel("xianyu_category_mapping.xlsx", index=False)
    defaults_df.to_excel("xianyu_publish_defaults.xlsx", index=False)
    accounts_df.to_excel("xianyu_accounts.xlsx", index=False)
    if accounts_df.empty:
        pd.DataFrame([
            {
                "account_name": "闲鱼账号1",
                "app_key": "",
                "app_secret": "",
                "merchant_id": "",
                "user_name": "",
                "province": "",
                "city": "",
                "district": "",
                "item_biz_type": "1",
                "sp_biz_type": "0",
                "stuff_status": "1",
                "channel_pv_json": "[]",
                "enabled": 1,
                "note": "",
            }
        ]).to_excel("xianyu_accounts.xlsx", index=False)

    selection_template = pd.DataFrame([
        {
            "batch_name": "2026-03-24-批次1",
            "account_name": "闲鱼账号1",
            "product_id": "",
            "branduid": "",
            "enabled": 1,
            "note": "",
        }
    ])
    selection_template.to_excel("xianyu_batch_selection.xlsx", index=False)


def ensure_parent_package():
    Path("xianyu_open").mkdir(exist_ok=True)


def main():
    ensure_parent_package()
    ensure_tables()
    export_templates()
    print("已初始化闲鱼开放平台配置")
    print("已生成: xianyu_category_mapping.xlsx")
    print("已生成: xianyu_publish_defaults.xlsx")
    print("已生成: xianyu_accounts.xlsx")
    print("已生成: xianyu_batch_selection.xlsx")


if __name__ == "__main__":
    main()
