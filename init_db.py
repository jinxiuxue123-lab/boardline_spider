import sqlite3
from pathlib import Path

DB_PATH = "products.db"


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ==========================
    # 1. 商品主表（升级版）
    # ==========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        source TEXT NOT NULL,
        branduid TEXT NOT NULL,

        category TEXT,
        name TEXT,
        color TEXT,
        url TEXT NOT NULL,

        -- 原始图片URL
        image_url TEXT,

        -- 本地图片路径（关键新增）
        local_image_path TEXT,

        -- 图片是否已下载（0=未下载，1=已下载）
        image_downloaded INTEGER DEFAULT 0,

        -- 是否已成功抓取详情页主图（0=未抓，1=已抓）
        detail_image_fetched INTEGER DEFAULT 0,

        status TEXT DEFAULT 'active',

        first_seen TEXT,
        last_seen TEXT,
        missing_days INTEGER DEFAULT 0,

        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,

        UNIQUE(source, branduid)
    )
    """)

    # ==========================
    # 2. 商品最新动态（库存/价格）
    # ==========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product_updates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,

        price TEXT,
        original_price TEXT,
        latest_discount_price TEXT,
        price_cny TEXT,
        original_price_cny TEXT,
        shipping_fee_cny TEXT,
        final_price_cny TEXT,
        exchange_rate TEXT,
        profit_rate TEXT,
        stock TEXT,

        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY(product_id) REFERENCES products(id)
    )
    """)

    # 每个商品只保留一条最新数据
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_product_updates_product_id
    ON product_updates(product_id)
    """)

    # ==========================
    # 3. 变化日志
    # ==========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS change_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,

        field_name TEXT NOT NULL,
        old_value TEXT,
        new_value TEXT,

        change_time TEXT DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY(product_id) REFERENCES products(id)
    )
    """)

    # ==========================
    # 4. 抓取任务日志
    # ==========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS crawl_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        run_type TEXT NOT NULL,
        source TEXT NOT NULL,

        start_time TEXT DEFAULT CURRENT_TIMESTAMP,
        end_time TEXT,

        total_count INTEGER DEFAULT 0,
        success_count INTEGER DEFAULT 0,
        failed_count INTEGER DEFAULT 0,

        status TEXT DEFAULT 'running',
        note TEXT
    )
    """)

    # ==========================
    # 5. 闲鱼发布任务表（提前帮你加好）
    # ==========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS xianyu_publish_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        product_id INTEGER NOT NULL,

        ai_title TEXT,
        ai_description TEXT,

        cover_image_path TEXT,

        publish_price REAL,

        status TEXT DEFAULT 'pending',

        last_error TEXT,
        retry_count INTEGER DEFAULT 0,

        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY(product_id) REFERENCES products(id)
    )
    """)

    cursor.execute("""
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS xianyu_publish_defaults (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key_name TEXT NOT NULL UNIQUE,
        key_value TEXT,
        note TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS xianyu_product_ai_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        account_name TEXT DEFAULT '',
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
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_product_ai_images_product_account
    ON xianyu_product_ai_images(product_id, account_name)
    """)

    cursor.execute("""
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS taobao_shops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_name TEXT NOT NULL UNIQUE,
        app_key TEXT NOT NULL,
        app_secret TEXT NOT NULL,
        redirect_uri TEXT NOT NULL,
        browser_profile_dir TEXT,
        chrome_user_data_dir TEXT,
        chrome_profile_name TEXT,
        chrome_cdp_url TEXT,
        login_url TEXT,
        publish_url TEXT,
        seller_nick TEXT,
        taobao_user_id TEXT,
        access_token TEXT,
        refresh_token TEXT,
        token_expires_at TEXT,
        refresh_token_expires_at TEXT,
        auth_status TEXT DEFAULT 'pending',
        last_auth_at TEXT,
        last_error TEXT,
        enabled INTEGER DEFAULT 1,
        note TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
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

    cursor.execute("""
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

    cursor.execute("""
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

    cursor.execute("""
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS xianyu_product_ai_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL UNIQUE,
        ai_main_image_path TEXT,
        source_image_path TEXT,
        provider TEXT,
        model_name TEXT,
        prompt_text TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS xianyu_product_ai_copy (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL UNIQUE,
        ai_title TEXT,
        ai_description TEXT,
        source TEXT DEFAULT 'gemini',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(product_id) REFERENCES products(id)
    )
    """)

    # ==========================
    # 索引优化
    # ==========================
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_products_source_branduid
    ON products(source, branduid)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_products_status
    ON products(status)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_products_image_downloaded
    ON products(image_downloaded)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_change_logs_product_id
    ON change_logs(product_id)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_publish_tasks_status
    ON xianyu_publish_tasks(status)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_category_mapping_source_category
    ON xianyu_category_mapping(source, source_category)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_category_properties_source_category
    ON xianyu_category_properties(source, source_category)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_product_property_values_product
    ON xianyu_product_property_values(product_id)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_product_ai_copy_product
    ON xianyu_product_ai_copy(product_id)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_accounts_enabled
    ON xianyu_accounts(enabled)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_batches_account_id
    ON xianyu_publish_batches(account_id)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_callbacks_product_id
    ON xianyu_callbacks(third_product_id)
    """)

    # 兼容已有数据库：补齐新增字段
    cursor.execute("PRAGMA table_info(product_updates)")
    columns = {row[1] for row in cursor.fetchall()}
    if "latest_discount_price" not in columns:
        cursor.execute("""
        ALTER TABLE product_updates
        ADD COLUMN latest_discount_price TEXT
        """)
    if "price_cny" not in columns:
        cursor.execute("""
        ALTER TABLE product_updates
        ADD COLUMN price_cny TEXT
        """)
    if "original_price_cny" not in columns:
        cursor.execute("""
        ALTER TABLE product_updates
        ADD COLUMN original_price_cny TEXT
        """)
    if "shipping_fee_cny" not in columns:
        cursor.execute("""
        ALTER TABLE product_updates
        ADD COLUMN shipping_fee_cny TEXT
        """)
    if "final_price_cny" not in columns:
        cursor.execute("""
        ALTER TABLE product_updates
        ADD COLUMN final_price_cny TEXT
        """)

    cursor.execute("PRAGMA table_info(products)")
    product_columns = {row[1] for row in cursor.fetchall()}
    if "detail_image_fetched" not in product_columns:
        cursor.execute("""
        ALTER TABLE products
        ADD COLUMN detail_image_fetched INTEGER DEFAULT 0
        """)
    if "color" not in product_columns:
        cursor.execute("""
        ALTER TABLE products
        ADD COLUMN color TEXT
        """)
    if "exchange_rate" not in columns:
        cursor.execute("""
        ALTER TABLE product_updates
        ADD COLUMN exchange_rate TEXT
        """)
    if "profit_rate" not in columns:
        cursor.execute("""
        ALTER TABLE product_updates
        ADD COLUMN profit_rate TEXT
        """)

    cursor.execute("PRAGMA table_info(xianyu_publish_tasks)")
    xianyu_columns = {row[1] for row in cursor.fetchall()}
    extra_xianyu_columns = {
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
    for name, col_type in extra_xianyu_columns.items():
        if name not in xianyu_columns:
            cursor.execute(f"ALTER TABLE xianyu_publish_tasks ADD COLUMN {name} {col_type}")

    cursor.execute("PRAGMA table_info(xianyu_category_mapping)")
    mapping_columns = {row[1] for row in cursor.fetchall()}
    if "item_biz_type" not in mapping_columns:
        cursor.execute("ALTER TABLE xianyu_category_mapping ADD COLUMN item_biz_type TEXT")
    if "sp_biz_type" not in mapping_columns:
        cursor.execute("ALTER TABLE xianyu_category_mapping ADD COLUMN sp_biz_type TEXT")

    cursor.execute("DROP INDEX IF EXISTS idx_xianyu_tasks_account_product")

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_tasks_batch_id
    ON xianyu_publish_tasks(batch_id)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_xianyu_tasks_account_product
    ON xianyu_publish_tasks(account_id, product_id)
    """)

    conn.commit()
    conn.close()

    print(f"数据库初始化完成: {Path(DB_PATH).resolve()}")


if __name__ == "__main__":
    init_db()
