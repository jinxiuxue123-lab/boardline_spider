import sqlite3
import json

import pandas as pd


DB_FILE = "products.db"


def get_connection():
    return sqlite3.connect(DB_FILE)


def ensure_ai_image_table():
    conn = get_connection()
    cur = conn.cursor()
    exists = cur.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='xianyu_product_ai_images'
    """).fetchone()

    def create_table():
        cur.execute("""
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
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_xianyu_product_ai_images_product_account
            ON xianyu_product_ai_images(product_id, account_name)
        """)

    if not exists:
        create_table()
        conn.commit()
        conn.close()
        return

    columns = [row[1] for row in cur.execute("PRAGMA table_info(xianyu_product_ai_images)").fetchall()]
    if "account_name" not in columns or "is_selected" not in columns:
        cur.execute("ALTER TABLE xianyu_product_ai_images RENAME TO xianyu_product_ai_images_old")
        create_table()
        cur.execute("""
            INSERT INTO xianyu_product_ai_images (
                product_id, account_name, ai_main_image_path, oss_url, source_image_path,
                provider, model_name, prompt_text, is_selected, created_at, updated_at
            )
            SELECT
                product_id,
                '',
                ai_main_image_path,
                '',
                source_image_path,
                COALESCE(provider, ''),
                COALESCE(model_name, ''),
                COALESCE(prompt_text, ''),
                0,
                COALESCE(created_at, CURRENT_TIMESTAMP),
                COALESCE(updated_at, CURRENT_TIMESTAMP)
            FROM xianyu_product_ai_images_old
        """)
        cur.execute("DROP TABLE xianyu_product_ai_images_old")
    else:
        create_table()
        if "oss_url" not in columns:
            cur.execute("ALTER TABLE xianyu_product_ai_images ADD COLUMN oss_url TEXT")
    conn.commit()
    conn.close()


def ensure_account_ai_copy_support():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_account_product_ai_copy (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            ai_title TEXT,
            ai_description TEXT,
            source TEXT DEFAULT 'gemini',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_id, account_name)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_xianyu_account_product_ai_copy_product_account
        ON xianyu_account_product_ai_copy(product_id, account_name)
    """)
    columns = [row[1] for row in cur.execute("PRAGMA table_info(xianyu_accounts)").fetchall()]
    if "independent_ai_assets" not in columns:
        cur.execute("ALTER TABLE xianyu_accounts ADD COLUMN independent_ai_assets INTEGER DEFAULT 0")
    conn.commit()
    conn.close()


def load_account_summary(account_name: str = "") -> pd.DataFrame:
    ensure_account_ai_copy_support()
    conn = get_connection()
    where_sql = ""
    params = []
    if account_name.strip():
        where_sql = "WHERE a.account_name = ?"
        params.append(account_name.strip())

    df = pd.read_sql_query(f"""
        SELECT
            a.account_name,
            COUNT(t.id) AS task_count,
            SUM(CASE WHEN t.status IN ('created', 'submitted', 'published') THEN 1 ELSE 0 END) AS uploaded_count,
            SUM(CASE WHEN t.status = 'published' THEN 1 ELSE 0 END) AS published_count,
            SUM(CASE WHEN t.status IN ('failed', 'publish_failed') THEN 1 ELSE 0 END) AS failed_count,
            SUM(CASE WHEN t.status = 'pending' THEN 1 ELSE 0 END) AS pending_count
        FROM xianyu_accounts a
        LEFT JOIN xianyu_publish_tasks t
          ON t.account_id = a.id
        {where_sql}
        GROUP BY a.id, a.account_name
        ORDER BY a.account_name
    """, conn, params=params)
    conn.close()
    return df


def load_task_details(account_name: str = "") -> pd.DataFrame:
    ensure_ai_image_table()
    ensure_account_ai_copy_support()
    conn = get_connection()
    where_sql = ""
    params = []
    if account_name.strip():
        where_sql = "WHERE a.account_name = ?"
        params.append(account_name.strip())

    df = pd.read_sql_query(f"""
        SELECT
            a.account_name,
            b.id AS batch_id,
            b.batch_name,
            b.status AS batch_status,
            t.id AS task_id,
            t.status,
            t.publish_status,
            t.callback_status,
            t.third_product_id,
            t.task_result,
            COALESCE(
                NULLIF(TRIM(t.ai_title), ''),
                CASE
                    WHEN COALESCE(a.independent_ai_assets, 0) = 1 THEN ai_account.ai_title
                    ELSE ai.ai_title
                END,
                ''
            ) AS ai_title,
            COALESCE(
                NULLIF(TRIM(t.ai_description), ''),
                CASE
                    WHEN COALESCE(a.independent_ai_assets, 0) = 1 THEN ai_account.ai_description
                    ELSE ai.ai_description
                END,
                ''
            ) AS ai_description,
            t.err_code,
            t.err_msg,
            p.id AS product_id,
            p.branduid,
            p.category,
            p.name,
            p.image_url,
            u.final_price_cny,
            u.stock,
            t.created_at,
            t.updated_at,
            t.published_at
        FROM xianyu_publish_tasks t
        LEFT JOIN xianyu_accounts a
          ON a.id = t.account_id
        LEFT JOIN xianyu_publish_batches b
          ON b.id = t.batch_id
        JOIN products p
          ON p.id = t.product_id
        LEFT JOIN xianyu_product_ai_copy ai
          ON ai.product_id = p.id
        LEFT JOIN xianyu_account_product_ai_copy ai_account
          ON ai_account.product_id = p.id
         AND ai_account.account_name = a.account_name
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        {where_sql}
        ORDER BY a.account_name, b.id, t.id
    """, conn, params=params)

    ai_rows = pd.read_sql_query("""
        SELECT product_id, COALESCE(account_name, '') AS account_name, id AS image_id, ai_main_image_path, is_selected
        FROM xianyu_product_ai_images
        ORDER BY id DESC
    """, conn)
    conn.close()

    ai_map = {}
    if not ai_rows.empty:
        for (product_id, image_account_name), group in ai_rows.groupby(["product_id", "account_name"]):
            items = []
            for _, row in group.iterrows():
                items.append({
                    "id": int(row["image_id"]),
                    "path": str(row["ai_main_image_path"] or "").strip(),
                    "selected": int(row["is_selected"] or 0),
                })
            ai_map[(int(product_id), str(image_account_name or ""))] = items

    if not df.empty:
        df = df.copy()
        df["ai_images_json"] = df.apply(
            lambda row: json.dumps(
                ai_map.get((int(row["product_id"]), str(row["account_name"] or "")), []),
                ensure_ascii=False,
            ),
            axis=1,
        )
        df["ai_main_image_path"] = df.apply(
            lambda row: (
                ai_map.get((int(row["product_id"]), str(row["account_name"] or "")), [{}])[0].get("path", "")
                if ai_map.get((int(row["product_id"]), str(row["account_name"] or "")))
                else ""
            ),
            axis=1,
        )
    else:
        df["ai_images_json"] = []
        df["ai_main_image_path"] = []
    return df


def load_account_product_pool(account_name: str) -> pd.DataFrame:
    ensure_ai_image_table()
    ensure_account_ai_copy_support()
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            a.account_name,
            p.id AS product_id,
            p.branduid,
            p.category,
            p.name,
            p.first_seen,
            p.image_url,
            u.final_price_cny,
            u.stock,
            CASE
                WHEN COALESCE(a.independent_ai_assets, 0) = 1 THEN COALESCE(ai_account.ai_title, '')
                ELSE COALESCE(ai.ai_title, '')
            END AS ai_title,
            CASE
                WHEN COALESCE(a.independent_ai_assets, 0) = 1 THEN COALESCE(ai_account.ai_description, '')
                ELSE COALESCE(ai.ai_description, '')
            END AS ai_description,
            p.url,
            COUNT(t.id) AS upload_count,
            MAX(t.id) AS latest_task_id,
            COALESCE((
                SELECT tt.status
                FROM xianyu_publish_tasks tt
                WHERE tt.account_id = a.id
                  AND tt.product_id = p.id
                ORDER BY tt.id DESC
                LIMIT 1
            ), '') AS latest_status,
            COALESCE((
                SELECT tt.publish_status
                FROM xianyu_publish_tasks tt
                WHERE tt.account_id = a.id
                  AND tt.product_id = p.id
                ORDER BY tt.id DESC
                LIMIT 1
            ), '') AS latest_publish_status
        FROM xianyu_accounts a
        JOIN products p
          ON 1 = 1
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        LEFT JOIN xianyu_product_ai_copy ai
          ON ai.product_id = p.id
        LEFT JOIN xianyu_account_product_ai_copy ai_account
          ON ai_account.product_id = p.id
         AND ai_account.account_name = a.account_name
        LEFT JOIN xianyu_publish_tasks t
          ON t.account_id = a.id
         AND t.product_id = p.id
        WHERE a.account_name = ?
          AND a.enabled = 1
          AND p.status = 'active'
        GROUP BY
            a.account_name,
            a.id,
            p.id,
            p.branduid,
            p.category,
            p.name,
            p.first_seen,
            p.image_url,
            u.final_price_cny,
            u.stock,
            ai.ai_title,
            ai.ai_description,
            p.url
        ORDER BY p.category, p.id
    """, conn, params=[account_name])

    ai_rows = pd.read_sql_query("""
        SELECT
            product_id,
            id AS image_id,
            COALESCE(account_name, '') AS account_name,
            ai_main_image_path,
            is_selected
        FROM xianyu_product_ai_images
        WHERE COALESCE(account_name, '') = ?
        ORDER BY id DESC
    """, conn, params=[account_name])
    conn.close()

    ai_map = {}
    if not ai_rows.empty:
        for product_id, group in ai_rows.groupby("product_id"):
            items = []
            for _, row in group.iterrows():
                items.append({
                    "id": int(row["image_id"]),
                    "path": str(row["ai_main_image_path"] or "").strip(),
                    "selected": int(row["is_selected"] or 0),
                    "account_name": str(row["account_name"] or "").strip(),
                })
            ai_map[int(product_id)] = items

    if not df.empty:
        df = df.copy()
        df["ai_images_json"] = df["product_id"].apply(lambda pid: json.dumps(ai_map.get(int(pid), []), ensure_ascii=False))
        df["ai_main_image_path"] = df["product_id"].apply(
            lambda pid: (ai_map.get(int(pid), [{}])[0].get("path", "") if ai_map.get(int(pid)) else "")
        )
    else:
        df["ai_images_json"] = []
        df["ai_main_image_path"] = []
    return df


def load_batches(account_name: str = "") -> pd.DataFrame:
    conn = get_connection()
    where_sql = ""
    params = []
    if account_name.strip():
        where_sql = "WHERE a.account_name = ?"
        params.append(account_name.strip())

    df = pd.read_sql_query(f"""
        SELECT
            b.id AS batch_id,
            a.account_name,
            b.batch_name,
            b.status,
            b.total_count,
            b.success_count,
            b.failed_count,
            b.created_at,
            b.updated_at
        FROM xianyu_publish_batches b
        JOIN xianyu_accounts a
          ON a.id = b.account_id
        {where_sql}
        ORDER BY b.id DESC
    """, conn, params=params)
    conn.close()
    return df
