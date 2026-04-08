import sqlite3
import json

import pandas as pd
from xianyu_open.stock_utils import parse_total_stock


DB_FILE = "products.db"


def get_connection():
    return sqlite3.connect(DB_FILE)


def load_hot_metrics_df(sources: tuple[str, ...] | None = None) -> pd.DataFrame:
    normalized_sources = tuple(source for source in (sources or ("boardline", "one8")) if str(source or "").strip())
    if not normalized_sources:
        normalized_sources = ("boardline", "one8")
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in normalized_sources)
    rows = conn.execute(
        f"""
        SELECT
            c.product_id,
            c.old_value,
            c.new_value
        FROM change_logs c
        JOIN products p
          ON p.id = c.product_id
        WHERE c.field_name = 'stock'
          AND p.source IN ({placeholders})
        ORDER BY c.change_time ASC, c.id ASC
        """,
        normalized_sources,
    ).fetchall()
    conn.close()

    metrics: dict[int, dict] = {}
    for row in rows:
        product_id = int(row["product_id"])
        old_total = parse_total_stock(row["old_value"])
        new_total = parse_total_stock(row["new_value"])
        if new_total >= old_total:
            continue
        sold_units = old_total - new_total
        if sold_units <= 0:
            continue
        item = metrics.setdefault(product_id, {"drop_events": 0, "sold_units": 0, "raw_score": 0})
        item["drop_events"] += 1
        item["sold_units"] += sold_units

    ranked_rows = []
    for product_id, item in metrics.items():
        raw_score = int(item["sold_units"]) * 10 + int(item["drop_events"]) * 5
        if raw_score <= 0:
            continue
        ranked_rows.append({
            "product_id": int(product_id),
            "drop_events": int(item["drop_events"]),
            "sold_units": int(item["sold_units"]),
            "raw_score": int(raw_score),
        })

    if not ranked_rows:
        return pd.DataFrame(columns=["product_id", "hot_index", "hot_rank", "stock_drop_events", "stock_sold_units"])

    ranked_rows.sort(key=lambda item: (-item["raw_score"], -item["sold_units"], -item["drop_events"], item["product_id"]))
    max_raw = int(ranked_rows[0]["raw_score"])

    for index, item in enumerate(ranked_rows, start=1):
        item["hot_index"] = int(round(item["raw_score"] * 100 / max_raw)) if max_raw > 0 else 0
        item["hot_rank"] = index
        item["stock_drop_events"] = item.pop("drop_events")
        item["stock_sold_units"] = item.pop("sold_units")
        item.pop("raw_score", None)

    return pd.DataFrame(ranked_rows)


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


def _build_ai_image_map(rows: pd.DataFrame, include_fallback_empty: bool = False) -> dict:
    ai_map = {}
    if rows.empty:
        return ai_map

    if include_fallback_empty:
        grouped = rows.groupby(["product_id", "account_name"])
        for (product_id, account_name), group in grouped:
            items = []
            for _, row in group.iterrows():
                items.append({
                    "id": int(row["image_id"]),
                    "path": str(row["ai_main_image_path"] or "").strip(),
                    "oss_url": str(row["oss_url"] or "").strip(),
                    "selected": int(row["is_selected"] or 0),
                    "account_name": str(row["account_name"] or "").strip(),
                })
            ai_map[(int(product_id), str(account_name or ""))] = items
        return ai_map

    for product_id, group in rows.groupby("product_id"):
        items = []
        for _, row in group.iterrows():
            items.append({
                "id": int(row["image_id"]),
                "path": str(row["ai_main_image_path"] or "").strip(),
                "oss_url": str(row["oss_url"] or "").strip(),
                "selected": int(row["is_selected"] or 0),
                "account_name": str(row["account_name"] or "").strip(),
            })
        ai_map[int(product_id)] = items
    return ai_map


def _dedupe_ai_items(items: list[dict]) -> list[dict]:
    deduped = []
    seen = set()
    for item in items:
        key = (
            str(item.get("oss_url") or "").strip(),
            str(item.get("path") or "").strip(),
            str(item.get("account_name") or "").strip(),
            int(item.get("id") or 0),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _resolve_ai_images(ai_map: dict, product_id: int, account_name: str = "", extra_accounts: tuple[str, ...] | None = None) -> list[dict]:
    normalized_account = str(account_name or "").strip()
    normalized_extra_accounts = tuple(
        str(item or "").strip()
        for item in (extra_accounts or ())
        if str(item or "").strip()
    )
    if normalized_account:
        primary = ai_map.get((int(product_id), normalized_account), []) or []
        extras: list[dict] = []
        for extra_account in normalized_extra_accounts:
            extras.extend(ai_map.get((int(product_id), extra_account), []) or [])
        shared = ai_map.get((int(product_id), ""), []) or []
        return _dedupe_ai_items(primary + extras + shared)
    return ai_map.get(int(product_id), []) or ai_map.get((int(product_id), ""), []) or []


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
            COALESCE(t.publish_mode, 'single') AS publish_mode,
            t.group_id,
            COALESCE(t.group_member_product_ids, '') AS group_member_product_ids,
            COALESCE(t.cover_image_path, '') AS cover_image_path,
            COALESCE(t.selected_group_images_json, '') AS selected_group_images_json,
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
        SELECT product_id, COALESCE(account_name, '') AS account_name, id AS image_id, ai_main_image_path, COALESCE(oss_url, '') AS oss_url, is_selected
        FROM xianyu_product_ai_images
        ORDER BY id DESC
    """, conn)
    conn.close()

    ai_map = _build_ai_image_map(ai_rows, include_fallback_empty=True)

    if not df.empty:
        df = df.copy()
        df["ai_images_json"] = df.apply(
            lambda row: json.dumps(
                _resolve_ai_images(ai_map, int(row["product_id"]), str(row["account_name"] or "")),
                ensure_ascii=False,
            ),
            axis=1,
        )
        df["ai_main_image_path"] = df.apply(
            lambda row: (
                _resolve_ai_images(ai_map, int(row["product_id"]), str(row["account_name"] or ""))[0].get("path", "")
                if _resolve_ai_images(ai_map, int(row["product_id"]), str(row["account_name"] or ""))
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
    taobao_account_rows = conn.execute("""
        SELECT DISTINCT COALESCE(shop_name, '') AS shop_name
        FROM taobao_shops
        WHERE COALESCE(shop_name, '') <> ''
    """).fetchall()
    taobao_account_names = tuple(
        str(row["shop_name"] or "").strip()
        for row in taobao_account_rows
        if str(row["shop_name"] or "").strip()
    )
    df = pd.read_sql_query("""
        SELECT
            a.account_name,
            p.id AS product_id,
            p.branduid,
            p.source,
            p.category,
            p.name,
            COALESCE(p.color, '') AS color,
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
            p.source,
            p.category,
            p.name,
            p.color,
            p.first_seen,
            p.image_url,
            u.final_price_cny,
            u.stock,
            ai.ai_title,
            ai.ai_description,
            p.url
        ORDER BY p.category, p.id
    """, conn, params=[account_name])

    ai_account_names = [account_name, "", *taobao_account_names]
    ai_account_names = list(dict.fromkeys(str(item or "").strip() for item in ai_account_names))
    placeholders = ",".join("?" for _ in ai_account_names)
    ai_rows = pd.read_sql_query(f"""
        SELECT
            product_id,
            id AS image_id,
            COALESCE(account_name, '') AS account_name,
            ai_main_image_path,
            COALESCE(oss_url, '') AS oss_url,
            is_selected
        FROM xianyu_product_ai_images
        WHERE COALESCE(account_name, '') IN ({placeholders})
        ORDER BY id DESC
    """, conn, params=ai_account_names)
    conn.close()
    hot_df = load_hot_metrics_df()

    ai_map = _build_ai_image_map(ai_rows, include_fallback_empty=True)

    if not df.empty:
        df = df.copy()
        if not hot_df.empty:
            df = df.merge(hot_df, on="product_id", how="left")
        df["ai_images_json"] = df["product_id"].apply(
            lambda pid: json.dumps(_resolve_ai_images(ai_map, int(pid), account_name, taobao_account_names), ensure_ascii=False)
        )
        df["ai_main_image_path"] = df["product_id"].apply(
            lambda pid: (
                _resolve_ai_images(ai_map, int(pid), account_name, taobao_account_names)[0].get("path", "")
                if _resolve_ai_images(ai_map, int(pid), account_name, taobao_account_names)
                else ""
            )
        )
    else:
        df["ai_images_json"] = []
        df["ai_main_image_path"] = []
    for column in ("hot_index", "hot_rank", "stock_drop_events", "stock_sold_units"):
        if column not in df.columns:
            df[column] = 0
        df[column] = df[column].fillna(0).astype(int)
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
