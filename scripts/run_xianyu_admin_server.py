import argparse
import html
import json
import math
import mimetypes
import os
import re
import sqlite3
import sys
import threading
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open.reporting import (
    load_account_product_pool,
    load_account_summary,
    load_batches,
    load_hot_metrics_df,
    load_task_details,
)
from xianyu_open.batch_runner import execute_batch, reconcile_remote_created_tasks
from xianyu_open.downshelf import execute_task_downshelf
from xianyu_open.delete_product import execute_task_delete_product
from xianyu_open.payload_builder import build_sku_items, get_publish_task
from xianyu_open.image_pipeline import build_standard_png_variant
from xianyu_open.task_ops import update_batch_counts
from services.product_image_ai_service import (
    build_watermarked_upload_variant,
    ensure_ai_image_table,
    generate_ai_detail_image,
    generate_group_ai_cover_image,
    generate_ai_main_image,
    list_ai_images,
    load_existing_ai_image,
    set_selected_ai_images,
)
from product_grouping import ensure_xianyu_group_task_support, find_group_by_member_ids, find_group_by_member_ids_relaxed, refresh_one8_product_groups
from taobao_browser import (
    build_publish_assist_payload,
    launch_login_browser,
    launch_publish_assistant,
    launch_publish_assistants,
)
from scripts.generate_xianyu_ai_copy import (
    ensure_ai_copy_table,
    load_product_attributes,
    generate_description_for_product,
    generate_descriptions_for_products,
    generate_description_for_task,
    generate_descriptions_for_batch,
    generate_for_batch,
    generate_for_product,
    generate_title_for_product,
    generate_titles_for_products,
    generate_title_for_task,
    generate_titles_for_batch,
)
from services.material_ai_service import build_xianyu_description, build_xianyu_title


DB_FILE = "products.db"
PAGE_SIZE = 30
DEFAULT_SHIPPING_REGION_GROUP_SIZE = 10
DEFAULT_MULTI_SHIPPING_REGIONS = [
    {"province": "110000", "city": "110100", "district": "110105", "label": "北京朝阳"},
    {"province": "310000", "city": "310100", "district": "310115", "label": "上海浦东"},
    {"province": "440000", "city": "440100", "district": "440106", "label": "广州天河"},
    {"province": "440000", "city": "440300", "district": "440305", "label": "深圳南山"},
    {"province": "330000", "city": "330100", "district": "330106", "label": "杭州西湖"},
    {"province": "510000", "city": "510100", "district": "510104", "label": "成都锦江"},
    {"province": "320000", "city": "320100", "district": "320106", "label": "南京鼓楼"},
    {"province": "420000", "city": "420100", "district": "420106", "label": "武汉武昌"},
    {"province": "500000", "city": "500100", "district": "500103", "label": "重庆渝中"},
    {"province": "320000", "city": "320500", "district": "320508", "label": "苏州姑苏"},
]
AI_IMAGE_JOBS: dict[str, dict] = {}
AI_IMAGE_JOBS_LOCK = threading.Lock()
BATCH_EXEC_JOBS: dict[str, dict] = {}
BATCH_EXEC_JOBS_LOCK = threading.Lock()
DELETE_BATCH_JOBS: dict[str, dict] = {}
DELETE_BATCH_JOBS_LOCK = threading.Lock()
BEIJING_TZ = timezone(timedelta(hours=8))
TAOBAO_OAUTH_AUTHORIZE_URL = os.getenv("TAOBAO_OAUTH_AUTHORIZE_URL", "https://oauth.taobao.com/authorize")
TAOBAO_OAUTH_TOKEN_URL = os.getenv("TAOBAO_OAUTH_TOKEN_URL", "https://oauth.taobao.com/token")


def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def build_group_ai_cover_output_name(account_name: str = "", channel: str = "xianyu") -> str:
    account_token = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_-]+", "_", str(account_name or "").strip())[:40].strip("_")
    channel_token = re.sub(r"[^0-9A-Za-z_-]+", "_", str(channel or "xianyu").strip().lower()) or "xianyu"
    if account_token:
        return f"ai_cover_{channel_token}_{account_token}.jpg"
    return f"ai_cover_{channel_token}.jpg"


def build_group_ai_cover_path(group_id: int, account_name: str = "", channel: str = "xianyu") -> str:
    return str((ROOT_DIR / "data" / "group_assets" / "one8" / f"group_{int(group_id)}" / build_group_ai_cover_output_name(account_name, channel)).resolve())


def display_beijing_time(value, *, date_only: bool = False) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        normalized = raw.replace("T", " ")
        if normalized.endswith("Z"):
            dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone(BEIJING_TZ)
        return local_dt.strftime("%Y-%m-%d" if date_only else "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return raw


def load_daily_runs(limit: int = 50):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            id,
            run_type,
            trigger_mode,
            status,
            host,
            pid,
            log_file,
            note,
            started_at,
            finished_at
        FROM daily_runs
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    conn.close()
    return rows


def load_daily_run(run_id: int):
    conn = get_conn()
    row = conn.execute(
        """
        SELECT
            id,
            run_type,
            trigger_mode,
            status,
            host,
            pid,
            log_file,
            note,
            started_at,
            finished_at
        FROM daily_runs
        WHERE id = ?
        LIMIT 1
        """,
        (int(run_id),),
    ).fetchone()
    conn.close()
    return row


def load_daily_run_steps(run_id: int):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            id,
            run_id,
            step_key,
            step_name,
            status,
            started_at,
            finished_at,
            progress_current,
            progress_total,
            message,
            log_excerpt,
            updated_at
        FROM daily_run_steps
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (int(run_id),),
    ).fetchall()
    conn.close()
    return rows


def format_step_progress(row) -> str:
    current = int(row["progress_current"] or 0)
    total = int(row["progress_total"] or 0)
    if total > 0:
        return f"{current}/{total}"
    if current > 0:
        return str(current)
    return "-"


TAOBAO_DAIGOU_SOURCES = {"boardline", "one", "one8"}
ONE8_GROUPABLE_CATEGORIES = {
    "固定器",
    "滑雪鞋",
    "滑雪服",
    "手套",
    "滑雪镜",
    "滑雪头盔",
    "滑雪帽衫和中间层",
}


def derive_taobao_inventory_tag(source: str) -> str:
    return "代购" if str(source or "").strip().lower() in TAOBAO_DAIGOU_SOURCES else "现货"


def summarize_price_display(series: pd.Series) -> str:
    values = []
    for value in series.tolist():
        text = str(value or "").strip()
        if not text or text.lower() == "nan":
            continue
        values.append(text)
    if not values:
        return ""
    unique_values = list(dict.fromkeys(values))
    if len(unique_values) == 1:
        return unique_values[0]

    numeric_values = []
    for text in unique_values:
        try:
            numeric_values.append(float(text))
        except Exception:
            numeric_values = []
            break
    if numeric_values:
        low = int(min(numeric_values))
        high = int(max(numeric_values))
        return str(low) if low == high else f"{low}-{high}"
    return unique_values[0]


def summarize_stock_by_color(group: pd.DataFrame) -> str:
    parts = []
    seen = set()
    for _, row in group.iterrows():
        color = str(row.get("color") or "").strip()
        stock = str(row.get("stock") or "").strip()
        if not color or color in seen:
            continue
        seen.add(color)
        if stock and stock not in ("-", "nan"):
            parts.append(f"{color}:{stock}")
        else:
            parts.append(color)
    return " | ".join(parts)


def ensure_group_ai_copy_table():
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_group_ai_copy (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            account_name TEXT NOT NULL DEFAULT '',
            channel TEXT NOT NULL DEFAULT 'xianyu',
            ai_title TEXT,
            ai_description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(group_id, account_name, channel)
        )
        """
    )
    conn.commit()
    conn.close()


def load_group_ai_copy(group_id: int, account_name: str = "", channel: str = "xianyu"):
    if int(group_id or 0) <= 0:
        return None
    ensure_group_ai_copy_table()
    conn = get_conn()
    row = conn.execute(
        """
        SELECT group_id, account_name, channel, ai_title, ai_description, created_at, updated_at
        FROM product_group_ai_copy
        WHERE group_id = ? AND account_name = ? AND channel = ?
        LIMIT 1
        """,
        (int(group_id), str(account_name or "").strip(), str(channel or "xianyu").strip().lower()),
    ).fetchone()
    conn.close()
    return row


def save_group_ai_copy(group_id: int, *, account_name: str = "", channel: str = "xianyu", title: str | None = None, description: str | None = None):
    ensure_group_ai_copy_table()
    conn = get_conn()
    current = conn.execute(
        """
        SELECT ai_title, ai_description
        FROM product_group_ai_copy
        WHERE group_id = ? AND account_name = ? AND channel = ?
        LIMIT 1
        """,
        (int(group_id), str(account_name or "").strip(), str(channel or "xianyu").strip().lower()),
    ).fetchone()
    final_title = title if title is not None else (str(current["ai_title"] or "").strip() if current else "")
    final_description = description if description is not None else (str(current["ai_description"] or "").strip() if current else "")
    conn.execute(
        """
        INSERT INTO product_group_ai_copy (group_id, account_name, channel, ai_title, ai_description, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(group_id, account_name, channel) DO UPDATE SET
            ai_title = excluded.ai_title,
            ai_description = excluded.ai_description,
            updated_at = CURRENT_TIMESTAMP
        """,
        (int(group_id), str(account_name or "").strip(), str(channel or "xianyu").strip().lower(), final_title, final_description),
    )
    conn.commit()
    conn.close()


def build_one8_group_row(group: pd.DataFrame) -> dict:
    group = group.copy().sort_values(["product_id"])
    first = group.iloc[0].to_dict()
    product_ids = [int(pid) for pid in group["product_id"].tolist()]
    group_row = find_group_by_member_ids_relaxed(product_ids)
    group_copy = load_group_ai_copy(int(group_row["id"])) if group_row else None
    colors = [str(color or "").strip() for color in group["color"].tolist() if str(color or "").strip()]
    unique_colors = list(dict.fromkeys(colors))
    hot_ranks = [int(rank) for rank in group["hot_rank"].tolist() if int(rank or 0) > 0]
    hot_indices = [int(value) for value in group["hot_index"].tolist() if int(value or 0) > 0]
    ai_titles = [str(value or "").strip() for value in group.get("ai_title", pd.Series(dtype=str)).tolist() if str(value or "").strip()]
    ai_descriptions = [str(value or "").strip() for value in group.get("ai_description", pd.Series(dtype=str)).tolist() if str(value or "").strip()]
    ai_taobao_titles = [str(value or "").strip() for value in group.get("ai_taobao_title", pd.Series(dtype=str)).tolist() if str(value or "").strip()]
    ai_taobao_guides = [str(value or "").strip() for value in group.get("ai_taobao_guide_title", pd.Series(dtype=str)).tolist() if str(value or "").strip()]
    first["merged_group"] = 1
    first["merged_group_id"] = int(group_row["id"]) if group_row else 0
    first["merged_group_count"] = len(product_ids)
    first["merged_product_ids"] = ",".join(str(pid) for pid in product_ids)
    first["merged_color_summary"] = " | ".join(unique_colors)
    first["branduid"] = ",".join(str(value) for value in group["branduid"].tolist())
    first["color"] = " | ".join(unique_colors)
    first["stock"] = summarize_stock_by_color(group)
    first["final_price_cny"] = summarize_price_display(group["final_price_cny"])
    first["upload_count"] = int(group["upload_count"].fillna(0).astype(int).sum()) if "upload_count" in group.columns else 0
    first["hot_index"] = max(hot_indices) if hot_indices else 0
    first["hot_rank"] = min(hot_ranks) if hot_ranks else 0
    first["stock_drop_events"] = int(group["stock_drop_events"].fillna(0).astype(int).sum()) if "stock_drop_events" in group.columns else 0
    first["stock_sold_units"] = int(group["stock_sold_units"].fillna(0).astype(int).sum()) if "stock_sold_units" in group.columns else 0
    first["ai_title"] = str((group_copy["ai_title"] if group_copy else "") or "").strip() or (ai_titles[0] if ai_titles else "")
    first["ai_description"] = str((group_copy["ai_description"] if group_copy else "") or "").strip() or (ai_descriptions[0] if ai_descriptions else "")
    if "ai_taobao_title" in first:
        first["ai_taobao_title"] = ai_taobao_titles[0] if ai_taobao_titles else ""
    if "ai_taobao_guide_title" in first:
        first["ai_taobao_guide_title"] = ai_taobao_guides[0] if ai_taobao_guides else ""
    first["ai_images_json"] = "[]"
    first["ai_detail_images_json"] = "[]"
    first["ai_main_image_path"] = ""
    first["group_has_ai_image"] = 0
    return first


def merge_one8_product_groups(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "source" not in df.columns:
        return df

    frame = df.copy()
    for column, default in (
        ("merged_group", 0),
        ("merged_group_id", 0),
        ("merged_group_count", 1),
        ("merged_product_ids", ""),
        ("merged_color_summary", ""),
        ("group_has_ai_image", 0),
        ("group_member_row", 0),
        ("parent_group_id", 0),
    ):
        if column not in frame.columns:
            frame[column] = default

    frame["source"] = frame["source"].fillna("").astype(str).str.strip().str.lower()
    frame["category"] = frame["category"].fillna("").astype(str).str.strip()
    frame["name"] = frame["name"].fillna("").astype(str).str.strip()
    frame["color"] = frame["color"].fillna("").astype(str).str.strip()

    candidates = frame[
        (frame["source"] == "one8")
        & (frame["category"].isin(ONE8_GROUPABLE_CATEGORIES))
        & (frame["name"] != "")
        & (frame["color"] != "")
    ].copy()
    if candidates.empty:
        return frame

    eligible_keys = set()
    for (category, name), group in candidates.groupby(["category", "name"], sort=False):
        if len(group) < 2:
            continue
        if group["color"].nunique() < 2:
            continue
        eligible_keys.add((category, name))

    if not eligible_keys:
        return frame

    output_rows = []
    emitted_keys = set()
    for _, row in frame.iterrows():
        key = (str(row.get("category") or "").strip(), str(row.get("name") or "").strip())
        if key not in eligible_keys:
            output_rows.append(row.to_dict())
            continue
        if key in emitted_keys:
            continue
        emitted_keys.add(key)
        grouped = frame[(frame["category"] == key[0]) & (frame["name"] == key[1]) & (frame["source"] == "one8")]
        group_row = build_one8_group_row(grouped)
        group_id = int(group_row.get("merged_group_id") or 0)
        output_rows.append(group_row)
        for _, member_row in grouped.sort_values(["product_id"]).iterrows():
            member = member_row.to_dict()
            member["group_member_row"] = 1
            member["parent_group_id"] = group_id
            output_rows.append(member)

    merged_df = pd.DataFrame(output_rows)
    for column in df.columns:
        if column not in merged_df.columns:
            merged_df[column] = ""
    return merged_df


def load_group_members(group_id: int):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            p.id AS product_id,
            p.name,
            p.category,
            COALESCE(p.color, '') AS color,
            COALESCE(u.final_price_cny, '') AS final_price_cny,
            COALESCE(u.stock, '') AS stock
        FROM product_group_members m
        JOIN products p ON p.id = m.product_id
        LEFT JOIN product_updates u ON u.product_id = p.id
        WHERE m.group_id = ?
        ORDER BY m.sort_order, p.id
        """,
        (int(group_id),),
    ).fetchall()
    conn.close()
    return rows


def build_group_copy_product(group_id: int) -> dict:
    conn = get_conn()
    group_row = conn.execute(
        "SELECT id, source, category, group_name FROM product_groups WHERE id = ? LIMIT 1",
        (int(group_id),),
    ).fetchone()
    conn.close()
    if not group_row:
        raise ValueError(f"找不到商品组: {group_id}")
    member_rows = load_group_members(group_id)
    if not member_rows:
        raise ValueError(f"商品组没有成员: {group_id}")
    first = member_rows[0]
    attrs = load_product_attributes(int(first["product_id"]))
    prices = []
    for row in member_rows:
        try:
            prices.append(float(str(row["final_price_cny"] or "").strip()))
        except Exception:
            pass
    price_text = str(int(max(prices))) if prices else ""
    stock_text = " | ".join(
        f"{str(row['color'] or '').strip()}:{str(row['stock'] or '').strip()}"
        for row in member_rows
        if str(row["color"] or "").strip()
    )
    return {
        "group_id": int(group_row["id"]),
        "product_id": int(first["product_id"]),
        "name": str(group_row["group_name"] or "").strip(),
        "category": str(group_row["category"] or "").strip(),
        "final_price_cny": price_text,
        "stock": stock_text,
        "attributes": attrs,
    }


def build_group_description_with_stock(group_product: dict, description: str) -> str:
    stock_text = str(group_product.get("stock") or "").strip()
    if not stock_text:
        return description
    lines = [part.strip() for part in stock_text.split("|") if part.strip()]
    if not lines:
        return description
    return f"{description.rstrip()}\n\n可选颜色及库存：\n" + "\n".join(lines)


def generate_title_for_group(group_id: int, account_name: str = "", channel: str = "xianyu") -> dict:
    group_product = build_group_copy_product(group_id)
    title = build_xianyu_title(group_product).strip()
    if not title:
        raise ValueError("AI 返回的组标题为空")
    save_group_ai_copy(group_id, account_name=account_name, channel=channel, title=title, description=None)
    return {"group_id": int(group_id), "ai_title": title}


def generate_description_for_group(group_id: int, account_name: str = "", channel: str = "xianyu") -> dict:
    group_product = build_group_copy_product(group_id)
    description = build_xianyu_description(group_product).strip()
    if not description:
        raise ValueError("AI 返回的组简介为空")
    description = build_group_description_with_stock(group_product, description)
    save_group_ai_copy(group_id, account_name=account_name, channel=channel, title=None, description=description)
    return {"group_id": int(group_id), "ai_description": description}


def ensure_account_ai_copy_support():
    conn = get_conn()
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
    columns = [row["name"] for row in cur.execute("PRAGMA table_info(xianyu_accounts)").fetchall()]
    if "independent_ai_assets" not in columns:
        cur.execute("ALTER TABLE xianyu_accounts ADD COLUMN independent_ai_assets INTEGER DEFAULT 0")
    if "shipping_regions_json" not in columns:
        cur.execute("ALTER TABLE xianyu_accounts ADD COLUMN shipping_regions_json TEXT DEFAULT '[]'")
    if "shipping_region_group_size" not in columns:
        cur.execute(f"ALTER TABLE xianyu_accounts ADD COLUMN shipping_region_group_size INTEGER DEFAULT {DEFAULT_SHIPPING_REGION_GROUP_SIZE}")
    conn.commit()
    conn.close()


def ensure_taobao_shop_support():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
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
    columns = [row["name"] for row in cur.execute("PRAGMA table_info(taobao_shops)").fetchall()]
    if "browser_profile_dir" not in columns:
        cur.execute("ALTER TABLE taobao_shops ADD COLUMN browser_profile_dir TEXT")
    if "chrome_user_data_dir" not in columns:
        cur.execute("ALTER TABLE taobao_shops ADD COLUMN chrome_user_data_dir TEXT")
    if "chrome_profile_name" not in columns:
        cur.execute("ALTER TABLE taobao_shops ADD COLUMN chrome_profile_name TEXT")
    if "chrome_cdp_url" not in columns:
        cur.execute("ALTER TABLE taobao_shops ADD COLUMN chrome_cdp_url TEXT")
    if "login_url" not in columns:
        cur.execute("ALTER TABLE taobao_shops ADD COLUMN login_url TEXT")
    if "publish_url" not in columns:
        cur.execute("ALTER TABLE taobao_shops ADD COLUMN publish_url TEXT")
    conn.commit()
    conn.close()


def load_taobao_shops():
    ensure_taobao_shop_support()
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            id,
            shop_name,
            browser_profile_dir,
            chrome_user_data_dir,
            chrome_profile_name,
            chrome_cdp_url,
            login_url,
            publish_url,
            seller_nick,
            taobao_user_id,
            redirect_uri,
            auth_status,
            token_expires_at,
            refresh_token_expires_at,
            last_auth_at,
            last_error,
            enabled,
            note
        FROM taobao_shops
        ORDER BY id DESC
    """).fetchall()
    conn.close()
    return rows


def load_enabled_taobao_shops():
    rows = [row for row in load_taobao_shops() if int(row["enabled"] or 0) == 1]
    return rows


def load_taobao_product_pool(account_name: str = "") -> sqlite3.Row:
    ensure_ai_copy_table()
    ensure_ai_image_table()
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
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
            COALESCE(ai.ai_title, '') AS ai_title,
            COALESCE(ai.ai_taobao_title, '') AS ai_taobao_title,
            COALESCE(ai.ai_taobao_guide_title, '') AS ai_taobao_guide_title,
            COALESCE(ai.ai_description, '') AS ai_description,
            COALESCE(ai.ai_main_image_plan, '') AS ai_main_image_plan,
            p.url
        FROM products p
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        LEFT JOIN xianyu_product_ai_copy ai
          ON ai.product_id = p.id
        WHERE p.status = 'active'
        ORDER BY p.category, p.id
        """
    ).fetchall()
    hot_df = load_hot_metrics_df()
    normalized_account_name = (account_name or "").strip()
    ai_rows = conn.execute(
        """
        SELECT product_id, id AS image_id, ai_main_image_path, COALESCE(oss_url, '') AS oss_url, is_selected, COALESCE(account_name, '') AS account_name, COALESCE(asset_type, 'main') AS asset_type
        FROM xianyu_product_ai_images
        WHERE COALESCE(account_name, '') IN (?, '')
        ORDER BY id DESC
        """,
        (normalized_account_name,),
    ).fetchall()
    conn.close()

    main_ai_map = {}
    detail_ai_map = {}
    for row in ai_rows:
        pid = int(row["product_id"])
        image_account_name = str(row["account_name"] or "").strip()
        item = {
            "id": int(row["image_id"]),
            "path": str(row["ai_main_image_path"] or "").strip(),
            "oss_url": str(row["oss_url"] or "").strip(),
            "selected": int(row["is_selected"] or 0),
            "account_name": image_account_name,
            "asset_type": str(row["asset_type"] or "main").strip() or "main",
        }
        target_map = detail_ai_map if item["asset_type"] == "detail" else main_ai_map
        target_map.setdefault((pid, image_account_name), []).append(
            {
                **item,
            }
        )

    def merge_ai_images(*groups: list[dict]) -> list[dict]:
        deduped = []
        seen = set()
        for group in groups:
            for item in group or []:
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

    def resolve_ai_images(target_map: dict, product_id: int) -> list[dict]:
        if normalized_account_name:
            return merge_ai_images(
                target_map.get((int(product_id), normalized_account_name), []) or [],
                target_map.get((int(product_id), ""), []) or [],
            )
        return target_map.get((int(product_id), ""), []) or []

    import pandas as pd
    df = pd.DataFrame([dict(row) for row in rows])
    if df.empty:
        df["ai_images_json"] = []
        df["ai_detail_images_json"] = []
        df["ai_main_image_path"] = []
        df["hot_index"] = []
        df["hot_rank"] = []
        df["stock_drop_events"] = []
        df["stock_sold_units"] = []
        return df
    df = df.copy()
    if not hot_df.empty:
        df = df.merge(hot_df, on="product_id", how="left")
    df["ai_images_json"] = df["product_id"].apply(lambda pid: json.dumps(resolve_ai_images(main_ai_map, int(pid)), ensure_ascii=False))
    df["ai_detail_images_json"] = df["product_id"].apply(lambda pid: json.dumps(resolve_ai_images(detail_ai_map, int(pid)), ensure_ascii=False))
    df["ai_main_image_path"] = df["product_id"].apply(
        lambda pid: (resolve_ai_images(main_ai_map, int(pid))[0].get("path", "") if resolve_ai_images(main_ai_map, int(pid)) else "")
    )
    for column in ("hot_index", "hot_rank", "stock_drop_events", "stock_sold_units"):
        if column not in df.columns:
            df[column] = 0
        df[column] = df[column].fillna(0).astype(int)
    return df


def build_taobao_oauth_url(shop_row: sqlite3.Row) -> str:
    params = {
        "response_type": "code",
        "client_id": str(shop_row["app_key"] or "").strip(),
        "redirect_uri": str(shop_row["redirect_uri"] or "").strip(),
        "state": f"tbshop:{int(shop_row['id'])}",
        "view": "web",
    }
    return f"{TAOBAO_OAUTH_AUTHORIZE_URL}?{urlencode(params)}"


def exchange_taobao_oauth_code(app_key: str, app_secret: str, code: str, redirect_uri: str) -> dict:
    payload = urlencode({
        "grant_type": "authorization_code",
        "client_id": app_key,
        "client_secret": app_secret,
        "code": code,
        "redirect_uri": redirect_uri,
        "view": "web",
    }).encode("utf-8")
    req = Request(
        TAOBAO_OAUTH_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"淘宝 token 接口失败: HTTP {e.code} | {raw}")
    except URLError as e:
        raise RuntimeError(f"淘宝 token 接口失败: {e}")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise RuntimeError(f"淘宝 token 返回非 JSON: {raw[:300]}")
    if isinstance(data, dict) and any(k in data for k in ("error", "sub_code", "msg", "sub_msg")) and "access_token" not in data:
        raise RuntimeError(json.dumps(data, ensure_ascii=False))
    return data


def page_html(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; background: #f5f1e8; color: #1f2a24; }}
    .shell {{ max-width: 1360px; margin: 0 auto; padding: 24px; }}
    .nav a {{ margin-right: 16px; color: #0f4c3a; text-decoration: none; font-weight: 600; }}
    .card {{ background: #fffdf8; border: 1px solid #d8cfbf; border-radius: 16px; padding: 18px; margin-bottom: 18px; min-width: 0; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid #ece3d3; padding: 10px 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f6efe2; }}
    .table-wrap {{ width: 100%; max-width: 100%; overflow-x: auto; overflow-y: visible; -webkit-overflow-scrolling: touch; }}
    .table-wrap > table {{ min-width: 1280px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; }}
    .metric {{ background: #1f4f46; color: #fff; border-radius: 16px; padding: 16px; }}
    .metric strong {{ display: block; font-size: 28px; margin-top: 8px; }}
    input[type="text"], select {{ padding: 8px 10px; border: 1px solid #ccbfa7; border-radius: 10px; min-width: 220px; background: white; }}
    button {{ background: #c95c2d; color: white; border: 0; border-radius: 10px; padding: 8px 12px; cursor: pointer; }}
    .muted {{ color: #746b5d; font-size: 13px; }}
    .toolbar {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }}
    .sticky-actions {{ position: sticky; top: 12px; z-index: 5; padding: 12px 14px; border-radius: 14px; background: rgba(255, 253, 248, 0.96); border: 1px solid #d8cfbf; box-shadow: 0 8px 24px rgba(31, 42, 36, 0.08); }}
    .select-cell {{ width: 52px; min-width: 52px; text-align: center; cursor: pointer; user-select: none; }}
    .select-cell input[type="checkbox"] {{ transform: scale(1.35); cursor: pointer; }}
    tr.row-selected td {{ background: #f1ead9; }}
    tr.row-success td {{ background: #eef7ef; }}
    tr.row-failed td {{ background: #fff0ed; }}
    .thumb {{ width: 64px; height: 64px; object-fit: cover; border-radius: 10px; background: #eee3cf; display:block; }}
    .thumb-grid {{ display:flex; flex-wrap:wrap; gap:8px; max-width: 360px; }}
    .thumb-item {{ display:flex; flex-direction:column; align-items:center; gap:4px; }}
    .thumb-check {{ transform: scale(0.95); }}
    .pager {{ margin-top: 12px; }}
    .pager a {{ margin-right: 10px; color: #0f4c3a; text-decoration: none; }}
    .chips a {{ display:inline-block; margin: 0 8px 8px 0; padding: 8px 12px; border-radius: 999px; border: 1px solid #ccbfa7; text-decoration:none; color:#1f2a24; background:#fff; }}
    .chips a.active {{ background:#1f4f46; color:#fff; border-color:#1f4f46; }}
    .mini-btn {{ padding: 6px 10px; font-size: 12px; border-radius: 8px; }}
    .ghost-btn {{ background: #fff; color: #1f4f46; border: 1px solid #1f4f46; }}
    .danger-btn {{ background: #8c3b26; }}
    .info-btn {{ background: #1f4f46; }}
    .badge-new {{ display:inline-block; margin-left:8px; padding:2px 8px; border-radius:999px; background:#c95c2d; color:#fff; font-size:12px; font-weight:700; vertical-align:middle; }}
    .group-member-label {{ display:inline-block; margin-right:6px; padding:2px 8px; border-radius:999px; background:#ece3d3; color:#6e5b3f; font-size:12px; font-weight:600; vertical-align:middle; }}
    .group-member-name {{ display:inline-block; padding-left:14px; }}
    .group-row {{ cursor: pointer; }}
    .group-member-row {{ display: none; }}
    .group-toggle-hint {{ display:inline-block; margin-left:8px; color:#7b6542; font-size:12px; }}
    .copy-preview {{ max-width: 360px; white-space: pre-wrap; line-height: 1.45; }}
    .copy-preview.clickable {{ cursor: pointer; }}
    .copy-preview.clickable:hover {{ background: #faf3e7; border-radius: 10px; }}
    .mask {{ position: fixed; inset: 0; background: rgba(20, 24, 22, 0.58); display: none; align-items: center; justify-content: center; z-index: 9999; }}
    .mask.show {{ display: flex; }}
    .mask-card {{ width: min(420px, calc(100vw - 40px)); background: #fffdf8; border-radius: 18px; padding: 22px; box-shadow: 0 24px 80px rgba(0,0,0,0.18); }}
    .progress-track {{ width: 100%; height: 12px; background: #ece3d3; border-radius: 999px; overflow: hidden; margin: 16px 0 10px; }}
    .progress-bar {{ height: 100%; width: 8%; background: linear-gradient(90deg, #c95c2d, #1f4f46); border-radius: 999px; transition: width 0.25s ease; }}
    .hint-line {{ display:flex; gap:12px; align-items:center; justify-content:space-between; }}
    .result-mask {{ position: fixed; inset: 0; background: rgba(20, 24, 22, 0.58); display: none; align-items: center; justify-content: center; z-index: 10000; }}
    .result-mask.show {{ display: flex; }}
    .result-card {{ width: min(720px, calc(100vw - 40px)); background: #fffdf8; border-radius: 18px; padding: 22px; box-shadow: 0 24px 80px rgba(0,0,0,0.18); }}
    .result-text {{ width: 100%; min-height: 220px; resize: vertical; border: 1px solid #ccbfa7; border-radius: 12px; padding: 12px; background: #fff; font: 13px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; color: #1f2a24; }}
    .result-actions {{ display:flex; gap:12px; justify-content:flex-end; margin-top: 12px; }}
    .copy-modal-mask {{ position: fixed; inset: 0; background: rgba(20, 24, 22, 0.58); display: none; align-items: center; justify-content: center; z-index: 10001; }}
    .copy-modal-mask.show {{ display: flex; }}
    .copy-modal-card {{ width: min(760px, calc(100vw - 40px)); max-height: calc(100vh - 40px); overflow: auto; background: #fffdf8; border-radius: 18px; padding: 22px; box-shadow: 0 24px 80px rgba(0,0,0,0.18); }}
    .copy-modal-block {{ white-space: pre-wrap; line-height: 1.6; background: #fff; border: 1px solid #ece3d3; border-radius: 12px; padding: 12px; margin-top: 8px; }}
    .copy-modal-section + .copy-modal-section {{ margin-top: 14px; }}
    th.sortable {{ cursor: pointer; user-select: none; white-space: nowrap; }}
    th.sortable:hover {{ background: #efe6d6; }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="nav card">
      <a href="/">总览</a>
      <a href="/accounts">账号</a>
      <a href="/taobao/shops">淘宝店铺</a>
      <a href="/batches">批次</a>
      <a href="/daily-runs">日更任务</a>
      <a href="/callbacks">回调</a>
    </div>
    {body}
  </div>
  <div id="progressMask" class="mask" aria-hidden="true">
    <div class="mask-card">
      <h3 style="margin:0 0 6px;">正在执行上传</h3>
      <div id="progressText" class="muted">后台正在处理，请不要关闭页面。</div>
      <div class="progress-track"><div id="progressBar" class="progress-bar"></div></div>
      <div class="hint-line">
        <span id="progressPercent" class="muted">8%</span>
        <span class="muted">创建批次并调用创建/上架</span>
      </div>
    </div>
  </div>
  <div id="resultMask" class="result-mask" aria-hidden="true">
    <div class="result-card">
      <h3 id="resultTitle" style="margin:0 0 10px;">执行结果</h3>
      <textarea id="resultText" class="result-text" readonly></textarea>
      <div class="result-actions">
        <button type="button" class="ghost-btn" onclick="copyResultText()">复制内容</button>
        <button type="button" onclick="closeResultModal()">关闭</button>
      </div>
    </div>
  </div>
  <div id="copyPreviewMask" class="copy-modal-mask" aria-hidden="true">
    <div class="copy-modal-card">
      <h3 style="margin:0 0 10px;">完整文案</h3>
      <div id="copyPreviewContent"></div>
      <div class="result-actions">
        <button type="button" onclick="closeCopyPreviewModal()">关闭</button>
      </div>
    </div>
  </div>
  <script>
    let progressTimer = null;
    let resultModalReload = false;
    let progressStartedAt = 0;
    let progressBaseText = "后台正在处理，请不要关闭页面。";
    function currentProductFilterStorageKey() {{
      return `accountProductFilter:${{window.location.pathname}}?${{window.location.search}}`;
    }}
    function escapeHtml(value) {{
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }}
    function parseSortValue(text) {{
      const raw = (text || "").replace(/\\s+/g, " ").trim();
      if (!raw) return {{ type: "text", value: "" }};
      const normalized = raw.replace(/[,￥¥]/g, "");
      const num = Number(normalized);
      if (!Number.isNaN(num) && normalized !== "") {{
        return {{ type: "number", value: num }};
      }}
      return {{ type: "text", value: raw.toLowerCase() }};
    }}
    function renderSortableHeader(header, dir = "") {{
      const label = header.dataset.sortLabel || header.textContent.replace(/[↑↓]/g, "").trim();
      header.dataset.sortLabel = label;
      header.innerHTML = `${{escapeHtml(label)}}${{dir === "asc" ? " ↑" : (dir === "desc" ? " ↓" : "")}}`;
    }}
    function initSortableTables() {{
      document.querySelectorAll("table[data-sortable='true']").forEach((table) => {{
        const headers = Array.from(table.querySelectorAll("th[data-sort-index]"));
        headers.forEach((header) => {{
          header.classList.add("sortable");
          renderSortableHeader(header, header.dataset.sortDir || "");
          header.addEventListener("click", () => {{
            const index = Number(header.dataset.sortIndex || "-1");
            if (index < 0) return;
            const tbody = table.tBodies[0];
            const rows = tbody
              ? Array.from(tbody.querySelectorAll("tr")).filter((row) => row.querySelectorAll("td").length > 0)
              : Array.from(table.querySelectorAll("tr")).filter((row) => row.querySelectorAll("td").length > 0);
            const currentDir = header.dataset.sortDir === "asc" ? "desc" : "asc";
            headers.forEach((item) => {{
              item.dataset.sortDir = "";
              renderSortableHeader(item, "");
            }});
            header.dataset.sortDir = currentDir;
            renderSortableHeader(header, currentDir);
            rows.sort((a, b) => {{
              const aCell = a.children[index];
              const bCell = b.children[index];
              const aValue = parseSortValue(aCell ? (aCell.dataset.sortValue || aCell.textContent || "") : "");
              const bValue = parseSortValue(bCell ? (bCell.dataset.sortValue || bCell.textContent || "") : "");
              let result = 0;
              if (aValue.type === "number" && bValue.type === "number") {{
                result = aValue.value - bValue.value;
              }} else {{
                result = String(aValue.value).localeCompare(String(bValue.value), "zh-CN");
              }}
              return currentDir === "asc" ? result : -result;
            }});
            if (tbody) {{
              rows.forEach((row) => tbody.appendChild(row));
            }} else {{
              rows.forEach((row) => table.appendChild(row));
            }}
          }});
        }});
      }});
    }}
    function toggleGroupMembers(groupId) {{
      const id = String(groupId || "").trim();
      if (!id) return;
      const rows = Array.from(document.querySelectorAll(`tr[data-parent-group-id="${{id}}"]`));
      if (!rows.length) return;
      const shouldShow = rows.every((row) => row.style.display === "none" || row.style.display === "");
      rows.forEach((row) => {{
        row.style.display = shouldShow ? "table-row" : "none";
      }});
    }}
    function showProgressMask(initialText = "后台正在处理，请不要关闭页面。") {{
      const mask = document.getElementById("progressMask");
      const bar = document.getElementById("progressBar");
      const text = document.getElementById("progressText");
      const percent = document.getElementById("progressPercent");
      let current = 8;
      progressStartedAt = Date.now();
      progressBaseText = initialText || "后台正在处理，请不要关闭页面。";
      bar.style.width = current + "%";
      percent.textContent = current + "%";
      text.textContent = progressBaseText;
      mask.classList.add("show");
      progressTimer = window.setInterval(() => {{
        const elapsed = Math.max(0, Math.floor((Date.now() - progressStartedAt) / 1000));
        if (current < 90) {{
          current = Math.min(90, current + (current < 50 ? 7 : 3));
          bar.style.width = current + "%";
          percent.textContent = current + "%";
        }}
        if (elapsed >= 10) {{
          text.textContent = `${{progressBaseText}} 已等待 ${{elapsed}} 秒，批量任务商品较多时属于正常现象。`;
        }} else {{
          text.textContent = progressBaseText;
        }}
      }}, 450);
    }}
    function hideProgressMask(finalText) {{
      const mask = document.getElementById("progressMask");
      const bar = document.getElementById("progressBar");
      const text = document.getElementById("progressText");
      const percent = document.getElementById("progressPercent");
      if (progressTimer) {{
        window.clearInterval(progressTimer);
        progressTimer = null;
      }}
      bar.style.width = "100%";
      percent.textContent = "100%";
      if (finalText) {{
        text.textContent = finalText;
      }}
      window.setTimeout(() => {{
        mask.classList.remove("show");
      }}, 350);
    }}
    function showResultModal(title, text, reloadAfterClose = false) {{
      resultModalReload = !!reloadAfterClose;
      document.getElementById("resultTitle").textContent = title || "执行结果";
      const textarea = document.getElementById("resultText");
      textarea.value = text || "";
      document.getElementById("resultMask").classList.add("show");
      window.setTimeout(() => textarea.focus(), 30);
      textarea.select();
    }}
    function closeResultModal() {{
      document.getElementById("resultMask").classList.remove("show");
      if (resultModalReload) {{
        resultModalReload = false;
        window.location.reload();
      }}
    }}
    function closeCopyPreviewModal() {{
      document.getElementById("copyPreviewMask").classList.remove("show");
    }}
    function openCopyPreviewModal(node) {{
      if (!node) return;
      const title = node.dataset.fullTitle || "";
      const guideTitle = node.dataset.fullGuideTitle || "";
      const description = node.dataset.fullDescription || "";
      const content = document.getElementById("copyPreviewContent");
      const sections = [];
      if (title) {{
        sections.push(`<div class="copy-modal-section"><strong>标题</strong><div class="copy-modal-block">${{escapeHtml(title)}}</div></div>`);
      }}
      if (guideTitle) {{
        sections.push(`<div class="copy-modal-section"><strong>导购标题</strong><div class="copy-modal-block">${{escapeHtml(guideTitle)}}</div></div>`);
      }}
      if (description) {{
        sections.push(`<div class="copy-modal-section"><strong>简介</strong><div class="copy-modal-block">${{escapeHtml(description)}}</div></div>`);
      }}
      content.innerHTML = sections.length ? sections.join("") : '<div class="muted">暂无完整文案</div>';
      document.getElementById("copyPreviewMask").classList.add("show");
    }}
    async function copyResultText() {{
      const textarea = document.getElementById("resultText");
      textarea.focus();
      textarea.select();
      try {{
        await navigator.clipboard.writeText(textarea.value);
      }} catch (error) {{
        document.execCommand("copy");
      }}
    }}
    async function uploadCurrentCategory(button) {{
      const accountName = button.dataset.accountName || "";
      const category = button.dataset.category || "";
      if (!accountName || !category) {{
        alert("缺少账号或分类信息");
        return;
      }}
      showProgressMask();
      try {{
        const resp = await fetch("/account/upload-category", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ account_name: accountName, category }}).toString(),
        }});
        const data = await resp.json();
        hideProgressMask(data.message || "处理完成");
        const summary = `${{data.message || "处理完成"}}\\n批次ID: ${{data.batch_id || "-"}}\\n总数: ${{data.total_count || 0}}\\n成功: ${{data.success_count || 0}}\\n失败: ${{data.failed_count || 0}}`;
        showResultModal("上传结果", summary);
        if (data.ok) {{
          window.location.href = `/batch?id=${{data.batch_id}}`;
        }} else {{
          window.location.reload();
        }}
      }} catch (error) {{
        hideProgressMask("请求失败");
        showResultModal("上传失败", `上传失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function openTaobaoLoginBrowser(shopId) {{
      if (!shopId) {{
        alert("缺少淘宝店铺");
        return;
      }}
      try {{
        const resp = await fetch("/taobao/browser/open-login", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ shop_id: String(shopId) }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "打开淘宝浏览器失败");
        }}
        showResultModal("淘宝浏览器", data.message || "淘宝浏览器已启动");
      }} catch (error) {{
        showResultModal("淘宝浏览器失败", `打开失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function publishProductToTaobao(button) {{
      const productId = button.dataset.productId || "";
      const accountName = button.dataset.accountName || "";
      const selector = document.getElementById("taobaoShopSelect");
      const shopId = selector ? String(selector.value || "").trim() : "";
      if (!productId) {{
        alert("缺少商品ID");
        return;
      }}
      if (!shopId) {{
        alert("请先选择淘宝店铺");
        return;
      }}
      showProgressMask("正在启动淘宝发布助手，请不要关闭页面。");
      try {{
        const resp = await fetch("/taobao/browser/publish-product", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{
            shop_id: shopId,
            product_id: productId,
            account_name: accountName,
          }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "淘宝发布助手启动失败");
        }}
        hideProgressMask("淘宝发布助手已启动");
        const lines = [
          data.message || "淘宝发布助手已启动",
          `商品ID: ${{data.product_id || "-"}}`,
          `店铺: ${{data.shop_name || "-"}}`,
        ];
        showResultModal("淘宝发布助手", lines.join("\\n"));
      }} catch (error) {{
        hideProgressMask("淘宝发布助手启动失败");
        showResultModal("淘宝发布助手失败", `启动失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function publishSelectedProductsToTaobao() {{
      const selector = document.getElementById("taobaoShopSelect");
      const shopId = selector ? String(selector.value || "").trim() : "";
      const productIds = getSelectedProductIds();
      if (!shopId) {{
        alert("请先选择淘宝店铺");
        return;
      }}
      if (!productIds.length) {{
        alert("请先勾选商品");
        return;
      }}
      showProgressMask("正在批量启动淘宝发布助手，请不要关闭页面。");
      try {{
        const resp = await fetch("/taobao/browser/publish-products", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{
            shop_id: shopId,
            product_ids: productIds.join(","),
          }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "批量启动失败");
        }}
        hideProgressMask("淘宝发布助手已批量启动");
        showResultModal("淘宝发布助手", `${{data.message || "批量启动成功"}}\\n商品数: ${{data.total_count || 0}}\\n店铺: ${{data.shop_name || "-"}}`);
      }} catch (error) {{
        hideProgressMask("批量启动失败");
        showResultModal("淘宝发布助手失败", `批量启动失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    function buildBatchSummary(data, actionLabel) {{
      const totalLine = typeof data.total_count === "number" ? `\\n处理数: ${{data.total_count}}` : "";
      const timing = data.timing || null;
      const timingLine = timing
        ? `\\n\\n耗时明细:\\n- 载入任务: ${{timing.load_tasks_seconds || 0}} 秒\\n- 预检查/远端校正: ${{timing.precheck_seconds || 0}} 秒\\n- 逐条准备/处理: ${{timing.prepare_and_run_seconds || 0}} 秒\\n- 批量创建: ${{timing.batch_create_seconds || 0}} 秒\\n- 失败校正: ${{timing.reconcile_seconds || 0}} 秒\\n- 总耗时: ${{timing.total_seconds || 0}} 秒`
        : "";
      if (actionLabel === "执行上架" || actionLabel === "查询并重试失败上架" || actionLabel === "只处理待发布商品") {{
        return `${{actionLabel}}完成\\n批次ID: ${{data.batch_id || "-"}}${{totalLine}}\\n已提交上架: ${{data.success_count || 0}}\\n提交失败: ${{data.failed_count || 0}}${{timingLine}}\\n\\n说明：上架接口是异步的，最终是否发布成功请以后台回调和任务状态为准。`;
      }}
      return `${{actionLabel}}完成\\n批次ID: ${{data.batch_id || "-"}}${{totalLine}}\\n成功: ${{data.success_count || 0}}\\n失败: ${{data.failed_count || 0}}${{timingLine}}`;
    }}
    async function runBatchAction(button, mode) {{
      const batchId = button.dataset.batchId || "";
      if (!batchId) {{
        alert("缺少批次ID");
        return;
      }}
      const isCreateMode = mode === "create" || mode === "create_failed";
      const actionLabel = mode === "create_failed" ? "仅重试失败创建" : (mode === "publish_created" ? "仅提交未上架商品" : (mode === "publish_retry_failed" ? "查询并重试失败上架" : (mode === "publish_pending_only" ? "只处理待发布商品" : (mode === "create" ? "执行创建" : "执行上架"))));
      let uploadWatermark = "0";
      let specifyPublishTime = "";
      if (isCreateMode) {{
        const confirmed = window.confirm("本次创建是否带水印上传？\\n点击“确定”= 带水印上传\\n点击“取消”= 不带水印上传");
        uploadWatermark = confirmed ? "1" : "0";
      }} else {{
        const input = document.getElementById("publishTimeInput");
        const rawValue = (input && input.value ? input.value : "").trim();
        if (rawValue) {{
          specifyPublishTime = rawValue.replace("T", " ");
          if (specifyPublishTime.length === 16) {{
            specifyPublishTime += ":00";
          }}
        }}
      }}
      showProgressMask();
      try {{
        const resp = await fetch("/batch/execute", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{
            batch_id: batchId,
            mode,
            upload_watermark: uploadWatermark,
            specify_publish_time: specifyPublishTime,
            auto_stagger_publish: (mode === "publish" || mode === "publish_pending_only") ? "1" : "0",
          }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || `${{actionLabel}}失败`);
        }}
        if (!data.job_id) {{
          throw new Error(`${{actionLabel}}任务提交成功，但缺少任务ID`);
        }}
        await pollBatchExecutionJob(data.job_id, actionLabel);
      }} catch (error) {{
        hideProgressMask(`${{actionLabel}}失败`);
        showResultModal(`${{actionLabel}}失败`, `${{actionLabel}}失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function refreshBatchStatus(button) {{
      const batchId = button.dataset.batchId || "";
      if (!batchId) {{
        alert("缺少批次ID");
        return;
      }}
      showProgressMask("正在查询当前批次远端状态，请不要关闭页面。");
      try {{
        const resp = await fetch("/batch/refresh-status", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ batch_id: batchId }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "刷新状态失败");
        }}
        hideProgressMask("状态已刷新");
        const summary = `状态刷新完成\\n批次ID: ${{data.batch_id || "-"}}\\n匹配远端商品: ${{data.matched_count || 0}}\\n已确认发布: ${{data.published_count || 0}}\\n仍为待发布: ${{data.created_count || 0}}`;
        showResultModal("刷新状态完成", summary);
        window.location.reload();
      }} catch (error) {{
        hideProgressMask("刷新状态失败");
        showResultModal("刷新状态失败", `刷新状态失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function refreshAccountStatus(button) {{
      const accountName = button.dataset.accountName || "";
      if (!accountName) {{
        alert("缺少账号名称");
        return;
      }}
      if (button.dataset.loading === "1") {{
        return;
      }}
      button.dataset.loading = "1";
      button.disabled = true;
      const originalText = button.textContent;
      button.textContent = "校验中...";
      showProgressMask(`正在校验账号 ${{accountName}} 的上架状态，请不要关闭页面。`);
      try {{
        const resp = await fetch("/account/refresh-status", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ account_name: accountName }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "校验上架状态失败");
        }}
        hideProgressMask("上架状态已校验");
        showResultModal("校验上架状态完成", data.message || "校验完成", true);
      }} catch (error) {{
        hideProgressMask("校验上架状态失败");
        showResultModal("校验上架状态失败", `校验上架状态失败\\n${{error && error.message ? error.message : error}}`);
      }} finally {{
        button.dataset.loading = "0";
        button.disabled = false;
        button.textContent = originalText;
      }}
    }}
    async function rebuildBatchWithPng(button) {{
      const batchId = button.dataset.batchId || "";
      if (!batchId) {{
        alert("缺少批次ID");
        return;
      }}
      const confirmed = window.confirm("将未上架商品的首图统一转成标准 PNG，并重新创建/上架。是否继续？");
      if (!confirmed) {{
        return;
      }}
      showProgressMask("正在准备 PNG 首图并重建未上架商品，请不要关闭页面。");
      try {{
        const resp = await fetch("/batch/rebuild-with-png", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ batch_id: batchId }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "PNG 重建失败");
        }}
        progressBaseText = data.message || "PNG 首图已准备，开始重新创建/上架。";
        await runBatchAction(button, 'publish');
      }} catch (error) {{
        hideProgressMask("PNG 重建失败");
        showResultModal("PNG 重建失败", `PNG 重建失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function pollBatchExecutionJob(jobId, actionLabel) {{
      while (true) {{
        await new Promise((resolve) => window.setTimeout(resolve, 2000));
        const elapsed = Math.max(0, Math.floor((Date.now() - progressStartedAt) / 1000));
        const textNode = document.getElementById("progressText");
        if (textNode) {{
          const waited = elapsed >= 10
            ? ` 已等待 ${{elapsed}} 秒，批量任务商品较多时属于正常现象。`
            : "";
          const baseText = progressBaseText || "后台正在处理，请不要关闭页面。";
          textNode.textContent = `${{baseText}}${{waited}}`;
        }}
        const resp = await fetch(`/batch/execute-status?job_id=${{encodeURIComponent(jobId)}}`);
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || `${{actionLabel}}状态查询失败`);
        }}
        if (data.status === "queued" || data.status === "running") {{
          if (data.progress_text) {{
            progressBaseText = data.progress_text;
          }}
          continue;
        }}
        if (data.status === "succeeded") {{
          hideProgressMask(data.message || `${{actionLabel}}完成`);
          showResultModal(actionLabel, buildBatchSummary({{...data, ...(data.result || {{}})}}, actionLabel), true);
          return;
        }}
        hideProgressMask(data.message || `${{actionLabel}}失败`);
        throw new Error(data.message || `${{actionLabel}}失败`);
      }}
    }}
    async function generateBatchAi(button, mode) {{
      const batchId = button.dataset.batchId || "";
      if (!batchId) {{
        alert("缺少批次ID");
        return;
      }}
      const modeLabel = mode === "title" ? "标题" : "简介";
      showProgressMask();
      try {{
        const resp = await fetch("/batch/generate-ai", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ batch_id: batchId, mode }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || `AI${{modeLabel}}生成失败`);
        }}
        hideProgressMask(data.message || `AI${{modeLabel}}生成完成`);
        const summary = `AI${{modeLabel}}生成完成\\n批次ID: ${{data.batch_id || "-"}}\\n处理数: ${{data.total_count || 0}}\\n成功: ${{data.success_count || 0}}\\n失败: ${{data.failed_count || 0}}`;
        showResultModal(`AI${{modeLabel}}结果`, summary, true);
      }} catch (error) {{
        hideProgressMask(`AI${{modeLabel}}生成失败`);
        showResultModal(`AI${{modeLabel}}生成失败`, `AI${{modeLabel}}生成失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function generateProductAi(button, mode) {{
      const productId = button.dataset.productId || "";
      const taskId = button.dataset.taskId || "";
      const accountName = button.dataset.accountName || new URLSearchParams(window.location.search).get("name") || "";
      const modeLabel = mode === "title" ? "标题" : "简介";
      if (!productId && !taskId) {{
        alert("缺少商品ID或任务ID");
        return;
      }}
      showProgressMask();
      try {{
        const endpoint = taskId ? "/task/generate-ai" : "/product/generate-ai";
        const payload = taskId ? {{ task_id: taskId, mode, account_name: accountName }} : {{ product_id: productId, mode, account_name: accountName }};
        const resp = await fetch(endpoint, {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams(payload).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || `AI${{modeLabel}}生成失败`);
        }}
        hideProgressMask(data.message || `AI${{modeLabel}}生成完成`);
        const summary = taskId
          ? `任务AI${{modeLabel}}生成完成\\n任务ID: ${{data.task_id || "-"}}\\n商品ID: ${{data.product_id || "-"}}`
          : `商品AI${{modeLabel}}生成完成\\n商品ID: ${{data.product_id || "-"}}`;
        showResultModal(`AI${{modeLabel}}结果`, summary, true);
      }} catch (error) {{
        hideProgressMask(`AI${{modeLabel}}生成失败`);
        showResultModal(`AI${{modeLabel}}生成失败`, `AI${{modeLabel}}生成失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function generateGroupProductAi(button, mode) {{
      const groupId = button.dataset.groupId || "";
      const rawProductIds = button.dataset.productIds || "";
      const accountName = button.dataset.accountName || new URLSearchParams(window.location.search).get("name") || "";
      const channel = button.dataset.aiChannel || (window.location.pathname.startsWith("/taobao/") ? "taobao" : "xianyu");
      const productIds = rawProductIds.split(",").map((item) => item.trim()).filter((item) => /^\\d+$/.test(item));
      const modeLabel = mode === "title" ? "标题" : "简介";
      if (!groupId || !productIds.length) {{
        alert("缺少组商品参数");
        return;
      }}
      showProgressMask();
      try {{
        const resp = await fetch("/product-group/generate-ai-copy", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{
            group_id: groupId,
            product_ids: productIds.join(","),
            mode,
            account_name: accountName,
            channel,
          }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || `组商品AI${{modeLabel}}生成失败`);
        }}
        hideProgressMask(data.message || `组商品AI${{modeLabel}}生成完成`);
        showResultModal(`组商品AI${{modeLabel}}结果`, [
          `组商品AI${{modeLabel}}生成完成`,
          `组ID: ${{groupId}}`,
          `组内商品数: ${{productIds.length}}`,
          `标题: ${{data.ai_title || "-"}}`,
          `简介: ${{data.ai_description ? "已生成" : "-"}}`
        ].join("\\n"), true);
      }} catch (error) {{
        hideProgressMask(`组商品AI${{modeLabel}}生成失败`);
        showResultModal(`组商品AI${{modeLabel}}生成失败`, `组商品AI${{modeLabel}}生成失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function generateProductImageAi(button, assetType = "main") {{
      const productId = button.dataset.productId || "";
      const accountName = button.dataset.accountName || new URLSearchParams(window.location.search).get("name") || "";
      const channel = button.dataset.aiChannel || (window.location.pathname.startsWith("/taobao/") ? "taobao" : "xianyu");
      if (!productId) {{
        alert("缺少商品ID");
        return;
      }}
      const isDetail = assetType === "detail";
      showProgressMask();
      try {{
        const resp = await fetch(isDetail ? "/product/generate-ai-detail-image" : "/product/generate-ai-image", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ product_id: productId, account_name: accountName, channel }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || (isDetail ? "AI详情图生成失败" : "AI主图生成失败"));
        }}
        if (!data.job_id) {{
          throw new Error(isDetail ? "AI详情图任务提交成功，但缺少任务ID" : "AI主图任务提交成功，但缺少任务ID");
        }}
        await pollImageJob(data.job_id, productId, {{ assetType }});
      }} catch (error) {{
        hideProgressMask(isDetail ? "AI详情图生成失败" : "AI主图生成失败");
        showResultModal(isDetail ? "AI详情图生成失败" : "AI主图生成失败", `${{isDetail ? "AI详情图生成失败" : "AI主图生成失败"}}\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function generateGroupProductImageAi(button) {{
      const rawProductIds = button.dataset.productIds || "";
      const accountName = button.dataset.accountName || new URLSearchParams(window.location.search).get("name") || "";
      const channel = button.dataset.aiChannel || (window.location.pathname.startsWith("/taobao/") ? "taobao" : "xianyu");
      const groupId = button.dataset.groupId || "";
      const productIds = rawProductIds.split(",").map((item) => item.trim()).filter((item) => /^\\d+$/.test(item));
      if (!groupId || !productIds.length) {{
        alert("缺少组商品参数");
        return;
      }}
      showProgressMask();
      try {{
        const resp = await fetch("/product-group/generate-ai-image", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{
            group_id: groupId,
            product_ids: productIds.join(","),
            account_name: accountName,
            channel,
          }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "组商品AI主图生成失败");
        }}
        if (!data.job_id) {{
          throw new Error("组商品AI主图任务提交成功，但缺少任务ID");
        }}
        const result = await pollImageJob(data.job_id, groupId, {{ assetType: "main", silent: true }});
        hideProgressMask(result.message || "组商品AI主图生成完成");
        showResultModal("组商品AI主图生成完成", [
          "组商品AI主图生成完成",
          `组ID: ${{groupId}}`,
          `组内商品数: ${{productIds.length}}`,
          `封面路径: ${{result.ai_main_image_path || ""}}`
        ].join("\\n"), true);
      }} catch (error) {{
        hideProgressMask("组商品AI主图生成失败");
        showResultModal("组商品AI主图生成失败", `组商品AI主图生成失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function pollImageJob(jobId, productId, options = {{}}) {{
      const silent = !!options.silent;
      const assetType = options.assetType || "main";
      const isDetail = assetType === "detail";
      while (true) {{
        await new Promise((resolve) => window.setTimeout(resolve, 2000));
        const elapsed = Math.max(0, Math.floor((Date.now() - progressStartedAt) / 1000));
        const textNode = document.getElementById("progressText");
        if (textNode) {{
          const waited = elapsed >= 10
            ? ` 已等待 ${{elapsed}} 秒，批量任务商品较多时属于正常现象。`
            : "";
          const baseText = progressBaseText || (isDetail ? "AI详情图后台生成中，请不要重复提交。" : "AI主图后台生成中，请不要重复提交。");
          textNode.textContent = `${{baseText}}${{waited}}`;
        }}
        const resp = await fetch(`/product/generate-ai-image-status?job_id=${{encodeURIComponent(jobId)}}`);
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || (isDetail ? "AI详情图任务查询失败" : "AI主图任务查询失败"));
        }}
        if (data.status === "queued" || data.status === "running") {{
          continue;
        }}
        if (data.status === "succeeded") {{
          if (!silent) {{
            hideProgressMask(data.message || (isDetail ? "AI详情图生成完成" : "AI主图生成完成"));
            showResultModal(isDetail ? "AI详情图生成完成" : "AI主图生成完成", `${{isDetail ? "AI详情图生成完成" : "AI主图生成完成"}}\n商品ID: ${{productId || "-"}}\n图片路径: ${{data.ai_main_image_path || ""}}`, true);
          }}
          return data;
        }}
        if (!silent) {{
          hideProgressMask(data.message || (isDetail ? "AI详情图生成失败" : "AI主图生成失败"));
        }}
        throw new Error(data.message || (isDetail ? "AI详情图生成失败" : "AI主图生成失败"));
      }}
    }}
    async function generateSelectedProductImages() {{
      const accountName = new URLSearchParams(window.location.search).get("name") || "";
      const selected = getVisibleSelectedProducts();
      if (!selected.length) {{
        alert("请先勾选商品");
        return;
      }}
      showProgressMask();
      let successCount = 0;
      let failedCount = 0;
      let skippedCount = 0;
      let completedCount = 0;
      const failedItems = [];
      const concurrency = 3;
      const jobs = selected.map((input) => {{
        const rawValue = String(input.value || "").trim();
        const productIds = expandProductIds(rawValue);
        const groupId = String(input.dataset.groupId || "").trim();
        const isGroup = String(input.dataset.isGroup || "0") === "1" && !!groupId;
        const channel = String(input.dataset.aiChannel || (window.location.pathname.startsWith("/taobao/") ? "taobao" : "xianyu")).trim();
        return {{
          isGroup,
          groupId,
          productIds,
          rawValue,
          channel,
        }};
      }});
      function updateBatchImageProgress() {{
        const text = `批量生成AI主图中（已完成 ${{completedCount}}/${{jobs.length}}） 成功 ${{successCount}}，跳过 ${{skippedCount}}，失败 ${{failedCount}}`;
        progressBaseText = text;
        const percent = Math.max(5, Math.min(95, Math.round((completedCount / jobs.length) * 95)));
        const bar = document.getElementById("progressBar");
        const percentNode = document.getElementById("progressPercent");
        const textNode = document.getElementById("progressText");
        if (bar) {{
          bar.style.width = `${{percent}}%`;
        }}
        if (percentNode) {{
          percentNode.textContent = `${{percent}}%`;
        }}
        if (textNode) {{
          const elapsed = Math.max(0, Math.floor((Date.now() - progressStartedAt) / 1000));
          const waited = elapsed >= 10
            ? ` 已等待 ${{elapsed}} 秒，批量任务商品较多时属于正常现象。`
            : "";
          textNode.textContent = `${{text}}${{waited}}`;
        }}
      }}
      try {{
        updateBatchImageProgress();
        async function processOne(job) {{
          if (!job || (!job.isGroup && !job.productIds.length)) {{
            completedCount += 1;
            updateBatchImageProgress();
            return;
          }}
          try {{
            if (job.isGroup) {{
              const resp = await fetch("/product-group/generate-ai-image", {{
                method: "POST",
                headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
                body: new URLSearchParams({{
                  group_id: job.groupId,
                  product_ids: job.productIds.join(","),
                  account_name: accountName,
                  channel: job.channel,
                }}).toString(),
              }});
              const data = await resp.json();
              if (!resp.ok || data.ok === false) {{
                throw new Error(data.message || "组商品AI主图生成失败");
              }}
              if (!data.job_id) {{
                throw new Error("组商品AI主图任务提交成功，但缺少任务ID");
              }}
              await pollImageJob(data.job_id, job.productIds[0] || "", {{ silent: true }});
            }} else {{
              const productId = job.productIds[0] || "";
              const resp = await fetch("/product/generate-ai-image", {{
                method: "POST",
                headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
                body: new URLSearchParams({{ product_id: productId, account_name: accountName, channel: job.channel }}).toString(),
              }});
              const data = await resp.json();
              if (!resp.ok || data.ok === false) {{
                throw new Error(data.message || "AI主图生成失败");
              }}
              if (!data.job_id) {{
                throw new Error("AI主图任务提交成功，但缺少任务ID");
              }}
              await pollImageJob(data.job_id, productId, {{ silent: true }});
            }}
            successCount += 1;
          }} catch (error) {{
            failedCount += 1;
            const label = job.isGroup
              ? `组商品(group_id=${{job.groupId}})`
              : `商品ID ${{job.productIds[0] || "-"}}`;
            failedItems.push(`${{label}}: ${{error && error.message ? error.message : error}}`);
          }} finally {{
            completedCount += 1;
            updateBatchImageProgress();
          }}
        }}
        let cursor = 0;
        async function worker() {{
          while (cursor < jobs.length) {{
            const currentIndex = cursor;
            cursor += 1;
            await processOne(jobs[currentIndex]);
          }}
        }}
        const workers = Array.from({{ length: Math.min(concurrency, jobs.length) }}, () => worker());
        await Promise.all(workers);
        hideProgressMask(`批量生成完成，成功 ${{successCount}}，跳过 ${{skippedCount}}，失败 ${{failedCount}}`);
        const lines = [
          "批量AI主图生成完成",
          `总数: ${{jobs.length}}`,
          `成功: ${{successCount}}`,
          `跳过: ${{skippedCount}}`,
          `失败: ${{failedCount}}`,
        ];
        if (failedItems.length) {{
          lines.push("", "失败明细:");
          lines.push(...failedItems);
        }}
        showResultModal("批量AI主图结果", lines.join("\\n"), true);
      }} catch (error) {{
        hideProgressMask("批量AI主图生成失败");
        showResultModal("批量AI主图生成失败", `批量AI主图生成失败\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function generateSelectedProductAi(mode) {{
      const accountName = new URLSearchParams(window.location.search).get("name") || "";
      const selectedInputs = getVisibleSelectedProducts();
      const modeLabel = mode === "title" ? "标题" : (mode === "description" ? "简介" : "标题和简介");
      if (!selectedInputs.length) {{
        alert("请先勾选商品");
        return;
      }}
      const jobs = selectedInputs.map((input) => {{
        const rawValue = String(input.value || "").trim();
        const productIds = expandProductIds(rawValue);
        const groupId = String(input.dataset.groupId || "").trim();
        const isGroup = String(input.dataset.isGroup || "0") === "1" && !!groupId;
        const channel = String(input.dataset.aiChannel || (window.location.pathname.startsWith("/taobao/") ? "taobao" : "xianyu")).trim();
        return {{
          isGroup,
          groupId,
          productIds,
          rawValue,
          channel,
        }};
      }}).filter((item) => item.isGroup ? !!item.groupId && item.productIds.length > 0 : item.productIds.length > 0);
      if (!jobs.length) {{
        alert("未找到有效商品ID");
        return;
      }}
      showProgressMask(`正在批量生成AI${{modeLabel}}，系统会在后台分批处理，请耐心等待。`);
      let successCount = 0;
      let failedCount = 0;
      const failedItems = [];
      try {{
        for (let index = 0; index < jobs.length; index += 1) {{
          const item = jobs[index] || {{}};
          progressBaseText = `正在批量生成AI${{modeLabel}}（已完成 ${{index}}/${{jobs.length}}）`;
          let resp;
          if (item.isGroup && item.groupId) {{
            resp = await fetch("/product-group/generate-ai-copy", {{
              method: "POST",
              headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
              body: new URLSearchParams({{
                group_id: String(item.groupId),
                product_ids: (item.productIds || []).join(","),
                mode,
                account_name: accountName,
                channel: item.channel || (window.location.pathname.startsWith("/taobao/") ? "taobao" : "xianyu"),
              }}).toString(),
            }});
          }} else {{
            resp = await fetch("/products/generate-ai-batch", {{
              method: "POST",
              headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
              body: new URLSearchParams({{
                product_ids: (item.productIds || []).join(","),
                mode,
                account_name: accountName,
              }}).toString(),
            }});
          }}
          const data = await resp.json();
          if (!resp.ok || data.ok === false) {{
            failedCount += 1;
            failedItems.push(item.isGroup ? `组ID ${{item.groupId || "-"}}: ${{data.message || "失败"}}` : `商品ID ${{(item.productIds || [])[0] || "-"}}: ${{data.message || "失败"}}`);
            continue;
          }}
          successCount += 1;
        }}
        hideProgressMask(`批量生成完成，成功 ${{successCount}}，失败 ${{failedCount}}`);
        const lines = [
          `批量AI${{modeLabel}}生成完成`,
          `总数: ${{jobs.length}}`,
          `成功: ${{successCount}}`,
          `失败: ${{failedCount}}`,
        ];
        if (failedItems.length) {{
          lines.push("", "失败明细:");
          lines.push(...failedItems);
        }}
        showResultModal(`批量AI${{modeLabel}}结果`, lines.join("\\n"), true);
      }} catch (error) {{
        hideProgressMask(`批量AI${{modeLabel}}生成失败`);
        showResultModal(`批量AI${{modeLabel}}生成失败`, `批量AI${{modeLabel}}生成失败\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    function toggleAllProducts(source) {{
      const checked = !!(source && source.checked);
      const items = document.querySelectorAll("input[name='product_id']");
      items.forEach((item) => {{
        const row = item.closest("tr");
        if (!row || row.style.display !== "none") {{
          item.checked = checked;
          updateSelectionRowState(item);
        }}
      }});
    }}
    function updateSelectionRowState(input) {{
      const row = input ? input.closest("tr") : null;
      if (!row) {{
        return;
      }}
      row.classList.toggle("row-selected", !!input.checked);
    }}
    function toggleRowCheckbox(cell) {{
      const input = cell ? cell.querySelector("input[type='checkbox']") : null;
      if (!input) {{
        return;
      }}
      input.checked = !input.checked;
      updateSelectionRowState(input);
    }}
    function bindSelectionRowStates() {{
      document.querySelectorAll("input[name='product_id'], input[name='task_batch_ids'], input[name='batch_ids']").forEach((input) => {{
        updateSelectionRowState(input);
      }});
    }}
    function expandProductIds(rawValue) {{
      return String(rawValue || "")
        .split(",")
        .map((item) => item.trim())
        .filter((item) => /^\\d+$/.test(item));
    }}
    function getSelectedProductIds() {{
      const ids = [];
      getVisibleSelectedProducts().forEach((input) => {{
        expandProductIds(input.value).forEach((productId) => ids.push(productId));
      }});
      return Array.from(new Set(ids));
    }}
    function getVisibleSelectedProducts() {{
      const selected = Array.from(document.querySelectorAll("input[name='product_id']:checked")).filter((item) => {{
        const row = item.closest("tr");
        return !row || row.style.display !== "none";
      }});
      const selectedGroupIds = new Set(
        selected
          .map((item) => String(item.dataset.groupId || "").trim())
          .filter((groupId, index) => {{
            if (!groupId) return false;
            const isGroup = String(selected[index].dataset.isGroup || "0") === "1";
            return isGroup;
          }})
      );
      return selected.filter((item) => {{
        const row = item.closest("tr");
        if (!row || !row.classList.contains("group-member-row")) {{
          return true;
        }}
        const parentGroupId = String(row.dataset.parentGroupId || "").trim();
        if (!parentGroupId) {{
          return true;
        }}
        return !selectedGroupIds.has(parentGroupId);
      }});
    }}
    function filterCurrentPageProducts(keyword) {{
      const normalized = String(keyword || "").trim().toLowerCase();
      const rows = Array.from(document.querySelectorAll("tr[data-product-filter-text]"));
      let visibleCount = 0;
      rows.forEach((row) => {{
        const haystack = String(row.dataset.productFilterText || "").toLowerCase();
        const matched = !normalized || haystack.includes(normalized);
        row.style.display = matched ? "" : "none";
        if (matched) {{
          visibleCount += 1;
        }}
      }});
      const summary = document.getElementById("currentPageProductFilterSummary");
      if (summary) {{
        summary.textContent = normalized ? `当前页匹配 ${{visibleCount}} 个商品` : "";
      }}
      const input = document.getElementById("currentPageProductFilterInput");
      if (input && input.value !== keyword) {{
        input.value = keyword;
      }}
      try {{
        const storageKey = currentProductFilterStorageKey();
        if (normalized) {{
          window.sessionStorage.setItem(storageKey, String(keyword || ""));
        }} else {{
          window.sessionStorage.removeItem(storageKey);
        }}
      }} catch (error) {{
      }}
    }}
    function restoreCurrentPageProductFilter() {{
      const input = document.getElementById("currentPageProductFilterInput");
      if (!input) {{
        return;
      }}
      try {{
        const saved = window.sessionStorage.getItem(currentProductFilterStorageKey()) || "";
        if (saved) {{
          input.value = saved;
          filterCurrentPageProducts(saved);
        }}
      }} catch (error) {{
      }}
    }}
    async function saveAiImageSelection(productId, accountName = "") {{
      let resolvedAccountName = (accountName || "").trim();
      if (!resolvedAccountName) {{
        const firstInput = document.querySelector(`input[data-ai-select-product='${{productId}}']`);
        resolvedAccountName = firstInput ? String(firstInput.dataset.aiSelectAccount || "").trim() : "";
      }}
      if (!resolvedAccountName) {{
        resolvedAccountName = new URLSearchParams(window.location.search).get("name") || "";
      }}
      if (!productId) {{
        return;
      }}
      const checked = Array.from(document.querySelectorAll(`input[data-ai-select-product='${{productId}}']:checked`));
      const imageIds = checked.map((item) => item.value).filter(Boolean).join(",");
      try {{
        const resp = await fetch("/product/select-ai-images", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ product_id: String(productId), account_name: resolvedAccountName, image_ids: imageIds }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "保存图片选择失败");
        }}
      }} catch (error) {{
        showResultModal("保存图片选择失败", `保存图片选择失败\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function downShelfAndDeleteBatch(button) {{
      const batchId = button.dataset.batchId || "";
      if (!batchId) {{
        alert("缺少批次ID");
        return;
      }}
      if (!window.confirm(`确认下架并删除批次 ${{batchId}} 吗？`)) {{
        return;
      }}
      showProgressMask();
      try {{
        const resp = await fetch("/batch/downshelf-delete", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ batch_id: batchId }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "批次下架删除失败");
        }}
        await pollDeleteBatchJob(data.job_id, "批次删除结果", true);
      }} catch (error) {{
        hideProgressMask("批次下架删除失败");
        alert(`批次下架删除失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    function toggleAllTaskBatches(source) {{
      const checkboxes = document.querySelectorAll("input[name='task_batch_ids']");
      checkboxes.forEach((checkbox) => {{
        checkbox.checked = !!source.checked;
      }});
    }}
    function toggleAllBatchRows(source) {{
      const checkboxes = document.querySelectorAll("input[name='batch_ids']");
      checkboxes.forEach((checkbox) => {{
        checkbox.checked = !!source.checked;
      }});
    }}
    async function downShelfAndDeleteSelectedBatches(accountName) {{
      const checked = Array.from(document.querySelectorAll("input[name='task_batch_ids']:checked"));
      if (!checked.length) {{
        alert("请先选择要删除的批次");
        return;
      }}
      const batchIds = Array.from(new Set(checked.map((item) => item.value).filter(Boolean)));
      if (!batchIds.length) {{
        alert("未找到有效批次ID");
        return;
      }}
      if (!window.confirm(`确认下架并删除选中的 ${{batchIds.length}} 个批次吗？`)) {{
        return;
      }}
      showProgressMask();
      try {{
        const resp = await fetch("/batch/downshelf-delete-multi", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{
            account_name: accountName || "",
            batch_ids: batchIds.join(","),
          }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "批量下架删除失败");
        }}
        await pollDeleteBatchJob(data.job_id, "批量删除结果", true);
      }} catch (error) {{
        hideProgressMask("批量下架删除失败");
        showResultModal("批量删除失败", `批量下架删除失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    async function downShelfAndDeleteBatchRows() {{
      const checked = Array.from(document.querySelectorAll("input[name='batch_ids']:checked"));
      if (!checked.length) {{
        alert("请先选择要删除的批次");
        return;
      }}
      const batchIds = Array.from(new Set(checked.map((item) => item.value).filter(Boolean)));
      if (!batchIds.length) {{
        alert("未找到有效批次ID");
        return;
      }}
      if (!window.confirm(`确认下架并删除选中的 ${{batchIds.length}} 个批次吗？`)) {{
        return;
      }}
      showProgressMask();
      try {{
        const resp = await fetch("/batch/downshelf-delete-multi", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ batch_ids: batchIds.join(",") }}).toString(),
        }});
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "批量下架删除失败");
        }}
        await pollDeleteBatchJob(data.job_id, "批量删除结果", true);
      }} catch (error) {{
        hideProgressMask("批量下架删除失败");
        showResultModal("批量删除失败", `批量下架删除失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    function buildDeleteSummary(data) {{
      const lines = [
        data.selected_batch_count && data.selected_batch_count > 1 ? "批量下架删除完成" : "批次下架删除完成",
        `选中批次: ${{data.selected_batch_count || 1}}`,
        `批次删除成功: ${{data.deleted_batch_count || 0}}`,
        `下架成功: ${{data.downshelf_success_count || 0}}`,
        `商品删除成功: ${{data.delete_success_count || 0}}`,
        `跳过: ${{data.skip_count || 0}}`,
        `失败: ${{data.failed_count || 0}}`,
      ];
      if ((data.failures || []).length) {{
        lines.push("", "失败明细:");
        lines.push(...data.failures.map((item) => `批次 ${{item.batch_id || "-"}} / 任务 ${{item.task_id || "-"}}: ${{item.error || "失败"}}`));
      }}
      return lines.join("\\n");
    }}
    async function pollDeleteBatchJob(jobId, modalTitle, redirectToBatches) {{
      while (true) {{
        await new Promise((resolve) => window.setTimeout(resolve, 1500));
        const elapsed = Math.max(0, Math.floor((Date.now() - progressStartedAt) / 1000));
        const textNode = document.getElementById("progressText");
        if (textNode) {{
          const waited = elapsed >= 10 ? ` 已等待 ${{elapsed}} 秒，批量任务商品较多时属于正常现象。` : "";
          const baseText = progressBaseText || "后台正在处理，请不要关闭页面。";
          textNode.textContent = `${{baseText}}${{waited}}`;
        }}
        const resp = await fetch(`/batch/delete-status?job_id=${{encodeURIComponent(jobId)}}`);
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {{
          throw new Error(data.message || "删除状态查询失败");
        }}
        if (data.status === "queued" || data.status === "running") {{
          if (data.progress_text) {{
            progressBaseText = data.progress_text;
          }}
          continue;
        }}
        if (data.status === "succeeded") {{
          hideProgressMask(data.message || "删除完成");
          showResultModal(modalTitle, buildDeleteSummary({{...data, ...(data.result || {{}})}}), true);
          if (redirectToBatches) {{
            window.location.href = "/batches";
          }}
          return;
        }}
        hideProgressMask(data.message || "删除失败");
        throw new Error(data.message || "删除失败");
      }}
    }}
    async function forceDeleteBatchRows() {{
      const checked = Array.from(document.querySelectorAll("input[name='batch_ids']:checked"));
      if (!checked.length) {{
        alert("请先选择要删除的批次");
        return;
      }}
      const batchIds = Array.from(new Set(checked.map((item) => item.value).filter(Boolean)));
      if (!batchIds.length) {{
        alert("未找到有效批次ID");
        return;
      }}
      if (!window.confirm(`确认强制删除本地记录？\\n这会直接删除本地批次和任务记录，共 ${{batchIds.length}} 个批次，不再要求远端下架成功。`)) {{
        return;
      }}
      showProgressMask("正在强制删除本地批次记录，请稍候。");
      try {{
        const resp = await fetch("/batch/delete-multi-force", {{
          method: "POST",
          headers: {{ "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" }},
          body: new URLSearchParams({{ batch_ids: batchIds.join(",") }}).toString(),
        }});
        const data = await resp.json();
        hideProgressMask(data.message || "强制删除完成");
        const summary = `强制删除完成\\n选中批次: ${{data.selected_batch_count || 0}}\\n删除成功: ${{data.deleted_batch_count || 0}}\\n失败: ${{data.failed_count || 0}}`;
        showResultModal("强制删除结果", summary, true);
      }} catch (error) {{
        hideProgressMask("强制删除失败");
        showResultModal("强制删除失败", `强制删除失败\\n${{error && error.message ? error.message : error}}`);
      }}
    }}
    window.addEventListener("load", () => {{
      restoreCurrentPageProductFilter();
      bindSelectionRowStates();
      initSortableTables();
    }});
  </script>
</body>
</html>"""


def int_param(params, key: str, default: int = 1) -> int:
    raw = (params.get(key) or [str(default)])[0]
    return int(raw) if raw.isdigit() and int(raw) > 0 else default


def build_url(path: str, **kwargs) -> str:
    clean = {k: v for k, v in kwargs.items() if v not in ("", None)}
    query = urlencode(clean)
    return f"{path}?{query}" if query else path


def paginate_df(df, page: int, show_all: bool = False):
    total = len(df)
    if show_all:
        return df, total, 1, 1
    total_pages = max((total + PAGE_SIZE - 1) // PAGE_SIZE, 1)
    page = min(max(page, 1), total_pages)
    start = (page - 1) * PAGE_SIZE
    return df.iloc[start:start + PAGE_SIZE], total, total_pages, page


def pager_html(path: str, page: int, total_pages: int, **kwargs) -> str:
    if total_pages <= 1:
        return ""
    parts = ["<div class='pager'>"]
    if page > 1:
        parts.append(f"<a href='{html.escape(build_url(path, page=page-1, **kwargs))}'>上一页</a>")
    parts.append(f"<span class='muted'>第 {page} / {total_pages} 页</span>")
    if page < total_pages:
        parts.append(f"<a href='{html.escape(build_url(path, page=page+1, **kwargs))}'>下一页</a>")
    parts.append("</div>")
    return "".join(parts)


def is_already_deleted_error(error: Exception) -> bool:
    text = str(error or "")
    return "该商品已被删除" in text


def is_not_eligible_for_downshelf_error(error: Exception) -> bool:
    text = str(error or "")
    return "当前商品不满足下架条件，不允许操作" in text


def image_cell(image_url: str) -> str:
    image_url = (image_url or "").strip()
    if image_url.startswith("http://") or image_url.startswith("https://"):
        safe_url = html.escape(image_url)
        return f"<a href='{safe_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{safe_url}' alt='product'></a>"
    return "<div class='thumb'></div>"


def local_media_url(path: str) -> str:
    path = resolve_local_media_path(path)
    version = ""
    try:
        version = str(int(Path(path).stat().st_mtime_ns))
    except Exception:
        version = ""
    if version:
        return build_url("/media/local", file_path=path, v=version)
    return build_url("/media/local", file_path=path)


def ai_media_url(item: dict | None) -> str:
    item = item or {}
    oss_url = str(item.get("oss_url") or "").strip()
    if oss_url.startswith("http://") or oss_url.startswith("https://"):
        return oss_url
    image_path = str(item.get("path") or "").strip()
    if image_path:
        return local_media_url(image_path)
    return ""


def resolve_local_media_path(path: str) -> str:
    raw_path = str(path or "").strip()
    if not raw_path:
        return raw_path

    candidate = Path(raw_path)
    if candidate.exists():
        return str(candidate.resolve())

    normalized = raw_path.replace("\\", "/")
    marker = "data/ai_generated/"
    if marker in normalized:
        suffix = normalized.split(marker, 1)[1].lstrip("/")
        remapped = (ROOT_DIR / "data" / "ai_generated" / suffix).resolve()
        if remapped.exists():
            return str(remapped)

    fallback = (ROOT_DIR / "data" / "ai_generated" / candidate.name).resolve()
    if fallback.exists():
        return str(fallback)

    return raw_path


def _pick_display_ai_images(ai_images_json: str = "", account_name: str = "") -> list[dict]:
    ai_images = _parse_ai_images(ai_images_json)
    normalized_account = str(account_name or "").strip()
    if not ai_images:
        return []
    if not normalized_account:
        return [item for item in ai_images if not str(item.get("account_name") or "").strip()]
    return [item for item in ai_images if str(item.get("account_name") or "").strip() == normalized_account]


def preview_image_cell(remote_url: str, local_ai_path: str = "") -> str:
    local_ai_path = (local_ai_path or "").strip()
    if local_ai_path:
        media_url = html.escape(local_media_url(local_ai_path))
        return f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='ai-product'></a>"
    return image_cell(remote_url)


def extract_publish_schedule(task_result_text: str, status: str, publish_status: str, callback_status: str = "") -> str:
    text = str(task_result_text or "").strip()
    payload = {}
    if text:
        try:
            payload = json.loads(text)
        except Exception:
            payload = {}
    publish_request = payload.get("publish_request") or {}
    specify_publish_time = str(publish_request.get("specify_publish_time") or "").strip()
    if not specify_publish_time:
        remote_sync = payload.get("remote_list_sync") or {}
        remote_publish_ts = remote_sync.get("specify_publish_time")
        try:
            remote_publish_ts = int(remote_publish_ts or 0)
        except Exception:
            remote_publish_ts = 0
        if remote_publish_ts > 0:
            specify_publish_time = datetime.fromtimestamp(remote_publish_ts).strftime("%Y-%m-%d %H:%M:%S")
    if specify_publish_time:
        return f"{specify_publish_time} 上架"

    current_statuses = {str(status or "").strip(), str(publish_status or "").strip(), str(callback_status or "").strip()}
    if current_statuses & {"published", "success"}:
        return "已上架"
    if current_statuses & {"failed", "publish_failed"}:
        return "上架失败"
    if "submitted" in current_statuses:
        return "已提交，等待上架"
    if "created" in current_statuses:
        return "待提交上架"
    return "-"


def extract_scheduled_publish_datetime(task_result_text: str) -> datetime | None:
    text = str(task_result_text or "").strip()
    payload = {}
    if text:
        try:
            payload = json.loads(text)
        except Exception:
            payload = {}

    publish_request = payload.get("publish_request") or {}
    specify_publish_time = str(publish_request.get("specify_publish_time") or "").strip()
    if specify_publish_time:
        try:
            return datetime.fromisoformat(specify_publish_time.replace("T", " "))
        except Exception:
            pass

    for key in ("remote_list_sync", "remote_pending_sync"):
        remote_sync = payload.get(key) or {}
        remote_publish_ts = remote_sync.get("specify_publish_time")
        try:
            remote_publish_ts = int(remote_publish_ts or 0)
        except Exception:
            remote_publish_ts = 0
        if remote_publish_ts > 0:
            return datetime.fromtimestamp(remote_publish_ts)
    return None


def display_publish_status(publish_status: str, callback_status: str = "") -> str:
    status = str(publish_status or "").strip().lower()
    callback = str(callback_status or "").strip().lower()

    if callback in {"published", "success"} or status in {"published", "success"}:
        return "已确认发布"
    if callback in {"publish_failed", "failed"} or status in {"publish_failed", "failed"}:
        return "发布失败"
    if status == "submitted":
        return "已提交上架"
    if status == "created":
        return "待发布"
    if status == "pending":
        return "待处理"
    return publish_status or "-"


def _parse_ai_images(ai_images_json: str = "") -> list[dict]:
    try:
        ai_images = json.loads(ai_images_json or "[]")
    except Exception:
        ai_images = []
    filtered = []
    for item in ai_images:
        image_id = int(item.get("id") or 0)
        image_path = str(item.get("path") or "").strip()
        if not image_id or not image_path:
            continue
        normalized_path = image_path.replace("\\", "/").lower().lstrip("./")
        if not (
            normalized_path.startswith("data/ai_generated/")
            or "/data/ai_generated/" in normalized_path
        ):
            continue
        if (
            normalized_path.startswith("data/upload_variants/")
            or "/data/upload_variants/" in normalized_path
            or normalized_path.endswith("/upload_variants")
        ):
            continue
        filtered.append(item)
    return filtered


def product_origin_image_cell(remote_url: str) -> str:
    remote_url = (remote_url or "").strip()
    if not (remote_url.startswith("http://") or remote_url.startswith("https://")):
        return "<span class='muted'>无</span>"
    safe = html.escape(remote_url)
    return (
        "<div class='thumb-grid'>"
        f"<div class='thumb-item'><a href='{safe}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{safe}' alt='product'></a><span class='muted'>原图</span></div>"
        "</div>"
    )


def product_ai_main_image_cell(product_id: int, ai_images_json: str = "", account_name: str = "") -> str:
    ai_images = _pick_display_ai_images(ai_images_json, account_name)
    parts = []
    for item in ai_images:
        image_id = int(item.get("id") or 0)
        raw_media_url = ai_media_url(item)
        if not raw_media_url:
            continue
        media_url = html.escape(raw_media_url)
        checked = " checked" if int(item.get("selected") or 0) == 1 else ""
        parts.append(
            "<div class='thumb-item'>"
            f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='ai-main'></a>"
            f"<label class='muted'><input class='thumb-check' type='checkbox' value='{image_id}' data-ai-select-product='{product_id}' data-ai-select-account='{html.escape(account_name)}' onchange='saveAiImageSelection({product_id}, {json.dumps(account_name, ensure_ascii=False)})'{checked}>上传</label>"
            "</div>"
        )
    if not parts:
        return "<span class='muted'>未生成</span>"
    return f"<div class='thumb-grid'>{''.join(parts)}</div>"


def _load_group_ai_gallery(group_id: int, merged_product_ids: str = "", account_name: str = "", channel: str = "xianyu") -> list[dict]:
    items: list[dict] = []
    if group_id > 0:
        cover_path = build_group_ai_cover_path(group_id, account_name, channel)
        if cover_path and Path(cover_path).exists():
            items.append(
                {
                    "type": "group_cover",
                    "label": "组封面",
                    "path": cover_path,
                    "product_id": 0,
                    "image_id": 0,
                    "selected": 0,
                }
            )
    for raw_id in str(merged_product_ids or "").split(","):
        raw_id = raw_id.strip()
        if not raw_id.isdigit():
            continue
        product_id = int(raw_id)
        conn = get_conn()
        product_row = conn.execute("SELECT COALESCE(color, '') AS color FROM products WHERE id = ? LIMIT 1", (product_id,)).fetchone()
        conn.close()
        color_label = str((product_row["color"] if product_row else "") or "").strip() or f"成员{product_id}"
        ai_images = list_ai_images(product_id, account_name=account_name, asset_type="main")
        for item in ai_images:
            image_path = str(item.get("ai_main_image_path") or "").strip()
            oss_url = str(item.get("oss_url") or "").strip()
            if not image_path and not oss_url:
                continue
            items.append(
                {
                    "type": "member_ai",
                    "label": color_label,
                    "path": image_path,
                    "oss_url": oss_url,
                    "product_id": product_id,
                    "image_id": int(item.get("id") or 0),
                    "selected": int(item.get("selected") or 0),
                }
            )
    return items


def product_ai_detail_cell(product_id: int, ai_images_json: str = "", account_name: str = "") -> str:
    parts = []
    ai_images = _pick_display_ai_images(ai_images_json, account_name)
    for item in ai_images:
        raw_media_url = ai_media_url(item)
        if not raw_media_url:
            continue
        media_url = html.escape(raw_media_url)
        parts.append(
            "<div class='thumb-item'>"
            f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='ai-product'></a>"
            "<span class='muted'>淘宝</span>"
            "</div>"
        )
    if not parts:
        return "<span class='muted'>未生成</span>"
    return f"<div class='thumb-grid'>{''.join(parts)}</div>"


def product_image_gallery_cell(product_id: int, remote_url: str, ai_images_json: str = "", account_name: str = "") -> str:
    parts = []
    remote_url = (remote_url or "").strip()
    if remote_url.startswith("http://") or remote_url.startswith("https://"):
        safe = html.escape(remote_url)
        parts.append(
            f"<div class='thumb-item'><a href='{safe}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{safe}' alt='product'></a><span class='muted'>原图</span></div>"
        )

    for item in _pick_display_ai_images(ai_images_json, account_name):
        image_id = int(item.get("id") or 0)
        raw_media_url = ai_media_url(item)
        if not raw_media_url:
            continue
        media_url = html.escape(raw_media_url)
        checked = " checked" if int(item.get("selected") or 0) == 1 else ""
        parts.append(
            "<div class='thumb-item'>"
            f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='ai-product'></a>"
            f"<label class='muted'><input class='thumb-check' type='checkbox' value='{image_id}' data-ai-select-product='{product_id}' data-ai-select-account='{html.escape(account_name)}' onchange='saveAiImageSelection({product_id}, {json.dumps(account_name, ensure_ascii=False)})'{checked}>上传</label>"
            "</div>"
        )

    if not parts:
        return "<div class='thumb'></div>"
    return f"<div class='thumb-grid'>{''.join(parts)}</div>"


def row_has_display_ai_images(row, account_name: str = "", channel: str = "xianyu") -> bool:
    if int(row.get("merged_group") or 0) == 1:
        group_id = int(row.get("merged_group_id") or 0)
        merged_product_ids = str(row.get("merged_product_ids") or "")
        return len(_load_group_ai_gallery(group_id, merged_product_ids, account_name, channel)) > 0
    return len(_pick_display_ai_images(str(row.get("ai_images_json") or "[]"), account_name)) > 0


def batch_task_image_gallery_cell(row) -> str:
    if str(row.get("publish_mode") or "single").strip() != "group":
        return product_image_gallery_cell(
            int(row["product_id"]),
            str(row.get("image_url") or ""),
            str(row.get("ai_images_json") or "[]"),
            str(row.get("account_name") or ""),
        )

    selected_group_images_json = str(row.get("selected_group_images_json") or "").strip()
    if selected_group_images_json:
        try:
            selected_group_images = json.loads(selected_group_images_json)
        except Exception:
            selected_group_images = []
        parts = []
        for item in selected_group_images if isinstance(selected_group_images, list) else []:
            if not isinstance(item, dict):
                continue
            raw_media_url = ai_media_url(item)
            if not raw_media_url:
                continue
            media_url = html.escape(raw_media_url)
            label = html.escape(str(item.get("label") or "图片"))
            parts.append(
                "<div class='thumb-item'>"
                f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='group-selected'></a>"
                f"<span class='muted'>{label}</span>"
                "</div>"
            )
        if parts:
            return f"<div class='thumb-grid'>{''.join(parts)}</div>"

    account_name = str(row.get("account_name") or "")
    group_id = int(row.get("group_id") or 0)
    merged_product_ids = str(row.get("group_member_product_ids") or "")
    cover_image_path = str(row.get("cover_image_path") or "").strip()
    parts = []

    if cover_image_path:
        media_url = html.escape(local_media_url(cover_image_path))
        parts.append(
            "<div class='thumb-item'>"
            f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='group-cover'></a>"
            "<span class='muted'>组封面</span>"
            "</div>"
        )

    for item in _load_group_ai_gallery(group_id, merged_product_ids, account_name, "xianyu"):
        if item.get("type") == "group_cover":
            continue
        raw_media_url = ai_media_url(item)
        if not raw_media_url:
            continue
        media_url = html.escape(raw_media_url)
        label = html.escape(str(item.get("label") or "AI图"))
        parts.append(
            "<div class='thumb-item'>"
            f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='group-gallery'></a>"
            f"<span class='muted'>{label}</span>"
            "</div>"
        )

    if not parts:
        return "<div class='thumb'></div>"
    return f"<div class='thumb-grid'>{''.join(parts)}</div>"


def product_ai_main_image_cell_for_row(row, account_name: str = "", channel: str = "xianyu") -> str:
    if int(row.get("merged_group") or 0) == 1:
        group_id = int(row.get("merged_group_id") or 0)
        merged_product_ids = str(row.get("merged_product_ids") or "")
        parts = []
        for item in _load_group_ai_gallery(group_id, merged_product_ids, account_name, channel):
            raw_media_url = ai_media_url(item)
            if not raw_media_url:
                continue
            media_url = html.escape(raw_media_url)
            label = html.escape(str(item.get("label") or "AI图"))
            if item.get("type") == "member_ai":
                product_id = int(item.get("product_id") or 0)
                image_id = int(item.get("image_id") or 0)
                checked = " checked" if int(item.get("selected") or 0) == 1 else ""
                parts.append(
                    "<div class='thumb-item'>"
                    f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='ai-main'></a>"
                    f"<label class='muted'><input class='thumb-check' type='checkbox' value='{image_id}' data-ai-select-product='{product_id}' data-ai-select-account='{html.escape(account_name)}' onchange='saveAiImageSelection({product_id}, {json.dumps(account_name, ensure_ascii=False)})'{checked}>上传</label>"
                    f"<span class='muted'>{label}</span>"
                    "</div>"
                )
            else:
                parts.append(
                    "<div class='thumb-item'>"
                    f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='group-cover'></a>"
                    f"<label class='muted'><input class='thumb-check' type='checkbox' name='group_cover_selected' value='{group_id}' checked onclick='event.stopPropagation()'>封面</label>"
                    f"<span class='muted'>{label}</span>"
                    "</div>"
                )
        if not parts:
            return "<span class='muted'>未生成</span>"
        return f"<div class='thumb-grid'>{''.join(parts)}</div>"
    return product_ai_main_image_cell(int(row["product_id"]), str(row.get("ai_images_json") or "[]"), account_name)


def product_image_gallery_cell_for_row(row, account_name: str = "", channel: str = "xianyu") -> str:
    if int(row.get("merged_group") or 0) == 1:
        parts = []
        remote_url = str(row.get("image_url") or "").strip()
        if remote_url.startswith("http://") or remote_url.startswith("https://"):
            safe = html.escape(remote_url)
            parts.append(
                f"<div class='thumb-item'><a href='{safe}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{safe}' alt='product'></a><span class='muted'>原图</span></div>"
            )
        group_id = int(row.get("merged_group_id") or 0)
        merged_product_ids = str(row.get("merged_product_ids") or "")
        for item in _load_group_ai_gallery(group_id, merged_product_ids, account_name, channel):
            raw_media_url = ai_media_url(item)
            if not raw_media_url:
                continue
            media_url = html.escape(raw_media_url)
            label = html.escape(str(item.get("label") or "AI图"))
            if item.get("type") == "member_ai":
                product_id = int(item.get("product_id") or 0)
                image_id = int(item.get("image_id") or 0)
                checked = " checked" if int(item.get("selected") or 0) == 1 else ""
                parts.append(
                    "<div class='thumb-item'>"
                    f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='group-gallery'></a>"
                    f"<label class='muted'><input class='thumb-check' type='checkbox' value='{image_id}' data-ai-select-product='{product_id}' data-ai-select-account='{html.escape(account_name)}' onchange='saveAiImageSelection({product_id}, {json.dumps(account_name, ensure_ascii=False)})'{checked}>上传</label>"
                    f"<span class='muted'>{label}</span>"
                    "</div>"
                )
            else:
                parts.append(
                    "<div class='thumb-item'>"
                    f"<a href='{media_url}' target='_blank' rel='noopener noreferrer'><img class='thumb' src='{media_url}' alt='group-gallery'></a>"
                    f"<label class='muted'><input class='thumb-check' type='checkbox' name='group_cover_selected' value='{group_id}' checked onclick='event.stopPropagation()'>封面</label>"
                    f"<span class='muted'>{label}</span>"
                    "</div>"
                )
        if not parts:
            return "<div class='thumb'></div>"
        return f"<div class='thumb-grid'>{''.join(parts)}</div>"
    return product_image_gallery_cell(int(row["product_id"]), str(row.get("image_url") or ""), str(row.get("ai_images_json") or "[]"), account_name)


def safe_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def preview_text(value, limit: int = 180) -> str:
    text = safe_text(value)
    if not text:
        return ""
    return text[:limit] + ("..." if len(text) > limit else "")


def copy_preview_modal_cell(title: str = "", description: str = "", guide_title: str = "", preview_html: str = "") -> str:
    full_title = safe_text(title)
    full_description = safe_text(description)
    full_guide_title = safe_text(guide_title)
    if not full_title and not full_description and not full_guide_title:
        return "<span class='muted'>未生成</span>"
    attrs = " ".join([
        f"data-full-title='{html.escape(full_title, quote=True)}'",
        f"data-full-guide-title='{html.escape(full_guide_title, quote=True)}'",
        f"data-full-description='{html.escape(full_description, quote=True)}'",
    ])
    return f"<div class='copy-preview clickable' onclick='openCopyPreviewModal(this)' {attrs}>{preview_html}</div>"


def group_copy_preview_cell(group_id: int, account_name: str = "", channel: str = "xianyu") -> str:
    if int(group_id or 0) <= 0:
        return "<span class='muted'>-</span>"
    group_copy = load_group_ai_copy(int(group_id), account_name=account_name, channel=channel)
    if not group_copy:
        return "<span class='muted'>未生成</span>"
    ai_title_raw = safe_text(group_copy["ai_title"])
    ai_description_full = safe_text(group_copy["ai_description"])
    ai_description_raw = preview_text(ai_description_full)
    if not ai_title_raw and not ai_description_full:
        return "<span class='muted'>未生成</span>"
    parts = []
    if ai_title_raw:
        parts.append(f"<div><strong>标题：</strong>{html.escape(ai_title_raw)}</div>")
    if ai_description_raw:
        parts.append(f"<div style='margin-top:6px;'><strong>简介：</strong>{html.escape(ai_description_raw)}</div>")
    return copy_preview_modal_cell(ai_title_raw, ai_description_full, "", "".join(parts))


def sku_preview_cell(row) -> str:
    try:
        sku_items, total_stock = build_sku_items(
            {
                "category": safe_text(row.get("category")),
                "stock": safe_text(row.get("stock")),
                "branduid": safe_text(row.get("branduid")),
            },
            1,
        )
    except Exception:
        sku_items = []
        total_stock = 10

    if not sku_items:
        stock_text = safe_text(row.get("stock"))
        parts = ["<div class='copy-preview'><div><strong>单规格</strong></div>"]
        if stock_text:
            parts.append(f"<div style='margin-top:6px;'>{html.escape(stock_text)}</div>")
        parts.append(f"<div class='muted' style='margin-top:6px;'>上传库存：{total_stock}</div></div>")
        return "".join(parts)

    sku_lines = "".join(
        f"<div>{html.escape(str(item.get('sku_text') or ''))}</div>"
        for item in sku_items
    )
    return f"<div class='copy-preview'>{sku_lines}<div class='muted' style='margin-top:6px;'>上传库存：{total_stock}</div></div>"


def batch_task_sku_preview_cell(row) -> str:
    if str(row.get("publish_mode") or "single").strip() != "group":
        return sku_preview_cell(row)

    try:
        task = get_publish_task(int(row["task_id"]))
        sku_items, total_stock = build_sku_items(task, 1)
    except Exception:
        stock_text = safe_text(row.get("stock"))
        if not stock_text:
            return "<span class='muted'>未生成</span>"
        return f"<div class='copy-preview'><div>{html.escape(stock_text)}</div></div>"

    if not sku_items:
        stock_text = safe_text(task.get("stock") or row.get("stock"))
        if not stock_text:
            return "<span class='muted'>未生成</span>"
        return f"<div class='copy-preview'><div>{html.escape(stock_text)}</div><div class='muted' style='margin-top:6px;'>上传库存：{total_stock}</div></div>"

    sku_lines = "".join(
        f"<div>{html.escape(str(item.get('sku_text') or ''))}</div>"
        for item in sku_items
    )
    return f"<div class='copy-preview'>{sku_lines}<div class='muted' style='margin-top:6px;'>上传库存：{total_stock}</div></div>"


class AdminHandler(BaseHTTPRequestHandler):
    def serve_local_media(self, params):
        path_value = (params.get("file_path") or params.get("path") or [""])[0].strip()
        if not path_value:
            self.send_html("缺少图片", "<div class='card'>缺少图片路径</div>", 400)
            return
        path = Path(path_value)
        try:
            resolved = path.resolve()
            data_root = (ROOT_DIR / "data").resolve()
            resolved.relative_to(data_root)
        except Exception:
            self.send_html("无效图片", "<div class='card'>图片路径无效</div>", 400)
            return

        if not resolved.exists() or not resolved.is_file():
            self.send_html("图片不存在", "<div class='card'>图片不存在</div>", 404)
            return

        body = resolved.read_bytes()
        mime_type, _ = mimetypes.guess_type(resolved.name)
        self.send_response(200)
        self.send_header("Content-Type", mime_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, title: str, body: str, status: int = 200):
        body_bytes = page_html(title, body).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.send_header("Content-Length", str(len(body_bytes)))
            self.end_headers()
            self.wfile.write(body_bytes)
        except (BrokenPipeError, ConnectionResetError):
            return

    def send_json(self, payload: dict, status: int = 200):
        body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        try:
            self.wfile.write(body_bytes)
        except BrokenPipeError:
            return

    def redirect(self, location: str):
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()

    def send_alert_and_redirect(self, title: str, message: str, location: str, status: int = 200):
        safe_message = json.dumps(message, ensure_ascii=False)
        safe_location = json.dumps(location, ensure_ascii=False)
        body = f"""
        <div class='card'>
          <h1>{html.escape(title)}</h1>
          <p class='muted'>{html.escape(message)}</p>
        </div>
        <script>
          alert({safe_message});
          window.location.href = {safe_location};
        </script>
        """
        self.send_html(title, body, status)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            params = parse_qs(parsed.query)

            if path == "/":
                return self.render_dashboard()
            if path == "/accounts":
                return self.render_accounts()
            if path == "/taobao/shops":
                return self.render_taobao_shops()
            if path == "/taobao/shops/edit":
                return self.render_taobao_shop_edit(params)
            if path == "/taobao/shop":
                return self.render_taobao_shop_detail(params)
            if path == "/taobao/oauth/start":
                return self.start_taobao_oauth(params)
            if path == "/taobao/oauth/callback":
                return self.handle_taobao_oauth_callback(params)
            if path == "/account":
                return self.render_account_detail(params)
            if path == "/media/local":
                return self.serve_local_media(params)
            if path == "/batches":
                return self.render_batches()
            if path == "/batch":
                return self.render_batch_detail(params)
            if path == "/daily-runs":
                return self.render_daily_runs()
            if path == "/daily-run":
                return self.render_daily_run_detail(params)
            if path == "/callbacks":
                return self.render_callbacks()
            if path == "/product/generate-ai-image-status":
                return self.get_product_ai_image_status(params)
            if path == "/batch/execute-status":
                return self.get_batch_execute_status(params)
            if path == "/batch/delete-status":
                return self.get_batch_delete_status(params)
            self.send_html("未找到", "<div class='card'><h1>404</h1></div>", 404)
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception:
            print(
                f"[admin] GET {self.path} from {self.client_address[0]} failed",
                file=sys.stderr,
                flush=True,
            )
            traceback.print_exc()
            return self.send_html("服务器错误", "<div class='card'><h1>500</h1><p>后台处理请求失败，请查看服务器日志。</p></div>", 500)

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/batches/create":
                return self.create_batch_from_form()
            if parsed.path == "/batch/run-create":
                return self.run_batch(skip_publish=True)
            if parsed.path == "/batch/run-publish":
                return self.run_batch(skip_publish=False)
            if parsed.path == "/batch/execute":
                return self.execute_batch_ajax()
            if parsed.path == "/batch/refresh-status":
                return self.refresh_batch_status()
            if parsed.path == "/account/refresh-status":
                return self.refresh_account_status()
            if parsed.path == "/batch/rebuild-with-png":
                return self.rebuild_batch_with_png()
            if parsed.path == "/batch/generate-ai":
                return self.generate_batch_ai()
            if parsed.path == "/product/generate-ai":
                return self.generate_product_ai()
            if parsed.path == "/products/generate-ai-batch":
                return self.generate_products_ai_batch()
            if parsed.path == "/product-group/generate-ai-copy":
                return self.generate_group_product_ai_copy()
            if parsed.path == "/product/generate-ai-image":
                return self.generate_product_ai_image()
            if parsed.path == "/product-group/generate-ai-image":
                return self.generate_group_product_ai_image()
            if parsed.path == "/product/generate-ai-detail-image":
                return self.generate_product_ai_detail_image()
            if parsed.path == "/product/select-ai-images":
                return self.select_product_ai_images()
            if parsed.path == "/task/generate-ai":
                return self.generate_task_ai()
            if parsed.path == "/batch/downshelf-delete":
                return self.downshelf_delete_batch()
            if parsed.path == "/batch/downshelf-delete-multi":
                return self.downshelf_delete_batches()
            if parsed.path == "/batch/delete-multi-force":
                return self.force_delete_batches()
            if parsed.path == "/account/upload-category":
                return self.upload_category()
            if parsed.path == "/accounts/create":
                return self.create_account()
            if parsed.path == "/taobao/shops/create":
                return self.create_taobao_shop()
            if parsed.path == "/taobao/shops/update":
                return self.update_taobao_shop()
            if parsed.path == "/taobao/browser/open-login":
                return self.open_taobao_login_browser()
            if parsed.path == "/taobao/browser/publish-product":
                return self.publish_product_to_taobao_browser()
            if parsed.path == "/taobao/browser/publish-products":
                return self.publish_products_to_taobao_browser()
            if parsed.path == "/task/downshelf":
                return self.downshelf_task()
            if parsed.path == "/task/delete-product":
                return self.delete_product_task()
            self.send_html("未找到", "<div class='card'><h1>404</h1></div>", 404)
        except Exception:
            print(
                f"[admin] POST {self.path} from {self.client_address[0]} failed",
                file=sys.stderr,
                flush=True,
            )
            traceback.print_exc()
            return self.send_json({"ok": False, "message": "后台处理请求失败，请查看服务器日志。"}, 500)

    def render_dashboard(self):
        summary = load_account_summary()
        batches = load_batches().head(10)
        tasks = load_task_details().head(20)
        daily_runs = load_daily_runs(8)

        total_accounts = len(summary)
        total_uploaded = int(summary["uploaded_count"].fillna(0).sum()) if not summary.empty else 0
        total_published = int(summary["published_count"].fillna(0).sum()) if not summary.empty else 0
        total_failed = int(summary["failed_count"].fillna(0).sum()) if not summary.empty else 0

        metrics = f"""
        <div class="grid">
          <div class="metric"><span>账号数</span><strong>{total_accounts}</strong></div>
          <div class="metric"><span>已上传</span><strong>{total_uploaded}</strong></div>
          <div class="metric"><span>已确认上架</span><strong>{total_published}</strong></div>
          <div class="metric"><span>失败</span><strong>{total_failed}</strong></div>
        </div>
        """
        batch_rows = "".join(
            f"<tr><td><a href='/batch?id={int(row['batch_id'])}'>{int(row['batch_id'])}</a></td><td>{html.escape(str(row['account_name']))}</td><td>{html.escape(str(row['batch_name']))}</td><td>{html.escape(str(row['status']))}</td><td>{int(row['total_count'] or 0)}</td><td>{int(row['success_count'] or 0)}</td><td>{int(row['failed_count'] or 0)}</td></tr>"
            for _, row in batches.iterrows()
        )
        task_rows = "".join(
            f"<tr><td>{int(row['task_id'])}</td><td>{image_cell(str(row.get('image_url') or ''))}</td><td>{html.escape(str(row['account_name']))}</td><td>{html.escape(str(row['name']))}</td><td>{html.escape(str(row['status']))}</td><td>{html.escape(str(row['publish_status'] or ''))}</td><td>{html.escape(str(row['callback_status'] or ''))}</td></tr>"
            for _, row in tasks.iterrows()
        )
        run_rows = "".join(
            f"<tr><td><a href='/daily-run?id={int(row['id'])}'>{int(row['id'])}</a></td><td>{html.escape(str(row['run_type'] or ''))}</td><td>{html.escape(str(row['status'] or ''))}</td><td>{html.escape(display_beijing_time(row['started_at']))}</td><td>{html.escape(display_beijing_time(row['finished_at']))}</td><td>{html.escape(str(row['note'] or ''))}</td></tr>"
            for row in daily_runs
        )
        self.send_html(
            "闲鱼后台总览",
            metrics
            + f"""
            <div class="card"><h2>最近批次</h2><table><tr><th>ID</th><th>账号</th><th>批次名</th><th>状态</th><th>总数</th><th>成功</th><th>失败</th></tr>{batch_rows}</table></div>
            <div class="card"><h2>最近任务</h2><table><tr><th>任务ID</th><th>图片</th><th>账号</th><th>商品</th><th>状态</th><th>上架状态</th><th>回调状态</th></tr>{task_rows}</table></div>
            <div class="card"><h2>最近日更任务</h2><table><tr><th>Run ID</th><th>类型</th><th>状态</th><th>开始时间</th><th>结束时间</th><th>备注</th></tr>{run_rows}</table></div>
            """,
        )

    def render_accounts(self):
        summary = load_account_summary()
        rows = "".join(
            f"<tr><td><a href='{html.escape(build_url('/account', name=row['account_name']))}'>{html.escape(str(row['account_name']))}</a></td><td>{int(row['task_count'] or 0)}</td><td>{int(row['uploaded_count'] or 0)}</td><td>{int(row['published_count'] or 0)}</td><td>{int(row['failed_count'] or 0)}</td><td>{int(row['pending_count'] or 0)}</td><td><button type='button' class='mini-btn ghost-btn' data-account-name='{html.escape(str(row['account_name']))}' onclick='refreshAccountStatus(this)'>校验上架状态</button></td></tr>"
            for _, row in summary.iterrows()
        )
        self.send_html(
            "账号概览",
            f"""
            <div class='card'>
              <h1>账号概览</h1>
              <div class='table-wrap'>
                <table>
                  <tr><th>账号</th><th>任务数</th><th>已上传</th><th>已确认上架</th><th>失败</th><th>待处理</th><th>操作</th></tr>
                  {rows}
                </table>
              </div>
            </div>
            <div class='card'>
              <h2>新增账号</h2>
              <div class='muted' style='margin-bottom:10px;'>新账号默认启用独立AI内容，并自动配置 10 个发货地区：北上广深 + 新一线城市。上传商品时每 10 个商品自动轮换一个地区。</div>
              <form method='post' action='/accounts/create'>
                <div class='toolbar'>
                  <input type='text' name='account_name' placeholder='账号名称' required>
                  <input type='text' name='app_key' placeholder='AppKey' required>
                  <input type='text' name='app_secret' placeholder='AppSecret' required>
                  <input type='text' name='user_name' placeholder='闲鱼用户名' required>
                </div>
                <div class='toolbar'>
                  <input type='text' name='province' placeholder='省份ID，如 110000' value='110000'>
                  <input type='text' name='city' placeholder='城市ID，如 110100' value='110100'>
                  <input type='text' name='district' placeholder='地区ID，如 110105' value='110105'>
                  <input type='text' name='item_biz_type' placeholder='item_biz_type' value='2'>
                  <input type='text' name='sp_biz_type' placeholder='sp_biz_type' value='2'>
                  <input type='text' name='stuff_status' placeholder='stuff_status' value='100'>
                </div>
                <div class='toolbar'>
                  <input type='text' name='merchant_id' placeholder='merchant_id，可空'>
                  <input type='text' name='note' placeholder='备注，可空'>
                  <button type='submit'>新增账号</button>
                </div>
              </form>
            </div>
            """,
        )

    def refresh_account_status(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        account_name = (form.get("account_name") or [""])[0].strip()
        if not account_name:
            return self.send_json({"ok": False, "message": "缺少账号名称"}, 400)

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT
                t.id,
                COALESCE(t.batch_id, 0) AS batch_id
            FROM xianyu_publish_tasks t
            JOIN xianyu_accounts a
              ON a.id = t.account_id
            WHERE a.account_name = ?
              AND (
                COALESCE(t.third_product_id, '') != ''
                OR t.status IN ('created', 'submitted', 'published')
                OR COALESCE(t.publish_status, '') IN ('created', 'submitted', 'published')
              )
            ORDER BY COALESCE(t.batch_id, 0), t.id
            """,
            (account_name,),
        ).fetchall()
        conn.close()

        task_ids_by_batch: dict[int, list[int]] = {}
        for row in rows:
            batch_id = int(row["batch_id"] or 0)
            task_ids_by_batch.setdefault(batch_id, []).append(int(row["id"]))

        matched_count = 0
        checked_count = sum(len(task_ids) for task_ids in task_ids_by_batch.values())
        for batch_id, task_ids in task_ids_by_batch.items():
            reconciled = reconcile_remote_created_tasks(batch_id, task_ids, use_batch_window=batch_id > 0)
            matched_count += int(reconciled.get("matched_count") or 0)

        refreshed = load_account_summary(account_name)
        uploaded_count = 0
        published_count = 0
        if not refreshed.empty:
            row = refreshed.iloc[0]
            uploaded_count = int(row["uploaded_count"] or 0)
            published_count = int(row["published_count"] or 0)

        return self.send_json(
            {
                "ok": True,
                "checked_count": checked_count,
                "matched_count": matched_count,
                "uploaded_count": uploaded_count,
                "published_count": published_count,
                "message": (
                    f"账号 {account_name} 校验完成：检查 {checked_count} 条任务，"
                    f"同步 {matched_count} 条远端状态，已上传 {uploaded_count}，已确认上架 {published_count}"
                ),
            }
        )

    def render_taobao_shops(self):
        ensure_taobao_shop_support()
        shops = load_taobao_shops()
        rows = "".join(
            f"<tr>"
            f"<td>{int(row['id'])}</td>"
            f"<td><a href='{html.escape(build_url('/taobao/shop', id=int(row['id'])))}'>{html.escape(str(row['shop_name'] or ''))}</a></td>"
            f"<td>{html.escape(str(row['browser_profile_dir'] or ''))}</td>"
            f"<td>{html.escape(str(row['chrome_user_data_dir'] or ''))}</td>"
            f"<td>{html.escape(str(row['chrome_profile_name'] or ''))}</td>"
            f"<td>{html.escape(str(row['chrome_cdp_url'] or ''))}</td>"
            f"<td>{html.escape(str(row['login_url'] or ''))}</td>"
            f"<td>{html.escape(str(row['publish_url'] or ''))}</td>"
            f"<td>{html.escape(str(row['seller_nick'] or ''))}</td>"
            f"<td>{html.escape(str(row['auth_status'] or 'pending'))}</td>"
            f"<td>{html.escape(display_beijing_time(row['token_expires_at']))}</td>"
            f"<td>{html.escape(display_beijing_time(row['last_auth_at']))}</td>"
            f"<td>{html.escape(str(row['last_error'] or ''))}</td>"
            f"<td>"
            f"<a class='mini-btn ghost-btn' href='{html.escape(build_url('/taobao/shops/edit', id=int(row['id'])))}' style='margin-right:6px;'>编辑</a>"
            f"<button type='button' class='mini-btn ghost-btn' onclick='openTaobaoLoginBrowser({int(row['id'])})'>打开登录浏览器</button>"
            f"</td>"
            f"</tr>"
            for row in shops
        )
        self.send_html(
            "淘宝店铺",
            f"""
            <div class='card'>
              <h1>淘宝店铺</h1>
              <div class='muted' style='margin-bottom:10px;'>当前淘宝渠道走浏览器自动化。优先推荐连接你手动启动的 Chrome 调试端口，其次才是复用本机 Chrome Profile。这样登录动作完全在你自己开的 Chrome 里完成，避免 Playwright 拉起浏览器时触发滑块风控。App Key / App Secret / 回调地址暂时不是必填。</div>
              <div class='table-wrap'>
                <table>
                  <tr><th>ID</th><th>店铺标识</th><th>自定义浏览器目录</th><th>Chrome用户目录</th><th>Chrome Profile</th><th>Chrome调试URL</th><th>登录URL</th><th>发布页URL</th><th>卖家昵称</th><th>授权状态</th><th>Token到期</th><th>最近授权</th><th>最近错误</th><th>操作</th></tr>
                  {rows}
                </table>
              </div>
            </div>
            <div class='card'>
              <h2>新增淘宝店铺</h2>
              <div class='muted' style='margin-bottom:10px;'>第一版浏览器自动化只要求店铺标识。最推荐填写 Chrome 调试 URL，例如 `http://127.0.0.1:9222`。你先手动启动 Chrome 调试模式并登录千牛，系统再连接这个浏览器。若不填调试 URL，才会尝试复用 Chrome 用户目录/Profile，最后才回退到项目内自定义浏览器目录。</div>
              <form method='post' action='/taobao/shops/create'>
                <div class='toolbar'>
                  <input type='text' name='shop_name' placeholder='店铺标识，如 tb-main' required>
                  <input type='text' name='browser_profile_dir' placeholder='浏览器资料目录，可空' style='min-width:280px;'>
                </div>
                <div class='toolbar'>
                  <input type='text' name='chrome_user_data_dir' placeholder='Chrome用户目录，如 ~/Library/Application Support/Google/Chrome' style='min-width:420px;'>
                  <input type='text' name='chrome_profile_name' placeholder='Chrome Profile 名称，如 Default / Profile 1' style='min-width:220px;'>
                  <input type='text' name='chrome_cdp_url' placeholder='Chrome调试URL，如 http://127.0.0.1:9222' style='min-width:320px;'>
                </div>
                <div class='toolbar'>
                  <input type='text' name='login_url' placeholder='千牛/店铺登录页URL，可空' style='min-width:420px;'>
                  <input type='text' name='publish_url' placeholder='淘宝发布页URL，可空' style='min-width:420px;'>
                  <input type='text' name='note' placeholder='备注，可空'>
                </div>
                <div class='toolbar'>
                  <input type='text' name='app_key' placeholder='App Key，可空' style='min-width:220px;'>
                  <input type='text' name='app_secret' placeholder='App Secret，可空' style='min-width:220px;'>
                  <input type='text' name='redirect_uri' placeholder='回调地址 redirect_uri，可空' style='min-width:320px;'>
                  <button type='submit'>新增店铺</button>
                </div>
              </form>
            </div>
            """,
        )

    def render_taobao_shop_edit(self, params):
        shop_id = (params.get("id") or [""])[0].strip()
        if not shop_id.isdigit():
            return self.send_html("参数错误", "<div class='card'>缺少有效店铺ID</div>", 400)
        ensure_taobao_shop_support()
        conn = get_conn()
        row = conn.execute("SELECT * FROM taobao_shops WHERE id = ?", (int(shop_id),)).fetchone()
        conn.close()
        if not row:
            return self.send_html("未找到店铺", "<div class='card'>淘宝店铺不存在</div>", 404)
        row = dict(row)
        self.send_html(
            f"编辑淘宝店铺 {row['shop_name']}",
            f"""
            <div class='card'>
              <h1>编辑淘宝店铺：{html.escape(str(row['shop_name'] or ''))}</h1>
              <div class='muted' style='margin-bottom:10px;'>如果你要避开千牛滑块，最推荐填写 Chrome 调试 URL。先手动启动 Chrome 调试模式并登录千牛，再让系统连接这个浏览器。</div>
              <form method='post' action='/taobao/shops/update'>
                <input type='hidden' name='id' value='{int(row['id'])}'>
                <div class='toolbar'>
                  <input type='text' name='shop_name' value='{html.escape(str(row.get("shop_name") or ""))}' placeholder='店铺标识' required>
                  <input type='text' name='browser_profile_dir' value='{html.escape(str(row.get("browser_profile_dir") or ""))}' placeholder='自定义浏览器目录，可空' style='min-width:280px;'>
                </div>
                <div class='toolbar'>
                  <input type='text' name='chrome_user_data_dir' value='{html.escape(str(row.get("chrome_user_data_dir") or ""))}' placeholder='Chrome用户目录' style='min-width:420px;'>
                  <input type='text' name='chrome_profile_name' value='{html.escape(str(row.get("chrome_profile_name") or ""))}' placeholder='Chrome Profile 名称' style='min-width:220px;'>
                  <input type='text' name='chrome_cdp_url' value='{html.escape(str(row.get("chrome_cdp_url") or ""))}' placeholder='Chrome调试URL，如 http://127.0.0.1:9222' style='min-width:320px;'>
                </div>
                <div class='toolbar'>
                  <input type='text' name='login_url' value='{html.escape(str(row.get("login_url") or ""))}' placeholder='千牛登录页URL，可空' style='min-width:420px;'>
                  <input type='text' name='publish_url' value='{html.escape(str(row.get("publish_url") or ""))}' placeholder='淘宝发布页URL，可空' style='min-width:420px;'>
                  <input type='text' name='note' value='{html.escape(str(row.get("note") or ""))}' placeholder='备注，可空'>
                </div>
                <div class='toolbar'>
                  <input type='text' name='app_key' value='{html.escape(str(row.get("app_key") or ""))}' placeholder='App Key，可空' style='min-width:220px;'>
                  <input type='text' name='app_secret' value='{html.escape(str(row.get("app_secret") or ""))}' placeholder='App Secret，可空' style='min-width:220px;'>
                  <input type='text' name='redirect_uri' value='{html.escape(str(row.get("redirect_uri") or ""))}' placeholder='回调地址 redirect_uri，可空' style='min-width:320px;'>
                </div>
                <div class='toolbar'>
                  <button type='submit'>保存店铺配置</button>
                  <a class='ghost-btn' href='/taobao/shops'>返回店铺列表</a>
                </div>
              </form>
              <div class='muted' style='margin-top:14px;'>
                macOS 启动 Chrome 调试模式示例：<br>
                <code>open -na "Google Chrome" --args --remote-debugging-port=9222</code><br>
                启动后，把 <code>http://127.0.0.1:9222</code> 填到“Chrome调试URL”。
              </div>
            </div>
            """,
        )

    def render_taobao_shop_detail(self, params):
        ensure_ai_copy_table()
        shop_id = (params.get("id") or [""])[0].strip()
        if not shop_id.isdigit():
            return self.send_html("缺少店铺", "<div class='card'>缺少有效淘宝店铺ID</div>", 400)
        conn = get_conn()
        shop = conn.execute("SELECT * FROM taobao_shops WHERE id = ? AND enabled = 1", (int(shop_id),)).fetchone()
        conn.close()
        if not shop:
            return self.send_html("店铺不存在", "<div class='card'>淘宝店铺不存在或未启用</div>", 404)

        category = (params.get("category") or ["全部"])[0]
        ai_image_filter = (params.get("ai_image_filter") or ["全部"])[0]
        page = int_param(params, "page", 1)
        show_mode = ((params.get("show") or ["page"])[0] or "page").strip().lower()
        show_all = show_mode == "all"

        shop_account_name = str(shop["shop_name"] or "").strip()
        product_pool = load_taobao_product_pool(shop_account_name)
        if category != "全部":
            product_pool = product_pool[product_pool["category"] == category]
        product_pool = merge_one8_product_groups(product_pool)
        if not product_pool.empty:
            product_pool = product_pool.copy()
            if "group_has_ai_image" not in product_pool.columns:
                product_pool["group_has_ai_image"] = 0
            product_pool["ai_image_status"] = product_pool.apply(
                lambda row: "已生成AI图" if row_has_display_ai_images(row, shop_account_name, "taobao") else "未生成AI图",
                axis=1,
            )
            if ai_image_filter != "全部":
                product_pool = product_pool[product_pool["ai_image_status"] == ai_image_filter]
        else:
            product_pool["ai_image_status"] = []

        categories = ["全部"] + sorted(set(product_pool["category"].dropna().tolist()))
        ai_image_filters = ["全部", "已生成AI图", "未生成AI图"]
        paged_pool, total_products, total_pages, page = paginate_df(product_pool, page, show_all=show_all)
        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

        def product_name_cell(row):
            name = html.escape(str(row["name"]))
            first_seen = display_beijing_time(row.get("first_seen"), date_only=True)
            group_badge = ""
            if int(row.get("merged_group") or 0) == 1:
                group_badge = f"<span class='badge-new'>组内{int(row.get('merged_group_count') or 0)}款</span><span class='group-toggle-hint'>点击展开</span>"
            if int(row.get("group_member_row") or 0) == 1:
                return f"<span class='group-member-label'>组内成员</span><span class='group-member-name'>{name}</span>"
            if first_seen == today:
                return f"{name}{group_badge}<span class='badge-new'>今日新增</span>"
            return f"{name}{group_badge}"

        def copy_preview_cell(row):
            ai_title_raw = safe_text(row.get("ai_taobao_title")) or safe_text(row.get("ai_title"))
            ai_guide_title_raw = safe_text(row.get("ai_taobao_guide_title"))
            ai_description_full = safe_text(row.get("ai_description"))
            ai_description_raw = preview_text(ai_description_full)
            ai_title = html.escape(ai_title_raw)
            ai_guide_title = html.escape(ai_guide_title_raw)
            ai_description = html.escape(ai_description_raw)
            if not ai_title and not ai_guide_title and not ai_description:
                return "<span class='muted'>未生成</span>"
            parts = []
            if ai_title:
                parts.append(f"<div><strong>标题：</strong>{ai_title}</div>")
            if ai_guide_title:
                parts.append(f"<div style='margin-top:6px;'><strong>导购标题：</strong>{ai_guide_title}</div>")
            if ai_description:
                parts.append(f"<div style='margin-top:6px;'><strong>简介：</strong>{ai_description}</div>")
            return copy_preview_modal_cell(ai_title_raw, ai_description_full, ai_guide_title_raw, ''.join(parts))

        def group_copy_cell(row):
            if int(row.get("merged_group") or 0) != 1:
                return "<span class='muted'>-</span>"
            return group_copy_preview_cell(int(row.get("merged_group_id") or 0), shop_account_name, "taobao")

        def row_action_cell(row):
            if int(row.get("merged_group") or 0) == 1:
                merged_ids = html.escape(str(row.get("merged_product_ids") or ""))
                group_id = int(row.get("merged_group_id") or 0)
                return (
                    f"<button type='button' class='mini-btn ghost-btn' data-group-id='{group_id}' data-product-ids='{merged_ids}' data-account-name='{html.escape(shop_account_name)}' data-ai-channel='taobao' onclick=\"generateGroupProductAi(this, 'title')\">生成标题</button> "
                    f"<button type='button' class='mini-btn ghost-btn' data-group-id='{group_id}' data-product-ids='{merged_ids}' data-account-name='{html.escape(shop_account_name)}' data-ai-channel='taobao' onclick=\"generateGroupProductAi(this, 'description')\">生成简介</button> "
                    f"<button type='button' class='mini-btn info-btn' data-group-id='{group_id}' data-product-ids='{merged_ids}' data-account-name='{html.escape(shop_account_name)}' data-ai-channel='taobao' onclick=\"generateGroupProductImageAi(this)\">生成AI主图</button>"
                )
            return (
                f"<button type='button' class='mini-btn ghost-btn' data-product-id='{int(row['product_id'])}' data-account-name='{html.escape(shop_account_name)}' onclick=\"generateProductAi(this, 'title')\">生成标题</button> "
                f"<button type='button' class='mini-btn ghost-btn' data-product-id='{int(row['product_id'])}' data-account-name='{html.escape(shop_account_name)}' onclick=\"generateProductAi(this, 'description')\">生成简介</button> "
                f"<button type='button' class='mini-btn info-btn' data-product-id='{int(row['product_id'])}' data-account-name='{html.escape(shop_account_name)}' data-ai-channel='taobao' onclick=\"generateProductImageAi(this, 'main')\">生成AI主图</button> "
                f"<button type='button' class='mini-btn info-btn' data-product-id='{int(row['product_id'])}' data-account-name='{html.escape(shop_account_name)}' data-ai-channel='taobao' onclick=\"generateProductImageAi(this, 'detail')\">生成AI详情图</button> "
                f"<button type='button' class='mini-btn danger-btn' data-product-id='{int(row['product_id'])}' data-account-name='{html.escape(shop_account_name)}' onclick='publishProductToTaobao(this)'>淘宝发布助手</button>"
            )

        option_row_parts = []
        for _, row in paged_pool.iterrows():
            is_group = int(row.get("merged_group") or 0) == 1
            is_member = int(row.get("group_member_row") or 0) == 1
            row_class = "group-row" if is_group else ("group-member-row" if is_member else "")
            row_click_attr = f'onclick="toggleGroupMembers({int(row.get("merged_group_id") or 0)})"' if is_group else ""
            option_row_parts.append(
                f"<tr class='{row_class}' data-parent-group-id='{int(row.get('parent_group_id') or 0)}' data-product-filter-text='{html.escape(' '.join([str(int(row['product_id'])), str(row['category']), str(row['name'])]))}' {row_click_attr}>"
                f"<td class='select-cell' onclick='toggleRowCheckbox(this)'><input type='checkbox' name='product_id' value='{html.escape(str(row.get('merged_product_ids') or int(row['product_id'])))}' data-group-id='{int(row.get('merged_group_id') or 0)}' data-is-group='{'1' if is_group else '0'}' data-ai-channel='taobao' data-has-ai-title='{'1' if safe_text(row.get('ai_title')) else '0'}' data-has-ai-description='{'1' if safe_text(row.get('ai_description')) else '0'}' data-has-ai-image='{'1' if row_has_display_ai_images(row, shop_account_name, 'taobao') else '0'}' onclick='event.stopPropagation(); updateSelectionRowState(this)'></td>"
                f"<td>{product_origin_image_cell(str(row.get('image_url') or ''))}</td>"
                f"<td>{product_ai_main_image_cell_for_row(row, shop_account_name, 'taobao')}</td>"
                f"<td>{product_ai_detail_cell(int(row['product_id']), str(row.get('ai_detail_images_json') or '[]'), shop_account_name)}</td>"
                f"<td>{int(row['product_id'])}</td>"
                f"<td>{html.escape(str(row.get('source') or ''))}</td>"
                f"<td>{derive_taobao_inventory_tag(str(row.get('source') or ''))}</td>"
                f"<td>{html.escape(str(row['category']))}</td>"
                f"<td>{product_name_cell(row)}</td>"
                f"<td>{html.escape(display_beijing_time(row.get('first_seen')))}</td>"
                f"<td>{html.escape(str(row['final_price_cny'] or ''))}</td>"
                f"<td>{html.escape(str(row['stock'] or ''))}</td>"
                f"<td>{int(row.get('hot_index') or 0)}</td>"
                f"<td>{copy_preview_cell(row)}</td>"
                f"<td>{group_copy_cell(row)}</td>"
                f"<td>{row_action_cell(row)}</td>"
                f"</tr>"
            )
        option_rows = "".join(option_row_parts)
        category_buttons = "".join(
            f"<a class='{'active' if cat == category else ''}' href='{html.escape(build_url('/taobao/shop', id=int(shop['id']), category=cat, ai_image_filter=ai_image_filter, show=show_mode))}'>{html.escape(cat)}</a>"
            for cat in categories
        )
        ai_image_buttons = "".join(
            f"<a class='{'active' if sf == ai_image_filter else ''}' href='{html.escape(build_url('/taobao/shop', id=int(shop['id']), category=category, ai_image_filter=sf, show=show_mode))}'>{html.escape(sf)}</a>"
            for sf in ai_image_filters
        )
        show_buttons = "".join(
            [
                f"<a class='{'active' if not show_all else ''}' href='{html.escape(build_url('/taobao/shop', id=int(shop['id']), category=category, ai_image_filter=ai_image_filter, show='page', page=1))}'>分页显示</a>",
                f"<a class='{'active' if show_all else ''}' href='{html.escape(build_url('/taobao/shop', id=int(shop['id']), category=category, ai_image_filter=ai_image_filter, show='all'))}'>显示全部商品</a>",
            ]
        )

        body = f"""
        <div class='card'>
          <h1>淘宝店铺: {html.escape(str(shop['shop_name'] or ''))}</h1>
          <div class='toolbar'>
            <span class='muted'>当前商品池 {total_products} 个</span>
            <button type='button' class='ghost-btn' onclick='openTaobaoLoginBrowser({int(shop["id"])})'>打开登录浏览器</button>
            <select id='taobaoShopSelect' style='min-width:220px;'>
              <option value='{int(shop["id"])}'>{html.escape(str(shop['shop_name'] or ''))}</option>
            </select>
            <button type='button' class='danger-btn' onclick='publishSelectedProductsToTaobao()'>批量打开淘宝发布助手</button>
          </div>
          <div class='chips'>{category_buttons}</div>
          <div class='toolbar'><span class='muted'>AI图状态：</span><div class='chips'>{ai_image_buttons}</div></div>
          <div class='chips'>{show_buttons}</div>
        </div>
        <div class='card'>
          <div class='toolbar'>
            <span class='muted'>可先勾选商品后批量生成 AI 主图、标题、简介，再决定上传哪些商品</span>
            <button type='button' class='ghost-btn' onclick="generateSelectedProductAi('both')">批量生成标题和简介</button>
            <button type='button' class='ghost-btn' onclick="generateSelectedProductAi('title')">批量生成标题</button>
            <button type='button' class='ghost-btn' onclick="generateSelectedProductAi('description')">批量生成简介</button>
            <button type='button' class='info-btn' onclick='generateSelectedProductImages()'>批量生成AI主图</button>
            <input type='search' id='currentPageProductFilterInput' placeholder='搜索当前分页商品' oninput='filterCurrentPageProducts(this.value)' style='min-width:220px;'>
            <span class='muted' id='currentPageProductFilterSummary'></span>
          </div>
          <div class='table-wrap'>
            <table data-sortable='true'>
              <tr><th><input type='checkbox' onclick='toggleAllProducts(this)' aria-label='全选商品'></th><th>图片</th><th>AI主图</th><th>AI详情</th><th>商品ID</th><th>来源</th><th>类型</th><th>分类</th><th>名称</th><th>首次发现</th><th>价格</th><th>库存</th><th data-sort-index='12' data-sort-label='热门指数'>热门指数</th><th>AI文案预览</th><th>组文案</th><th>操作</th></tr>
              {option_rows}
            </table>
          </div>
          {pager_html('/taobao/shop', page, total_pages, id=int(shop['id']), category=category, ai_image_filter=ai_image_filter, show=show_mode)}
        </div>
        """
        self.send_html(f"淘宝店铺 {shop['shop_name']}", body)

    def start_taobao_oauth(self, params):
        shop_id = ((params.get("id") or [""])[0]).strip()
        if not shop_id.isdigit():
            return self.send_html("参数错误", "<div class='card'>缺少有效店铺ID</div>", 400)
        ensure_taobao_shop_support()
        conn = get_conn()
        row = conn.execute(
            "SELECT id, app_key, redirect_uri FROM taobao_shops WHERE id = ? AND enabled = 1",
            (int(shop_id),),
        ).fetchone()
        conn.close()
        if not row:
            return self.send_html("未找到店铺", "<div class='card'>店铺不存在或未启用</div>", 404)
        if not str(row["app_key"] or "").strip() or not str(row["redirect_uri"] or "").strip():
            return self.send_alert_and_redirect("无法授权", "该店铺缺少 app_key 或 redirect_uri", "/taobao/shops", 200)
        self.redirect(build_taobao_oauth_url(row))

    def handle_taobao_oauth_callback(self, params):
        ensure_taobao_shop_support()
        code = ((params.get("code") or [""])[0]).strip()
        state = ((params.get("state") or [""])[0]).strip()
        error = ((params.get("error") or [""])[0]).strip() or ((params.get("error_description") or [""])[0]).strip()
        if not state.startswith("tbshop:"):
            return self.send_alert_and_redirect("授权失败", "缺少有效 state", "/taobao/shops", 200)
        shop_id = state.split(":", 1)[1].strip()
        if not shop_id.isdigit():
            return self.send_alert_and_redirect("授权失败", "state 中的店铺ID无效", "/taobao/shops", 200)

        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM taobao_shops WHERE id = ?",
            (int(shop_id),),
        ).fetchone()
        if not row:
            conn.close()
            return self.send_alert_and_redirect("授权失败", "店铺不存在", "/taobao/shops", 200)

        if error:
            conn.execute(
                """
                UPDATE taobao_shops
                SET auth_status = 'failed',
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (error, int(shop_id)),
            )
            conn.commit()
            conn.close()
            return self.send_alert_and_redirect("授权失败", f"淘宝返回错误：{error}", "/taobao/shops", 200)

        if not code:
            conn.close()
            return self.send_alert_and_redirect("授权失败", "缺少授权 code", "/taobao/shops", 200)

        try:
            token_data = exchange_taobao_oauth_code(
                str(row["app_key"] or "").strip(),
                str(row["app_secret"] or "").strip(),
                code,
                str(row["redirect_uri"] or "").strip(),
            )
            expires_in = int(token_data.get("expires_in") or 0)
            re_expires_in = int(token_data.get("re_expires_in") or 0)
            now = datetime.now(BEIJING_TZ)
            token_expires_at = (now + timedelta(seconds=expires_in)).strftime("%Y-%m-%d %H:%M:%S") if expires_in > 0 else ""
            refresh_expires_at = (now + timedelta(seconds=re_expires_in)).strftime("%Y-%m-%d %H:%M:%S") if re_expires_in > 0 else ""
            conn.execute(
                """
                UPDATE taobao_shops
                SET seller_nick = ?,
                    taobao_user_id = ?,
                    access_token = ?,
                    refresh_token = ?,
                    token_expires_at = ?,
                    refresh_token_expires_at = ?,
                    auth_status = 'authorized',
                    last_auth_at = CURRENT_TIMESTAMP,
                    last_error = '',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    str(token_data.get("taobao_user_nick") or token_data.get("nick") or "").strip(),
                    str(token_data.get("taobao_user_id") or token_data.get("user_id") or "").strip(),
                    str(token_data.get("access_token") or "").strip(),
                    str(token_data.get("refresh_token") or "").strip(),
                    token_expires_at,
                    refresh_expires_at,
                    int(shop_id),
                ),
            )
            conn.commit()
            conn.close()
            return self.send_alert_and_redirect("授权成功", "淘宝店铺授权成功，token 已写入本地数据库。", "/taobao/shops", 200)
        except Exception as e:
            conn.execute(
                """
                UPDATE taobao_shops
                SET auth_status = 'failed',
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (str(e), int(shop_id)),
            )
            conn.commit()
            conn.close()
            return self.send_alert_and_redirect("授权失败", f"换取 token 失败：{e}", "/taobao/shops", 200)

    def render_account_detail(self, params):
        ensure_ai_copy_table()
        account_name = (params.get("name") or [""])[0]
        if not account_name:
            return self.send_html("缺少账号", "<div class='card'>缺少账号名</div>", 400)

        category = (params.get("category") or ["全部"])[0]
        status_filter = (params.get("status_filter") or ["全部"])[0]
        upload_status_filter = (params.get("upload_status_filter") or [status_filter])[0]
        ai_image_filter = (params.get("ai_image_filter") or ["全部"])[0]
        page = int_param(params, "page", 1)
        show_mode = ((params.get("show") or ["page"])[0] or "page").strip().lower()
        show_all = show_mode == "all"

        tasks = load_task_details(account_name)
        product_pool = load_account_product_pool(account_name)
        product_pool = merge_one8_product_groups(product_pool)

        def upload_status_label(row):
            latest_status = str(row.get("latest_status") or "")
            if latest_status in ("published", "success"):
                return "已确认上架"
            return "未确认上架"

        def matches_upload_status_filter(row, selected_filter: str) -> bool:
            latest_status = str(row.get("latest_status") or "")
            upload_count = int(row.get("upload_count") or 0)
            if selected_filter == "全部":
                return True
            if selected_filter == "已上传":
                return upload_count > 0
            if selected_filter == "已确认上架":
                return latest_status in ("published", "success")
            if selected_filter == "未确认上架":
                return latest_status not in ("published", "success")
            if selected_filter == "失败":
                return latest_status in ("failed", "publish_failed")
            if selected_filter == "已下架":
                return latest_status in ("off_shelved", "off_shelf_failed")
            if selected_filter == "未上传":
                return upload_count <= 0
            return str(row.get("display_status") or "") == selected_filter

        if not product_pool.empty:
            product_pool = product_pool.copy()
            product_pool["display_status"] = product_pool.apply(upload_status_label, axis=1)
            product_pool["ai_image_status"] = product_pool.apply(
                lambda row: "已生成AI图" if row_has_display_ai_images(row, account_name, "xianyu") else "未生成AI图",
                axis=1,
            )
            if upload_status_filter != "全部":
                product_pool = product_pool[product_pool.apply(lambda row: matches_upload_status_filter(row, upload_status_filter), axis=1)]
            if ai_image_filter != "全部":
                product_pool = product_pool[product_pool["ai_image_status"] == ai_image_filter]
        else:
            product_pool["display_status"] = []
            product_pool["ai_image_status"] = []

        filtered_pool_categories = set(product_pool["category"].dropna().tolist()) if not product_pool.empty else set()
        categories = ["全部"] + sorted(filtered_pool_categories)
        if category != "全部":
            tasks = tasks[tasks["category"] == category]
            product_pool = product_pool[product_pool["category"] == category]
        upload_status_filters = ["全部", "未上传", "已上传", "已确认上架", "未确认上架", "失败", "已下架"]
        ai_image_filters = ["全部", "已生成AI图", "未生成AI图"]
        paged_pool, total_products, total_pages, page = paginate_df(product_pool, page, show_all=show_all)
        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        taobao_shops = load_enabled_taobao_shops()
        taobao_shop_options = "".join(
            f"<option value='{int(row['id'])}'>{html.escape(str(row['shop_name'] or ''))}</option>"
            for row in taobao_shops
        )

        def product_name_cell(row):
            name = html.escape(str(row["name"]))
            first_seen = display_beijing_time(row.get("first_seen"), date_only=True)
            group_badge = ""
            if int(row.get("merged_group") or 0) == 1:
                group_badge = f"<span class='badge-new'>组内{int(row.get('merged_group_count') or 0)}款</span><span class='group-toggle-hint'>点击展开</span>"
            if int(row.get("group_member_row") or 0) == 1:
                return f"<span class='group-member-label'>组内成员</span><span class='group-member-name'>{name}</span>"
            if first_seen == today:
                return f"{name}{group_badge}<span class='badge-new'>今日新增</span>"
            return f"{name}{group_badge}"

        def copy_preview_cell(row):
            ai_title_raw = safe_text(row.get("ai_title"))
            ai_description_full = safe_text(row.get("ai_description"))
            ai_description_raw = preview_text(ai_description_full)
            ai_title = html.escape(ai_title_raw)
            ai_description = html.escape(ai_description_raw)
            if not ai_title and not ai_description:
                return "<span class='muted'>未生成</span>"
            parts = []
            if ai_title:
                parts.append(f"<div><strong>标题：</strong>{ai_title}</div>")
            if ai_description:
                parts.append(f"<div style='margin-top:6px;'><strong>简介：</strong>{ai_description}</div>")
            return copy_preview_modal_cell(ai_title_raw, ai_description_full, "", ''.join(parts))

        def group_copy_cell(row):
            if int(row.get("merged_group") or 0) != 1:
                return "<span class='muted'>-</span>"
            return group_copy_preview_cell(int(row.get("merged_group_id") or 0), account_name, "xianyu")

        def row_action_cell(row):
            if int(row.get("merged_group") or 0) == 1:
                merged_ids = html.escape(str(row.get("merged_product_ids") or ""))
                group_id = int(row.get("merged_group_id") or 0)
                return (
                    f"<button type='button' class='mini-btn ghost-btn' data-group-id='{group_id}' data-product-ids='{merged_ids}' data-account-name='{html.escape(account_name)}' data-ai-channel='xianyu' onclick=\"generateGroupProductAi(this, 'title')\">生成标题</button> "
                    f"<button type='button' class='mini-btn ghost-btn' data-group-id='{group_id}' data-product-ids='{merged_ids}' data-account-name='{html.escape(account_name)}' data-ai-channel='xianyu' onclick=\"generateGroupProductAi(this, 'description')\">生成简介</button> "
                    f"<button type='button' class='mini-btn info-btn' data-group-id='{group_id}' data-product-ids='{merged_ids}' data-account-name='{html.escape(account_name)}' data-ai-channel='xianyu' onclick=\"generateGroupProductImageAi(this)\">生成AI主图</button>"
                )
            return (
                f"<button type='button' class='mini-btn ghost-btn' data-product-id='{int(row['product_id'])}' data-account-name='{html.escape(account_name)}' onclick=\"generateProductAi(this, 'title')\">生成标题</button> "
                f"<button type='button' class='mini-btn ghost-btn' data-product-id='{int(row['product_id'])}' data-account-name='{html.escape(account_name)}' onclick=\"generateProductAi(this, 'description')\">生成简介</button> "
                f"<button type='button' class='mini-btn info-btn' data-product-id='{int(row['product_id'])}' data-account-name='{html.escape(account_name)}' data-ai-channel='xianyu' onclick=\"generateProductImageAi(this)\">生成AI主图</button>"
            )

        selected_batch_ids = set()

        task_row_parts = []
        for _, row in tasks.iterrows():
            batch_id = int(row["batch_id"])
            selectable = batch_id not in selected_batch_ids
            if selectable:
                selected_batch_ids.add(batch_id)
            checkbox = (
                f"<input type='checkbox' name='task_batch_ids' value='{batch_id}' aria-label='选择批次 {batch_id}' onclick='event.stopPropagation(); updateSelectionRowState(this)'>"
                if selectable else ""
            )
            task_row_parts.append(
                f"<tr><td class='select-cell' onclick='toggleRowCheckbox(this)'>{checkbox}</td><td>{int(row['task_id'])}</td><td>{image_cell(str(row.get('image_url') or ''))}</td><td><a href='/batch?id={batch_id}'>{batch_id}</a></td><td>{html.escape(str(row['batch_name']))}</td><td>{html.escape(str(row['category']))}</td><td>{html.escape(str(row['name']))}</td><td>{copy_preview_cell(row)}</td><td>{html.escape(str(row['status']))}</td><td>{html.escape(str(row['publish_status'] or ''))}</td><td>{html.escape(str(row['callback_status'] or ''))}</td><td>{html.escape(str(row['err_msg'] or ''))}</td><td>{self.task_action_cell(row)}</td></tr>"
            )
        task_rows = "".join(task_row_parts)
        option_row_parts = []
        for _, row in paged_pool.iterrows():
            is_group = int(row.get("merged_group") or 0) == 1
            is_member = int(row.get("group_member_row") or 0) == 1
            row_class = "group-row" if is_group else ("group-member-row" if is_member else "")
            row_click_attr = f'onclick="toggleGroupMembers({int(row.get("merged_group_id") or 0)})"' if is_group else ""
            option_row_parts.append(
                f"<tr class='{row_class}' data-parent-group-id='{int(row.get('parent_group_id') or 0)}' data-product-filter-text='{html.escape(' '.join([str(int(row['product_id'])), str(row['category']), str(row['name']), str(row.get('merged_color_summary') or '')]))}' {row_click_attr}>"
                f"<td class='select-cell' onclick='toggleRowCheckbox(this)'><input type='checkbox' name='product_id' value='{html.escape(str(row.get('merged_product_ids') or int(row['product_id'])))}' data-group-id='{int(row.get('merged_group_id') or 0)}' data-is-group='{'1' if is_group else '0'}' data-ai-channel='xianyu' data-has-ai-title='{'1' if safe_text(row.get('ai_title')) else '0'}' data-has-ai-description='{'1' if safe_text(row.get('ai_description')) else '0'}' data-has-ai-image='{'1' if row_has_display_ai_images(row, account_name, 'xianyu') else '0'}' onclick='event.stopPropagation(); updateSelectionRowState(this)'></td>"
                f"<td>{product_image_gallery_cell_for_row(row, account_name, 'xianyu')}</td>"
                f"<td>{int(row['product_id'])}</td>"
                f"<td>{html.escape(str(row['category']))}</td>"
                f"<td>{product_name_cell(row)}</td>"
                f"<td>{html.escape(display_beijing_time(row.get('first_seen')))}</td>"
                f"<td>{html.escape(str(row['final_price_cny'] or ''))}</td>"
                f"<td>{html.escape(str(row['stock'] or ''))}</td>"
                f"<td>{int(row.get('hot_index') or 0)}</td>"
                f"<td>{copy_preview_cell(row)}</td>"
                f"<td>{group_copy_cell(row)}</td>"
                f"<td>{row_action_cell(row)}</td>"
                f"<td>{html.escape(str(row.get('display_status') or ''))}</td>"
                f"<td>{html.escape(str(row.get('latest_status') or ''))}</td>"
                f"<td>{int(row.get('upload_count') or 0)}</td>"
                f"</tr>"
            )
        option_rows = "".join(option_row_parts)
        category_buttons = "".join(
            f"<a class='{'active' if cat == category else ''}' href='{html.escape(build_url('/account', name=account_name, category=cat, upload_status_filter=upload_status_filter, ai_image_filter=ai_image_filter, show=show_mode))}'>{html.escape(cat)}</a>"
            for cat in categories
        )
        upload_status_buttons = "".join(
            f"<a class='{'active' if sf == upload_status_filter else ''}' href='{html.escape(build_url('/account', name=account_name, category=category, upload_status_filter=sf, ai_image_filter=ai_image_filter, show=show_mode))}'>{html.escape(sf)}</a>"
            for sf in upload_status_filters
        )
        ai_image_buttons = "".join(
            f"<a class='{'active' if sf == ai_image_filter else ''}' href='{html.escape(build_url('/account', name=account_name, category=category, upload_status_filter=upload_status_filter, ai_image_filter=sf, show=show_mode))}'>{html.escape(sf)}</a>"
            for sf in ai_image_filters
        )
        show_buttons = "".join(
            [
                f"<a class='{'active' if not show_all else ''}' href='{html.escape(build_url('/account', name=account_name, category=category, upload_status_filter=upload_status_filter, ai_image_filter=ai_image_filter, show='page', page=1))}'>分页显示</a>",
                f"<a class='{'active' if show_all else ''}' href='{html.escape(build_url('/account', name=account_name, category=category, upload_status_filter=upload_status_filter, ai_image_filter=ai_image_filter, show='all'))}'>显示全部商品</a>",
            ]
        )
        upload_all_button = (
            f"<button type='button' class='danger-btn' data-account-name='{html.escape(account_name)}' "
            f"data-category='{html.escape(category)}' onclick='uploadCurrentCategory(this)'>上传当前分类全部商品</button>"
            if category and not product_pool.empty
            else ""
        )

        body = f"""
        <div class='card'>
          <h1>账号: {html.escape(account_name)}</h1>
          <div class='toolbar'>
            <span class='muted'>账号商品池 {total_products} 个</span>
            {upload_all_button}
          </div>
          <div class='chips'>{category_buttons}</div>
          <div class='toolbar'><span class='muted'>上传状态：</span><div class='chips'>{upload_status_buttons}</div></div>
          <div class='toolbar'><span class='muted'>AI图状态：</span><div class='chips'>{ai_image_buttons}</div></div>
          <div class='chips'>{show_buttons}</div>
        </div>
        <div class='card'>
          <h2>生成批次</h2>
          <form method='post' action='/batches/create'>
            <input type='hidden' name='account_name' value='{html.escape(account_name)}'>
            <input type='hidden' name='category' value='{html.escape(category)}'>
            <input type='hidden' name='page' value='{page}'>
            <p class='muted'>批次名将自动生成：分类 + 当前日期时间</p>
            <div class='toolbar'>
              <span class='muted'>可先勾选商品后批量生成 AI 主图</span>
              <button type='button' class='ghost-btn' onclick="generateSelectedProductAi('both')">批量生成标题和简介</button>
              <button type='button' class='ghost-btn' onclick="generateSelectedProductAi('title')">批量生成标题</button>
              <button type='button' class='ghost-btn' onclick="generateSelectedProductAi('description')">批量生成简介</button>
              <button type='button' class='info-btn' onclick='generateSelectedProductImages()'>批量生成AI主图</button>
              <input type='search' id='currentPageProductFilterInput' placeholder='搜索当前分页商品' oninput='filterCurrentPageProducts(this.value)' style='min-width:220px;'>
              <span class='muted' id='currentPageProductFilterSummary'></span>
            </div>
            <div class='toolbar sticky-actions'>
              <span class='muted'>勾选完成后可直接在这里提交，不用翻到表格底部</span>
              <button type='submit'>生成批次</button>
            </div>
            <div class='table-wrap'>
                <table data-sortable='true'>
                  <tr><th><input type='checkbox' onclick='toggleAllProducts(this)' aria-label='全选商品'></th><th>图片</th><th>商品ID</th><th>分类</th><th>名称</th><th>首次发现</th><th>价格</th><th>库存</th><th data-sort-index='8' data-sort-label='热门指数'>热门指数</th><th>AI文案预览</th><th>组文案</th><th>AI操作</th><th>上传标记</th><th>最近状态</th><th>上传次数</th></tr>
                {option_rows}
              </table>
            </div>
            {pager_html('/account', page, total_pages, name=account_name, category=category, upload_status_filter=upload_status_filter, ai_image_filter=ai_image_filter, show=show_mode)}
            <p><button type='submit'>生成批次</button></p>
          </form>
        </div>
        <div class='card'>
          <h2>当前账号任务</h2>
          <div class='toolbar'>
            <span class='muted'>已按批次去重勾选，可全选后批量删除</span>
            <button type='button' class='danger-btn' onclick="downShelfAndDeleteSelectedBatches('{html.escape(account_name)}')">批量下架并删除批次</button>
          </div>
          <div class='table-wrap'>
            <table>
              <tr><th><input type='checkbox' onclick='toggleAllTaskBatches(this)' aria-label='全选批次'></th><th>任务ID</th><th>图片</th><th>批次ID</th><th>批次</th><th>分类</th><th>商品</th><th>AI文案预览</th><th>状态</th><th>上架状态</th><th>回调状态</th><th>错误</th><th>操作</th></tr>
              {task_rows}
            </table>
          </div>
        </div>
        """
        self.send_html(f"账号 {account_name}", body)

    def render_batches(self):
        batches = load_batches()
        rows = "".join(
            f"<tr><td class='select-cell' onclick='toggleRowCheckbox(this)'><input type='checkbox' name='batch_ids' value='{int(row['batch_id'])}' aria-label='选择批次 {int(row['batch_id'])}' onclick='event.stopPropagation(); updateSelectionRowState(this)'></td><td><a href='/batch?id={int(row['batch_id'])}'>{int(row['batch_id'])}</a></td><td>{html.escape(str(row['account_name']))}</td><td>{html.escape(str(row['batch_name']))}</td><td>{html.escape(str(row['status']))}</td><td>{int(row['total_count'] or 0)}</td><td>{int(row['success_count'] or 0)}</td><td>{int(row['failed_count'] or 0)}</td><td><button type='button' class='mini-btn danger-btn' data-batch-id='{int(row['batch_id'])}' onclick='downShelfAndDeleteBatch(this)'>下架并删除</button></td></tr>"
            for _, row in batches.iterrows()
        )
        self.send_html(
            "批次列表",
            f"""
            <div class='card'>
              <h1>批次列表</h1>
              <div class='toolbar'>
                <span class='muted'>可勾选后批量删除批次</span>
                <button type='button' class='danger-btn' onclick='downShelfAndDeleteBatchRows()'>批量删除</button>
                <button type='button' class='ghost-btn' onclick='forceDeleteBatchRows()'>强制删除本地记录</button>
              </div>
              <div class='table-wrap'>
                <table>
                  <tr><th><input type='checkbox' onclick='toggleAllBatchRows(this)' aria-label='全选批次'></th><th>ID</th><th>账号</th><th>批次名</th><th>状态</th><th>总数</th><th>成功</th><th>失败</th><th>操作</th></tr>
                  {rows}
                </table>
              </div>
            </div>
            """,
        )

    def render_daily_runs(self):
        rows = load_daily_runs(100)
        html_rows = "".join(
            f"<tr><td><a href='/daily-run?id={int(row['id'])}'>{int(row['id'])}</a></td><td>{html.escape(str(row['run_type'] or ''))}</td><td>{html.escape(str(row['trigger_mode'] or ''))}</td><td>{html.escape(str(row['status'] or ''))}</td><td>{html.escape(str(row['host'] or ''))}</td><td>{html.escape(str(row['pid'] or ''))}</td><td>{html.escape(display_beijing_time(row['started_at']))}</td><td>{html.escape(display_beijing_time(row['finished_at']))}</td><td>{html.escape(str(row['log_file'] or ''))}</td><td>{html.escape(str(row['note'] or ''))}</td></tr>"
            for row in rows
        )
        self.send_html(
            "日更任务",
            f"""
            <div class='card'>
              <h1>日更任务</h1>
              <div class='muted'>展示最近 100 轮日更运行记录，可点击查看每一步详情。</div>
              <div class='table-wrap'>
                <table data-sortable='true'>
                  <thead>
                    <tr>
                      <th data-sort-index='0'>Run ID</th>
                      <th data-sort-index='1'>类型</th>
                      <th data-sort-index='2'>触发方式</th>
                      <th data-sort-index='3'>状态</th>
                      <th data-sort-index='4'>主机</th>
                      <th data-sort-index='5'>PID</th>
                      <th data-sort-index='6'>开始时间</th>
                      <th data-sort-index='7'>结束时间</th>
                      <th data-sort-index='8'>日志文件</th>
                      <th data-sort-index='9'>备注</th>
                    </tr>
                  </thead>
                  <tbody>{html_rows}</tbody>
                </table>
              </div>
            </div>
            """,
        )

    def render_daily_run_detail(self, params):
        run_id = (params.get("id") or [""])[0]
        if not run_id.isdigit():
            return self.send_html("参数错误", "<div class='card'>缺少有效日更任务ID</div>", 400)
        run_row = load_daily_run(int(run_id))
        if not run_row:
            return self.send_html("未找到任务", "<div class='card'>日更任务不存在</div>", 404)
        step_rows = load_daily_run_steps(int(run_id))
        rows_html = "".join(
            f"<tr><td>{html.escape(str(row['step_key'] or ''))}</td><td>{html.escape(str(row['step_name'] or ''))}</td><td>{html.escape(str(row['status'] or ''))}</td><td>{html.escape(format_step_progress(row))}</td><td>{html.escape(str(row['message'] or ''))}</td><td>{html.escape(display_beijing_time(row['started_at']))}</td><td>{html.escape(display_beijing_time(row['finished_at']))}</td><td>{html.escape(display_beijing_time(row['updated_at']))}</td></tr>"
            for row in step_rows
        )
        meta = f"""
        <div class='card'>
          <h1>日更任务 #{int(run_row['id'])}</h1>
          <div class='grid'>
            <div class='metric'><span>类型</span><strong style='font-size:20px;'>{html.escape(str(run_row['run_type'] or ''))}</strong></div>
            <div class='metric'><span>状态</span><strong style='font-size:20px;'>{html.escape(str(run_row['status'] or ''))}</strong></div>
            <div class='metric'><span>触发方式</span><strong style='font-size:20px;'>{html.escape(str(run_row['trigger_mode'] or ''))}</strong></div>
            <div class='metric'><span>PID</span><strong style='font-size:20px;'>{html.escape(str(run_row['pid'] or ''))}</strong></div>
          </div>
          <div class='toolbar' style='margin-top:14px;'>
            <span class='muted'>开始：{html.escape(display_beijing_time(run_row['started_at']))}</span>
            <span class='muted'>结束：{html.escape(display_beijing_time(run_row['finished_at']))}</span>
            <span class='muted'>主机：{html.escape(str(run_row['host'] or ''))}</span>
          </div>
          <div class='muted' style='margin-top:8px;'>日志：{html.escape(str(run_row['log_file'] or ''))}</div>
          <div class='muted' style='margin-top:6px;'>备注：{html.escape(str(run_row['note'] or ''))}</div>
        </div>
        """
        self.send_html(
            f"日更任务 #{int(run_row['id'])}",
            meta
            + f"""
            <div class='card'>
              <h2>步骤详情</h2>
              <div class='table-wrap'>
                <table data-sortable='true'>
                  <thead>
                    <tr>
                      <th data-sort-index='0'>步骤Key</th>
                      <th data-sort-index='1'>步骤名称</th>
                      <th data-sort-index='2'>状态</th>
                      <th data-sort-index='3'>进度</th>
                      <th data-sort-index='4'>消息</th>
                      <th data-sort-index='5'>开始时间</th>
                      <th data-sort-index='6'>结束时间</th>
                      <th data-sort-index='7'>最近更新时间</th>
                    </tr>
                  </thead>
                  <tbody>{rows_html}</tbody>
                </table>
              </div>
            </div>
            """,
        )

    def render_batch_detail(self, params):
        ensure_ai_copy_table()
        batch_id = (params.get("id") or [""])[0]
        if not batch_id.isdigit():
            return self.send_html("缺少批次", "<div class='card'>缺少有效批次ID</div>", 400)
        tasks = load_task_details()
        tasks = tasks[tasks["batch_id"] == int(batch_id)]
        total_count = len(tasks)
        created_count = int(((tasks["status"] == "created") | (tasks["publish_status"] == "created")).sum()) if not tasks.empty else 0
        submitted_count = int(((tasks["status"] == "submitted") | (tasks["publish_status"] == "submitted")).sum()) if not tasks.empty else 0
        published_count = int(((tasks["status"].isin(["published", "success"])) | (tasks["publish_status"].isin(["published", "success"])) | (tasks["callback_status"].isin(["published", "success"]))).sum()) if not tasks.empty else 0
        failed_count = int(((tasks["status"].isin(["failed", "publish_failed"])) | (tasks["publish_status"].isin(["failed", "publish_failed"])) | (tasks["callback_status"].isin(["publish_failed", "failed"]))).sum()) if not tasks.empty else 0
        action_bar = f"""
        <div class='card'>
          <h1>批次 {batch_id}</h1>
          <div class='toolbar'>
            <span class='muted'>总数：{total_count}</span>
            <span class='muted'>已创建未上架：{created_count}</span>
            <span class='muted'>已提交上架：{submitted_count}</span>
            <span class='muted'>已确认发布：{published_count}</span>
            <span class='muted'>失败：{failed_count}</span>
            <button type='button' class='ghost-btn' data-batch-id='{batch_id}' onclick="refreshBatchStatus(this)">刷新状态</button>
          </div>
          <div class='toolbar'>
            <button type='button' class='ghost-btn' data-batch-id='{batch_id}' onclick="generateBatchAi(this, 'title')">批量生成标题</button>
            <button type='button' class='ghost-btn' data-batch-id='{batch_id}' onclick="generateBatchAi(this, 'description')">批量生成简介</button>
            <button type='button' class='info-btn' data-batch-id='{batch_id}' onclick="runBatchAction(this, 'create')">执行创建</button>
            <button type='button' class='ghost-btn' data-batch-id='{batch_id}' onclick="runBatchAction(this, 'create_failed')">仅重试失败创建</button>
            <input type='datetime-local' id='publishTimeInput' step='60'>
            <button type='button' class='danger-btn' data-batch-id='{batch_id}' onclick="runBatchAction(this, 'publish')">执行上架</button>
            <button type='button' class='ghost-btn' data-batch-id='{batch_id}' onclick="rebuildBatchWithPng(this)">首图转PNG并重建未上架商品</button>
            <button type='button' class='ghost-btn' data-batch-id='{batch_id}' onclick="runBatchAction(this, 'publish_created')">仅提交未上架商品</button>
            <button type='button' class='ghost-btn' data-batch-id='{batch_id}' onclick="runBatchAction(this, 'publish_pending_only')">只处理待发布商品</button>
            <button type='button' class='ghost-btn' data-batch-id='{batch_id}' onclick="runBatchAction(this, 'publish_retry_failed')">查询并重试失败上架</button>
            <span class='muted'>不填则立即上架，填写后按该时间定时上架</span>
          </div>
          <div class='muted'>状态说明：`created`=已创建未提交上架，`submitted`=已提交上架等待回调，`published`/`success`=已确认发布成功，`failed`/`publish_failed`=处理失败。</div>
        </div>
        """
        def batch_row_class(row):
            status = str(row.get("status") or "")
            third_product_id = str(row.get("third_product_id") or "").strip()
            if status in ("failed", "publish_failed"):
                return "row-failed"
            if third_product_id or status in ("created", "submitted", "published", "success"):
                return "row-success"
            return ""
        row_parts = []
        for _, row in tasks.iterrows():
            title_raw = safe_text(row.get("ai_title"))
            description_full = safe_text(row.get("ai_description"))
            title_preview = html.escape(title_raw)
            if not title_preview:
                title_preview = "<span class='muted'>未生成</span>"
            description_preview = html.escape(preview_text(description_full))
            preview_html = f"<div><strong>标题：</strong>{title_preview}</div><div style='margin-top:6px;'><strong>简介：</strong>{description_preview}</div>"
            display_name = safe_text(row.get("name"))
            if str(row.get("publish_mode") or "single").strip() == "group":
                member_count = len([item for item in str(row.get("group_member_product_ids") or "").split(",") if item.strip()])
                if member_count > 0:
                    display_name = f"{display_name} 组内{member_count}款"
            row_parts.append(
                f"<tr class='{batch_row_class(row)}'><td>{int(row['task_id'])}</td><td>{batch_task_image_gallery_cell(row)}</td><td>{html.escape(str(row['account_name']))}</td><td>{html.escape(display_name)}</td><td>{batch_task_sku_preview_cell(row)}</td><td>{copy_preview_modal_cell(title_raw, description_full, '', preview_html)}</td><td><button type='button' class='mini-btn ghost-btn' data-task-id='{int(row['task_id'])}' data-account-name='{html.escape(str(row.get('account_name') or ''))}' onclick=\"generateProductAi(this, 'title')\">生成标题</button> <button type='button' class='mini-btn ghost-btn' data-task-id='{int(row['task_id'])}' data-account-name='{html.escape(str(row.get('account_name') or ''))}' onclick=\"generateProductAi(this, 'description')\">生成简介</button></td><td>{html.escape(str(row['status']))}</td><td>{html.escape(display_publish_status(str(row.get('publish_status') or ''), str(row.get('callback_status') or '')))}</td><td>{html.escape(extract_publish_schedule(str(row.get('task_result') or ''), str(row.get('status') or ''), str(row.get('publish_status') or ''), str(row.get('callback_status') or '')))}</td><td>{html.escape(str(row['callback_status'] or ''))}</td><td>{html.escape(str(row['third_product_id'] or ''))}</td><td>{html.escape(str(row['err_msg'] or ''))}</td></tr>"
            )
        rows = "".join(row_parts)
        self.send_html(
            f"批次 {batch_id}",
            action_bar
            + f"<div class='card'><div class='table-wrap'><table data-sortable='true'><thead><tr>"
              "<th data-sort-index='0'>任务ID</th>"
              "<th data-sort-index='1'>图片</th>"
              "<th data-sort-index='2'>账号</th>"
              "<th data-sort-index='3'>商品</th>"
              "<th data-sort-index='4'>SKU预览</th>"
              "<th data-sort-index='5'>AI文案预览</th>"
              "<th data-sort-index='6'>AI操作</th>"
              "<th data-sort-index='7'>状态</th>"
              "<th data-sort-index='8'>上架状态</th>"
              "<th data-sort-index='9'>定时上架</th>"
              "<th data-sort-index='10'>回调状态</th>"
              "<th data-sort-index='11'>第三方商品ID</th>"
              "<th data-sort-index='12'>错误</th>"
              f"</tr></thead><tbody>{rows}</tbody></table></div></div>",
        )

    def render_callbacks(self):
        conn = get_conn()
        rows = conn.execute("""
            SELECT id, third_product_id, callback_type, callback_status, err_code, err_msg, created_at
            FROM xianyu_callbacks
            ORDER BY id DESC
            LIMIT 200
        """).fetchall()
        conn.close()
        html_rows = "".join(
            f"<tr><td>{row['id']}</td><td>{html.escape(str(row['third_product_id'] or ''))}</td><td>{html.escape(str(row['callback_type'] or ''))}</td><td>{html.escape(str(row['callback_status'] or ''))}</td><td>{html.escape(str(row['err_code'] or ''))}</td><td>{html.escape(str(row['err_msg'] or ''))}</td><td>{html.escape(display_beijing_time(row['created_at']))}</td></tr>"
            for row in rows
        )
        self.send_html("回调记录", f"<div class='card'><h1>回调记录</h1><table><tr><th>ID</th><th>第三方商品ID</th><th>类型</th><th>状态</th><th>错误码</th><th>错误信息</th><th>时间</th></tr>{html_rows}</table></div>")

    def create_batch_from_form(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        account_name = (form.get("account_name") or [""])[0].strip()
        category = (form.get("category") or ["全部"])[0].strip()
        page = (form.get("page") or ["1"])[0].strip()
        selected_group_cover_ids = {
            int(item.strip())
            for item in form.get("group_cover_selected", [])
            if str(item or "").strip().isdigit()
        }
        selected_items = []
        for raw_value in form.get("product_id", []):
            ids = [int(item.strip()) for item in str(raw_value or "").split(",") if item.strip().isdigit()]
            if not ids:
                continue
            selected_items.append({"raw": str(raw_value or "").strip(), "product_ids": list(dict.fromkeys(ids))})

        if not account_name or not selected_items:
            return self.send_html("参数错误", "<div class='card'>账号和商品不能为空</div>", 400)

        batch_name = self.build_auto_batch_name(category)
        ensure_xianyu_group_task_support()

        conn = get_conn()
        cur = conn.cursor()
        account_row = cur.execute("SELECT id FROM xianyu_accounts WHERE account_name = ? AND enabled = 1", (account_name,)).fetchone()
        if not account_row:
            conn.close()
            return self.send_html("账号不存在", "<div class='card'>账号不存在或未启用</div>", 400)

        cur.execute("""
            INSERT INTO xianyu_publish_batches (account_id, batch_name, status)
            VALUES (?, ?, 'pending')
        """, (account_row["id"], batch_name))
        batch_id = cur.lastrowid

        for item in selected_items:
            product_ids = item["product_ids"]
            if len(product_ids) <= 1:
                cur.execute("""
                    INSERT INTO xianyu_publish_tasks (
                        account_id, batch_id, product_id, publish_mode, cover_product_id, status, publish_status
                    )
                    VALUES (?, ?, ?, 'single', ?, 'pending', 'pending')
                """, (account_row["id"], batch_id, int(product_ids[0]), int(product_ids[0])))
                continue

            group_row = find_group_by_member_ids(product_ids)
            cover_product_id = int(product_ids[0])
            group_id = int(group_row["id"]) if group_row else None
            group_copy = load_group_ai_copy(group_id, account_name=account_name, channel="xianyu") if group_id else None
            group_ai_title = str((group_copy["ai_title"] if group_copy else "") or "").strip()
            group_ai_description = str((group_copy["ai_description"] if group_copy else "") or "").strip()
            cover_image_path = ""
            selected_group_images = []
            if group_id and group_id in selected_group_cover_ids:
                ai_cover_path = Path(build_group_ai_cover_path(group_id, account_name, "xianyu"))
                if ai_cover_path.exists():
                    cover_image_path = str(ai_cover_path)
                    selected_group_images.append({"type": "group_cover", "label": "组封面", "path": cover_image_path})
            for product_id in product_ids:
                conn2 = get_conn()
                product_row = conn2.execute("SELECT COALESCE(color, '') AS color FROM products WHERE id = ? LIMIT 1", (product_id,)).fetchone()
                conn2.close()
                color_label = str((product_row["color"] if product_row else "") or "").strip() or f"成员{product_id}"
                for ai_row in list_ai_images(product_id, account_name=account_name, asset_type="main"):
                    if int(ai_row.get("is_selected") or 0) != 1:
                        continue
                    image_path = str(ai_row.get("ai_main_image_path") or "").strip()
                    if not image_path:
                        continue
                    selected_group_images.append(
                        {
                            "type": "member_ai",
                            "label": color_label,
                            "path": image_path,
                            "product_id": int(product_id),
                            "image_id": int(ai_row.get("id") or 0),
                        }
                    )
            cur.execute("""
                INSERT INTO xianyu_publish_tasks (
                    account_id,
                    batch_id,
                    product_id,
                    publish_mode,
                    group_id,
                    group_member_product_ids,
                    cover_product_id,
                    cover_image_path,
                    ai_title,
                    ai_description,
                    selected_group_images_json,
                    status,
                    publish_status
                )
                VALUES (?, ?, ?, 'group', ?, ?, ?, ?, ?, ?, ?, 'pending', 'pending')
            """, (
                account_row["id"],
                batch_id,
                cover_product_id,
                group_id,
                ",".join(str(pid) for pid in product_ids),
                cover_product_id,
                cover_image_path,
                group_ai_title,
                group_ai_description,
                json.dumps(selected_group_images, ensure_ascii=False),
            ))

        cur.execute("""
            UPDATE xianyu_publish_batches
            SET total_count = (SELECT COUNT(*) FROM xianyu_publish_tasks WHERE batch_id = ?),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (batch_id, batch_id))
        conn.commit()
        conn.close()
        self.redirect(build_url("/batch", id=batch_id, category=category, page=page))

    def run_batch(self, skip_publish: bool):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        batch_id = (form.get("batch_id") or [""])[0].strip()
        if not batch_id.isdigit():
            return self.send_html("参数错误", "<div class='card'>缺少有效批次ID</div>", 400)

        execute_batch(int(batch_id), execute=True, skip_publish=skip_publish)
        self.redirect(build_url("/batch", id=batch_id))

    def upload_category(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        account_name = (form.get("account_name") or [""])[0].strip()
        category = (form.get("category") or [""])[0].strip()
        if not account_name or not category:
            return self.send_json({"ok": False, "message": "缺少账号或分类"}, 400)

        product_pool = load_account_product_pool(account_name)
        if category != "全部":
            product_pool = product_pool[product_pool["category"] == category]
        if product_pool.empty:
            return self.send_json({"ok": False, "message": "当前分类没有可上传商品"}, 400)

        batch_id = self.create_batch(
            account_name=account_name,
            batch_name=self.build_auto_batch_name(category),
            product_ids=[int(pid) for pid in product_pool["product_id"].tolist()],
        )
        result = execute_batch(batch_id, execute=True, skip_publish=False)
        ok = result.get("failed_count", 0) == 0
        return self.send_json({
            "ok": ok,
            "message": "当前分类上传完成" if ok else "当前分类上传完成，但有失败任务",
            "batch_id": batch_id,
            "total_count": len(product_pool),
            "success_count": result.get("success_count", 0),
            "failed_count": result.get("failed_count", 0),
            "failures": result.get("failures", []),
        })

    def execute_batch_ajax(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        batch_id = (form.get("batch_id") or [""])[0].strip()
        mode = (form.get("mode") or [""])[0].strip()
        upload_watermark = (form.get("upload_watermark") or ["0"])[0].strip() == "1"
        specify_publish_time = (form.get("specify_publish_time") or [""])[0].strip()
        auto_stagger_publish = (form.get("auto_stagger_publish") or ["0"])[0].strip() == "1"
        if not batch_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效批次ID"}, 400)
        if mode not in ("create", "create_failed", "publish", "publish_created", "publish_retry_failed", "publish_pending_only"):
            return self.send_json({"ok": False, "message": "缺少有效执行模式"}, 400)
        batch_id_int = int(batch_id)
        job_id = uuid.uuid4().hex
        action_label = (
            "重试失败创建"
            if mode == "create_failed"
            else ("仅提交未上架商品" if mode == "publish_created" else ("查询并重试失败上架" if mode == "publish_retry_failed" else ("只处理待发布商品" if mode == "publish_pending_only" else ("创建" if mode == "create" else "上架"))))
        )
        progress_text = f"后台正在执行批次{action_label}，请不要关闭页面。"

        with BATCH_EXEC_JOBS_LOCK:
            BATCH_EXEC_JOBS[job_id] = {
                "job_id": job_id,
                "batch_id": batch_id_int,
                "mode": mode,
                "status": "queued",
                "message": f"批次{action_label}任务已提交",
                "progress_text": progress_text,
                "result": None,
            }

        thread = threading.Thread(
            target=self._run_batch_execute_job,
            args=(job_id, batch_id_int, mode, upload_watermark, specify_publish_time, auto_stagger_publish),
            daemon=True,
        )
        thread.start()
        return self.send_json({
            "ok": True,
            "job_id": job_id,
            "batch_id": batch_id_int,
            "message": f"批次{action_label}任务已提交",
        })

    def refresh_batch_status(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        batch_id = (form.get("batch_id") or [""])[0].strip()
        if not batch_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效批次ID"}, 400)

        batch_id_int = int(batch_id)
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id
            FROM xianyu_publish_tasks
            WHERE batch_id = ?
            ORDER BY id
            """,
            (batch_id_int,),
        ).fetchall()
        conn.close()

        task_ids = [int(row["id"]) for row in rows]
        if not task_ids:
            return self.send_json({"ok": False, "message": "当前批次没有任务"}, 404)

        reconciled = reconcile_remote_created_tasks(batch_id_int, task_ids, use_batch_window=True)
        conn = get_conn()
        now_local = datetime.now()
        timed_out_task_ids = []
        timeout_message = "定时上架时间已过仍未发布，已标记为失败，可重新创建"
        rows = conn.execute(
            """
            SELECT id, status, publish_status, callback_status, third_product_id, task_result
            FROM xianyu_publish_tasks
            WHERE batch_id = ?
            ORDER BY id
            """,
            (batch_id_int,),
        ).fetchall()
        for row in rows:
            status = str(row["status"] or "").strip()
            publish_status = str(row["publish_status"] or "").strip()
            callback_status = str(row["callback_status"] or "").strip()
            current_statuses = {status, publish_status, callback_status}
            if current_statuses & {"published", "success", "failed", "publish_failed"}:
                continue
            scheduled_dt = extract_scheduled_publish_datetime(row["task_result"])
            if not scheduled_dt or scheduled_dt > now_local:
                continue
            conn.execute(
                """
                UPDATE xianyu_publish_tasks
                SET status = 'failed',
                    publish_status = 'failed',
                    callback_status = 'publish_failed',
                    third_product_id = '',
                    err_msg = ?,
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (timeout_message, timeout_message, int(row["id"])),
            )
            timed_out_task_ids.append(int(row["id"]))
        conn.commit()
        conn.close()

        update_batch_counts(batch_id_int)

        conn = get_conn()
        summary = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'published' OR publish_status = 'published' THEN 1 ELSE 0 END) AS published_count,
                SUM(CASE WHEN status = 'created' OR publish_status = 'created' THEN 1 ELSE 0 END) AS created_count,
                SUM(CASE WHEN status IN ('failed', 'publish_failed') OR publish_status IN ('failed', 'publish_failed') THEN 1 ELSE 0 END) AS failed_count
            FROM xianyu_publish_tasks
            WHERE batch_id = ?
            """,
            (batch_id_int,),
        ).fetchone()
        conn.close()

        return self.send_json({
            "ok": True,
            "batch_id": batch_id_int,
            "matched_count": int(reconciled.get("matched_count") or 0),
            "published_count": int(summary["published_count"] or 0) if summary else 0,
            "created_count": int(summary["created_count"] or 0) if summary else 0,
            "failed_count": int(summary["failed_count"] or 0) if summary else 0,
            "timed_out_count": len(timed_out_task_ids),
            "message": "批次状态已刷新",
        })

    def rebuild_batch_with_png(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        batch_id = (form.get("batch_id") or [""])[0].strip()
        if not batch_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效批次ID"}, 400)

        batch_id_int = int(batch_id)
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT
                t.id,
                t.product_id,
                t.cover_image_path,
                p.local_image_path,
                COALESCE(a.account_name, '') AS account_name
            FROM xianyu_publish_tasks t
            JOIN products p ON p.id = t.product_id
            LEFT JOIN xianyu_accounts a ON a.id = t.account_id
            WHERE t.batch_id = ?
              AND t.status NOT IN ('published', 'success')
            ORDER BY t.id
            """,
            (batch_id_int,),
        ).fetchall()
        conn.close()
        if not rows:
            return self.send_json({"ok": False, "message": "当前批次没有可重建的未上架商品"}, 404)

        converted_count = 0
        reset_count = 0
        failures = []
        for row in rows:
            task_id = int(row["id"])
            product_id = int(row["product_id"] or 0)
            account_name = str(row["account_name"] or "").strip()
            source_path = ""

            if product_id and account_name:
                conn = get_conn()
                ai_row = conn.execute(
                    """
                    SELECT ai_main_image_path
                    FROM xianyu_product_ai_images
                    WHERE product_id = ?
                      AND COALESCE(account_name, '') = ?
                      AND COALESCE(is_selected, 0) = 1
                      AND TRIM(COALESCE(ai_main_image_path, '')) != ''
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (product_id, account_name),
                ).fetchone()
                conn.close()
                if ai_row:
                    ai_path = str(ai_row["ai_main_image_path"] or "").strip()
                    if ai_path:
                        try:
                            source_path = build_watermarked_upload_variant(ai_path, account_name)
                        except Exception:
                            source_path = ai_path

            if not source_path:
                source_path = str(row["cover_image_path"] or "").strip()
            if not source_path or source_path.startswith("http://") or source_path.startswith("https://"):
                source_path = str(row["local_image_path"] or "").strip()
            if not source_path:
                failures.append({"task_id": task_id, "error": "缺少可用本地图"})
                continue
            try:
                png_path = build_standard_png_variant(source_path)
                converted_count += 1
                update_task_meta(
                    task_id,
                    cover_image_path=png_path,
                    third_product_id="",
                    status="pending",
                    publish_status="",
                    callback_status="",
                    last_error="",
                    err_code="",
                    err_msg="",
                    task_result="",
                    published_at=None,
                    last_callback_time=None,
                )
                reset_count += 1
            except Exception as e:
                failures.append({"task_id": task_id, "error": str(e)})

        update_batch_counts(batch_id_int)
        return self.send_json({
            "ok": not failures,
            "batch_id": batch_id_int,
            "converted_count": converted_count,
            "reset_count": reset_count,
            "failed_count": len(failures),
            "failures": failures,
            "message": f"已为 {reset_count} 个未上架商品生成 PNG 首图并重置为可重建状态",
        }, 200 if not failures else 207)

    def _run_batch_execute_job(
        self,
        job_id: str,
        batch_id: int,
        mode: str,
        upload_watermark: bool,
        specify_publish_time: str,
        auto_stagger_publish: bool,
    ):
        action_label = "创建" if mode == "create" else "上架"
        def update_job_progress(message: str):
            with BATCH_EXEC_JOBS_LOCK:
                live_job = BATCH_EXEC_JOBS.get(job_id)
                if live_job:
                    live_job["progress_text"] = message
                    live_job["message"] = message

        with BATCH_EXEC_JOBS_LOCK:
            job = BATCH_EXEC_JOBS.get(job_id)
            if job:
                job["status"] = "running"
                job["message"] = f"批次{action_label}执行中"
                job["progress_text"] = f"后台正在执行批次{action_label}，请不要关闭页面。"
        try:
            result = execute_batch(
                batch_id,
                execute=True,
                skip_publish=(mode in ("create", "create_failed")),
                failed_only=(mode == "create_failed"),
                created_only=(mode == "publish_created"),
                publish_retry_only=(mode == "publish_retry_failed"),
                recreate_pending_only=(mode == "publish_pending_only"),
                upload_watermark=upload_watermark,
                specify_publish_time=specify_publish_time,
                auto_stagger_publish=auto_stagger_publish,
                progress_callback=update_job_progress,
            )
            with BATCH_EXEC_JOBS_LOCK:
                job = BATCH_EXEC_JOBS.get(job_id)
                if job:
                    job["status"] = "succeeded"
                    job["message"] = f"执行{action_label}完成"
                    job["progress_text"] = f"批次{action_label}已完成，正在整理结果。"
                    job["result"] = result
        except Exception as exc:
            with BATCH_EXEC_JOBS_LOCK:
                job = BATCH_EXEC_JOBS.get(job_id)
                if job:
                    job["status"] = "failed"
                    job["message"] = str(exc)
                    job["progress_text"] = f"批次{action_label}执行失败"
                    job["result"] = {
                        "success_count": 0,
                        "failed_count": 1,
                        "failures": [{"batch_id": batch_id, "error": str(exc)}],
                    }

    def get_batch_execute_status(self, params):
        job_id = (params.get("job_id") or [""])[0].strip()
        if not job_id:
            return self.send_json({"ok": False, "message": "缺少任务ID"}, 400)
        with BATCH_EXEC_JOBS_LOCK:
            job = dict(BATCH_EXEC_JOBS.get(job_id) or {})
        if not job:
            return self.send_json({"ok": False, "message": "批次任务不存在"}, 404)
        batch_id = job.get("batch_id")
        if batch_id and job.get("status") in ("queued", "running") and job.get("result") is None and job.get("mode") != "create_failed":
            conn = get_conn()
            batch_row = conn.execute(
                """
                SELECT id, status, total_count, success_count, failed_count
                FROM xianyu_publish_batches
                WHERE id = ?
                """,
                (batch_id,),
            ).fetchone()
            failure_rows = []
            if batch_row and str(batch_row["status"] or "") in ("completed", "partial_failed"):
                if int(batch_row["failed_count"] or 0) > 0:
                    failure_rows = conn.execute(
                        """
                        SELECT id, substr(COALESCE(err_msg, last_error, ''), 1, 300) AS error
                        FROM xianyu_publish_tasks
                        WHERE batch_id = ? AND status = 'failed'
                        ORDER BY id
                        LIMIT 50
                        """,
                        (batch_id,),
                    ).fetchall()
                result = {
                    "success_count": int(batch_row["success_count"] or 0),
                    "failed_count": int(batch_row["failed_count"] or 0),
                    "total_count": int(batch_row["total_count"] or 0),
                    "failures": [
                        {"batch_id": batch_id, "task_id": int(row["id"]), "error": str(row["error"] or "失败")}
                        for row in failure_rows
                    ],
                }
                with BATCH_EXEC_JOBS_LOCK:
                    live_job = BATCH_EXEC_JOBS.get(job_id)
                    if live_job and live_job.get("result") is None:
                        live_job["status"] = "succeeded"
                        live_job["message"] = (
                            "仅重试失败创建完成"
                            if job.get("mode") == "create_failed"
                            else ("执行创建完成" if job.get("mode") == "create" else "执行上架完成")
                        )
                        live_job["progress_text"] = "批次任务已完成，正在整理结果。"
                        live_job["result"] = result
                        job = dict(live_job)
            conn.close()
        payload = {
            "ok": True,
            "job_id": job_id,
            "batch_id": job.get("batch_id"),
            "status": job.get("status") or "queued",
            "message": job.get("message") or "",
            "progress_text": job.get("progress_text") or "后台正在处理，请不要关闭页面。",
        }
        if job.get("result") is not None:
            result = job["result"] or {}
            payload["result"] = result
            for key in ("success_count", "failed_count", "failures", "skip_count", "total_count"):
                if key in result:
                    payload[key] = result[key]
        return self.send_json(payload)

    def generate_batch_ai(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        batch_id = (form.get("batch_id") or [""])[0].strip()
        mode = (form.get("mode") or ["title"])[0].strip()
        if not batch_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效批次ID"}, 400)
        if mode not in ("title", "description"):
            return self.send_json({"ok": False, "message": "缺少有效生成模式"}, 400)
        if mode == "title":
            result = generate_titles_for_batch(int(batch_id), force=True, sleep_seconds=1.5)
        else:
            result = generate_descriptions_for_batch(int(batch_id), force=True, sleep_seconds=1.5)
        ok = result.get("failed_count", 0) == 0
        return self.send_json({
            "ok": ok,
            "message": f"AI{'标题' if mode == 'title' else '简介'}生成完成" if ok else f"AI{'标题' if mode == 'title' else '简介'}生成完成，但有失败任务",
            "batch_id": int(batch_id),
            "total_count": result.get("total_count", 0),
            "success_count": result.get("success_count", 0),
            "failed_count": result.get("failed_count", 0),
            "failures": result.get("failures", []),
        })

    def generate_product_ai(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        product_id = (form.get("product_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        mode = (form.get("mode") or ["title"])[0].strip()
        if not product_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效商品ID"}, 400)
        if mode not in ("title", "description"):
            return self.send_json({"ok": False, "message": "缺少有效生成模式"}, 400)
        try:
            if mode == "title":
                result = generate_title_for_product(int(product_id), force=True, account_name=account_name)
            else:
                result = generate_description_for_product(int(product_id), force=True, account_name=account_name)
        except Exception as e:
            return self.send_json({"ok": False, "message": str(e)}, 500)
        payload = {"ok": True, "message": f"商品AI{'标题' if mode == 'title' else '简介'}生成完成", "product_id": int(product_id)}
        payload.update(result)
        return self.send_json(payload)

    def generate_products_ai_batch(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        mode = (form.get("mode") or ["title"])[0].strip().lower()
        account_name = (form.get("account_name") or [""])[0].strip()
        product_ids_text = (form.get("product_ids") or [""])[0].strip()
        product_ids = [int(pid) for pid in product_ids_text.split(",") if pid.strip().isdigit()]
        if not product_ids:
            return self.send_json({"ok": False, "message": "缺少有效商品ID"}, 400)
        if mode not in ("title", "description", "both"):
            return self.send_json({"ok": False, "message": "缺少有效生成模式"}, 400)
        try:
            if mode == "title":
                result = generate_titles_for_products(product_ids, force=False, account_name=account_name)
            elif mode == "description":
                result = generate_descriptions_for_products(product_ids, force=False, account_name=account_name)
            else:
                title_result = generate_titles_for_products(product_ids, force=False, account_name=account_name)
                description_result = generate_descriptions_for_products(product_ids, force=False, account_name=account_name)
                result = {
                    "total_count": len(product_ids),
                    "success_count": int(title_result.get("success_count") or 0),
                    "skipped_count": int(title_result.get("skipped_count") or 0),
                    "failed_count": int(title_result.get("failed_count") or 0),
                    "failures": title_result.get("failures") or [],
                    "description_success_count": int(description_result.get("success_count") or 0),
                    "description_skipped_count": int(description_result.get("skipped_count") or 0),
                    "description_failed_count": int(description_result.get("failed_count") or 0),
                    "description_failures": description_result.get("failures") or [],
                }
        except Exception as e:
            return self.send_json({"ok": False, "message": str(e)}, 500)
        return self.send_json({
            "ok": True,
            "message": f"批量AI{'标题' if mode == 'title' else ('简介' if mode == 'description' else '标题和简介')}生成完成",
            "total_count": int(result.get("total_count") or len(product_ids)),
            "success_count": int(result.get("success_count") or 0),
            "skipped_count": int(result.get("skipped_count") or 0),
            "failed_count": int(result.get("failed_count") or 0),
            "failures": result.get("failures") or [],
            "description_success_count": int(result.get("description_success_count") or 0),
            "description_skipped_count": int(result.get("description_skipped_count") or 0),
            "description_failed_count": int(result.get("description_failed_count") or 0),
            "description_failures": result.get("description_failures") or [],
            "data": result,
        })

    def generate_group_product_ai_copy(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        group_id = (form.get("group_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        channel = (form.get("channel") or ["xianyu"])[0].strip().lower() or "xianyu"
        mode = (form.get("mode") or ["title"])[0].strip().lower()
        if not group_id.isdigit() or int(group_id) <= 0:
            return self.send_json({"ok": False, "message": "缺少有效商品组ID"}, 400)
        if mode not in ("title", "description", "both"):
            return self.send_json({"ok": False, "message": "缺少有效生成模式"}, 400)
        try:
            title_result = None
            description_result = None
            if mode in ("title", "both"):
                title_result = generate_title_for_group(int(group_id), account_name=account_name, channel=channel)
            if mode in ("description", "both"):
                description_result = generate_description_for_group(int(group_id), account_name=account_name, channel=channel)
        except Exception as e:
            return self.send_json({"ok": False, "message": str(e)}, 500)
        payload = {
            "ok": True,
            "message": f"组商品AI{'标题' if mode == 'title' else ('简介' if mode == 'description' else '标题和简介')}生成完成",
            "group_id": int(group_id),
            "ai_title": (title_result or {}).get("ai_title") or "",
            "ai_description": (description_result or {}).get("ai_description") or "",
        }
        if mode == "both" and not payload["ai_title"] and title_result:
            payload["ai_title"] = title_result.get("ai_title") or ""
        if mode == "both" and not payload["ai_description"] and description_result:
            payload["ai_description"] = description_result.get("ai_description") or ""
        return self.send_json(payload)

    def generate_product_ai_image(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        product_id = (form.get("product_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        channel = (form.get("channel") or ["xianyu"])[0].strip().lower()
        if not product_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效商品ID"}, 400)
        job_id = uuid.uuid4().hex
        now_text = datetime.now().isoformat(timespec="seconds")
        with AI_IMAGE_JOBS_LOCK:
            AI_IMAGE_JOBS[job_id] = {
                "job_id": job_id,
                "product_id": int(product_id),
                "account_name": account_name,
                "channel": channel,
                "status": "queued",
                "message": "AI主图任务已提交，后台生成中",
                "ai_main_image_path": "",
                "error": "",
                "created_at": now_text,
                "updated_at": now_text,
            }
        threading.Thread(
            target=self._run_ai_image_job,
            args=(job_id, int(product_id), account_name, "main", channel),
            daemon=True,
        ).start()
        return self.send_json({
            "ok": True,
            "message": "AI主图任务已提交，后台生成中",
            "job_id": job_id,
            "product_id": int(product_id),
            "status": "queued",
        })

    def generate_group_product_ai_image(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        group_id = (form.get("group_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        channel = (form.get("channel") or ["xianyu"])[0].strip().lower()
        raw_product_ids = (form.get("product_ids") or [""])[0].strip()
        product_ids = [int(item) for item in raw_product_ids.split(",") if item.strip().isdigit()]
        if not product_ids:
            return self.send_json({"ok": False, "message": "缺少有效组内商品ID"}, 400)
        resolved_group_id = int(group_id) if group_id.isdigit() and int(group_id) > 0 else 0
        if resolved_group_id <= 0:
            group_row = find_group_by_member_ids_relaxed(product_ids)
            if not group_row:
                refresh_one8_product_groups()
                group_row = find_group_by_member_ids_relaxed(product_ids)
            if not group_row:
                return self.send_json({"ok": False, "message": "找不到商品组，请先刷新 one8 商品组"}, 400)
            resolved_group_id = int(group_row["id"])
        job_id = uuid.uuid4().hex
        now_text = datetime.now().isoformat(timespec="seconds")
        with AI_IMAGE_JOBS_LOCK:
            AI_IMAGE_JOBS[job_id] = {
                "job_id": job_id,
                "group_id": resolved_group_id,
                "product_id": int(product_ids[0]),
                "product_ids": product_ids,
                "account_name": account_name,
                "channel": channel,
                "status": "queued",
                "message": "组商品AI主图任务已提交，后台生成中",
                "ai_main_image_path": "",
                "error": "",
                "created_at": now_text,
                "updated_at": now_text,
            }
        threading.Thread(
            target=self._run_group_ai_image_job,
            args=(job_id, resolved_group_id, product_ids, account_name, channel),
            daemon=True,
        ).start()
        return self.send_json({
            "ok": True,
            "message": "组商品AI主图任务已提交，后台生成中",
            "job_id": job_id,
            "group_id": resolved_group_id,
            "status": "queued",
        })

    def generate_product_ai_detail_image(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        product_id = (form.get("product_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        channel = (form.get("channel") or ["xianyu"])[0].strip().lower()
        if not product_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效商品ID"}, 400)
        job_id = uuid.uuid4().hex
        now_text = datetime.now().isoformat(timespec="seconds")
        with AI_IMAGE_JOBS_LOCK:
            AI_IMAGE_JOBS[job_id] = {
                "job_id": job_id,
                "product_id": int(product_id),
                "account_name": account_name,
                "channel": channel,
                "asset_type": "detail",
                "status": "queued",
                "message": "AI详情图任务已提交，后台生成中",
                "ai_main_image_path": "",
                "error": "",
                "created_at": now_text,
                "updated_at": now_text,
            }
        threading.Thread(
            target=self._run_ai_image_job,
            args=(job_id, int(product_id), account_name, "detail", channel),
            daemon=True,
        ).start()
        return self.send_json({
            "ok": True,
            "message": "AI详情图任务已提交，后台生成中",
            "job_id": job_id,
            "product_id": int(product_id),
            "status": "queued",
        })

    def _run_ai_image_job(self, job_id: str, product_id: int, account_name: str, asset_type: str = "main", channel: str = "xianyu"):
        with AI_IMAGE_JOBS_LOCK:
            job = AI_IMAGE_JOBS.get(job_id)
            if job:
                job["status"] = "running"
                job["message"] = "AI详情图后台生成中" if asset_type == "detail" else "AI主图后台生成中"
                job["updated_at"] = datetime.now().isoformat(timespec="seconds")
        try:
            result = generate_ai_detail_image(product_id, force=True, account_name=account_name, channel=channel) if asset_type == "detail" else generate_ai_main_image(product_id, force=True, account_name=account_name, channel=channel)
            with AI_IMAGE_JOBS_LOCK:
                job = AI_IMAGE_JOBS.get(job_id)
                if job:
                    job["status"] = "succeeded"
                    job["message"] = "AI详情图生成完成" if asset_type == "detail" else "AI主图生成完成"
                    job["asset_type"] = asset_type
                    job["ai_main_image_path"] = str(result.get("ai_main_image_path") or "")
                    job["updated_at"] = datetime.now().isoformat(timespec="seconds")
        except Exception as e:
            with AI_IMAGE_JOBS_LOCK:
                job = AI_IMAGE_JOBS.get(job_id)
                if job:
                    job["status"] = "failed"
                    job["asset_type"] = asset_type
                    job["message"] = str(e)
                    job["error"] = str(e)
                    job["updated_at"] = datetime.now().isoformat(timespec="seconds")

    def _run_group_ai_image_job(self, job_id: str, group_id: int, product_ids: list[int], account_name: str, channel: str = "xianyu"):
        with AI_IMAGE_JOBS_LOCK:
            job = AI_IMAGE_JOBS.get(job_id)
            if job:
                job["status"] = "running"
                job["message"] = "组商品AI主图后台生成中"
                job["updated_at"] = datetime.now().isoformat(timespec="seconds")
        try:
            conn = get_conn()
            group_row = conn.execute(
                "SELECT id, group_name, category FROM product_groups WHERE id = ? LIMIT 1",
                (group_id,),
            ).fetchone()
            member_rows = conn.execute(
                """
                SELECT
                    p.id AS product_id,
                    COALESCE(p.color, '') AS color
                FROM product_group_members m
                JOIN products p
                  ON p.id = m.product_id
                WHERE m.group_id = ?
                ORDER BY m.sort_order, p.id
                """,
                (group_id,),
            ).fetchall()
            conn.close()
            if not group_row:
                raise ValueError(f"找不到商品组: {group_id}")

            image_items = []
            failures = []
            generated_count = 0
            reused_count = 0
            for member in member_rows:
                product_id = int(member["product_id"] or 0)
                if product_id not in product_ids:
                    continue
                existing = load_existing_ai_image(product_id, account_name=account_name, asset_type="main")
                image_path = str(existing["ai_main_image_path"] or "").strip() if existing else ""
                if image_path and Path(image_path).exists():
                    reused_count += 1
                else:
                    generate_ai_main_image(product_id, force=True, account_name=account_name, channel=channel)
                    existing = load_existing_ai_image(product_id, account_name=account_name, asset_type="main")
                    image_path = str(existing["ai_main_image_path"] or "").strip() if existing else ""
                    if image_path and Path(image_path).exists():
                        generated_count += 1
                if not image_path or not Path(image_path).exists():
                    failures.append(str(product_id))
                    continue
                image_items.append({
                    "ai_image_path": image_path,
                    "color": str(member["color"] or "").strip(),
                })
            if failures:
                raise ValueError(f"以下商品AI图生成失败或不存在: {', '.join(failures)}")
            output_path = generate_group_ai_cover_image(
                source="one8",
                group_id=group_id,
                group_name=str(group_row["group_name"] or ""),
                category=str(group_row["category"] or ""),
                items=image_items,
                account_name=account_name,
                channel=channel,
                output_name=build_group_ai_cover_output_name(account_name, channel),
            )
            with AI_IMAGE_JOBS_LOCK:
                job = AI_IMAGE_JOBS.get(job_id)
                if job:
                    job["status"] = "succeeded"
                    job["message"] = f"组商品AI主图生成完成（复用{reused_count}，新生成{generated_count}，封面1张）"
                    job["ai_main_image_path"] = str(output_path)
                    job["updated_at"] = datetime.now().isoformat(timespec="seconds")
        except Exception as e:
            with AI_IMAGE_JOBS_LOCK:
                job = AI_IMAGE_JOBS.get(job_id)
                if job:
                    job["status"] = "failed"
                    job["message"] = str(e)
                    job["error"] = str(e)
                    job["updated_at"] = datetime.now().isoformat(timespec="seconds")

    def get_product_ai_image_status(self, params):
        job_id = (params.get("job_id") or [""])[0].strip()
        if not job_id:
            return self.send_json({"ok": False, "message": "缺少任务ID"}, 400)
        with AI_IMAGE_JOBS_LOCK:
            job = dict(AI_IMAGE_JOBS.get(job_id) or {})
        if not job:
            return self.send_json({"ok": False, "message": "任务不存在或已过期"}, 404)
        payload = {
            "ok": True,
            "job_id": job_id,
            "product_id": int(job.get("product_id") or 0),
            "asset_type": str(job.get("asset_type") or "main"),
            "status": str(job.get("status") or "unknown"),
            "message": str(job.get("message") or ""),
            "ai_main_image_path": str(job.get("ai_main_image_path") or ""),
        }
        if job.get("status") == "failed":
            payload["error"] = str(job.get("error") or job.get("message") or "")
        return self.send_json(payload)

    def select_product_ai_images(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        product_id = (form.get("product_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        image_ids_text = (form.get("image_ids") or [""])[0].strip()
        if not product_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效商品ID"}, 400)
        image_ids = [int(part) for part in image_ids_text.split(",") if part.strip().isdigit()]
        try:
            set_selected_ai_images(int(product_id), account_name, image_ids)
        except Exception as e:
            return self.send_json({"ok": False, "message": str(e)}, 500)
        return self.send_json({
            "ok": True,
            "message": "图片选择已保存",
            "product_id": int(product_id),
            "selected_count": len(image_ids),
        })

    def generate_task_ai(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        task_id = (form.get("task_id") or [""])[0].strip()
        mode = (form.get("mode") or ["title"])[0].strip()
        if not task_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效任务ID"}, 400)
        if mode not in ("title", "description"):
            return self.send_json({"ok": False, "message": "缺少有效生成模式"}, 400)
        try:
            if mode == "title":
                result = generate_title_for_task(int(task_id))
            else:
                result = generate_description_for_task(int(task_id))
        except Exception as e:
            return self.send_json({"ok": False, "message": str(e)}, 500)
        payload = {"ok": True, "message": f"任务AI{'标题' if mode == 'title' else '简介'}生成完成", "task_id": int(task_id)}
        payload.update(result)
        return self.send_json(payload)

    def downshelf_delete_batch(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        batch_id = (form.get("batch_id") or [""])[0].strip()
        if not batch_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效批次ID"}, 400)
        return self.submit_delete_batch_job([int(batch_id)])

    def downshelf_delete_batches(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        raw_batch_ids = (form.get("batch_ids") or [""])[0].strip()
        batch_ids = []
        for item in raw_batch_ids.split(","):
            item = item.strip()
            if item.isdigit():
                batch_ids.append(int(item))
        batch_ids = list(dict.fromkeys(batch_ids))
        if not batch_ids:
            return self.send_json({"ok": False, "message": "缺少有效批次ID"}, 400)
        return self.submit_delete_batch_job(batch_ids)

    def submit_delete_batch_job(self, batch_ids: list[int]):
        job_id = uuid.uuid4().hex
        with DELETE_BATCH_JOBS_LOCK:
            DELETE_BATCH_JOBS[job_id] = {
                "job_id": job_id,
                "batch_ids": batch_ids,
                "status": "queued",
                "message": "批量删除任务已提交",
                "progress_text": "后台正在处理批量删除，请不要关闭页面。",
                "result": None,
            }
        threading.Thread(
            target=self._run_delete_batch_job,
            args=(job_id, batch_ids),
            daemon=True,
        ).start()
        return self.send_json({"ok": True, "job_id": job_id, "message": "批量删除任务已提交"})

    def _run_delete_batch_job(self, job_id: str, batch_ids: list[int]):
        deleted_batch_count = 0
        downshelf_success_count = 0
        delete_success_count = 0
        skip_count = 0
        failures = []
        with DELETE_BATCH_JOBS_LOCK:
            job = DELETE_BATCH_JOBS.get(job_id)
            if job:
                job["status"] = "running"
                job["message"] = "批量删除执行中"
        try:
            for index, batch_id in enumerate(batch_ids, start=1):
                with DELETE_BATCH_JOBS_LOCK:
                    job = DELETE_BATCH_JOBS.get(job_id)
                    if job:
                        job["progress_text"] = f"正在删除批次 {index}/{len(batch_ids)}，批次ID {batch_id}。"
                        job["message"] = job["progress_text"]
                result = self.perform_batch_downshelf_delete(batch_id)
                downshelf_success_count += result["downshelf_success_count"]
                delete_success_count += result.get("delete_success_count", 0)
                skip_count += result["skip_count"]
                if result["ok"]:
                    deleted_batch_count += 1
                else:
                    for failure in result["failures"]:
                        failures.append({"batch_id": batch_id, **failure})
                    if result["not_found"]:
                        failures.append({"batch_id": batch_id, "error": "批次下没有任务"})
            result_payload = {
                "selected_batch_count": len(batch_ids),
                "deleted_batch_count": deleted_batch_count,
                "downshelf_success_count": downshelf_success_count,
                "delete_success_count": delete_success_count,
                "skip_count": skip_count,
                "failed_count": len(failures),
                "failures": failures,
            }
            with DELETE_BATCH_JOBS_LOCK:
                job = DELETE_BATCH_JOBS.get(job_id)
                if job:
                    job["status"] = "succeeded"
                    job["message"] = "批量删除完成"
                    job["progress_text"] = "批量删除已完成，正在整理结果。"
                    job["result"] = result_payload
        except Exception as exc:
            with DELETE_BATCH_JOBS_LOCK:
                job = DELETE_BATCH_JOBS.get(job_id)
                if job:
                    job["status"] = "failed"
                    job["message"] = str(exc)
                    job["progress_text"] = "批量删除失败"
                    job["result"] = {
                        "selected_batch_count": len(batch_ids),
                        "deleted_batch_count": deleted_batch_count,
                        "downshelf_success_count": downshelf_success_count,
                        "delete_success_count": delete_success_count,
                        "skip_count": skip_count,
                        "failed_count": len(failures) + 1,
                        "failures": failures + [{"error": str(exc)}],
                    }

    def get_batch_delete_status(self, params):
        job_id = (params.get("job_id") or [""])[0].strip()
        if not job_id:
            return self.send_json({"ok": False, "message": "缺少任务ID"}, 400)
        with DELETE_BATCH_JOBS_LOCK:
            job = dict(DELETE_BATCH_JOBS.get(job_id) or {})
        if not job:
            return self.send_json({"ok": False, "message": "删除任务不存在"}, 404)
        payload = {
            "ok": True,
            "job_id": job_id,
            "status": job.get("status") or "queued",
            "message": job.get("message") or "",
            "progress_text": job.get("progress_text") or "后台正在处理删除，请不要关闭页面。",
        }
        if job.get("result") is not None:
            result = job["result"] or {}
            payload["result"] = result
            payload.update(result)
        return self.send_json(payload)

    def force_delete_batches(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        raw_batch_ids = (form.get("batch_ids") or [""])[0].strip()
        batch_ids = []
        for item in raw_batch_ids.split(","):
            item = item.strip()
            if item.isdigit():
                batch_ids.append(int(item))
        batch_ids = list(dict.fromkeys(batch_ids))
        if not batch_ids:
            return self.send_json({"ok": False, "message": "缺少有效批次ID"}, 400)

        conn = get_conn()
        cur = conn.cursor()
        deleted_batch_count = 0
        failures = []
        for batch_id in batch_ids:
            try:
                cur.execute("DELETE FROM xianyu_publish_tasks WHERE batch_id = ?", (batch_id,))
                cur.execute("DELETE FROM xianyu_publish_batches WHERE id = ?", (batch_id,))
                deleted_batch_count += 1
            except Exception as e:
                failures.append({"batch_id": batch_id, "error": str(e)})
        conn.commit()
        conn.close()

        return self.send_json({
            "ok": not failures,
            "message": "强制删除完成" if not failures else "强制删除完成，但有失败批次",
            "selected_batch_count": len(batch_ids),
            "deleted_batch_count": deleted_batch_count,
            "failed_count": len(failures),
            "failures": failures,
        })

    def perform_batch_downshelf_delete(self, batch_id: int) -> dict:
        conn = get_conn()
        rows = conn.execute("""
            SELECT id, third_product_id, status
            FROM xianyu_publish_tasks
            WHERE batch_id = ?
            ORDER BY id
        """, (batch_id,)).fetchall()
        conn.close()

        if not rows:
            return {
                "ok": False,
                "batch_id": batch_id,
                "not_found": True,
                "downshelf_success_count": 0,
                "skip_count": 0,
                "failed_count": 0,
                "failures": [],
            }

        downshelf_success_count = 0
        delete_success_count = 0
        skip_count = 0
        failed = []

        for row in rows:
            task_id = int(row["id"])
            third_product_id = str(row["third_product_id"] or "").strip()
            status = str(row["status"] or "")

            if not third_product_id or status in ("off_shelf_failed", "pending"):
                skip_count += 1
                continue

            try:
                if status in ("created", "payload_ready", "publish_failed", "failed"):
                    execute_task_delete_product(task_id)
                    delete_success_count += 1
                elif status in ("submitted", "published", "success"):
                    try:
                        execute_task_downshelf(task_id)
                        downshelf_success_count += 1
                    except Exception as e:
                        if not is_not_eligible_for_downshelf_error(e):
                            raise
                    execute_task_delete_product(task_id)
                    delete_success_count += 1
                elif status in ("deleted", "off_shelved"):
                    skip_count += 1
                else:
                    execute_task_delete_product(task_id)
                    delete_success_count += 1
            except Exception as e:
                if is_already_deleted_error(e):
                    delete_success_count += 1
                    continue
                failed.append({"task_id": task_id, "error": str(e)})

        if failed:
            return {
                "ok": False,
                "batch_id": batch_id,
                "not_found": False,
                "downshelf_success_count": downshelf_success_count,
                "delete_success_count": delete_success_count,
                "skip_count": skip_count,
                "failed_count": len(failed),
                "failures": failed,
            }

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM xianyu_publish_tasks WHERE batch_id = ?", (batch_id,))
        cur.execute("DELETE FROM xianyu_publish_batches WHERE id = ?", (batch_id,))
        conn.commit()
        conn.close()

        return {
            "ok": True,
            "batch_id": batch_id,
            "not_found": False,
            "downshelf_success_count": downshelf_success_count,
            "delete_success_count": delete_success_count,
            "skip_count": skip_count,
            "failed_count": 0,
            "failures": [],
        }

    def task_action_cell(self, row) -> str:
        status = str(row.get("status") or "")
        third_product_id = str(row.get("third_product_id") or "")
        task_id = int(row["task_id"])
        account_name = html.escape(str(row.get("account_name") or ""))
        if not third_product_id:
            return ""
        actions = []
        if status not in ("off_shelved", "off_shelf_failed", "deleted"):
            actions.append(
                "<form method='post' action='/task/downshelf' style='display:inline-block;margin-right:6px;'>"
                f"<input type='hidden' name='task_id' value='{task_id}'>"
                f"<input type='hidden' name='account_name' value='{account_name}'>"
                "<button class='mini-btn' type='submit'>下架</button>"
                "</form>"
            )
        if status in ("created", "submitted", "payload_ready", "publish_failed"):
            actions.append(
                "<form method='post' action='/task/delete-product' style='display:inline-block;'>"
                f"<input type='hidden' name='task_id' value='{task_id}'>"
                f"<input type='hidden' name='account_name' value='{account_name}'>"
                "<button class='mini-btn danger-btn' type='submit'>删除待发布</button>"
                "</form>"
            )
        return "".join(actions)

    def downshelf_task(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        task_id = (form.get("task_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        if not task_id.isdigit():
            return self.send_html("参数错误", "<div class='card'>缺少有效任务ID</div>", 400)
        redirect_to = build_url("/account", name=account_name) if account_name else "/accounts"
        try:
            execute_task_downshelf(int(task_id))
        except Exception as e:
            return self.send_alert_and_redirect("下架失败", f"下架失败：{e}", redirect_to, 200)
        self.redirect(redirect_to)

    def delete_product_task(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        task_id = (form.get("task_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        if not task_id.isdigit():
            return self.send_html("参数错误", "<div class='card'>缺少有效任务ID</div>", 400)
        redirect_to = build_url("/account", name=account_name) if account_name else "/accounts"
        try:
            execute_task_delete_product(int(task_id))
        except Exception as e:
            return self.send_alert_and_redirect("删除失败", f"删除失败：{e}", redirect_to, 200)
        self.redirect(redirect_to)

    def create_account(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        ensure_account_ai_copy_support()

        def value(key: str, default: str = "") -> str:
            return (form.get(key) or [default])[0].strip()

        account_name = value("account_name")
        app_key = value("app_key")
        app_secret = value("app_secret")
        user_name = value("user_name")
        merchant_id = value("merchant_id")
        province = value("province", "110000")
        city = value("city", "110100")
        district = value("district", "110105")
        item_biz_type = value("item_biz_type", "2")
        sp_biz_type = value("sp_biz_type", "2")
        stuff_status = value("stuff_status", "100")
        note = value("note")

        if not account_name or not app_key or not app_secret or not user_name:
            return self.send_alert_and_redirect("新增失败", "账号名称、AppKey、AppSecret、闲鱼用户名不能为空", "/accounts", 200)

        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO xianyu_accounts (
                    account_name, app_key, app_secret, merchant_id, user_name,
                    province, city, district, item_biz_type, sp_biz_type,
                    stuff_status, channel_pv_json, enabled, note, independent_ai_assets,
                    shipping_regions_json, shipping_region_group_size
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', 1, ?, 1, ?, ?)
            """, (
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
                note,
                json.dumps(DEFAULT_MULTI_SHIPPING_REGIONS, ensure_ascii=False),
                DEFAULT_SHIPPING_REGION_GROUP_SIZE,
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return self.send_alert_and_redirect("新增失败", f"账号 {account_name} 已存在", "/accounts", 200)
        except Exception as e:
            conn.close()
            return self.send_alert_and_redirect("新增失败", f"新增账号失败：{e}", "/accounts", 200)
        conn.close()
        self.redirect(build_url("/account", name=account_name))

    def create_taobao_shop(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        ensure_taobao_shop_support()

        def value(key: str, default: str = "") -> str:
            return (form.get(key) or [default])[0].strip()

        shop_name = value("shop_name")
        app_key = value("app_key")
        app_secret = value("app_secret")
        redirect_uri = value("redirect_uri")
        browser_profile_dir = value("browser_profile_dir")
        chrome_user_data_dir = value("chrome_user_data_dir")
        chrome_profile_name = value("chrome_profile_name")
        chrome_cdp_url = value("chrome_cdp_url")
        login_url = value("login_url")
        publish_url = value("publish_url")
        note = value("note")
        if not shop_name:
            return self.send_alert_and_redirect("新增失败", "店铺标识不能为空", "/taobao/shops", 200)

        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO taobao_shops (
                    shop_name, app_key, app_secret, redirect_uri, browser_profile_dir, chrome_user_data_dir, chrome_profile_name, chrome_cdp_url, login_url, publish_url, note, auth_status, enabled
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 1)
                """,
                (shop_name, app_key, app_secret, redirect_uri, browser_profile_dir, chrome_user_data_dir, chrome_profile_name, chrome_cdp_url, login_url, publish_url, note),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return self.send_alert_and_redirect("新增失败", f"淘宝店铺 {shop_name} 已存在", "/taobao/shops", 200)
        except Exception as e:
            conn.close()
            return self.send_alert_and_redirect("新增失败", f"新增淘宝店铺失败：{e}", "/taobao/shops", 200)
        conn.close()
        return self.send_alert_and_redirect("新增成功", "淘宝店铺配置已保存，现在可以点击“打开登录浏览器”。", "/taobao/shops", 200)

    def update_taobao_shop(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))

        def value(key: str, default: str = "") -> str:
            return (form.get(key) or [default])[0].strip()

        shop_id = value("id")
        if not shop_id.isdigit():
            return self.send_alert_and_redirect("保存失败", "缺少有效店铺ID", "/taobao/shops", 200)
        shop_name = value("shop_name")
        if not shop_name:
            return self.send_alert_and_redirect("保存失败", "店铺标识不能为空", build_url("/taobao/shops/edit", id=shop_id), 200)

        payload = {
            "shop_name": shop_name,
            "app_key": value("app_key"),
            "app_secret": value("app_secret"),
            "redirect_uri": value("redirect_uri"),
            "browser_profile_dir": value("browser_profile_dir"),
            "chrome_user_data_dir": value("chrome_user_data_dir"),
            "chrome_profile_name": value("chrome_profile_name"),
            "chrome_cdp_url": value("chrome_cdp_url"),
            "login_url": value("login_url"),
            "publish_url": value("publish_url"),
            "note": value("note"),
        }
        conn = get_conn()
        try:
            conn.execute(
                """
                UPDATE taobao_shops
                SET shop_name = ?,
                    app_key = ?,
                    app_secret = ?,
                    redirect_uri = ?,
                    browser_profile_dir = ?,
                    chrome_user_data_dir = ?,
                    chrome_profile_name = ?,
                    chrome_cdp_url = ?,
                    login_url = ?,
                    publish_url = ?,
                    note = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload["shop_name"],
                    payload["app_key"],
                    payload["app_secret"],
                    payload["redirect_uri"],
                    payload["browser_profile_dir"],
                    payload["chrome_user_data_dir"],
                    payload["chrome_profile_name"],
                    payload["chrome_cdp_url"],
                    payload["login_url"],
                    payload["publish_url"],
                    payload["note"],
                    int(shop_id),
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return self.send_alert_and_redirect("保存失败", f"淘宝店铺 {shop_name} 已存在", build_url("/taobao/shops/edit", id=shop_id), 200)
        except Exception as e:
            conn.close()
            return self.send_alert_and_redirect("保存失败", f"保存淘宝店铺失败：{e}", build_url("/taobao/shops/edit", id=shop_id), 200)
        conn.close()
        return self.send_alert_and_redirect("保存成功", "淘宝店铺配置已更新。", "/taobao/shops", 200)

    def open_taobao_login_browser(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        shop_id = (form.get("shop_id") or [""])[0].strip()
        if not shop_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效淘宝店铺ID"}, 400)
        conn = get_conn()
        row = conn.execute("SELECT * FROM taobao_shops WHERE id = ? AND enabled = 1", (int(shop_id),)).fetchone()
        conn.close()
        if not row:
            return self.send_json({"ok": False, "message": "淘宝店铺不存在或未启用"}, 404)
        launch_login_browser(dict(row))
        return self.send_json({
            "ok": True,
            "shop_id": int(shop_id),
            "shop_name": str(row["shop_name"] or ""),
            "message": f"已启动淘宝登录浏览器：{str(row['shop_name'] or '')}",
        })

    def publish_product_to_taobao_browser(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        shop_id = (form.get("shop_id") or [""])[0].strip()
        product_id = (form.get("product_id") or [""])[0].strip()
        account_name = (form.get("account_name") or [""])[0].strip()
        if not shop_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效淘宝店铺ID"}, 400)
        if not product_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效商品ID"}, 400)

        conn = get_conn()
        row = conn.execute("SELECT * FROM taobao_shops WHERE id = ? AND enabled = 1", (int(shop_id),)).fetchone()
        conn.close()
        if not row:
            return self.send_json({"ok": False, "message": "淘宝店铺不存在或未启用"}, 404)

        try:
            payload = build_publish_assist_payload(int(product_id), account_name=account_name)
        except Exception as e:
            return self.send_json({"ok": False, "message": f"生成淘宝发布数据失败：{e}"}, 500)

        launch_publish_assistant(dict(row), payload)
        return self.send_json({
            "ok": True,
            "shop_id": int(shop_id),
            "shop_name": str(row["shop_name"] or ""),
            "product_id": int(product_id),
            "message": "淘宝发布助手已启动，浏览器会打开发布页并注入商品数据面板。",
        })

    def publish_products_to_taobao_browser(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        form = parse_qs(self.rfile.read(length).decode("utf-8"))
        shop_id = (form.get("shop_id") or [""])[0].strip()
        raw_product_ids = (form.get("product_ids") or [""])[0].strip()
        if not shop_id.isdigit():
            return self.send_json({"ok": False, "message": "缺少有效淘宝店铺ID"}, 400)
        product_ids = []
        for item in raw_product_ids.split(","):
            item = item.strip()
            if item.isdigit():
                product_ids.append(int(item))
        product_ids = list(dict.fromkeys(product_ids))
        if not product_ids:
            return self.send_json({"ok": False, "message": "缺少有效商品ID"}, 400)

        conn = get_conn()
        row = conn.execute("SELECT * FROM taobao_shops WHERE id = ? AND enabled = 1", (int(shop_id),)).fetchone()
        conn.close()
        if not row:
            return self.send_json({"ok": False, "message": "淘宝店铺不存在或未启用"}, 404)

        payloads = []
        failures = []
        for product_id in product_ids:
            try:
                payloads.append(build_publish_assist_payload(product_id, account_name=""))
            except Exception as e:
                failures.append({"product_id": product_id, "error": str(e)})
        if not payloads:
            return self.send_json({"ok": False, "message": "没有可打开的商品", "failures": failures}, 400)

        launch_publish_assistants(dict(row), payloads)
        return self.send_json({
            "ok": True,
            "shop_id": int(shop_id),
            "shop_name": str(row["shop_name"] or ""),
            "total_count": len(payloads),
            "failed_count": len(failures),
            "failures": failures,
            "message": "已批量启动淘宝发布助手，浏览器会为每个商品打开一个发布页。",
        })

    def create_batch(self, account_name: str, batch_name: str, product_ids: list[int]) -> int:
        ensure_xianyu_group_task_support()
        conn = get_conn()
        cur = conn.cursor()
        account_row = cur.execute(
            "SELECT id FROM xianyu_accounts WHERE account_name = ? AND enabled = 1",
            (account_name,),
        ).fetchone()
        if not account_row:
            conn.close()
            raise ValueError("账号不存在或未启用")

        cur.execute("""
            INSERT INTO xianyu_publish_batches (account_id, batch_name, status)
            VALUES (?, ?, 'pending')
        """, (account_row["id"], batch_name))
        batch_id = cur.lastrowid

        for product_id in product_ids:
            cur.execute("""
                INSERT INTO xianyu_publish_tasks (account_id, batch_id, product_id, status, publish_status)
                VALUES (?, ?, ?, 'pending', 'pending')
            """, (account_row["id"], batch_id, int(product_id)))

        cur.execute("""
            UPDATE xianyu_publish_batches
            SET total_count = (SELECT COUNT(*) FROM xianyu_publish_tasks WHERE batch_id = ?),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (batch_id, batch_id))
        conn.commit()
        conn.close()
        return batch_id

    def build_auto_batch_name(self, category: str) -> str:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        safe_category = (category or "全部").strip() or "全部"
        return f"{safe_category}-{timestamp}"

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8790)
    args = parser.parse_args()

    ensure_account_ai_copy_support()
    ensure_taobao_shop_support()
    server = ThreadingHTTPServer((args.host, args.port), AdminHandler)
    print(f"闲鱼管理后台已启动: http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
