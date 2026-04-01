import json
import os
import re
import sqlite3

from .image_pipeline import build_hosted_image_url
from .stock_utils import parse_total_stock

DB_FILE = "products.db"
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


def ensure_account_ai_copy_support():
    conn = sqlite3.connect(DB_FILE)
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
    columns = [row[1] for row in cur.execute("PRAGMA table_info(xianyu_accounts)").fetchall()]
    if "independent_ai_assets" not in columns:
        cur.execute("ALTER TABLE xianyu_accounts ADD COLUMN independent_ai_assets INTEGER DEFAULT 0")
    if "shipping_regions_json" not in columns:
        cur.execute("ALTER TABLE xianyu_accounts ADD COLUMN shipping_regions_json TEXT DEFAULT '[]'")
    if "shipping_region_group_size" not in columns:
        cur.execute(f"ALTER TABLE xianyu_accounts ADD COLUMN shipping_region_group_size INTEGER DEFAULT {DEFAULT_SHIPPING_REGION_GROUP_SIZE}")
    conn.commit()
    conn.close()


def load_publish_defaults() -> dict:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT key_name, key_value
        FROM xianyu_publish_defaults
    """)
    rows = cur.fetchall()
    conn.close()
    result = {key: value for key, value in rows}
    callback_override = (os.getenv("XIANYU_CALLBACK_PUBLIC_URL") or "").strip()
    if callback_override:
        result["callback_url"] = callback_override
    return result


def get_category_mapping(category: str) -> dict:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT item_biz_type, sp_biz_type, channel_cat_id, channel_cat_name
        FROM xianyu_category_mapping
        WHERE source = 'boardline'
          AND source_category = ?
          AND enabled = 1
        LIMIT 1
    """, (category,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise ValueError(f"分类未映射: {category}")
    return {
        "item_biz_type": row[0] or "",
        "sp_biz_type": row[1] or "",
        "channel_cat_id": row[2] or "",
        "channel_cat_name": row[3] or "",
    }


def get_publish_task(task_id: int) -> dict:
    ensure_account_ai_copy_support()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT
            t.id,
            t.product_id,
            t.third_product_id,
            t.ai_title,
            t.ai_description,
            t.cover_image_path,
            t.publish_price,
            t.channel_pv_json,
            t.publish_payload_json,
            t.account_id,
            a.account_name,
            a.app_key,
            a.app_secret,
            a.merchant_id,
            COALESCE(a.independent_ai_assets, 0) AS account_independent_ai_assets,
            COALESCE(a.shipping_regions_json, '[]') AS account_shipping_regions_json,
            COALESCE(a.shipping_region_group_size, 0) AS account_shipping_region_group_size,
            a.user_name AS account_user_name,
            a.province AS account_province,
            a.city AS account_city,
            a.district AS account_district,
            a.item_biz_type AS account_item_biz_type,
            a.sp_biz_type AS account_sp_biz_type,
            a.stuff_status AS account_stuff_status,
            a.channel_pv_json AS account_channel_pv_json,
            p.branduid,
            p.category,
            p.name,
            CASE
                WHEN COALESCE(a.independent_ai_assets, 0) = 1 THEN ai_account.ai_title
                ELSE ai.ai_title
            END AS product_ai_title,
            CASE
                WHEN COALESCE(a.independent_ai_assets, 0) = 1 THEN ai_account.ai_description
                ELSE ai.ai_description
            END AS product_ai_description,
            p.local_image_path,
            p.image_url,
            u.price,
            u.original_price,
            u.latest_discount_price,
            u.original_price_cny,
            u.final_price_cny,
            u.stock
        FROM xianyu_publish_tasks t
        LEFT JOIN xianyu_accounts a
          ON a.id = t.account_id
        JOIN products p
          ON p.id = t.product_id
        LEFT JOIN xianyu_product_ai_copy ai
          ON ai.product_id = p.id
        LEFT JOIN xianyu_account_product_ai_copy ai_account
          ON ai_account.product_id = p.id
         AND ai_account.account_name = a.account_name
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        WHERE t.id = ?
    """, (task_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise ValueError(f"找不到发布任务: {task_id}")
    return dict(row)


def load_product_channel_pv(product_id: int) -> list[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT property_id, property_name, value_id, value_name
        FROM xianyu_product_property_values
        WHERE product_id = ?
          AND TRIM(COALESCE(value_name, '')) != ''
        ORDER BY id
    """, (product_id,)).fetchall()
    conn.close()
    return [
        {
            "property_id": row["property_id"] or "",
            "property_name": row["property_name"] or "",
            "value_id": row["value_id"] or "",
            "value_name": row["value_name"] or "",
        }
        for row in rows
        if (row["property_id"] or "").strip() and (row["value_name"] or "").strip()
    ]


def load_product_publish_meta(product_id: int) -> dict:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    row = cur.execute("""
        SELECT stuff_status, note
        FROM xianyu_product_publish_meta
        WHERE product_id = ?
        LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def normalize_price_text(value) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip()


def is_remote_url(value) -> bool:
    text = normalize_price_text(value)
    return text.startswith("http://") or text.startswith("https://")


def load_account_ai_images(product_id: int, account_name: str) -> list[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, ai_main_image_path, oss_url, is_selected
        FROM xianyu_product_ai_images
        WHERE product_id = ?
          AND COALESCE(account_name, '') = ?
        ORDER BY id DESC
    """, (product_id, account_name or "")).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def collect_publish_images(task: dict, upload_watermark: bool = False) -> list[str]:
    image_candidates = []
    account_name = normalize_price_text(task.get("account_name"))
    product_id = int(task.get("product_id") or 0)
    watermark_text = account_name if upload_watermark else ""

    ai_rows = load_account_ai_images(product_id, account_name) if product_id else []
    selected_rows = [row for row in ai_rows if int(row.get("is_selected") or 0) == 1]
    active_ai_rows = selected_rows or ai_rows
    missing_hostable_ai = []
    for row in active_ai_rows:
        oss_url = normalize_price_text(row.get("oss_url"))
        if oss_url and is_remote_url(oss_url) and not watermark_text:
            image_candidates.append(oss_url)
            continue
        ai_path = normalize_price_text(row.get("ai_main_image_path"))
        hosted_ai = build_hosted_image_url(ai_path, watermark_text=watermark_text) if ai_path else ""
        if hosted_ai:
            image_candidates.append(hosted_ai)
        elif ai_path:
            missing_hostable_ai.append(ai_path)

    if image_candidates:
        return list(dict.fromkeys(image_candidates))
    if active_ai_rows and missing_hostable_ai:
        raise ValueError(
            "当前商品已选择 AI 图片，但这些图片没有可上传的公网地址。"
            "请先配置 PUBLIC_MEDIA_BASE_URL 或 XIANYU_IMAGE_CDN_BASE_URL，"
            "然后重新执行创建。"
        )

    task_cover_path = normalize_price_text(task.get("cover_image_path"))
    hosted_cover = ""
    if task_cover_path and not is_remote_url(task_cover_path):
        hosted_cover = build_hosted_image_url(task_cover_path, watermark_text=watermark_text)
    if hosted_cover:
        return [hosted_cover]

    local_path = normalize_price_text(task.get("local_image_path"))
    hosted_local = build_hosted_image_url(local_path, watermark_text=watermark_text) if local_path else ""
    if hosted_local:
        return [hosted_local]

    for image_path in [task_cover_path, task.get("image_url")]:
        image_path = normalize_price_text(image_path)
        if image_path and is_remote_url(image_path):
            image_candidates.append(image_path)

    return list(dict.fromkeys(image_candidates))


def normalize_price_number(value) -> int:
    text = normalize_price_text(value)
    if not text:
        return 0

    cleaned = text.replace(",", "").replace(" ", "")
    try:
        return int(round(float(cleaned) * 100))
    except ValueError as e:
        raise ValueError(f"价格格式无法转换为数字: {value}") from e


def normalize_int_field(value, field_name: str) -> int:
    text = normalize_price_text(value)
    if not text:
        raise ValueError(f"缺少字段: {field_name}")

    cleaned = text.replace(",", "").replace(" ", "")
    if not re.fullmatch(r"\d+", cleaned):
        raise ValueError(f"{field_name} 必须是数字编码，当前值: {value}")
    return int(cleaned)


def build_property_map(product_channel_pv: list[dict]) -> dict[str, str]:
    return {
        normalize_price_text(item.get("property_name")): normalize_price_text(item.get("value_name"))
        for item in product_channel_pv
        if normalize_price_text(item.get("property_name")) and normalize_price_text(item.get("value_name"))
    }


def compact_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def extract_season_prefix(name: str) -> str:
    cleaned = compact_spaces(name)
    match = re.match(r"^(\d{2}/\d{2})\s*", cleaned)
    return match.group(1) if match else ""


def strip_leading_season(name: str) -> str:
    cleaned = compact_spaces(name)
    cleaned = re.sub(r"^\d{2}/\d{2}\s*", "", cleaned)
    return cleaned.strip(" -")


def extract_model_text(name: str) -> str:
    cleaned = strip_leading_season(name)
    cleaned = re.sub(r"\s*-\s*.*$", "", cleaned)
    return compact_spaces(cleaned)


def build_template_title(task: dict, property_map: dict[str, str]) -> str:
    category = normalize_price_text(task.get("category"))
    raw_name = normalize_price_text(task.get("name"))
    season = extract_season_prefix(raw_name)
    model = extract_model_text(raw_name)
    brand = property_map.get("品牌", "")
    length = property_map.get("长度", "")
    audience = property_map.get("适用对象", "")
    board_type = property_map.get("滑雪板类型", "")
    gender = property_map.get("适用性别", "") or audience

    if category == "滑雪板":
        parts = [season, brand, model]
        if length:
            parts.append(length)
        if board_type:
            parts.append(board_type)
        parts.append("滑雪板")
        return compact_spaces(" ".join(part for part in parts if part))

    if category == "固定器":
        parts = [season, brand, model]
        if gender and gender != "中性":
            parts.append(f"{gender}款")
        parts.append("固定器")
        return compact_spaces(" ".join(part for part in parts if part))

    if category == "滑雪鞋":
        parts = [season, brand, model]
        if gender and gender != "中性":
            parts.append(f"{gender}款")
        parts.append("滑雪鞋")
        return compact_spaces(" ".join(part for part in parts if part))

    if category == "滑雪镜":
        parts = [season, brand, model, "滑雪镜"]
        return compact_spaces(" ".join(part for part in parts if part))

    return compact_spaces(" ".join(part for part in [season, strip_leading_season(raw_name) or raw_name] if part))


def build_default_description(task: dict, product_channel_pv: list[dict]) -> str:
    category = normalize_price_text(task.get("category"))
    name = normalize_price_text(task.get("name"))
    property_map = build_property_map(product_channel_pv)
    lines = []

    title_seed = build_template_title(task, property_map)
    if title_seed:
        lines.append(title_seed)

    line_labels = []
    if category == "滑雪板":
        line_labels = ["品牌", "长度", "滑雪板类型", "适用对象", "成色"]
    elif category == "固定器":
        line_labels = ["品牌", "适用性别", "固定器穿脱方式", "成色"]
    elif category == "滑雪鞋":
        line_labels = ["品牌", "适用对象", "鞋码", "成色"]
    elif category == "滑雪镜":
        line_labels = ["品牌", "成色"]
    else:
        line_labels = ["品牌", "适用对象", "成色"]

    info_parts = []
    for label in line_labels:
        value = property_map.get(label)
        if value:
            info_parts.append(f"{label}：{value}")
    if info_parts:
        lines.append(" / ".join(info_parts))

    stock = normalize_price_text(task.get("stock"))
    if stock:
        lines.append(f"库存：{stock}")

    if name and name != title_seed:
        lines.append(f"原始款名：{name}")

    lines.append("默认按实物现状发货，具体以商品页面信息为准。")
    lines.append("如有问题可先沟通确认。")
    return "\n".join(line for line in lines if line).strip()


def parse_stock_entries(stock_text: str | None) -> list[tuple[str, int]]:
    stock_text = normalize_price_text(stock_text)
    if not stock_text:
        return []

    entries = []
    for chunk in stock_text.split("|"):
        chunk = chunk.strip()
        if not chunk or ":" not in chunk:
            continue
        key, qty_text = chunk.split(":", 1)
        key = key.strip()
        qty_match = re.search(r"\d+", qty_text)
        qty = int(qty_match.group()) if qty_match else 0
        if key and qty > 0:
            entries.append((key, qty))
    return entries


def parse_stock_entries_all(stock_text: str | None) -> list[tuple[str, int]]:
    stock_text = normalize_price_text(stock_text)
    if not stock_text:
        return []

    entries = []
    for chunk in stock_text.split("|"):
        chunk = chunk.strip()
        if not chunk or ":" not in chunk:
            continue
        key, qty_text = chunk.split(":", 1)
        key = key.strip()
        qty_match = re.search(r"\d+", qty_text)
        qty = int(qty_match.group()) if qty_match else 0
        if key:
            entries.append((key, qty))
    return entries


def sanitize_sku_value(value: str) -> str:
    text = normalize_price_text(value)
    if not text:
        return ""
    text = text.replace("~", "-")
    text = re.sub(r"[()（）\[\]{}]", " ", text)
    text = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff/\-+ ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -/")
    return text


def trim_xianyu_sku_value(value: str, max_units: int = 20) -> str:
    trimmed = normalize_price_text(value)
    if not trimmed:
        return ""
    units = 0
    chars = []
    for ch in trimmed:
        cost = 2 if "\u4e00" <= ch <= "\u9fff" else 1
        if units + cost > max_units:
            break
        chars.append(ch)
        units += cost
    return "".join(chars).strip(" -/")


def build_sku_items(task: dict, price_value: int) -> tuple[list[dict], int]:
    entries = parse_stock_entries_all(task.get("stock"))
    positive_entries = [(sku_value, qty) for sku_value, qty in entries if int(qty) > 0]
    if len(positive_entries) <= 1:
        total_stock = sum(int(qty) for _, qty in positive_entries)
        return [], total_stock

    category = normalize_price_text(task.get("category"))
    if category == "滑雪板":
        spec_name = "长度"
    elif category == "滑雪鞋":
        spec_name = "鞋码"
    else:
        spec_name = "规格"

    sku_items = []
    seen_values = set()
    total_stock = 0
    for sku_value, qty in positive_entries:
        clean_value = sanitize_sku_value(sku_value.replace("()", "").strip())
        clean_value = trim_xianyu_sku_value(clean_value, max_units=20)
        if not clean_value:
            continue
        if clean_value in seen_values:
            continue
        seen_values.add(clean_value)
        qty = int(qty)
        total_stock += qty
        sku_items.append({
            "price": price_value,
            "stock": qty,
            "outer_id": f"{task.get('branduid')}-{clean_value}",
            "sku_text": f"{spec_name}:{clean_value}",
        })

    if len(sku_items) <= 1:
        return [], total_stock
    return sku_items, total_stock


def build_edit_sku_items(task: dict, existing_payload: dict | None = None) -> tuple[list[dict], int]:
    stock_entries = parse_stock_entries_all(task.get("stock"))
    if len(stock_entries) <= 1:
        total_stock = sum(qty for _, qty in stock_entries)
        return [], total_stock

    current_qty_map: dict[str, int] = {}
    for sku_value, qty in stock_entries:
        clean_value = sanitize_sku_value(sku_value.replace("()", "").strip())
        clean_value = trim_xianyu_sku_value(clean_value, max_units=20)
        if clean_value:
            current_qty_map[clean_value] = qty

    if not current_qty_map:
        return [], 0

    existing_payload = existing_payload or {}
    existing_sku_items = existing_payload.get("sku_items") or []
    default_price = int(existing_payload.get("price") or 0)
    sku_items = []
    used_values = set()

    for item in existing_sku_items if isinstance(existing_sku_items, list) else []:
        if not isinstance(item, dict):
            continue
        sku_text = normalize_price_text(item.get("sku_text"))
        if ":" not in sku_text:
            continue
        _, raw_value = sku_text.split(":", 1)
        value = trim_xianyu_sku_value(sanitize_sku_value(raw_value), max_units=20)
        if not value:
            continue
        used_values.add(value)
        sku_items.append({
            "price": int(item.get("price") or default_price or 1),
            "stock": int(current_qty_map.get(value, 0)),
            "outer_id": normalize_price_text(item.get("outer_id")) or f"{task.get('branduid')}-{value}",
            "sku_text": sku_text,
        })

    if not sku_items:
        category = normalize_price_text(task.get("category"))
        if category == "滑雪板":
            spec_name = "长度"
        elif category == "滑雪鞋":
            spec_name = "鞋码"
        else:
            spec_name = "规格"
        for value, qty in current_qty_map.items():
            sku_items.append({
                "price": default_price or 1,
                "stock": int(qty),
                "outer_id": f"{task.get('branduid')}-{value}",
                "sku_text": f"{spec_name}:{value}",
            })
            used_values.add(value)

    for value, qty in current_qty_map.items():
        if value in used_values:
            continue
        sku_items.append({
            "price": default_price or 1,
            "stock": int(qty),
            "outer_id": f"{task.get('branduid')}-{value}",
            "sku_text": f"规格:{value}",
        })

    total_stock = sum(int(item.get("stock") or 0) for item in sku_items)
    return sku_items, total_stock


def trim_xianyu_title(title: str, max_units: int = 60) -> str:
    trimmed = (title or "").strip()
    if not trimmed:
        return ""

    units = 0
    chars = []
    for ch in trimmed:
        # 闲鱼标题规则：中文按 2 个字符，其余按 1 个字符处理
        cost = 2 if "\u4e00" <= ch <= "\u9fff" else 1
        if units + cost > max_units:
            break
        chars.append(ch)
        units += cost
    return "".join(chars).strip()


def load_account_shipping_regions(task: dict) -> tuple[list[dict], int]:
    raw = str(task.get("account_shipping_regions_json") or "[]").strip() or "[]"
    try:
        regions = json.loads(raw)
    except Exception:
        regions = []
    normalized = []
    for item in regions if isinstance(regions, list) else []:
        if not isinstance(item, dict):
            continue
        province = normalize_price_text(item.get("province"))
        city = normalize_price_text(item.get("city"))
        district = normalize_price_text(item.get("district"))
        if province and city and district:
            normalized.append({"province": province, "city": city, "district": district})
    try:
        group_size = int(task.get("account_shipping_region_group_size") or 0)
    except Exception:
        group_size = 0
    return normalized, max(1, group_size) if group_size > 0 else DEFAULT_SHIPPING_REGION_GROUP_SIZE


def build_create_payload(
    task: dict,
    defaults: dict,
    category_mapping: dict,
    upload_watermark: bool = False,
    shipping_region_override: dict | None = None,
) -> dict:
    publish_price = task.get("publish_price") or task.get("final_price_cny") or ""
    if publish_price in (None, ""):
        raise ValueError("缺少发布价格，请先确认 final_price_cny 或 publish_price")

    original_price = normalize_price_text(task.get("original_price_cny"))
    if not original_price:
        original_price = normalize_price_text(task.get("final_price_cny"))

    image_candidates = collect_publish_images(task, upload_watermark=upload_watermark)

    if not image_candidates:
        raise ValueError("缺少可用图片")

    product_channel_pv = load_product_channel_pv(int(task["product_id"]))
    channel_pv = defaults.get("channel_pv_json", "[]") or "[]"
    account_channel_pv = (task.get("account_channel_pv_json") or "").strip()
    if account_channel_pv:
        channel_pv = account_channel_pv
    task_channel_pv = (task.get("channel_pv_json") or "").strip()
    if task_channel_pv:
        channel_pv = task_channel_pv
    if product_channel_pv:
        channel_pv_payload = product_channel_pv
    else:
        channel_pv_payload = json.loads(channel_pv)
    property_map = build_property_map(channel_pv_payload)
    template_title = build_template_title(task, property_map)
    title = trim_xianyu_title(task.get("ai_title") or task.get("product_ai_title") or template_title or task.get("name") or "")
    if not title:
        raise ValueError("缺少标题，无法组装 payload")
    description = (task.get("ai_description") or task.get("product_ai_description") or "").strip()
    if not description:
        description = build_default_description(task, channel_pv_payload)

    publish_meta = load_product_publish_meta(int(task["product_id"]))
    stuff_status_value = publish_meta.get("stuff_status") or task.get("account_stuff_status") or defaults.get("stuff_status", "1")
    service_support = normalize_price_text(defaults.get("service_support", ""))
    price_value = normalize_price_number(publish_price)
    sku_items, total_stock = build_sku_items(task, price_value)

    payload = {
        "item_biz_type": int(category_mapping.get("item_biz_type") or task.get("account_item_biz_type") or defaults.get("item_biz_type", "1")),
        "sp_biz_type": int(category_mapping.get("sp_biz_type") or task.get("account_sp_biz_type") or defaults.get("sp_biz_type", "0")),
        "channel_cat_id": str(category_mapping.get("channel_cat_id") or ""),
        "channel_pv": channel_pv_payload,
        "price": price_value,
        "original_price": normalize_price_number(original_price),
        "stock": total_stock,
        "outer_id": str(task.get("branduid") or task.get("product_id")),
        "stuff_status": int(stuff_status_value),
        "publish_shop": [
            {
                "images": image_candidates,
                "user_name": task.get("account_user_name") or defaults.get("user_name", ""),
                "province": normalize_int_field((shipping_region_override or {}).get("province") or task.get("account_province") or defaults.get("province", ""), "province"),
                "city": normalize_int_field((shipping_region_override or {}).get("city") or task.get("account_city") or defaults.get("city", ""), "city"),
                "district": normalize_int_field((shipping_region_override or {}).get("district") or task.get("account_district") or defaults.get("district", ""), "district"),
                "title": title,
                "content": description,
            }
        ],
    }
    if service_support:
        payload["publish_shop"][0]["service_support"] = service_support
    if sku_items:
        payload["sku_items"] = sku_items
    return payload


def build_publish_payload(third_product_id, user_name: str, notify_url: str = "", specify_publish_time: str = "") -> dict:
    payload = {
        "product_id": normalize_int_field(third_product_id, "product_id"),
        "user_name": [normalize_price_text(user_name)],
    }
    notify_url = normalize_price_text(notify_url)
    if notify_url:
        payload["notify_url"] = notify_url
    specify_publish_time = normalize_price_text(specify_publish_time)
    if specify_publish_time:
        payload["specify_publish_time"] = specify_publish_time
    return payload


def build_downshelf_payload(third_product_id, user_name: str, notify_url: str = "") -> dict:
    payload = {
        "product_id": normalize_int_field(third_product_id, "product_id"),
        "user_name": [normalize_price_text(user_name)],
    }
    notify_url = normalize_price_text(notify_url)
    if notify_url:
        payload["notify_url"] = notify_url
    return payload


def build_edit_payload(task: dict, notify_url: str = "") -> dict:
    existing_payload_text = normalize_price_text(task.get("publish_payload_json"))
    try:
        existing_payload = json.loads(existing_payload_text) if existing_payload_text else {}
    except Exception:
        existing_payload = {}

    product_id = normalize_int_field(task.get("third_product_id"), "product_id")
    current_stock_entries = parse_stock_entries_all(task.get("stock"))
    sku_items, total_stock = build_edit_sku_items(task, existing_payload=existing_payload)

    payload = {
        "product_id": product_id,
    }

    notify_url = normalize_price_text(notify_url)
    if notify_url:
        payload["notify_url"] = notify_url

    price_value = int(existing_payload.get("price") or 0)
    if price_value > 0:
        payload["price"] = price_value

    if sku_items:
        payload["sku_items"] = sku_items
        payload["stock"] = max(0, total_stock)
    else:
        total = sum(qty for _, qty in current_stock_entries)
        payload["stock"] = max(0, total)

    return payload
