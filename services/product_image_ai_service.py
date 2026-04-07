import base64
import hashlib
import io
import json
import os
import random
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import requests
from PIL import Image, ImageDraw, ImageFont
from requests.exceptions import ConnectionError, ProxyError, ReadTimeout, SSLError, Timeout

from services.oss_storage_service import is_oss_configured, upload_local_file_to_oss

DB_PATH = "products.db"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"


def _resolve_storage_dir(env_name: str, default_path: Path) -> Path:
    raw_value = str(os.getenv(env_name) or "").strip()
    if not raw_value:
        return default_path
    candidate = Path(raw_value).expanduser()
    if not candidate.is_absolute():
        candidate = (PROJECT_ROOT / candidate).resolve()
    return candidate


AI_IMAGE_DIR = _resolve_storage_dir("AI_IMAGE_STORAGE_DIR", DATA_DIR / "ai_generated")
UPLOAD_VARIANT_DIR = _resolve_storage_dir("AI_UPLOAD_VARIANT_DIR", DATA_DIR / "upload_variants")
PREPROCESSED_INPUT_DIR = _resolve_storage_dir("AI_PREPROCESSED_INPUT_DIR", DATA_DIR / "ai_preprocessed_inputs")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def http_post_with_retry(url: str, *, max_retries: int = 3, **kwargs):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, **kwargs)
            if resp.status_code in (502, 503, 504, 524):
                last_error = RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '')[:500]}")
                if attempt >= max_retries:
                    return resp
                import time
                time.sleep(2 ** (attempt - 1))
                continue
            return resp
        except (SSLError, ConnectionError, ProxyError, ReadTimeout, Timeout) as e:
            last_error = e
            if attempt >= max_retries:
                raise
            import time
            time.sleep(2 ** (attempt - 1))
    raise last_error  # pragma: no cover


def http_get_with_retry(url: str, *, max_retries: int = 3, **kwargs):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, **kwargs)
            if resp.status_code in (502, 503, 504, 524):
                last_error = RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '')[:500]}")
                if attempt >= max_retries:
                    return resp
                import time
                time.sleep(2 ** (attempt - 1))
                continue
            return resp
        except (SSLError, ConnectionError, ProxyError, ReadTimeout, Timeout) as e:
            last_error = e
            if attempt >= max_retries:
                raise
            import time
            time.sleep(2 ** (attempt - 1))
    raise last_error  # pragma: no cover


def ensure_ai_image_table():
    conn = get_conn()
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

    columns = [row["name"] for row in cur.execute("PRAGMA table_info(xianyu_product_ai_images)").fetchall()]
    if "account_name" not in columns or "is_selected" not in columns or "asset_type" not in columns:
        cur.execute("ALTER TABLE xianyu_product_ai_images RENAME TO xianyu_product_ai_images_old")
        create_table()
        old_columns = [row["name"] for row in cur.execute("PRAGMA table_info(xianyu_product_ai_images_old)").fetchall()]
        select_columns = [
            "product_id",
            "'' AS account_name",
            "'main' AS asset_type",
            "ai_main_image_path",
            "'' AS oss_url",
            "source_image_path",
            "provider",
            "model_name",
            "prompt_text",
            "0 AS is_selected",
            "COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at",
            "COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at",
        ]
        if "provider" not in old_columns:
            select_columns[5] = "'' AS provider"
        if "model_name" not in old_columns:
            select_columns[6] = "'' AS model_name"
        if "prompt_text" not in old_columns:
            select_columns[7] = "'' AS prompt_text"
        cur.execute(f"""
            INSERT INTO xianyu_product_ai_images (
                product_id, account_name, asset_type, ai_main_image_path, oss_url, source_image_path,
                provider, model_name, prompt_text, is_selected, created_at, updated_at
            )
            SELECT {", ".join(select_columns)}
            FROM xianyu_product_ai_images_old
        """)
        cur.execute("DROP TABLE xianyu_product_ai_images_old")
    else:
        create_table()
        if "oss_url" not in columns:
            cur.execute("ALTER TABLE xianyu_product_ai_images ADD COLUMN oss_url TEXT")
        if "asset_type" not in columns:
            cur.execute("ALTER TABLE xianyu_product_ai_images ADD COLUMN asset_type TEXT DEFAULT 'main'")
    conn.commit()
    conn.close()


def get_image_provider() -> str:
    return (os.getenv("IMAGE_PROVIDER") or "nanobanana").strip().lower()


def get_image_api_key() -> str:
    return (os.getenv("IMAGE_API_KEY") or "").strip()


def get_image_model() -> str:
    return (os.getenv("IMAGE_MODEL") or "fal-ai/nano-banana/edit").strip()


def get_image_base_url() -> str:
    return (os.getenv("IMAGE_BASE_URL") or "https://api.n1n.ai").strip().rstrip("/")


def get_public_media_base_url() -> str:
    return (os.getenv("PUBLIC_MEDIA_BASE_URL") or "").strip().rstrip("/")


def load_product(product_id: int):
    conn = get_conn()
    row = conn.execute("""
        SELECT p.id, p.name, p.category, p.local_image_path, p.image_url
        FROM products p
        WHERE p.id = ?
        LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return row


def load_product_ai_marketing(product_id: int, account_name: str = "") -> dict:
    conn = get_conn()
    if account_name:
        row = conn.execute("""
            SELECT COALESCE(ai_title, '') AS ai_title, COALESCE(ai_description, '') AS ai_description, COALESCE(ai_main_image_plan, '') AS ai_main_image_plan, COALESCE(ai_main_image_model_text, '') AS ai_main_image_model_text, COALESCE(ai_target_audience, '') AS ai_target_audience, COALESCE(ai_style_positioning, '') AS ai_style_positioning, COALESCE(ai_flex_feel, '') AS ai_flex_feel, COALESCE(ai_board_profile, '') AS ai_board_profile, COALESCE(ai_performance_feel, '') AS ai_performance_feel, COALESCE(ai_terrain_focus, '') AS ai_terrain_focus, COALESCE(ai_skill_level, '') AS ai_skill_level
            FROM xianyu_account_product_ai_copy
            WHERE product_id = ? AND account_name = ?
            LIMIT 1
        """, (product_id, account_name)).fetchone()
        if row:
            conn.close()
            return dict(row)
    row = conn.execute("""
        SELECT COALESCE(ai_title, '') AS ai_title, COALESCE(ai_description, '') AS ai_description, COALESCE(ai_main_image_plan, '') AS ai_main_image_plan, COALESCE(ai_main_image_model_text, '') AS ai_main_image_model_text, COALESCE(ai_target_audience, '') AS ai_target_audience, COALESCE(ai_style_positioning, '') AS ai_style_positioning, COALESCE(ai_flex_feel, '') AS ai_flex_feel, COALESCE(ai_board_profile, '') AS ai_board_profile, COALESCE(ai_performance_feel, '') AS ai_performance_feel, COALESCE(ai_terrain_focus, '') AS ai_terrain_focus, COALESCE(ai_skill_level, '') AS ai_skill_level
        FROM xianyu_product_ai_copy
        WHERE product_id = ?
        LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return dict(row) if row else {"ai_title": "", "ai_description": "", "ai_main_image_plan": "", "ai_main_image_model_text": "", "ai_target_audience": "", "ai_style_positioning": "", "ai_flex_feel": "", "ai_board_profile": "", "ai_performance_feel": "", "ai_terrain_focus": "", "ai_skill_level": ""}


def load_existing_ai_image(product_id: int, account_name: str = "", asset_type: str = "main"):
    ensure_ai_image_table()
    conn = get_conn()
    row = conn.execute("""
        SELECT id, ai_main_image_path, oss_url, source_image_path, provider, model_name, prompt_text, updated_at, is_selected, account_name, asset_type
        FROM xianyu_product_ai_images
        WHERE product_id = ?
          AND COALESCE(account_name, '') = ?
          AND COALESCE(asset_type, 'main') = ?
        ORDER BY id DESC
        LIMIT 1
    """, (product_id, account_name or "", asset_type or "main")).fetchone()
    conn.close()
    return row


def list_ai_images(product_id: int, account_name: str = "", asset_type: str | None = "main") -> list[dict]:
    ensure_ai_image_table()
    conn = get_conn()
    if asset_type is None:
        rows = conn.execute("""
            SELECT id, product_id, account_name, asset_type, ai_main_image_path, oss_url, source_image_path, provider, model_name, prompt_text, is_selected, created_at, updated_at
            FROM xianyu_product_ai_images
            WHERE product_id = ?
              AND COALESCE(account_name, '') = ?
            ORDER BY id DESC
        """, (product_id, account_name or "")).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, product_id, account_name, asset_type, ai_main_image_path, oss_url, source_image_path, provider, model_name, prompt_text, is_selected, created_at, updated_at
            FROM xianyu_product_ai_images
            WHERE product_id = ?
              AND COALESCE(account_name, '') = ?
              AND COALESCE(asset_type, 'main') = ?
            ORDER BY id DESC
        """, (product_id, account_name or "", asset_type or "main")).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def save_ai_image(product_id: int, ai_main_image_path: str, source_image_path: str, provider: str, model_name: str, prompt_text: str, account_name: str = "", oss_url: str = "", asset_type: str = "main"):
    ensure_ai_image_table()
    conn = get_conn()
    conn.execute("""
        INSERT INTO xianyu_product_ai_images (
            product_id, account_name, asset_type, ai_main_image_path, oss_url, source_image_path, provider, model_name, prompt_text, is_selected, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
    """, (product_id, account_name or "", asset_type or "main", ai_main_image_path, oss_url or "", source_image_path, provider, model_name, prompt_text))
    conn.commit()
    conn.close()


def set_selected_ai_images(product_id: int, account_name: str, selected_ids: list[int], asset_type: str = "main"):
    ensure_ai_image_table()
    conn = get_conn()
    conn.execute("""
        UPDATE xianyu_product_ai_images
        SET is_selected = 0, updated_at = CURRENT_TIMESTAMP
        WHERE product_id = ?
          AND COALESCE(account_name, '') = ?
          AND COALESCE(asset_type, 'main') = ?
    """, (product_id, account_name or "", asset_type or "main"))
    if selected_ids:
        placeholders = ",".join("?" for _ in selected_ids)
        conn.execute(f"""
            UPDATE xianyu_product_ai_images
            SET is_selected = 1, updated_at = CURRENT_TIMESTAMP
            WHERE product_id = ?
              AND COALESCE(account_name, '') = ?
              AND COALESCE(asset_type, 'main') = ?
              AND id IN ({placeholders})
        """, [product_id, account_name or "", asset_type or "main", *selected_ids])
    conn.commit()
    conn.close()


def build_public_image_url(local_path: Path) -> str:
    if is_oss_configured():
        return upload_local_file_to_oss(str(local_path))

    public_base = get_public_media_base_url()
    if not public_base:
        raise ValueError("没有检测到 PUBLIC_MEDIA_BASE_URL，请先配置可公网访问的图片地址")
    query = urlencode({"path": str(local_path)})
    return f"{public_base}/media/local?{query}"


def build_public_image_urls(local_paths: list[Path]) -> list[str]:
    urls = []
    for path in local_paths:
        if not path:
            continue
        urls.append(build_public_image_url(path))
    return urls


def sample_background_color(image: Image.Image) -> tuple[int, int, int, int]:
    rgba = image.convert("RGBA")
    width, height = rgba.size
    if width <= 0 or height <= 0:
        return (245, 246, 248, 255)

    sample_points = [
        (0, 0),
        (max(0, width - 1), 0),
        (0, max(0, height - 1)),
        (max(0, width - 1), max(0, height - 1)),
        (width // 2, 0),
        (width // 2, max(0, height - 1)),
        (0, height // 2),
        (max(0, width - 1), height // 2),
    ]
    pixels = [rgba.getpixel((x, y)) for x, y in sample_points]
    r = round(sum(p[0] for p in pixels) / len(pixels))
    g = round(sum(p[1] for p in pixels) / len(pixels))
    b = round(sum(p[2] for p in pixels) / len(pixels))
    a = round(sum(p[3] for p in pixels) / len(pixels))
    return (r, g, b, a)


def detect_subject_bbox(image: Image.Image) -> tuple[int, int, int, int]:
    rgba = image.convert("RGBA")
    width, height = rgba.size
    bg = sample_background_color(rgba)
    threshold = 28

    min_x, min_y = width, height
    max_x, max_y = -1, -1
    pixels = rgba.load()
    for y in range(height):
        for x in range(width):
            r, g, b, a = pixels[x, y]
            if a < 20:
                continue
            diff = abs(r - bg[0]) + abs(g - bg[1]) + abs(b - bg[2])
            if diff >= threshold:
                if x < min_x:
                    min_x = x
                if y < min_y:
                    min_y = y
                if x > max_x:
                    max_x = x
                if y > max_y:
                    max_y = y

    if max_x < min_x or max_y < min_y:
        return (0, 0, width, height)
    return (min_x, min_y, max_x + 1, max_y + 1)


def should_skip_padding_for_model_photo(image: Image.Image, category: str) -> bool:
    category = str(category or "").strip()
    if category not in ("滑雪镜", "滑雪服", "滑雪帽衫和中间层", "儿童装备", "滑雪头盔"):
        return False

    rgba = image.convert("RGBA")
    width, height = rgba.size
    if width <= 0 or height <= 0:
        return False

    bbox_left, bbox_top, bbox_right, bbox_bottom = detect_subject_bbox(rgba)
    bbox_width = max(1, bbox_right - bbox_left)
    bbox_height = max(1, bbox_bottom - bbox_top)
    width_ratio = bbox_width / width
    height_ratio = bbox_height / height

    # 模特图通常主体更接近整张图高度，且宽度不会像纯商品静物那样极度集中。
    if height_ratio >= 0.82 and width_ratio >= 0.42:
        return True
    return False


def use_art_background_account_rules(account_name: str) -> bool:
    return str(account_name or "").strip() == "YY雪友小铺"


def use_2k_output_rules(account_name: str) -> bool:
    return str(account_name or "").strip() == "YY雪友小铺"


def build_preprocessed_input_image(source_path: Path, category: str, product_id: int) -> Path:
    category = str(category or "").strip()

    PREPROCESSED_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PREPROCESSED_INPUT_DIR / f"{product_id}_{source_path.stem}_preprocessed.png"
    if output_path.exists() and output_path.stat().st_mtime_ns >= source_path.stat().st_mtime_ns:
        return output_path

    image = Image.open(source_path).convert("RGBA")
    width, height = image.size
    if width <= 0 or height <= 0:
        return source_path
    if should_skip_padding_for_model_photo(image, category):
        return source_path

    bg_color = sample_background_color(image)
    base_canvas_size = max(width, height)
    square_canvas = Image.new("RGBA", (base_canvas_size, base_canvas_size), bg_color)
    square_offset_x = (base_canvas_size - width) // 2
    square_offset_y = (base_canvas_size - height) // 2
    square_canvas.alpha_composite(image, (square_offset_x, square_offset_y))

    bbox_left, bbox_top, bbox_right, bbox_bottom = detect_subject_bbox(square_canvas)
    bbox_width = max(1, bbox_right - bbox_left)
    bbox_height = max(1, bbox_bottom - bbox_top)
    current_ratio = max(bbox_width / base_canvas_size, bbox_height / base_canvas_size)
    current_margins = (
        bbox_left / base_canvas_size,
        bbox_top / base_canvas_size,
        (base_canvas_size - bbox_right) / base_canvas_size,
        (base_canvas_size - bbox_bottom) / base_canvas_size,
    )

    target_margin = 0.15 if category == "固定器" else 0.12
    target_inner_ratio = 1.0 - (target_margin * 2.0)
    needs_padding = current_ratio > target_inner_ratio or min(current_margins) < target_margin
    if not needs_padding:
        return source_path

    scale = min((target_inner_ratio * base_canvas_size) / bbox_width, (target_inner_ratio * base_canvas_size) / bbox_height)
    scale = min(scale, 1.0)
    scaled_size = (
        max(1, round(base_canvas_size * scale)),
        max(1, round(base_canvas_size * scale)),
    )
    resized_square = square_canvas.resize(scaled_size, Image.Resampling.LANCZOS)
    resized_bbox = (
        round(bbox_left * scale),
        round(bbox_top * scale),
        round(bbox_right * scale),
        round(bbox_bottom * scale),
    )
    resized_bbox_cx = (resized_bbox[0] + resized_bbox[2]) / 2.0
    resized_bbox_cy = (resized_bbox[1] + resized_bbox[3]) / 2.0

    canvas = Image.new("RGBA", (base_canvas_size, base_canvas_size), bg_color)
    target_cx = base_canvas_size / 2.0
    target_cy = base_canvas_size / 2.0
    offset_x = round(target_cx - resized_bbox_cx)
    offset_y = round(target_cy - resized_bbox_cy)
    canvas.alpha_composite(resized_square, (offset_x, offset_y))
    canvas.save(output_path, format="PNG")
    return output_path


def _build_fixed_main_image_prompt(name: str, category: str, ai_title: str, model_text: str, slot_index: int, *, target_audience: str = "", style_positioning: str = "", flex_feel: str = "", board_profile: str = "", performance_feel: str = "", terrain_focus: str = "", skill_level: str = "") -> str:
    slots = [
        f"""
你是一名淘宝电商主图设计助手。请基于输入商品图，为这件商品生成第1张点击主图。

商品名称：
{name}

淘宝标题：
{ai_title or name}

商品分类：
{category or "未分类"}

第1张固定标准：
1. 产品必须绝对居中，整体版式统一。
2. 可以加入基于产品本身设计语言延展出来的艺术风格元素，例如配色呼应、图案延展、几何构成、雪地质感、冷调光影。
3. 商品本体始终是第一主角，不能被背景或艺术元素抢掉。
4. 画面里只允许出现一个信息点，而且只能使用这个型号词：{model_text or "型号词"}
5. 禁止出现品牌、年份、参数、卖点堆叠或其他额外文案。
6. 背景可以有高级感和艺术性，但必须服务于点击率，不能做成纯海报。
7. 输出 1:1 正方形电商成图，无水印，无无关人物，无无关道具。
""".strip(),
        f"""
你是一名淘宝电商主图设计助手。请基于输入商品图，为这件商品生成第2张核心卖点图。

商品名称：
{name}

淘宝标题：
{ai_title or name}

商品分类：
{category or "未分类"}

第2张固定标准：
1. 商品仍然是主体。
2. 让买家一眼知道这件商品适合谁、适合什么玩法、是什么定位。
3. 可以有少量标签，但不要堆满文字。
4. 只允许围绕这三个字段组织信息：
   - 适合人群：{target_audience or "未提供"}
   - 风格定位：{style_positioning or "未提供"}
   - 软硬取向：{flex_feel or "未提供"}
5. 不要额外扩展到别的参数或卖点。
5. 输出要像卖点图，不要像详情长图。
6. 输出 1:1 正方形电商成图，无水印。
""".strip(),
        f"""
你是一名淘宝电商主图设计助手。请基于输入商品图，为这件商品生成第3张性能参数图。

商品名称：
{name}

淘宝标题：
{ai_title or name}

商品分类：
{category or "未分类"}

第3张固定标准：
1. 做成清晰的信息分区，方便懂行买家快速判断。
2. 商品仍然要出现，但参数信息可以更突出。
3. 只允许围绕这些已确认字段组织参数信息：
   - 板型/类型：{board_profile or "未提供"}
   - 软硬/弹性：{performance_feel or "未提供"}
   - 适合地形：{terrain_focus or "未提供"}
   - 适合水平：{skill_level or "未提供"}
4. 可结合商品基础信息表达品牌、型号、长度，但不要额外扩展其他未经确认的参数。
5. 不要把信息做得太密，不要像详情页长图。
6. 不能编造没有明确依据的参数。
7. 输出 1:1 正方形电商成图，无水印。
""".strip(),
        f"""
你是一名淘宝电商主图设计助手。请基于输入商品图，为这件商品生成第4张细节质感图。

商品名称：
{name}

淘宝标题：
{ai_title or name}

商品分类：
{category or "未分类"}

第4张固定标准：
1. 整体做成高质量买家晒单感、收到商品后的分享图氛围，有自然生活化实拍感和满意收货后的展示感。
2. 不是棚拍硬广，也不是杂乱随手拍，画面要真实、自然、有轻社交分享感。
3. 商品必须仍然是主角，重点突出做工、纹理、涂装、边缘、Logo、材质细节。
4. 可以使用局部特写拼图或单张近景，但商品识别必须清楚。
5. 要让买家感觉“实物真好看，收到货会有惊喜”，从而建立质感和信任感。
6. 不要做成远景场景图，不要让背景抢主体，不要加入无关人物或杂乱道具。
7. 颜色和细节必须真实，不能过度滤镜化。
8. 输出 1:1 正方形电商成图，无水印。
""".strip(),
        f"""
你是一名淘宝电商主图设计助手。请基于输入商品图，为这件商品生成第5张白底标准图。

商品名称：
{name}

淘宝标题：
{ai_title or name}

商品分类：
{category or "未分类"}

第5张固定标准：
1. 纯白底。
2. 单主体。
3. 商品完整、居中、轮廓清楚。
4. 无复杂文案、无场景、无水印、无多余装饰。
5. 平台适配优先。
6. 输出 1:1 正方形电商成图。
""".strip(),
    ]
    return slots[slot_index % len(slots)]


def normalize_image_channel(channel: str = "") -> str:
    normalized = str(channel or "").strip().lower()
    if normalized == "taobao":
        return "taobao"
    return "xianyu"


def _build_taobao_detail_image_prompt(name: str, category: str, ai_title: str) -> str:
    return f"""
你是一名淘宝电商详情图设计助手。请基于输入商品图，生成一张适合淘宝详情页/辅图使用的商品图。

商品名称：
{name}

淘宝标题：
{ai_title or name}

商品分类：
{category or "未分类"}

要求：
1. 商品必须保持真实结构、比例、材质、颜色、Logo 和细节，不得改款、改色、改结构。
2. 整体风格偏淘宝电商详情图，不要做成闲鱼雪场氛围图，也不要做成艺术海报。
3. 画面可以有更清晰的信息分层、细节质感或轻场景化背景，但商品仍然必须是主角。
4. 可以强化做工、材质、结构、轮廓和高级感，但不要加入多余文字、水印、Logo 贴纸或夸张特效。
5. 背景应当干净、克制、带电商视觉质感，优先服务商品展示。
6. 输出 1:1 正方形成图，适合淘宝商品详情页与辅图。
""".strip()


def build_image_prompt(product: dict, variant_index: int = 0, asset_type: str = "main", channel: str = "xianyu") -> str:
    name = str(product.get("name") or "").strip()
    category = str(product.get("category") or "").strip()
    color = str(product.get("color") or "").strip()
    account_name = str(product.get("account_name") or "").strip()
    channel = normalize_image_channel(channel or product.get("image_channel") or "")
    ai_title = str(product.get("ai_title") or "").strip()
    main_image_model_text = str(product.get("ai_main_image_model_text") or "").strip()
    target_audience = str(product.get("ai_target_audience") or "").strip()
    style_positioning = str(product.get("ai_style_positioning") or "").strip()
    flex_feel = str(product.get("ai_flex_feel") or "").strip()
    board_profile = str(product.get("ai_board_profile") or "").strip()
    performance_feel = str(product.get("ai_performance_feel") or "").strip()
    terrain_focus = str(product.get("ai_terrain_focus") or "").strip()
    skill_level = str(product.get("ai_skill_level") or "").strip()
    variant_seed = variant_index % 8
    if channel == "taobao" and asset_type == "main":
        return _build_fixed_main_image_prompt(name, category, ai_title, main_image_model_text, variant_index, target_audience=target_audience, style_positioning=style_positioning, flex_feel=flex_feel, board_profile=board_profile, performance_feel=performance_feel, terrain_focus=terrain_focus, skill_level=skill_level)
    if channel == "taobao" and asset_type == "detail":
        return _build_taobao_detail_image_prompt(name, category, ai_title)
    if use_art_background_account_rules(account_name):
        art_background_variants = [
            "背景偏高级品牌海报风，带抽象雪地纹理、冷色渐变和轻微速度感光影。",
            "背景偏艺术装置风雪场陈列，带克制的几何结构、雾感层次和冷调环境光。",
            "背景偏杂志大片风格，带抽象冰晶肌理、柔和体积光和品牌视觉氛围。",
            "背景偏未来感雪地艺术场景，带流动光带、雪雾层次和高级冷调质感。",
            "背景偏高端户外广告视觉，带简洁图形、雪地颗粒肌理和轻微金属反光。",
            "背景偏先锋艺术摄影棚景，带抽象雪山轮廓、留有空间感但不空洞。",
        ]
        art_hint = art_background_variants[variant_index % len(art_background_variants)]
        return f"""
你是一名高端电商视觉设计师。请基于输入商品图，生成一张更有设计感的艺术风格电商主图。

商品名称：
{name}

商品分类：
{category or "未分类"}

商品颜色/配色：
{color or "未提供，请以原图实际配色为准并严格保持"}

核心任务：
1. 保留商品本体的真实结构、比例、Logo、颜色、材质、纹理和细节。
2. 不要沿用之前那种简单去背景、留白、纯雪场替换的处理方式。
3. 请先识别原商品主图本身的设计语言、品牌气质、配色倾向、材质表达和画面风格，再据此延展出一个更有艺术感、统一感的背景。
4. 背景应当像为这个商品量身设计的视觉场景，而不是通用雪山模板。

风格要求：
- 背景要有明显设计感、艺术感、品牌海报感，但主体商品必须仍然最清楚。
- 主体商品必须成为画面第一视觉焦点，第一眼只能先看到商品，再看到背景设计。
- 背景层次必须主动后退，不能比商品更亮、更锐、更高对比，也不能抢主体边缘。
- 整体效果更接近高端电商广告主图，而不是纯艺术海报，商品销售感优先于背景表现。
- 背景可以使用抽象雪地纹理、渐变、图形结构、光影层次、冰晶肌理、速度感线条、装置感空间或杂志风场景语言。
- 背景要与商品原始配色、品牌气质和主图风格协调，像同一套视觉系统的延展。
- 不要生成单调纯白底、简单抠图底、普通留白底，也不要只是替换成常见雪山远景。
- 不要让背景喧宾夺主，不要加入多余人物、文字、水印、Logo贴图、夸张特效。
- 如果原图中带有网站水印、半透明店铺字样、网址、平台标记或重复文字压印，输出时必须彻底去除这些来源痕迹，不能保留任何原站点水印文字。
- 商品周围要有清楚的轮廓分离感，可以通过轻微边缘高光、接触阴影、景深层次来强化主体存在感。
- 禁止在商品正后方放置最亮区域、最强对比区域或最复杂的图形中心，避免主体被背景吞没。
- 可以用更克制的背景虚化、明暗层次和空间纵深来衬托商品，但不要让背景成为主视觉。

商品保真要求：
- 商品结构、比例、边缘、轮廓、材质、五金、绑带、镜片、鞋型、板型、服装版型必须保真。
- 不允许改变商品主颜色、关键图案、品牌标识和设计细节。
- 如果商品原图本身已经有明确的光影和设计语言，输出时要继承这种视觉逻辑，而不是完全推翻。

构图要求：
- 输出 1:1 正方形主图。
- 商品仍然要居中、完整、清楚，但不必强行套用之前那套明显留白安全边距规则。
- 商品主体占画面视觉面积控制在约 38% 到 48%，需要清楚突出，但不能过大、不能压满画面。
- 整体更像高端电商视觉海报中的商品主图，而不是纯静物证件照。
- 画面需要有完整的艺术背景，不要出现空洞大留白。
- 禁止把背景做成比商品更抢眼的主视觉，禁止背景中心高亮压在商品后面导致主体发虚。
- 商品四周必须保留足够呼吸感，避免商品边缘过于贴近画布边界。

账号风格要求：
- 这是账号“YY雪友小铺”的专属主图风格。
- 请把重点放在“识别原图设计风格并延展艺术背景”。
- 每次生成都要在艺术背景表现上有变化，但商品表达方式保持专业统一。

背景风格倾向：
{art_hint}

输出单张图片即可。
""".strip()

    background_variants = [
        "背景偏高级雪场平台和雪地近景，冷调、干净、克制，像高端雪具陈列环境。",
        "背景偏雪道起点区域和柔和雪地纹理，空间感清楚但不过分复杂。",
        "背景偏雪道边缘与远处山脊，可有少量飞雪，整体更像商业静物拍摄场景。",
        "背景偏高级雪场建筑外立面和雪地环境光，氛围精致但不喧宾夺主。",
        "背景偏缆车站外区域、雪场导视附近的虚化环境，强调真实雪场氛围而不是风景大片。",
        "背景偏器材区或雪地休息平台的高级商业环境，带轻微雪地反光，整体更像电商陈列主图。",
        "背景偏雪场木屋区、器材租赁店外部或品牌陈列区，商业感更强，但主体仍然必须最突出。",
        "背景偏压雪车走过后的干净雪地与近景雪纹，画面更简洁，像高端棚拍与雪场环境结合的效果。",
    ]
    background_hint = background_variants[variant_seed]
    category_hint = ""

    if category == "滑雪板":
        category_hint = """
补充要求（滑雪板）：
- 板身必须完整展示，不能裁掉板头或板尾。
- 板面图形、品牌字样、走线、孔位、弧线必须保真。
- 整块板在画面里的视觉占比要再克制一些，四周需要明显留出背景空间，不要让板头板尾过于贴边。
- 可以让板身轻微斜放，但仍要以商品展示为主，不要做成运动海报。
""".strip()
    elif category == "固定器":
        category_hint = """
补充要求（固定器）：
- 固定器主体结构、绑带、后倾板、底盘、扣具、五金位置必须保真。
- 左右结构和角度要自然，不要出现缺件、错位、镜像错误。
- 商品展示尺寸必须明显克制，主体占画面约 35% 到 42% 为宜。
- 固定器外轮廓距离画布四边，目标是都保留约 10% 的安全边距；不要贴边、压边或让固定器接近铺满画面。
- 如果固定器看起来偏大，必须主动缩小主体，而不是继续放大。
- 商品可以稍微带角度展示，但主体必须清楚、立体、完整，整体更像高级电商静物图。
""".strip()
    elif category == "滑雪鞋":
        category_hint = """
补充要求（滑雪鞋）：
- 鞋型、鞋帮高度、鞋带/BOA/快穿系统、鞋底轮廓必须保真。
- 不要把雪鞋改成运动鞋或靴子风格。
- 要保留鞋面纹理、包裹结构和品牌细节，画面更偏商业静物摄影。
""".strip()
    elif category == "滑雪镜":
        category_hint = """
补充要求（滑雪镜）：
- 镜框形状、镜片颜色、反光质感、海绵包边、固定带图案必须保真。
- 必须严格保持原图镜片的主颜色、镀膜颜色、综合色调和明暗关系，不允许擅自改成别的镜片颜色。
- 如果原图是银色、黑色、茶色、蓝色、粉色、彩膜、虹彩或低反射镜片，输出时必须继续保持同一类真实镜片观感，不能改色、偏色或换成另一种膜层效果。
- 镜片高光和反射要自然，不要把镜片改成完全透明或金属面。
- 镜片必须尽量还原原图的真实外观、原始色调、通透度和反射逻辑，不要把新的雪场背景、雪山、人物、建筑或天空大面积倒映到镜片上。
- 严禁为了营造环境氛围，在镜片表面额外生成明显的背景反射、风景倒影、雪道倒影或夸张高光；镜片可以有原本合理的轻微反光，但不能出现与原图不一致的场景映射。
- 如果原图镜片偏纯色、偏镀膜、偏低反射或偏深色，必须保持这种真实状态，不要擅自改成强反射镜面。
- 背景氛围可以更冷感，但镜片仍然必须是画面重点之一。
- 如果原图是模特佩戴滑雪镜的人像展示图，必须保留“模特佩戴展示”的方式，不要改成单独滑雪镜静物图。
- 如果需要扩展画面边缘，必须自然补全模特缺失的头部、帽子、头盔、发丝、肩颈或服装边缘，让画面像原本就拍得更完整；不要只把边缘留成空背景。
- 对模特佩戴图优先做清晰度增强、镜片质感增强、边缘补全和背景氛围优化，不要改变原始展示主体逻辑。
""".strip()
    elif category == "滑雪头盔":
        category_hint = """
补充要求（滑雪头盔）：
- 头盔壳体轮廓、开孔、护耳、调节结构和表面质感必须保真。
- 不要把头盔做成骑行头盔或摩托头盔风格。
- 画面要偏产品静物主图，不要加入人物佩戴效果。
""".strip()
    elif category in ("滑雪服", "滑雪帽衫和中间层", "儿童装备"):
        category_hint = """
补充要求（服装类）：
- 衣物版型、拉链、口袋、面料纹理、印花和配色必须保真。
- 不要擅自增加人物、模特或夸张飘带效果。
- 如果商品是外套或裤子，要让整件衣物轮廓清楚，不要堆叠或折叠得过度复杂。
- 如果原图是模特穿着、半身上身、挂拍、平铺或人台展示，必须保留原有展示方式和视角逻辑，不要擅自改成单独衣服正面静物图。
- 对服装类图片优先做清晰度增强、质感增强、边缘修整和背景环境优化，不要改变原始展示主体的存在方式。
- 如果原图里有人物穿着商品，可以保留人物作为商品展示载体，但人物只能服务于展示衣服本身，不能被重绘成新的姿态或新的场景主角。
""".strip()
    elif category in ("手套", "帽子护脸", "袜子以及周边配件"):
        category_hint = """
补充要求（配件类）：
- 商品主体要明显放大，避免在雪景背景里显得过小。
- 材质、缝线、图案、Logo 和边缘要清楚，不要因为背景复杂而丢失细节。
- 构图更偏近景商业静物，而不是环境大片。
""".strip()

    return f"""
你是一名电商主图设计助手。请基于输入商品图，生成一张适合闲鱼、电商平台发布的主图。

商品名称：
{name}

商品分类：
{category or "未分类"}

任务：根据商品原图生成电商主图。

目标：
保留商品原有外观、结构、Logo、颜色、绑带、金属件、镜片、纹理和材质细节，不改变产品比例、形态和核心设计。
在此基础上，将背景替换为高级感雪山雪场环境，增强单板滑雪氛围，使图片更适合电商平台展示。

颜色锁定要求：
1. 如果当前商品存在明确颜色/配色信息，必须严格保持该颜色版本，不允许改成其它颜色。
2. 即使同款存在其它颜色，也不能混入其它颜色版本的配色元素。
3. 如果原图与标题中的颜色信息存在轻微差异，以原图可见主体颜色为最高优先级，但绝不能偏离当前颜色版本。

画面要求：
1. 最终画面必须直接输出为 1:1 正方形电商主图构图，画面内容自然铺满整个画布，不要出现大块留白、补底色、空边或后期裁切感。
2. 商品主体必须居中展示，占据主要视觉区域，是画面唯一视觉核心，但主体尺寸必须明显克制，不能做成怼满画面的构图。主体整体建议只占最终画面的约 38% 到 48%。
3. 商品边缘清晰，轮廓完整，不得模糊、断裂、变形、裁切异常或被背景遮挡。
4. 保留商品真实质感，特别是黑色材质、金属件、高光、反光、磨砂、织带、扣具、镜片、纹理等细节。
5. 不得改变商品比例、结构、孔位、绑带、五金位置、镜片形状、鞋型、板型、固定器结构等核心设计。
6. 背景为高级感雪山、雪道、雪场环境，可加入轻微飞雪、冷色环境光、远景雪山、雪地纹理。
7. 背景虚化适度，只做氛围衬托，不能比商品更抢眼。
8. 光线以冷色调为主，可有少量雪地反射光，整体干净通透，有商业产品摄影质感。
9. 阴影自然，商品与地面或环境关系合理，不要出现漂浮感或假合成感。
10. 画面整体适合电商主图、商品详情页、广告展示，但不要做成海报感。
11. 不要加入多余文字、水印、边框、图标、品牌贴纸、夸张特效。
11.a 如果输入图里自带网站水印、半透明文字、网址、店铺标记或重复压印，输出图必须去除这些原始来源文字，不得保留任何可识别站点痕迹。
12. 不要修改商品设计，不要新增不存在的零件、功能结构、花纹和配色。
13. 如果模型无法确定某些细节，宁可保持原样，也不要擅自重绘或篡改。
14. 构图更偏电商静物主图，而不是广告大片；商品清楚比背景氛围更重要。
15. 如果背景和商品发生冲突，优先保留商品真实细节，弱化背景表现。
16. 背景里不要出现人物、雪板脚印主体、夸张动作姿态或任何会分散注意力的元素；可以有轻微雪场建筑、平台、缆车站环境，但只能做远景氛围。
17. 每次生成时，背景氛围可以在不同雪场场景之间自然变化，避免多张图背景过于相似，但商品表现方式仍要统一专业。
18. 如果输入图本身是正反面、多视角、拼图或并排展示，必须保留原有视角数量和展示逻辑，不要擅自删减、合并或新增商品主体。
19. 不要根据标题或分类去脑补新商品，图片内容优先级高于标题；如果原图里没有出现某个板、鞋、固定器或衣物，就绝对不要新增。
20. 最终效果要像已经完成排版的电商正方形主图，不依赖任何额外代码裁切、补边或二次构图。
21. 这是一张新的变体图，请主动与同商品常见生成结果拉开差异，尤其是背景环境、雪场位置、远景层次和光线细节，不要重复常见雪山远景模板。
22. 严禁把商品主体放大到接近铺满画面，也不要让主体边缘距离画布边缘过近；宁可略小一点，也要保留电商主图应有的留白和环境层次。
23. 商品主体外轮廓距离画布四边，必须都留出明显背景空间；目标是四边都保留约 10% 的安全边距，任何一边都不能贴边或接近贴边。
24. 如果当前构图会让商品显得过大，必须主动把商品再缩小，而不是继续放大；商品略小是允许的，主体过大是不允许的。
25. 输出时优先保证留白和安全边距，再考虑主体存在感；不要为了“突出商品”而牺牲四周留白。

Composition guidance:
A professional e-commerce product photograph of the product using a clean, modern medium-shot composition. The product must be precisely centered in the frame, with clearly visible, clean, and balanced negative space separating the product's outer edges from all four frame edges. The margin on each side should be roughly 10% of the image width or height. Visually, the main product body should occupy about 55% to 65% of the total frame, not larger, and must not touch or approach the frame edges.

Stronger layout constraint:
E-commerce photo of the product, centered. Approximately 15% of clean margin must be left on the top, bottom, left, and right sides of the product, respectively. The main product body is strictly confined within the central 70% area. It is strictly forbidden for the product to touch or approach any image edges. The margin area must remain clean, simple, and visually quiet, without busy background elements.

背景风格倾向：
{background_hint}

{category_hint}

输出单张图片即可。
""".strip()


def build_group_cover_prompt(group_name: str, category: str, colors: list[str], channel: str = "xianyu") -> str:
    color_text = " / ".join([str(color or "").strip() for color in colors if str(color or "").strip()])
    channel = normalize_image_channel(channel)
    base = f"""
你是一名电商主图设计助手。请基于输入图片，生成一张“同款多色”的电商组商品主图。

商品名称：
{group_name}

商品分类：
{category or "未分类"}

颜色列表：
{color_text or "以输入图中的颜色版本为准"}

核心要求：
1. 输入图中已经包含同款多个颜色版本，必须完整保留这些颜色版本，不允许删掉、替换、增减或改色。
2. 输出必须是 1:1 正方形电商主图，画面更统一、更高级，但仍然一眼能看出这是同款多色商品。
3. 不要只做机械拼贴感排版，要让整体更像专业电商海报主图；但也不能把多个颜色融合成一个商品。
4. 每个颜色版本都必须清楚可见，颜色真实，结构、Logo、材质和细节保真。
5. 不允许改变任一成员商品的主颜色，不允许把一个颜色版本生成成另一个颜色版本。
6. 不要添加人物、文字、水印、品牌贴纸、夸张特效。
7. 背景可以更统一、更有设计感，但主体仍然必须是这些商品本身。
8. 输出的是“多色组合主图”，不是单个商品主图。
"""
    if channel == "taobao":
        base += "\n9. 画面更偏淘宝商品主图风格，干净、明确、适合商品卡展示。"
    else:
        base += "\n9. 画面更偏闲鱼电商主图风格，真实、清楚、兼顾转化感。"
    return base.strip()


def generate_group_ai_cover_image(
    *,
    source: str,
    group_id: int,
    group_name: str,
    category: str,
    items: list[dict],
    account_name: str = "",
    channel: str = "xianyu",
    output_name: str = "ai_cover.jpg",
) -> str:
    provider = get_image_provider()
    if provider not in ("nanobanana", "n1n", "n1n_nanobanana", "gemini_image", "openai", "openai_image", "openai_compatible", "flux", "flux_edit"):
        raise ValueError(f"暂不支持的图片生成 provider: {provider}")

    image_paths: list[Path] = []
    for item in items:
        image_path = str(item.get("ai_image_path") or item.get("local_image_path") or "").strip()
        if not image_path:
            continue
        path_obj = Path(image_path)
        if path_obj.exists():
            image_paths.append(path_obj)
    if not image_paths:
        raise ValueError("组商品缺少可用成员图片")

    prompt_text = build_group_cover_prompt(
        group_name=group_name,
        category=category,
        colors=[str(item.get("color") or "").strip() for item in items],
        channel=channel,
    )
    model_name = get_image_model().strip().lower()
    if provider in ("openai", "openai_image", "openai_compatible", "flux", "flux_edit") or model_name.startswith("gpt-image") or model_name.startswith("flux"):
        image_bytes = call_openai_compatible_image(image_paths, prompt_text)
    elif model_name.startswith("fal-ai/"):
        image_bytes = call_nanobanana_image(image_paths, prompt_text)
    elif model_name.startswith("gemini-"):
        image_bytes = call_gemini_image_via_n1n(image_paths, prompt_text, account_name=account_name)
    else:
        raise ValueError(f"暂不支持的图片模型路由: {get_image_model()}")

    output_dir = DATA_DIR / "group_assets" / source / f"group_{int(group_id)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_name
    output_path.write_bytes(image_bytes)
    return str(output_path)


def add_account_watermark(image_bytes: bytes, account_name: str) -> bytes:
    watermark_text = str(account_name or "").strip()
    if not watermark_text:
        return image_bytes

    base_image = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    width, height = base_image.size

    overlay = Image.new("RGBA", base_image.size, (255, 255, 255, 0))
    wm_w, wm_h = 234, 67

    # 中文字体候选，兼容本地 macOS 和服务器 Linux 环境
    font_candidates = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc",
        "/System/Library/Fonts/Supplemental/Songti.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/arphic/uming.ttc",
        "/usr/share/fonts/truetype/arphic/ukai.ttc",
    ]

    def load_font(size: int):
        for font_path in font_candidates:
            try:
                return ImageFont.truetype(font_path, size)
            except Exception:
                continue
        return ImageFont.load_default()

    def make_stamp(stamp_w: int, stamp_h: int, label_size: int, name_size: int):
        stamp = Image.new("RGBA", (stamp_w, stamp_h), (255, 255, 255, 0))
        draw = ImageDraw.Draw(stamp)
        draw.rounded_rectangle(
            (0, 0, stamp_w - 1, stamp_h - 1),
            radius=max(8, round(stamp_h * 0.16)),
            fill=(36, 46, 58, 76),
            outline=(255, 255, 255, 118),
            width=1,
        )
        draw.rounded_rectangle(
            (5, 5, stamp_w - 6, stamp_h - 6),
            radius=max(6, round(stamp_h * 0.12)),
            outline=(255, 255, 255, 38),
            width=1,
        )

        label_font = load_font(label_size)
        name_font = load_font(name_size)
        label = "闲鱼店铺："
        draw.text(
            (12, 7),
            label,
            font=label_font,
            fill=(255, 255, 255, 255),
        )

        while True:
            name_bbox = draw.textbbox((0, 0), watermark_text, font=name_font)
            name_w = name_bbox[2] - name_bbox[0]
            name_h = name_bbox[3] - name_bbox[1]
            if name_w <= stamp_w - 24 or getattr(name_font, "size", 0) <= 11:
                break
            name_font = load_font(max(11, getattr(name_font, "size", name_size) - 1))

        text_x = (stamp_w - name_w) // 2
        text_y = max(23, (stamp_h - name_h) // 2 + 4)

        draw.text(
            (text_x + 1, text_y + 1),
            watermark_text,
            font=name_font,
            fill=(0, 0, 0, 108),
        )
        draw.text(
            (text_x, text_y),
            watermark_text,
            font=name_font,
            fill=(255, 255, 255, 250),
        )
        return stamp

    center_stamp = make_stamp(wm_w, wm_h, 15, 26)

    center_x = max(0, (width - wm_w) // 2)
    center_y = max(0, (height - wm_h) // 2)
    overlay.alpha_composite(center_stamp, (center_x, center_y))

    merged = Image.alpha_composite(base_image, overlay).convert("RGB")
    output = io.BytesIO()
    merged.save(output, format="PNG")
    return output.getvalue()


def build_watermarked_upload_variant(local_image_path: str, account_name: str) -> str:
    account_name = str(account_name or "").strip()
    if not account_name:
        return local_image_path

    source_path = Path(local_image_path)
    if not source_path.exists():
        raise FileNotFoundError(f"本地图片不存在: {local_image_path}")

    stat = source_path.stat()
    source_hash = hashlib.md5(str(source_path.resolve()).encode("utf-8")).hexdigest()[:6]
    account_hash = hashlib.md5(account_name.encode("utf-8")).hexdigest()[:4]
    time_token = format(int(stat.st_mtime_ns), "x")[-5:]
    out_name = f"w{source_hash}{account_hash}{time_token}.jpg"
    UPLOAD_VARIANT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = UPLOAD_VARIANT_DIR / out_name
    if out_path.exists():
        return str(out_path)

    image_bytes = source_path.read_bytes()
    watermarked = add_account_watermark(image_bytes, account_name)
    merged = Image.open(io.BytesIO(watermarked)).convert("RGB")
    output = io.BytesIO()
    merged.save(output, format="JPEG", quality=90, optimize=True, progressive=True)
    out_path.write_bytes(output.getvalue())
    return str(out_path)


def extract_result_image_url(payload: dict) -> str:
    candidates = []
    if isinstance(payload.get("images"), list):
        candidates.extend(payload.get("images") or [])
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("images"), list):
        candidates.extend(data.get("images") or [])

    for item in candidates:
        if isinstance(item, str) and item.startswith("http"):
            return item
        if isinstance(item, dict):
            for key in ("url", "image_url", "src"):
                value = str(item.get(key) or "").strip()
                if value.startswith("http"):
                    return value
    for key in ("image_url", "url"):
        value = str(payload.get(key) or "").strip()
        if value.startswith("http"):
            return value
    raise RuntimeError(f"图片结果里未找到可下载图片地址: {json.dumps(payload, ensure_ascii=False)}")


def wait_for_n1n_result(response_url: str, api_key: str) -> str:
    headers = {"Authorization": f"Bearer {api_key}"}
    last_payload = {}
    for _ in range(60):
        resp = http_get_with_retry(response_url, headers=headers, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"查询图片生成结果失败: {resp.text}")
        payload = resp.json()
        last_payload = payload
        status = str(payload.get("status") or "").upper()
        if status in ("COMPLETED", "SUCCEEDED", "SUCCESS"):
            return extract_result_image_url(payload)
        if status in ("FAILED", "ERROR", "CANCELLED"):
            raise RuntimeError(f"图片生成失败: {json.dumps(payload, ensure_ascii=False)}")
        import time
        time.sleep(2)
    raise RuntimeError(f"图片生成超时: {json.dumps(last_payload, ensure_ascii=False)}")


def download_remote_image(url: str) -> bytes:
    resp = http_get_with_retry(url, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"下载生成图片失败: {resp.text[:300]}")
    return resp.content


def call_nanobanana_image(image_input, prompt_text: str) -> bytes:
    api_key = get_image_api_key()
    if not api_key:
        raise ValueError("没有检测到 IMAGE_API_KEY，请先 export IMAGE_API_KEY")

    model_name = get_image_model()
    base_url = get_image_base_url()
    if isinstance(image_input, (list, tuple)):
        image_paths = [Path(item) for item in image_input if item]
    else:
        image_paths = [Path(image_input)]
    public_image_urls = build_public_image_urls(image_paths)
    if not public_image_urls:
        raise ValueError("缺少可用参考图")
    url = f"{base_url}/{model_name.lstrip('/')}"

    payload = {
        "prompt": prompt_text,
        "image_urls": public_image_urls,
        "num_images": 1,
    }

    resp = http_post_with_retry(
        url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        json=payload,
        max_retries=1,
        timeout=180,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"图片模型接口错误: {resp.text}")

    try:
        result = resp.json()
    except ValueError:
        body = (resp.text or "").strip()
        raise RuntimeError(
            f"图片模型返回非JSON响应: status={resp.status_code} | body={body[:1200]}"
        )
    response_url = str(result.get("response_url") or "").strip()
    if not response_url:
        raise RuntimeError(f"图片模型未返回 response_url: {json.dumps(result, ensure_ascii=False)}")
    final_image_url = wait_for_n1n_result(response_url, api_key)
    return download_remote_image(final_image_url)


def call_gemini_image_via_n1n(image_input, prompt_text: str, account_name: str = "") -> bytes:
    api_key = get_image_api_key()
    if not api_key:
        raise ValueError("没有检测到 IMAGE_API_KEY，请先 export IMAGE_API_KEY")

    model_name = get_image_model()
    base_url = get_image_base_url()
    if isinstance(image_input, (list, tuple)):
        image_paths = [Path(item) for item in image_input if item]
    else:
        image_paths = [Path(image_input)]
    if not image_paths:
        raise ValueError("缺少可用参考图")

    url = f"{base_url}/v1beta/models/{model_name}:generateContent"
    parts = [{"text": prompt_text}]
    for image_path in image_paths:
        image_bytes = image_path.read_bytes()
        encoded = base64.b64encode(image_bytes).decode("utf-8")
        mime_type = "image/png"
        suffix = image_path.suffix.lower()
        if suffix in (".jpg", ".jpeg"):
            mime_type = "image/jpeg"
        elif suffix == ".webp":
            mime_type = "image/webp"
        parts.append(
            {
                "inline_data": {
                    "mime_type": mime_type,
                    "data": encoded,
                }
            }
        )
    payload = {
        "contents": [
            {
                "parts": parts
            }
        ]
    }
    if use_2k_output_rules(account_name):
        payload["generationConfig"] = {
            "imageConfig": {
                "aspectRatio": "1:1",
                "imageSize": "2K",
            }
        }

    resp = http_post_with_retry(
        url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        json=payload,
        timeout=180,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"图片模型接口错误: {resp.text}")

    try:
        result = resp.json()
    except ValueError:
        body = (resp.text or "").strip()
        raise RuntimeError(f"图片模型返回非JSON响应: status={resp.status_code} | body={body[:1200]}")

    candidates = result.get("candidates") or []
    if not candidates:
        raise RuntimeError(f"图片模型返回为空: {json.dumps(result, ensure_ascii=False)}")

    for cand in candidates:
        parts = (cand.get("content") or {}).get("parts") or []
        for part in parts:
            inline = part.get("inlineData") or part.get("inline_data")
            if inline and inline.get("data"):
                return base64.b64decode(inline["data"])

    raise RuntimeError(f"图片模型未返回图片数据: {json.dumps(result, ensure_ascii=False)}")


def call_openai_compatible_image(image_input, prompt_text: str) -> bytes:
    api_key = get_image_api_key()
    if not api_key:
        raise ValueError("没有检测到 IMAGE_API_KEY，请先 export IMAGE_API_KEY")

    model_name = get_image_model()
    base_url = get_image_base_url().rstrip("/")
    url = f"{base_url}/v1/images/edits"
    provider = get_image_provider()
    if isinstance(image_input, (list, tuple)):
        image_paths = [Path(item) for item in image_input if item]
    else:
        image_paths = [Path(image_input)]
    if not image_paths:
        raise ValueError("缺少可用参考图")

    files = []
    opened_files = []
    try:
        for idx, image_path in enumerate(image_paths):
            suffix = image_path.suffix.lower()
            mime_type = "image/png"
            if suffix in (".jpg", ".jpeg"):
                mime_type = "image/jpeg"
            elif suffix == ".webp":
                mime_type = "image/webp"
            image_file = image_path.open("rb")
            opened_files.append(image_file)
            field_name = "image[]" if len(image_paths) > 1 else "image"
            files.append((field_name, (image_path.name, image_file, mime_type)))

        data = {
            "model": model_name,
            "prompt": prompt_text,
            "n": "1",
        }
        if provider in ("flux", "flux_edit") or model_name.startswith("flux"):
            data["aspect_ratio"] = "1:1"
        else:
            data["size"] = "1024x1024"
        resp = http_post_with_retry(
            url,
            headers={
                "Authorization": f"Bearer {api_key}",
            },
            data=data,
            files=files,
            max_retries=1,
            timeout=180,
        )
    finally:
        for file_obj in opened_files:
            try:
                file_obj.close()
            except Exception:
                pass

    if resp.status_code != 200:
        raise RuntimeError(f"图片模型接口错误: {resp.text}")

    try:
        result = resp.json()
    except ValueError:
        body = (resp.text or "").strip()
        raise RuntimeError(f"图片模型返回非JSON响应: status={resp.status_code} | body={body[:1200]}")

    data_items = result.get("data") or []
    if not data_items:
        raise RuntimeError(f"图片模型返回为空: {json.dumps(result, ensure_ascii=False)}")

    first = data_items[0] or {}
    b64_json = str(first.get("b64_json") or "").strip()
    if b64_json:
        return base64.b64decode(b64_json)

    image_url = str(first.get("url") or "").strip()
    if image_url.startswith("http"):
        return download_remote_image(image_url)

    raise RuntimeError(f"图片模型未返回图片数据: {json.dumps(result, ensure_ascii=False)}")


def generate_ai_main_image(product_id: int, force: bool = True, account_name: str = "", channel: str = "xianyu") -> dict:
    ensure_ai_image_table()
    row = load_product(product_id)
    if not row:
        raise ValueError(f"找不到商品: {product_id}")

    source_image_path = str(row["local_image_path"] or "").strip()
    if not source_image_path:
        raise ValueError("当前商品没有本地图片，请先手动替换成高清图后再生成")

    original_source_path = Path(source_image_path)
    if not original_source_path.exists():
        raise ValueError("当前商品本地图片不存在，请先确认图片路径")
    source_path = build_preprocessed_input_image(original_source_path, str(row["category"] or ""), product_id)

    if not force:
        existing = load_existing_ai_image(product_id, account_name=account_name, asset_type="main")
        if existing and str(existing["ai_main_image_path"] or "").strip():
            ai_path = Path(str(existing["ai_main_image_path"]))
            if ai_path.exists():
                return {
                    "product_id": product_id,
                    "ai_main_image_path": str(ai_path),
                    "source_image_path": source_image_path,
                    "skipped": True,
                }

    provider = get_image_provider()
    if provider not in ("nanobanana", "n1n", "n1n_nanobanana", "gemini_image", "openai", "openai_image", "openai_compatible", "flux", "flux_edit"):
        raise ValueError(f"暂不支持的图片生成 provider: {provider}")

    existing_images = list_ai_images(product_id, account_name=account_name, asset_type="main")
    row_data = dict(row)
    row_data["account_name"] = account_name
    row_data["image_channel"] = normalize_image_channel(channel)
    row_data.update(load_product_ai_marketing(product_id, account_name=account_name))
    AI_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_-]+", "_", str(row['name'] or "").strip())[:80].strip("_") or f"product_{product_id}"
    generate_count = 5 if not account_name else 1
    output_path = None
    oss_url = ""
    for offset in range(generate_count):
        variant_index = len(existing_images) + offset
        prompt_text = build_image_prompt(row_data, variant_index=variant_index, asset_type="main", channel=channel)
        model_name = get_image_model().strip().lower()
        if provider in ("openai", "openai_image", "openai_compatible", "flux", "flux_edit") or model_name.startswith("gpt-image") or model_name.startswith("flux"):
            image_bytes = call_openai_compatible_image(source_path, prompt_text)
        elif model_name.startswith("fal-ai/"):
            image_bytes = call_nanobanana_image(source_path, prompt_text)
        elif model_name.startswith("gemini-"):
            image_bytes = call_gemini_image_via_n1n(source_path, prompt_text, account_name=account_name)
        else:
            raise ValueError(f"暂不支持的图片模型路由: {get_image_model()}")

        output_path = AI_IMAGE_DIR / f"{product_id}_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{offset + 1}.png"
        output_path.write_bytes(image_bytes)
        oss_url = ""
        if is_oss_configured():
            try:
                oss_url = upload_local_file_to_oss(str(output_path))
            except Exception:
                oss_url = ""
        save_ai_image(
            product_id=product_id,
            ai_main_image_path=str(output_path),
            oss_url=oss_url,
            source_image_path=source_image_path,
            provider=provider,
            model_name=get_image_model(),
            prompt_text=prompt_text,
            account_name=account_name,
            asset_type="main",
        )
    return {
        "product_id": product_id,
        "ai_main_image_path": str(output_path or ""),
        "oss_url": oss_url,
        "source_image_path": source_image_path,
        "skipped": False,
    }


def generate_ai_detail_image(product_id: int, force: bool = True, account_name: str = "", channel: str = "xianyu") -> dict:
    ensure_ai_image_table()
    row = load_product(product_id)
    if not row:
        raise ValueError(f"找不到商品: {product_id}")

    source_image_path = str(row["local_image_path"] or "").strip()
    if not source_image_path:
        raise ValueError("当前商品没有本地图片，请先手动替换成高清图后再生成")

    original_source_path = Path(source_image_path)
    if not original_source_path.exists():
        raise ValueError("当前商品本地图片不存在，请先确认图片路径")
    source_path = build_preprocessed_input_image(original_source_path, str(row["category"] or ""), product_id)

    if not force:
        existing = load_existing_ai_image(product_id, account_name=account_name, asset_type="detail")
        if existing and str(existing["ai_main_image_path"] or "").strip():
            ai_path = Path(str(existing["ai_main_image_path"]))
            if ai_path.exists():
                return {
                    "product_id": product_id,
                    "ai_main_image_path": str(ai_path),
                    "source_image_path": source_image_path,
                    "skipped": True,
                }

    provider = get_image_provider()
    if provider not in ("nanobanana", "n1n", "n1n_nanobanana", "gemini_image", "openai", "openai_image", "openai_compatible", "flux", "flux_edit"):
        raise ValueError(f"暂不支持的图片生成 provider: {provider}")

    existing_images = list_ai_images(product_id, account_name=account_name, asset_type="detail")
    variant_index = len(existing_images) + random.randint(0, 9999)
    row_data = dict(row)
    row_data["account_name"] = account_name
    row_data["image_channel"] = normalize_image_channel(channel)
    row_data.update(load_product_ai_marketing(product_id, account_name=account_name))
    prompt_text = build_image_prompt(row_data, variant_index=variant_index, asset_type="detail", channel=channel)
    model_name = get_image_model().strip().lower()
    if provider in ("openai", "openai_image", "openai_compatible", "flux", "flux_edit") or model_name.startswith("gpt-image") or model_name.startswith("flux"):
        image_bytes = call_openai_compatible_image(source_path, prompt_text)
    elif model_name.startswith("fal-ai/"):
        image_bytes = call_nanobanana_image(source_path, prompt_text)
    elif model_name.startswith("gemini-"):
        image_bytes = call_gemini_image_via_n1n(source_path, prompt_text, account_name=account_name)
    else:
        raise ValueError(f"暂不支持的图片模型路由: {get_image_model()}")

    AI_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_-]+", "_", str(row['name'] or "").strip())[:80].strip("_") or f"product_{product_id}"
    output_path = AI_IMAGE_DIR / f"{product_id}_{safe_name}_detail_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    output_path.write_bytes(image_bytes)
    oss_url = ""
    if is_oss_configured():
        try:
            oss_url = upload_local_file_to_oss(str(output_path))
        except Exception:
            oss_url = ""

    save_ai_image(
        product_id=product_id,
        ai_main_image_path=str(output_path),
        oss_url=oss_url,
        source_image_path=source_image_path,
        provider=provider,
        model_name=get_image_model(),
        prompt_text=prompt_text,
        account_name=account_name,
        asset_type="detail",
    )
    return {
        "product_id": product_id,
        "ai_main_image_path": str(output_path),
        "oss_url": oss_url,
        "source_image_path": source_image_path,
        "skipped": False,
    }
