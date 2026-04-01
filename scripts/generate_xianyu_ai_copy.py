import argparse
import re
import sqlite3
import time

from services.material_ai_service import (
    build_taobao_title_bundle,
    build_taobao_main_image_bundle,
    build_xianyu_copy,
    build_xianyu_description,
    build_xianyu_descriptions_batch,
    build_xianyu_title,
    build_xianyu_titles_batch,
)


DB_PATH = "products.db"
TITLE_BATCH_SIZE = 60
DESCRIPTION_BATCH_SIZE = 25


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_ai_copy_table():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_product_ai_copy (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL UNIQUE,
            ai_title TEXT,
            ai_description TEXT,
            ai_main_image_plan TEXT,
            ai_main_image_model_text TEXT,
            ai_taobao_title TEXT,
            ai_taobao_guide_title TEXT,
            ai_target_audience TEXT,
            ai_style_positioning TEXT,
            ai_flex_feel TEXT,
            ai_board_profile TEXT,
            ai_performance_feel TEXT,
            ai_terrain_focus TEXT,
            ai_skill_level TEXT,
            source TEXT DEFAULT 'gemini',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(xianyu_product_ai_copy)").fetchall()]
    if "ai_main_image_plan" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_main_image_plan TEXT")
    if "ai_main_image_model_text" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_main_image_model_text TEXT")
    if "ai_taobao_title" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_taobao_title TEXT")
    if "ai_taobao_guide_title" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_taobao_guide_title TEXT")
    if "ai_target_audience" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_target_audience TEXT")
    if "ai_style_positioning" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_style_positioning TEXT")
    if "ai_flex_feel" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_flex_feel TEXT")
    if "ai_board_profile" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_board_profile TEXT")
    if "ai_performance_feel" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_performance_feel TEXT")
    if "ai_terrain_focus" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_terrain_focus TEXT")
    if "ai_skill_level" not in columns:
        conn.execute("ALTER TABLE xianyu_product_ai_copy ADD COLUMN ai_skill_level TEXT")
    conn.commit()
    conn.close()


def ensure_account_ai_copy_support():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS xianyu_account_product_ai_copy (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            ai_title TEXT,
            ai_description TEXT,
            ai_main_image_plan TEXT,
            ai_main_image_model_text TEXT,
            ai_taobao_title TEXT,
            ai_taobao_guide_title TEXT,
            ai_target_audience TEXT,
            ai_style_positioning TEXT,
            ai_flex_feel TEXT,
            ai_board_profile TEXT,
            ai_performance_feel TEXT,
            ai_terrain_focus TEXT,
            ai_skill_level TEXT,
            source TEXT DEFAULT 'gemini',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_id, account_name)
        )
    """)
    table_columns = [row["name"] for row in conn.execute("PRAGMA table_info(xianyu_account_product_ai_copy)").fetchall()]
    if "ai_main_image_plan" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_main_image_plan TEXT")
    if "ai_main_image_model_text" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_main_image_model_text TEXT")
    if "ai_taobao_title" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_taobao_title TEXT")
    if "ai_taobao_guide_title" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_taobao_guide_title TEXT")
    if "ai_target_audience" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_target_audience TEXT")
    if "ai_style_positioning" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_style_positioning TEXT")
    if "ai_flex_feel" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_flex_feel TEXT")
    if "ai_board_profile" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_board_profile TEXT")
    if "ai_performance_feel" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_performance_feel TEXT")
    if "ai_terrain_focus" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_terrain_focus TEXT")
    if "ai_skill_level" not in table_columns:
        conn.execute("ALTER TABLE xianyu_account_product_ai_copy ADD COLUMN ai_skill_level TEXT")
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(xianyu_accounts)").fetchall()]
    if "independent_ai_assets" not in columns:
        conn.execute("ALTER TABLE xianyu_accounts ADD COLUMN independent_ai_assets INTEGER DEFAULT 0")
    conn.commit()
    conn.close()


def is_account_independent_ai(account_name: str) -> bool:
    account_name = str(account_name or "").strip()
    if not account_name:
        return False
    ensure_account_ai_copy_support()
    conn = get_conn()
    row = conn.execute("""
        SELECT COALESCE(independent_ai_assets, 0) AS independent_ai_assets
        FROM xianyu_accounts
        WHERE account_name = ?
        LIMIT 1
    """, (account_name,)).fetchone()
    conn.close()
    return bool(int(row["independent_ai_assets"] or 0)) if row else False


def load_tasks(batch_id: int, force: bool = False):
    conn = get_conn()
    where_ai = ""
    if not force:
        where_ai = """
          AND (
                TRIM(COALESCE(t.ai_title, '')) = ''
             OR TRIM(COALESCE(t.ai_description, '')) = ''
          )
        """
    rows = conn.execute(f"""
        SELECT
            t.id AS task_id,
            t.product_id,
            a.account_name,
            p.name,
            p.category,
            u.final_price_cny,
            u.stock
        FROM xianyu_publish_tasks t
        LEFT JOIN xianyu_accounts a
          ON a.id = t.account_id
        JOIN products p
          ON p.id = t.product_id
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        WHERE t.batch_id = ?
        {where_ai}
        ORDER BY t.id
    """, (batch_id,)).fetchall()
    conn.close()
    return rows


def load_product_attributes(product_id: int) -> dict:
    conn = get_conn()
    rows = conn.execute("""
        SELECT property_name, value_name
        FROM xianyu_product_property_values
        WHERE product_id = ?
          AND TRIM(COALESCE(value_name, '')) != ''
        ORDER BY id
    """, (product_id,)).fetchall()
    conn.close()
    result = {}
    for row in rows:
        key = str(row["property_name"] or "").strip()
        value = str(row["value_name"] or "").strip()
        if key and value and key not in result:
            result[key] = value
    return result


def save_ai_copy(task_id: int, title: str | None = None, description: str | None = None):
    conn = get_conn()
    sets = []
    params = []
    if title is not None:
        sets.append("ai_title = ?")
        params.append(title)
    if description is not None:
        sets.append("ai_description = ?")
        params.append(description)
    sets.append("updated_at = CURRENT_TIMESTAMP")
    params.append(task_id)
    conn.execute(f"""
        UPDATE xianyu_publish_tasks
        SET {", ".join(sets)}
        WHERE id = ?
    """, params)
    conn.commit()
    conn.close()


def derive_main_image_model_text(product_name: str, ai_title: str = "") -> str:
    title_text = str(ai_title or "").strip()
    name_text = str(product_name or "").strip()
    text = title_text or name_text
    if not text:
        return ""
    text = re.sub(r"\b\d{2}/\d{2}\b", " ", text, flags=re.I)
    text = re.sub(r"\b\d{4}\b", " ", text, flags=re.I)
    text = re.sub(r"\b(?:SNOWBOARD|BOARD|BINDING|BINDINGS|BOOTS?)\b", " ", text, flags=re.I)
    tokens = [tok for tok in re.split(r"\s+", text) if tok.strip()]
    if not tokens:
        return ""
    brand = tokens[0]
    body = tokens[1:] if len(tokens) > 1 else tokens
    stop_words = {
        "单板滑雪板", "单板", "滑雪板", "全新", "通用", "女款", "男款", "女士", "男子",
        "双板", "滑雪", "雪板", "wide", "w", "unisex",
    }
    body = [tok for tok in body if tok.lower() != brand.lower() and tok.lower() not in {w.lower() for w in stop_words}]
    if not body:
        body = [tok for tok in tokens if tok.lower() != brand.lower()]
    return " ".join(body[:3]).strip()


def save_product_ai_copy(product_id: int, title: str | None = None, description: str | None = None, main_image_plan: str | None = None, main_image_model_text: str | None = None, taobao_title: str | None = None, taobao_guide_title: str | None = None, target_audience: str | None = None, style_positioning: str | None = None, flex_feel: str | None = None, board_profile: str | None = None, performance_feel: str | None = None, terrain_focus: str | None = None, skill_level: str | None = None, account_name: str = ""):
    ensure_ai_copy_table()
    ensure_account_ai_copy_support()
    if is_account_independent_ai(account_name):
        conn = get_conn()
        current = conn.execute("""
            SELECT ai_title, ai_description, ai_main_image_plan, ai_main_image_model_text, ai_taobao_title, ai_taobao_guide_title, ai_target_audience, ai_style_positioning, ai_flex_feel, ai_board_profile, ai_performance_feel, ai_terrain_focus, ai_skill_level
            FROM xianyu_account_product_ai_copy
            WHERE product_id = ? AND account_name = ?
            LIMIT 1
        """, (product_id, account_name)).fetchone()
        final_title = title if title is not None else (str(current["ai_title"] or "").strip() if current else "")
        final_description = description if description is not None else (str(current["ai_description"] or "").strip() if current else "")
        final_main_image_plan = main_image_plan if main_image_plan is not None else (str(current["ai_main_image_plan"] or "").strip() if current else "")
        final_main_image_model_text = main_image_model_text if main_image_model_text is not None else (str(current["ai_main_image_model_text"] or "").strip() if current else "")
        final_taobao_title = taobao_title if taobao_title is not None else (str(current["ai_taobao_title"] or "").strip() if current else "")
        final_taobao_guide_title = taobao_guide_title if taobao_guide_title is not None else (str(current["ai_taobao_guide_title"] or "").strip() if current else "")
        final_target_audience = target_audience if target_audience is not None else (str(current["ai_target_audience"] or "").strip() if current else "")
        final_style_positioning = style_positioning if style_positioning is not None else (str(current["ai_style_positioning"] or "").strip() if current else "")
        final_flex_feel = flex_feel if flex_feel is not None else (str(current["ai_flex_feel"] or "").strip() if current else "")
        final_board_profile = board_profile if board_profile is not None else (str(current["ai_board_profile"] or "").strip() if current else "")
        final_performance_feel = performance_feel if performance_feel is not None else (str(current["ai_performance_feel"] or "").strip() if current else "")
        final_terrain_focus = terrain_focus if terrain_focus is not None else (str(current["ai_terrain_focus"] or "").strip() if current else "")
        final_skill_level = skill_level if skill_level is not None else (str(current["ai_skill_level"] or "").strip() if current else "")
        conn.execute("""
            INSERT INTO xianyu_account_product_ai_copy (product_id, account_name, ai_title, ai_description, ai_main_image_plan, ai_main_image_model_text, ai_taobao_title, ai_taobao_guide_title, ai_target_audience, ai_style_positioning, ai_flex_feel, ai_board_profile, ai_performance_feel, ai_terrain_focus, ai_skill_level, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'gemini', CURRENT_TIMESTAMP)
            ON CONFLICT(product_id, account_name) DO UPDATE SET
                ai_title = excluded.ai_title,
                ai_description = excluded.ai_description,
                ai_main_image_plan = excluded.ai_main_image_plan,
                ai_main_image_model_text = excluded.ai_main_image_model_text,
                ai_taobao_title = excluded.ai_taobao_title,
                ai_taobao_guide_title = excluded.ai_taobao_guide_title,
                ai_target_audience = excluded.ai_target_audience,
                ai_style_positioning = excluded.ai_style_positioning,
                ai_flex_feel = excluded.ai_flex_feel,
                ai_board_profile = excluded.ai_board_profile,
                ai_performance_feel = excluded.ai_performance_feel,
                ai_terrain_focus = excluded.ai_terrain_focus,
                ai_skill_level = excluded.ai_skill_level,
                source = excluded.source,
                updated_at = CURRENT_TIMESTAMP
        """, (product_id, account_name, final_title, final_description, final_main_image_plan, final_main_image_model_text, final_taobao_title, final_taobao_guide_title, final_target_audience, final_style_positioning, final_flex_feel, final_board_profile, final_performance_feel, final_terrain_focus, final_skill_level))
        conn.commit()
        conn.close()
        return
    conn = get_conn()
    current = conn.execute("""
        SELECT ai_title, ai_description, ai_main_image_plan, ai_main_image_model_text, ai_taobao_title, ai_taobao_guide_title, ai_target_audience, ai_style_positioning, ai_flex_feel, ai_board_profile, ai_performance_feel, ai_terrain_focus, ai_skill_level
        FROM xianyu_product_ai_copy
        WHERE product_id = ?
        LIMIT 1
    """, (product_id,)).fetchone()
    final_title = title if title is not None else (str(current["ai_title"] or "").strip() if current else "")
    final_description = description if description is not None else (str(current["ai_description"] or "").strip() if current else "")
    final_main_image_plan = main_image_plan if main_image_plan is not None else (str(current["ai_main_image_plan"] or "").strip() if current else "")
    final_main_image_model_text = main_image_model_text if main_image_model_text is not None else (str(current["ai_main_image_model_text"] or "").strip() if current else "")
    final_taobao_title = taobao_title if taobao_title is not None else (str(current["ai_taobao_title"] or "").strip() if current else "")
    final_taobao_guide_title = taobao_guide_title if taobao_guide_title is not None else (str(current["ai_taobao_guide_title"] or "").strip() if current else "")
    final_target_audience = target_audience if target_audience is not None else (str(current["ai_target_audience"] or "").strip() if current else "")
    final_style_positioning = style_positioning if style_positioning is not None else (str(current["ai_style_positioning"] or "").strip() if current else "")
    final_flex_feel = flex_feel if flex_feel is not None else (str(current["ai_flex_feel"] or "").strip() if current else "")
    final_board_profile = board_profile if board_profile is not None else (str(current["ai_board_profile"] or "").strip() if current else "")
    final_performance_feel = performance_feel if performance_feel is not None else (str(current["ai_performance_feel"] or "").strip() if current else "")
    final_terrain_focus = terrain_focus if terrain_focus is not None else (str(current["ai_terrain_focus"] or "").strip() if current else "")
    final_skill_level = skill_level if skill_level is not None else (str(current["ai_skill_level"] or "").strip() if current else "")
    conn.execute("""
        INSERT INTO xianyu_product_ai_copy (product_id, ai_title, ai_description, ai_main_image_plan, ai_main_image_model_text, ai_taobao_title, ai_taobao_guide_title, ai_target_audience, ai_style_positioning, ai_flex_feel, ai_board_profile, ai_performance_feel, ai_terrain_focus, ai_skill_level, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'gemini', CURRENT_TIMESTAMP)
        ON CONFLICT(product_id) DO UPDATE SET
            ai_title = excluded.ai_title,
            ai_description = excluded.ai_description,
            ai_main_image_plan = excluded.ai_main_image_plan,
            ai_main_image_model_text = excluded.ai_main_image_model_text,
            ai_taobao_title = excluded.ai_taobao_title,
            ai_taobao_guide_title = excluded.ai_taobao_guide_title,
            ai_target_audience = excluded.ai_target_audience,
            ai_style_positioning = excluded.ai_style_positioning,
            ai_flex_feel = excluded.ai_flex_feel,
            ai_board_profile = excluded.ai_board_profile,
            ai_performance_feel = excluded.ai_performance_feel,
            ai_terrain_focus = excluded.ai_terrain_focus,
            ai_skill_level = excluded.ai_skill_level,
            source = excluded.source,
            updated_at = CURRENT_TIMESTAMP
    """, (product_id, final_title, final_description, final_main_image_plan, final_main_image_model_text, final_taobao_title, final_taobao_guide_title, final_target_audience, final_style_positioning, final_flex_feel, final_board_profile, final_performance_feel, final_terrain_focus, final_skill_level))
    conn.commit()
    conn.close()


def load_product(product_id: int):
    conn = get_conn()
    row = conn.execute("""
        SELECT
            p.id AS product_id,
            p.name,
            p.category,
            u.final_price_cny,
            u.stock
        FROM products p
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        WHERE p.id = ?
        LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return row


def load_products(product_ids: list[int]):
    if not product_ids:
        return []
    conn = get_conn()
    placeholders = ",".join("?" for _ in product_ids)
    rows = conn.execute(f"""
        SELECT
            p.id AS product_id,
            p.name,
            p.category,
            u.final_price_cny,
            u.stock
        FROM products p
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        WHERE p.id IN ({placeholders})
    """, product_ids).fetchall()
    conn.close()
    return rows


def load_existing_product_ai(product_id: int, account_name: str = ""):
    ensure_ai_copy_table()
    ensure_account_ai_copy_support()
    if is_account_independent_ai(account_name):
        conn = get_conn()
        row = conn.execute("""
            SELECT ai_title, ai_description, ai_main_image_plan, ai_main_image_model_text, ai_taobao_title, ai_taobao_guide_title, ai_target_audience, ai_style_positioning, ai_flex_feel
            FROM xianyu_account_product_ai_copy
            WHERE product_id = ? AND account_name = ?
            LIMIT 1
        """, (product_id, account_name)).fetchone()
        conn.close()
        return row
    conn = get_conn()
    row = conn.execute("""
        SELECT ai_title, ai_description, ai_main_image_plan, ai_main_image_model_text, ai_taobao_title, ai_taobao_guide_title, ai_target_audience, ai_style_positioning, ai_flex_feel
        FROM xianyu_product_ai_copy
        WHERE product_id = ?
        LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return row


def generate_for_product(product_id: int, force: bool = False, account_name: str = "") -> dict:
    ensure_ai_copy_table()
    product_row = load_product(product_id)
    if not product_row:
        raise ValueError(f"找不到商品: {product_id}")

    if not force:
        existing = load_existing_product_ai(product_id, account_name=account_name)
        if existing and (str(existing["ai_title"] or "").strip() and str(existing["ai_description"] or "").strip()):
            return {
                "product_id": product_id,
                "success_count": 1,
                "failed_count": 0,
                "ai_title": str(existing["ai_title"] or "").strip(),
                "ai_description": str(existing["ai_description"] or "").strip(),
                "skipped": True,
            }

    product = {
        "product_id": int(product_row["product_id"]),
        "name": product_row["name"] or "",
        "category": product_row["category"] or "",
        "final_price_cny": product_row["final_price_cny"] or "",
        "stock": product_row["stock"] or "",
        "attributes": load_product_attributes(int(product_row["product_id"])),
    }
    result = build_xianyu_copy(product)
    title = (result.get("title") or "").strip()
    description = (result.get("description") or "").strip()
    if not title or not description:
        raise ValueError("AI 返回的标题或描述为空")
    main_image_bundle = build_taobao_main_image_bundle(product, title=title) or {}
    main_image_model_text = str(main_image_bundle.get("main_image_model_text") or "").strip()
    target_audience = str(main_image_bundle.get("target_audience") or "").strip()
    style_positioning = str(main_image_bundle.get("style_positioning") or "").strip()
    flex_feel = str(main_image_bundle.get("flex_feel") or "").strip()
    board_profile = str(main_image_bundle.get("board_profile") or "").strip()
    performance_feel = str(main_image_bundle.get("performance_feel") or "").strip()
    terrain_focus = str(main_image_bundle.get("terrain_focus") or "").strip()
    skill_level = str(main_image_bundle.get("skill_level") or "").strip()
    if not main_image_model_text:
        main_image_model_text = derive_main_image_model_text(product.get("name") or "", title)
    save_product_ai_copy(product_id, title, description, main_image_plan=None, main_image_model_text=main_image_model_text, target_audience=target_audience, style_positioning=style_positioning, flex_feel=flex_feel, board_profile=board_profile, performance_feel=performance_feel, terrain_focus=terrain_focus, skill_level=skill_level, account_name=account_name)
    return {
        "product_id": product_id,
        "success_count": 1,
        "failed_count": 0,
        "ai_title": title,
        "ai_description": description,
        "ai_main_image_model_text": main_image_model_text,
        "ai_target_audience": target_audience,
        "ai_style_positioning": style_positioning,
        "ai_flex_feel": flex_feel,
        "ai_board_profile": board_profile,
        "ai_performance_feel": performance_feel,
        "ai_terrain_focus": terrain_focus,
        "ai_skill_level": skill_level,
        "skipped": False,
    }


def generate_title_for_product(product_id: int, force: bool = True, account_name: str = "") -> dict:
    ensure_ai_copy_table()
    product_row = load_product(product_id)
    if not product_row:
        raise ValueError(f"找不到商品: {product_id}")
    product = {
        "product_id": int(product_row["product_id"]),
        "name": product_row["name"] or "",
        "category": product_row["category"] or "",
        "final_price_cny": product_row["final_price_cny"] or "",
        "stock": product_row["stock"] or "",
        "attributes": load_product_attributes(int(product_row["product_id"])),
    }
    if account_name:
        title = build_xianyu_title(product).strip()
        if not title:
            raise ValueError("AI 返回的标题为空")
        taobao_title = ""
        taobao_guide_title = ""
    else:
        taobao_bundle = build_taobao_title_bundle(product) or {}
        taobao_title = str(taobao_bundle.get("taobao_title") or "").strip()
        taobao_guide_title = str(taobao_bundle.get("taobao_guide_title") or "").strip()
        if not taobao_title:
            raise ValueError("AI 返回的淘宝商品标题为空")
        title = taobao_title
    main_image_bundle = build_taobao_main_image_bundle(product, title=title) or {}
    main_image_model_text = str(main_image_bundle.get("main_image_model_text") or "").strip()
    target_audience = str(main_image_bundle.get("target_audience") or "").strip()
    style_positioning = str(main_image_bundle.get("style_positioning") or "").strip()
    flex_feel = str(main_image_bundle.get("flex_feel") or "").strip()
    board_profile = str(main_image_bundle.get("board_profile") or "").strip()
    performance_feel = str(main_image_bundle.get("performance_feel") or "").strip()
    terrain_focus = str(main_image_bundle.get("terrain_focus") or "").strip()
    skill_level = str(main_image_bundle.get("skill_level") or "").strip()
    if not main_image_model_text:
        main_image_model_text = derive_main_image_model_text(product.get("name") or "", title)
    save_product_ai_copy(product_id, title=title if account_name else None, description=None, main_image_plan=None, main_image_model_text=main_image_model_text, taobao_title=taobao_title if not account_name else None, taobao_guide_title=taobao_guide_title if not account_name else None, target_audience=target_audience, style_positioning=style_positioning, flex_feel=flex_feel, board_profile=board_profile, performance_feel=performance_feel, terrain_focus=terrain_focus, skill_level=skill_level, account_name=account_name)
    return {
        "product_id": product_id,
        "ai_title": title if account_name else "",
        "ai_taobao_title": taobao_title if not account_name else "",
        "ai_taobao_guide_title": taobao_guide_title if not account_name else "",
        "ai_main_image_model_text": main_image_model_text,
        "ai_target_audience": target_audience,
        "ai_style_positioning": style_positioning,
        "ai_flex_feel": flex_feel,
        "ai_board_profile": board_profile,
        "ai_performance_feel": performance_feel,
        "ai_terrain_focus": terrain_focus,
        "ai_skill_level": skill_level,
    }


def generate_description_for_product(product_id: int, force: bool = True, account_name: str = "") -> dict:
    ensure_ai_copy_table()
    product_row = load_product(product_id)
    if not product_row:
        raise ValueError(f"找不到商品: {product_id}")
    product = {
        "product_id": int(product_row["product_id"]),
        "name": product_row["name"] or "",
        "category": product_row["category"] or "",
        "final_price_cny": product_row["final_price_cny"] or "",
        "stock": product_row["stock"] or "",
        "attributes": load_product_attributes(int(product_row["product_id"])),
    }
    description = build_xianyu_description(product).strip()
    if not description:
        raise ValueError("AI 返回的简介为空")
    save_product_ai_copy(product_id, title=None, description=description, account_name=account_name)
    return {"product_id": product_id, "ai_description": description}


def generate_titles_for_products(product_ids: list[int], force: bool = False, account_name: str = "") -> dict:
    ensure_ai_copy_table()
    if not account_name:
        results = []
        failures = []
        skipped = []
        for product_id in product_ids:
            existing = load_existing_product_ai(int(product_id), account_name=account_name)
            if not force and existing and str(existing["ai_taobao_title"] or "").strip():
                skipped.append(int(product_id))
                continue
            try:
                results.append(generate_title_for_product(int(product_id), force=True, account_name=account_name))
            except Exception as e:
                failures.append({"product_id": int(product_id), "error": str(e)})
        return {
            "total_count": len(product_ids),
            "success_count": len(results),
            "failed_count": len(failures),
            "skipped_count": len(skipped),
            "successes": results,
            "failures": failures,
            "skipped": skipped,
        }
    rows = load_products(product_ids)
    rows_by_id = {int(row["product_id"]): row for row in rows}
    targets = []
    skipped = []
    for product_id in product_ids:
        row = rows_by_id.get(int(product_id))
        if not row:
            continue
        existing = load_existing_product_ai(int(product_id), account_name=account_name)
        if not force and existing and str(existing["ai_title"] or "").strip():
            skipped.append(int(product_id))
            continue
        targets.append({
            "product_id": int(row["product_id"]),
            "name": row["name"] or "",
            "category": row["category"] or "",
            "final_price_cny": row["final_price_cny"] or "",
            "stock": row["stock"] or "",
            "attributes": load_product_attributes(int(row["product_id"])),
        })
    success_count = 0
    failures = []
    if not targets:
        return {"total_count": len(product_ids), "success_count": 0, "failed_count": 0, "skipped_count": len(skipped), "failures": []}
    for start in range(0, len(targets), TITLE_BATCH_SIZE):
        chunk = targets[start:start + TITLE_BATCH_SIZE]
        titles = build_xianyu_titles_batch(chunk)
        for product in chunk:
            product_id = int(product["product_id"])
            title = str(titles.get(product_id) or "").strip()
            if not title:
                failures.append({"product_id": product_id, "error": "批量AI未返回标题"})
                continue
            save_product_ai_copy(product_id, title=title, description=None, account_name=account_name)
            success_count += 1
    return {"total_count": len(product_ids), "success_count": success_count, "failed_count": len(failures), "skipped_count": len(skipped), "failures": failures}


def generate_descriptions_for_products(product_ids: list[int], force: bool = False, account_name: str = "") -> dict:
    ensure_ai_copy_table()
    rows = load_products(product_ids)
    rows_by_id = {int(row["product_id"]): row for row in rows}
    targets = []
    skipped = []
    for product_id in product_ids:
        row = rows_by_id.get(int(product_id))
        if not row:
            continue
        existing = load_existing_product_ai(int(product_id), account_name=account_name)
        if not force and existing and str(existing["ai_description"] or "").strip():
            skipped.append(int(product_id))
            continue
        targets.append({
            "product_id": int(row["product_id"]),
            "name": row["name"] or "",
            "category": row["category"] or "",
            "final_price_cny": row["final_price_cny"] or "",
            "stock": row["stock"] or "",
            "attributes": load_product_attributes(int(row["product_id"])),
        })
    success_count = 0
    failures = []
    if not targets:
        return {"total_count": len(product_ids), "success_count": 0, "failed_count": 0, "skipped_count": len(skipped), "failures": []}
    for start in range(0, len(targets), DESCRIPTION_BATCH_SIZE):
        chunk = targets[start:start + DESCRIPTION_BATCH_SIZE]
        descriptions = build_xianyu_descriptions_batch(chunk)
        for product in chunk:
            product_id = int(product["product_id"])
            description = str(descriptions.get(product_id) or "").strip()
            if not description:
                failures.append({"product_id": product_id, "error": "批量AI未返回简介"})
                continue
            save_product_ai_copy(product_id, title=None, description=description, account_name=account_name)
            success_count += 1
    return {"total_count": len(product_ids), "success_count": success_count, "failed_count": len(failures), "skipped_count": len(skipped), "failures": failures}


def load_task(task_id: int):
    conn = get_conn()
    row = conn.execute("""
        SELECT
            t.id AS task_id,
            t.product_id,
            a.account_name,
            p.name,
            p.category,
            u.final_price_cny,
            u.stock
        FROM xianyu_publish_tasks t
        LEFT JOIN xianyu_accounts a
          ON a.id = t.account_id
        JOIN products p
          ON p.id = t.product_id
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        WHERE t.id = ?
        LIMIT 1
    """, (task_id,)).fetchone()
    conn.close()
    return row


def generate_title_for_task(task_id: int) -> dict:
    row = load_task(task_id)
    if not row:
        raise ValueError(f"找不到任务: {task_id}")
    product = {
        "task_id": int(row["task_id"]),
        "product_id": int(row["product_id"]),
        "name": row["name"] or "",
        "category": row["category"] or "",
        "final_price_cny": row["final_price_cny"] or "",
        "stock": row["stock"] or "",
        "attributes": load_product_attributes(int(row["product_id"])),
    }
    title = build_xianyu_title(product).strip()
    if not title:
        raise ValueError("AI 返回的标题为空")
    save_ai_copy(task_id, title=title)
    save_product_ai_copy(int(row["product_id"]), title=title, description=None, account_name=str(row["account_name"] or ""))
    return {"task_id": task_id, "product_id": int(row["product_id"]), "ai_title": title}


def generate_description_for_task(task_id: int) -> dict:
    row = load_task(task_id)
    if not row:
        raise ValueError(f"找不到任务: {task_id}")
    product = {
        "task_id": int(row["task_id"]),
        "product_id": int(row["product_id"]),
        "name": row["name"] or "",
        "category": row["category"] or "",
        "final_price_cny": row["final_price_cny"] or "",
        "stock": row["stock"] or "",
        "attributes": load_product_attributes(int(row["product_id"])),
    }
    description = build_xianyu_description(product).strip()
    if not description:
        raise ValueError("AI 返回的简介为空")
    save_ai_copy(task_id, description=description)
    save_product_ai_copy(int(row["product_id"]), title=None, description=description, account_name=str(row["account_name"] or ""))
    return {"task_id": task_id, "product_id": int(row["product_id"]), "ai_description": description}


def generate_for_batch(batch_id: int, force: bool = False, sleep_seconds: float = 3.0) -> dict:
    ensure_ai_copy_table()
    rows = load_tasks(batch_id, force=force)
    success_count = 0
    failed = []

    for index, row in enumerate(rows, start=1):
        task_id = int(row["task_id"])
        product = {
            "task_id": task_id,
            "product_id": int(row["product_id"]),
            "name": row["name"] or "",
            "category": row["category"] or "",
            "final_price_cny": row["final_price_cny"] or "",
            "stock": row["stock"] or "",
            "attributes": load_product_attributes(int(row["product_id"])),
        }

        try:
            result = build_xianyu_copy(product)
            title = (result.get("title") or "").strip()
            description = (result.get("description") or "").strip()
            if not title or not description:
                raise ValueError("AI 返回的标题或描述为空")
            save_ai_copy(task_id, title, description)
            save_product_ai_copy(int(row["product_id"]), title, description, account_name=str(row["account_name"] or ""))
            success_count += 1
            print(f"生成成功: task={task_id} | {product['name']}")
        except Exception as e:
            failed.append({"task_id": task_id, "error": str(e)})
            print(f"生成失败: task={task_id} | {product['name']} | {e}")

        if index < len(rows) and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return {
        "batch_id": batch_id,
        "total_count": len(rows),
        "success_count": success_count,
        "failed_count": len(failed),
        "failures": failed,
    }


def generate_titles_for_batch(batch_id: int, force: bool = False, sleep_seconds: float = 1.5) -> dict:
    ensure_ai_copy_table()
    rows = load_tasks(batch_id, force=force)
    success_count = 0
    failed = []
    for index, row in enumerate(rows, start=1):
        task_id = int(row["task_id"])
        try:
            generate_title_for_task(task_id)
            success_count += 1
            print(f"标题生成成功: task={task_id} | {row['name']}")
        except Exception as e:
            failed.append({"task_id": task_id, "error": str(e)})
            print(f"标题生成失败: task={task_id} | {row['name']} | {e}")
        if index < len(rows) and sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return {"batch_id": batch_id, "total_count": len(rows), "success_count": success_count, "failed_count": len(failed), "failures": failed}


def generate_descriptions_for_batch(batch_id: int, force: bool = False, sleep_seconds: float = 1.5) -> dict:
    ensure_ai_copy_table()
    rows = load_tasks(batch_id, force=force)
    success_count = 0
    failed = []
    for index, row in enumerate(rows, start=1):
        task_id = int(row["task_id"])
        try:
            generate_description_for_task(task_id)
            success_count += 1
            print(f"简介生成成功: task={task_id} | {row['name']}")
        except Exception as e:
            failed.append({"task_id": task_id, "error": str(e)})
            print(f"简介生成失败: task={task_id} | {row['name']} | {e}")
        if index < len(rows) and sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return {"batch_id": batch_id, "total_count": len(rows), "success_count": success_count, "failed_count": len(failed), "failures": failed}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", type=int, required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=3.0)
    args = parser.parse_args()

    result = generate_for_batch(
        batch_id=args.batch_id,
        force=args.force,
        sleep_seconds=args.sleep_seconds,
    )
    print(f"批次AI文案生成完成: batch={result['batch_id']} | 成功={result['success_count']} | 失败={result['failed_count']}")


if __name__ == "__main__":
    main()
