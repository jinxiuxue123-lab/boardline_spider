import json
import random
import re
import sqlite3
import threading
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_FILE = ROOT_DIR / "products.db"
DEFAULT_TAOBAO_LOGIN_URL = "https://loginmyseller.taobao.com/?from=&f=top&style=&sub=true&redirect_url=https%3A%2F%2Fmyseller.taobao.com%2Fhome.htm%2Fshop-manage%2Fshop-center"
DEFAULT_TAOBAO_PUBLISH_URL = "https://item.upload.taobao.com/sell/ai/category.htm?spm=a21dvs.23580594.0.0.3f062c1bZmISK6"

TAOBAO_CATEGORY_KEYWORDS = {
    "滑雪板": "单板滑雪板",
    "固定器": "单板滑雪固定器",
    "滑雪鞋": "单板滑雪鞋",
    "滑雪服": "滑雪服",
    "滑雪帽衫和中间层": "滑雪中间层",
    "滑雪镜": "滑雪镜",
    "手套": "滑雪手套",
    "滑雪头盔": "滑雪头盔",
    "帽子护脸": "滑雪帽子护脸",
    "袜子以及周边配件": "滑雪配件",
    "儿童装备": "儿童滑雪装备",
}
TAOBAO_DAIGOU_SOURCES = {"boardline", "one", "one8"}


def _debug(message: str):
    try:
        print(f"[taobao] {message}", flush=True)
    except Exception:
        pass


def derive_taobao_inventory_tag(source: str) -> str:
    return "代购" if str(source or "").strip().lower() in TAOBAO_DAIGOU_SOURCES else "现货"


def _get_conn():
    conn = sqlite3.connect(str(DB_FILE))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_shop_profile_dir(shop_name: str, profile_dir: str = "") -> str:
    if profile_dir.strip():
        path = Path(profile_dir).expanduser()
    else:
        path = ROOT_DIR / "data" / "taobao_profiles" / shop_name
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def resolve_browser_launch_config(shop_row: dict) -> tuple[str, dict]:
    chrome_user_data_dir = str(shop_row.get("chrome_user_data_dir") or "").strip()
    chrome_profile_name = str(shop_row.get("chrome_profile_name") or "").strip() or "Default"
    if chrome_user_data_dir:
        user_data_dir = str(Path(chrome_user_data_dir).expanduser())
        launch_kwargs = {
            "channel": "chrome",
            "headless": False,
            "viewport": {"width": 1440, "height": 960},
            "args": [
                f"--profile-directory={chrome_profile_name}",
                "--disable-blink-features=AutomationControlled",
            ],
            "ignore_default_args": ["--enable-automation"],
        }
        return user_data_dir, launch_kwargs

    shop_name = str(shop_row.get("shop_name") or "").strip() or "taobao-shop"
    user_data_dir = ensure_shop_profile_dir(shop_name, str(shop_row.get("browser_profile_dir") or ""))
    launch_kwargs = {
        "headless": False,
        "viewport": {"width": 1440, "height": 960},
    }
    return user_data_dir, launch_kwargs


def open_login_browser_instructions(shop_row: dict) -> str:
    login_url = str(shop_row.get("login_url") or "").strip() or DEFAULT_TAOBAO_LOGIN_URL
    cdp_url = str(shop_row.get("chrome_cdp_url") or "").strip()
    if cdp_url:
        return (
            f"请先手动启动 Chrome 调试模式并打开：{login_url}\n"
            f"当前店铺配置的 Chrome 调试 URL：{cdp_url}\n"
            "建议先在这个 Chrome 里手动登录千牛，登录成功后再点“淘宝发布助手”。"
        )
    return (
        f"将尝试打开登录页：{login_url}\n"
        "如果仍触发滑块失败，建议改为手动启动 Chrome 调试模式，再在店铺配置里填写 Chrome 调试 URL。"
    )


def connect_or_launch_context(playwright, shop_row: dict):
    cdp_url = str(shop_row.get("chrome_cdp_url") or "").strip()
    if cdp_url:
        browser = playwright.chromium.connect_over_cdp(cdp_url)
        context = browser.contexts[0] if browser.contexts else browser.new_context(viewport={"width": 1440, "height": 960})
        return browser, context, True
    user_data_dir, launch_kwargs = resolve_browser_launch_config(shop_row)
    context = playwright.chromium.launch_persistent_context(user_data_dir=user_data_dir, **launch_kwargs)
    return None, context, False


def load_selected_ai_local_images(product_id: int, account_name: str) -> list[str]:
    conn = _get_conn()
    account_name = (account_name or "").strip()
    rows = []
    if account_name:
        rows = conn.execute(
            """
            SELECT ai_main_image_path
            FROM xianyu_product_ai_images
            WHERE product_id = ?
              AND COALESCE(account_name, '') = ?
              AND COALESCE(is_selected, 0) = 1
              AND TRIM(COALESCE(ai_main_image_path, '')) != ''
            ORDER BY id DESC
            """,
            (product_id, account_name),
        ).fetchall()
    if not rows:
        rows = conn.execute(
            """
            SELECT ai_main_image_path
            FROM xianyu_product_ai_images
            WHERE product_id = ?
              AND COALESCE(is_selected, 0) = 1
              AND TRIM(COALESCE(ai_main_image_path, '')) != ''
            ORDER BY id DESC
            """,
            (product_id,),
        ).fetchall()
    conn.close()
    results = []
    seen = set()
    for row in rows:
        path = str(row["ai_main_image_path"] or "").strip()
        if path and path not in seen and Path(path).exists():
            seen.add(path)
            results.append(path)
    return results


def derive_brand_keyword(name: str) -> str:
    tokens = [token for token in str(name or "").split() if token]
    if not tokens:
        return ""
    start_idx = 0
    first = tokens[0].upper()
    if first.isdigit() or (len(first) == 4 and first[:2].isdigit() and first[2:].isdigit()) or (len(first) == 5 and first[:2].isdigit() and first[2] == "/" and first[3:].isdigit()):
        start_idx = 1
    if start_idx >= len(tokens):
        return ""
    brand = tokens[start_idx].upper()
    if brand in {"WOMEN'S", "WOMENS", "MEN'S", "MENS", "KIDS", "JR", "YOUTH"} and start_idx + 1 < len(tokens):
        brand = tokens[start_idx + 1].upper()
    return brand


def parse_stock_entries(stock_text: str) -> list[tuple[str, int]]:
    results = []
    for part in [item.strip() for item in str(stock_text or "").split("|") if item.strip()]:
        if ":" not in part:
            continue
        label, qty = part.rsplit(":", 1)
        try:
            qty_num = int(qty.strip())
        except Exception:
            continue
        results.append((label.strip(), qty_num))
    return results


def extract_snowboard_length(label: str) -> str:
    text = str(label or "").strip()
    match = re.search(r"\b(\d{3}(?:\.\d+)?)(?=\s*(?:w|wide)?\b)", text, re.IGNORECASE)
    return match.group(1) if match else ""


def derive_snowboard_model(name: str) -> str:
    text = str(name or "").strip()
    season_prefix = ""
    season_match = re.match(r"^(\d{2}/\d{2}|\d{4})\b", text)
    if season_match:
        season_prefix = season_match.group(1).strip()
        text = text[season_match.end():].strip()
    brand = derive_brand_keyword(text)
    if brand:
        brand_idx = text.upper().find(brand)
        if brand_idx >= 0:
            text = text[brand_idx + len(brand):].strip()
    text = re.sub(r"\b\d{3}(?:\s*,\s*\d{3})+\b.*$", "", text).strip()
    text = re.sub(r"\b\d{3}\b.*$", "", text).strip()
    text = re.sub(r"\s+-\s+.*$", "", text).strip()
    text = re.sub(r"\bBOARD\b", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\s{2,}", " ", text).strip()
    if season_prefix:
        combined = f"{season_prefix} {text}".strip()
        return combined or season_prefix
    return text or derive_brand_keyword(name)


def derive_available_lengths(stock_text: str) -> list[str]:
    lengths = []
    for label, qty in parse_stock_entries(stock_text):
        if qty <= 0:
            continue
        length = extract_snowboard_length(label)
        if length:
            lengths.append(length)
    deduped = []
    seen = set()
    for item in lengths:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def compute_total_stock(stock_text: str) -> int:
    total = 0
    for _, qty in parse_stock_entries(stock_text):
        if qty > 0:
            total += qty
    return max(total, 1)


def derive_length_stock_map(stock_text: str) -> dict[str, int]:
    results = {}
    for label, qty in parse_stock_entries(stock_text):
        length = extract_snowboard_length(label)
        if not length:
            continue
        results[length] = max(int(qty), 0)
    return results


def derive_snowboard_variant_data(model: str, stock_text: str) -> tuple[list[str], list[str], dict[tuple[str, str], int]]:
    base_model = str(model or "").strip()
    models = []
    lengths = []
    seen_lengths = set()
    color_length_stock_map: dict[tuple[str, str], int] = {}
    has_standard = False
    has_wide = False

    for label, qty in parse_stock_entries(stock_text):
        base_length = extract_snowboard_length(label)
        if not base_length:
            continue
        if base_length not in seen_lengths:
            seen_lengths.add(base_length)
            lengths.append(base_length)
        is_wide = bool(
            re.search(
                r"\bwide\b|(?<=\d)wide\b|(?<=\d)w\b",
                label,
                re.IGNORECASE,
            )
        )
        color_name = f"{base_model}加宽" if is_wide and base_model else base_model
        color_length_stock_map[(color_name, base_length)] = max(int(qty), 0)
        if is_wide:
            has_wide = True
        else:
            has_standard = True

    if has_standard and base_model:
        models.append(base_model)
    if has_wide and base_model:
        models.append(f"{base_model}加宽")
    if not models and base_model:
        models.append(base_model)

    for color_name in models:
        for base_length in lengths:
            color_length_stock_map.setdefault((color_name, base_length), 0)

    return models, lengths, color_length_stock_map


def build_publish_assist_payload(product_id: int, account_name: str = "") -> dict:
    conn = _get_conn()
    row = conn.execute(
        """
        SELECT
            p.id AS product_id,
            p.source,
            p.branduid,
            p.category,
            p.name,
            p.url,
            p.local_image_path,
            p.image_url,
            u.final_price_cny,
            u.price_cny,
            u.original_price_cny,
            u.stock,
            COALESCE(aic.ai_title, xpc.ai_title, '') AS ai_title,
            COALESCE(aic.ai_taobao_title, xpc.ai_taobao_title, '') AS ai_taobao_title,
            COALESCE(aic.ai_taobao_guide_title, xpc.ai_taobao_guide_title, '') AS ai_taobao_guide_title,
            COALESCE(aic.ai_description, xpc.ai_description, '') AS ai_description
        FROM products p
        LEFT JOIN product_updates u
          ON u.product_id = p.id
        LEFT JOIN xianyu_account_product_ai_copy aic
          ON aic.product_id = p.id
         AND aic.account_name = ?
        LEFT JOIN xianyu_product_ai_copy xpc
          ON xpc.product_id = p.id
        WHERE p.id = ?
        LIMIT 1
        """,
        (account_name or "", product_id),
    ).fetchone()
    conn.close()
    if not row:
        raise ValueError(f"商品不存在: {product_id}")

    title = str(row["ai_taobao_title"] or "").strip() or str(row["ai_title"] or "").strip() or str(row["name"] or "").strip()
    guide_title = str(row["ai_taobao_guide_title"] or "").strip()
    if not guide_title:
        guide_title = truncate_weighted_text(title, 30)
    description = str(row["ai_description"] or "").strip()
    if not description:
        description = (
            f"{row['name'] or ''}\n"
            f"分类：{row['category'] or ''}\n"
            f"库存：{row['stock'] or ''}\n"
            f"来源：{row['source'] or ''}\n"
            f"原链接：{row['url'] or ''}"
        ).strip()

    image_paths = load_selected_ai_local_images(product_id, account_name)
    local_image_path = str(row["local_image_path"] or "").strip()
    if not image_paths and local_image_path and Path(local_image_path).exists():
        image_paths.append(local_image_path)

    category_name = str(row["category"] or "").strip()
    suggested_category_keyword = TAOBAO_CATEGORY_KEYWORDS.get(category_name, category_name)
    suggested_brand_keyword = derive_brand_keyword(str(row["name"] or "").strip())
    if "nitro" in str(row["name"] or "").lower():
        suggested_brand_keyword = "NITRO/尼卓"
    elif "jones" in str(row["name"] or "").lower():
        suggested_brand_keyword = "Jones Snowboards"
    elif "salomon" in str(row["name"] or "").lower():
        suggested_brand_keyword = "SALOMON/萨洛蒙"
    inventory_tag = derive_taobao_inventory_tag(str(row["source"] or "").strip())
    snowboard_model = ""
    snowboard_lengths = []
    if category_name == "滑雪板":
        snowboard_model = derive_snowboard_model(str(row["name"] or "").strip())
        snowboard_lengths = derive_available_lengths(str(row["stock"] or "").strip())

    return {
        "product_id": int(row["product_id"]),
        "source": str(row["source"] or "").strip(),
        "branduid": str(row["branduid"] or "").strip(),
        "category": str(row["category"] or "").strip(),
        "name": str(row["name"] or "").strip(),
        "url": str(row["url"] or "").strip(),
        "title": title,
        "guide_title": guide_title,
        "description": description,
        "price_cny": str(row["final_price_cny"] or row["price_cny"] or "").strip(),
        "original_price_cny": str(row["original_price_cny"] or "").strip(),
        "stock": str(row["stock"] or "").strip(),
        "local_image_path": local_image_path,
        "image_url": str(row["image_url"] or "").strip(),
        "image_paths": image_paths,
        "account_name": account_name or "",
        "inventory_tag": inventory_tag,
        "suggested_category_keyword": suggested_category_keyword,
        "suggested_brand_keyword": suggested_brand_keyword,
        "suggested_snowboard_model": snowboard_model,
        "suggested_snowboard_lengths": snowboard_lengths,
    }


def _inject_assistant_overlay(page, payload: dict):
    page.evaluate(
        """
        (payload) => {
          const old = document.getElementById('__tb_publish_assistant__');
          if (old) old.remove();
          const wrap = document.createElement('div');
          wrap.id = '__tb_publish_assistant__';
          wrap.style.cssText = 'position:fixed;top:16px;right:16px;z-index:2147483647;width:360px;max-height:85vh;overflow:auto;background:#fff;border:1px solid #ddd;border-radius:12px;box-shadow:0 12px 32px rgba(0,0,0,.18);padding:14px;font:12px/1.5 -apple-system,BlinkMacSystemFont,sans-serif;color:#222;';
          const safe = (v) => String(v || '');
          const box = (label, value) => `
            <div style="margin-top:10px;">
              <div style="font-weight:700;margin-bottom:4px;">${label}</div>
              <textarea readonly style="width:100%;min-height:${label==='描述'?120:50}px;border:1px solid #ddd;border-radius:8px;padding:8px;font:12px/1.5 ui-monospace,monospace;">${safe(value)}</textarea>
            </div>`;
          const images = (payload.image_paths || []).map((item) => `<div style="word-break:break-all;margin-bottom:4px;">${safe(item)}</div>`).join('') || '<div style="color:#888;">暂无本地图</div>';
          wrap.innerHTML = `
            <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
              <strong style="font-size:14px;">淘宝发布助手</strong>
              <button type="button" id="__tb_publish_close__" style="border:0;background:#f3f3f3;border-radius:8px;padding:4px 8px;cursor:pointer;">关闭</button>
            </div>
            <div style="margin-top:6px;color:#666;">商品ID ${safe(payload.product_id)} / ${safe(payload.branduid)}</div>
            ${box('推荐类目关键词', payload.suggested_category_keyword)}
            ${box('推荐品牌', payload.suggested_brand_keyword)}
            ${payload.category === '滑雪板' ? box('雪板型号', payload.suggested_snowboard_model) : ''}
            ${payload.category === '滑雪板' ? box('有货长度', (payload.suggested_snowboard_lengths || []).join(' | ')) : ''}
            ${box('商品标题', payload.title)}
            ${box('导购标题', payload.guide_title)}
            ${box('价格', payload.price_cny)}
            ${box('库存', payload.stock)}
            ${box('描述', payload.description)}
            <div style="margin-top:10px;">
              <div style="font-weight:700;margin-bottom:4px;">本地图片</div>
              ${images}
            </div>
            <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
              <button type="button" id="__tb_copy_title__" style="border:0;background:#c95c2d;color:#fff;border-radius:8px;padding:6px 10px;cursor:pointer;">复制商品标题</button>
              <button type="button" id="__tb_copy_guide_title__" style="border:0;background:#9a3412;color:#fff;border-radius:8px;padding:6px 10px;cursor:pointer;">复制导购标题</button>
              <button type="button" id="__tb_copy_desc__" style="border:0;background:#1f4f46;color:#fff;border-radius:8px;padding:6px 10px;cursor:pointer;">复制描述</button>
              <button type="button" id="__tb_copy_stock__" style="border:0;background:#444;color:#fff;border-radius:8px;padding:6px 10px;cursor:pointer;">复制库存</button>
              <button type="button" id="__tb_copy_cat__" style="border:0;background:#2457c5;color:#fff;border-radius:8px;padding:6px 10px;cursor:pointer;">复制类目词</button>
              <button type="button" id="__tb_copy_brand__" style="border:0;background:#6b46c1;color:#fff;border-radius:8px;padding:6px 10px;cursor:pointer;">复制品牌</button>
              ${payload.category === '滑雪板' ? '<button type="button" id="__tb_copy_model__" style="border:0;background:#0b7a75;color:#fff;border-radius:8px;padding:6px 10px;cursor:pointer;">复制型号</button>' : ''}
            </div>
          `;
          document.body.appendChild(wrap);
          const copy = async (text) => {
            try { await navigator.clipboard.writeText(String(text || '')); }
            catch (e) {}
          };
          document.getElementById('__tb_publish_close__')?.addEventListener('click', () => wrap.remove());
          document.getElementById('__tb_copy_title__')?.addEventListener('click', () => copy(payload.title));
          document.getElementById('__tb_copy_guide_title__')?.addEventListener('click', () => copy(payload.guide_title));
          document.getElementById('__tb_copy_desc__')?.addEventListener('click', () => copy(payload.description));
          document.getElementById('__tb_copy_stock__')?.addEventListener('click', () => copy(payload.stock));
          document.getElementById('__tb_copy_cat__')?.addEventListener('click', () => copy(payload.suggested_category_keyword));
          document.getElementById('__tb_copy_brand__')?.addEventListener('click', () => copy(payload.suggested_brand_keyword));
          document.getElementById('__tb_copy_model__')?.addEventListener('click', () => copy(payload.suggested_snowboard_model));
          window.__TAOBAO_PUBLISH_ASSIST_PAYLOAD__ = payload;
        }
        """,
        payload,
    )


def truncate_weighted_text(text: str, limit: int) -> str:
    result = []
    used = 0
    for ch in str(text or ""):
        weight = 1 if ord(ch) < 128 else 2
        if used + weight > limit:
            break
        result.append(ch)
        used += weight
    return "".join(result)


def _install_dom_fill_agent(page, payload: dict):
    return


def _best_effort_fill_basic_fields(page, payload: dict):
    title = str(payload.get("title") or "").strip()
    guide_title = str(payload.get("guide_title") or "").strip()
    stock = str(payload.get("stock") or "").strip()
    inventory_tag = str(payload.get("inventory_tag") or "").strip()
    full_title = truncate_weighted_text(title, 60)
    short_title = truncate_weighted_text(guide_title or title, 30)

    def _is_radio_selected(section_selector: str, text: str, label_selector: str = ".next-radio-label, .item-label") -> bool:
        try:
            selected = page.locator(
                f"{section_selector} label.next-radio-wrapper.checked:has({label_selector}:text-is('{text}'))"
            ).first
            return selected.count() > 0
        except Exception:
            return False

    def _is_dropdown_selected(section_selector: str, option_text: str) -> bool:
        try:
            container_text = (
                page.locator(f"{section_selector} .next-select-values").first.inner_text(timeout=300) or ""
            ).strip()
            return option_text in container_text
        except Exception:
            return False

    def _click_labeled_radio(section_selector: str, text: str, label_selector: str = ".next-radio-label, .item-label") -> bool:
        if _is_radio_selected(section_selector, text, label_selector=label_selector):
            return True
        candidates = [
            page.locator(f"{section_selector} label.next-radio-wrapper:has({label_selector}:text-is('{text}'))").first,
            page.locator(f"{section_selector} {label_selector}:text-is('{text}')").first,
        ]
        for target in candidates:
            try:
                if target.count() == 0:
                    continue
                target.click(force=True)
                page.wait_for_timeout(300)
                return True
            except Exception:
                continue
        return False

    def _select_labeled_dropdown_option(section_selector: str, option_text: str) -> bool:
        if _is_dropdown_selected(section_selector, option_text):
            return True
        trigger_candidates = [
            page.locator(f"{section_selector} .next-select").first,
            page.locator(f"{section_selector} input[role='combobox']").first,
            page.locator(f"{section_selector} .next-select-inner").first,
        ]
        opened = False
        for trigger in trigger_candidates:
            try:
                if trigger.count() == 0:
                    continue
                trigger.click(force=True)
                page.wait_for_timeout(500)
                opened = True
                break
            except Exception:
                continue
        if not opened:
            return False
        search_candidates = [
            page.locator(".sell-o-select-options .options-search input").first,
            page.locator(".sell-o-select-options input[autocomplete='off']").first,
        ]
        for search_input in search_candidates:
            try:
                if search_input.count() == 0:
                    continue
                search_input.click(force=True)
                search_input.fill("")
                search_input.type(option_text, delay=40)
                page.wait_for_timeout(600)
                break
            except Exception:
                continue
        option_candidates = [
            page.locator(f".sell-o-select-options .options-item[title='{option_text}']").first,
            page.locator(f".sell-o-select-options .options-item:has(.info-content:text-is('{option_text}'))").first,
            page.locator(f".next-select-menu .next-menu-item:text-is('{option_text}')").first,
            page.locator(f".next-overlay-wrapper .next-menu-item:text-is('{option_text}')").first,
            page.locator(f"[role='option']:text-is('{option_text}')").first,
            page.locator(f"text='{option_text}'").last,
        ]
        for option in option_candidates:
            try:
                if option.count() == 0:
                    continue
                option.click(force=True)
                page.wait_for_timeout(400)
                return True
            except Exception:
                continue
        return False

    # Taobao publish page targeted selectors
    try:
        title_locator = page.locator("#sell-field-title input").first
        if title and title_locator.count() > 0:
            current = (title_locator.input_value() or "").strip()
            if current != full_title:
                title_locator.fill(full_title)
    except Exception:
        pass

    try:
        shopping_title_locator = page.locator("#sell-field-shopping_title input").first
        if title and shopping_title_locator.count() > 0:
            current = (shopping_title_locator.input_value() or "").strip()
            if current != short_title:
                shopping_title_locator.fill(short_title)
    except Exception:
        pass

    try:
        stock_locator = page.locator("#struct-quantity input").first
        if stock_locator.count() > 0:
            total_stock = compute_total_stock(stock)
            current = (stock_locator.input_value() or "").strip()
            if current != str(total_stock):
                stock_locator.fill(str(total_stock))
    except Exception:
        pass

    try:
        target_label_text = (
            "中国港澳台地区及其他国家和地区"
            if inventory_tag == "代购"
            else "中国内地（大陆）"
        )
        _click_labeled_radio("#struct-globalStock .tab-nest-radio-group", target_label_text, ".next-radio-label")
        if inventory_tag == "代购":
            _select_labeled_dropdown_option("#struct-globalStock .cat-sub-items:has(.sellhoc-label:text-is('地区/国家'))", "日本")
            _click_labeled_radio("#struct-globalStock .cat-sub-items:has(.sellhoc-label:text-is('库存类型'))", "非现货（无现货，需采购）", ".item-label")
            _click_labeled_radio("#struct-departurePlace", "中国内地（大陆）", ".item-label")
    except Exception:
        pass


def _fill_input_with_events(locator, value: str):
    locator.click()
    locator.fill("")
    locator.fill(value)
    locator.press("End")
    locator.dispatch_event("input")
    locator.dispatch_event("change")


def _blur_preserve_scroll(page, locator):
    try:
        scroll_y = page.evaluate("() => window.scrollY")
    except Exception:
        scroll_y = None
    try:
        locator.evaluate("(el) => el.blur()")
    except Exception:
        try:
            locator.press("Tab")
        except Exception:
            return
    page.wait_for_timeout(150)
    if scroll_y is not None:
        try:
            page.evaluate("(y) => window.scrollTo(0, y)", scroll_y)
        except Exception:
            pass


def _confirm_snowboard_length(page, overlay_input=None):
    if overlay_input is not None:
        try:
            overlay_input.evaluate("(el) => el.blur()")
            page.wait_for_timeout(250)
            return
        except Exception:
            pass
    try:
        page.keyboard.press("Tab")
        page.wait_for_timeout(250)
    except Exception:
        pass


def _scroll_sku_row_into_view(page, row):
    try:
        row.scroll_into_view_if_needed()
        page.wait_for_timeout(120)
    except Exception:
        pass
    try:
        page.evaluate(
            """(el) => {
                const rect = el.getBoundingClientRect();
                const targetY = window.scrollY + rect.top - Math.max(120, window.innerHeight * 0.2);
                window.scrollTo(0, Math.max(0, targetY));
            }""",
            row,
        )
        page.wait_for_timeout(120)
    except Exception:
        pass


def _fill_visible_sku_rows(page, sku_price: str, lengths: list[str], length_stock_map: dict[str, int], multi_color_mode: bool = False, color_length_stock_map: dict[tuple[str, str], int] | None = None) -> int:
    def _input_matches(locator, expected: str) -> bool:
        try:
            current = (locator.input_value() or "").strip()
        except Exception:
            return False
        if current == expected:
            return True
        try:
            return float(current) == float(expected)
        except Exception:
            return False

    def _fill_cell_input(locator, expected: str) -> bool:
        if not expected:
            return True
        try:
            locator.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            locator.click(force=True)
        except Exception:
            pass
        try:
            locator.press("Meta+A")
        except Exception:
            pass
        try:
            locator.fill("")
        except Exception:
            pass
        try:
            locator.type(expected, delay=25)
        except Exception:
            return _input_matches(locator, expected)
        for _ in range(6):
            page.wait_for_timeout(150)
            if _input_matches(locator, expected):
                return True
        return _input_matches(locator, expected)

    filled_rows = 0
    sku_rows = page.locator("tr.sku-table-row")
    row_count = sku_rows.count()
    visible_row_ids = []
    for idx in range(row_count):
        row = sku_rows.nth(idx)
        try:
            row_id_attr = row.locator('td[id$="skuPrice"]').first.get_attribute("id") or ""
        except Exception:
            row_id_attr = ""
        if row_id_attr:
            visible_row_ids.append(row_id_attr)

    def _row_sort_key(row_id: str):
        try:
            return int(row_id.split("-", 1)[0])
        except Exception:
            return 10**9

    for row_id_attr in sorted(set(visible_row_ids), key=_row_sort_key):
        price_cell = page.locator(f'td[id="{row_id_attr}"]').first
        if price_cell.count() == 0:
            continue
        row = price_cell.locator("xpath=ancestor::tr[1]")
        _scroll_sku_row_into_view(page, row)
        try:
            color_text = (
                row.locator('td[id$="p-1627207"] span[title]').first.get_attribute("title") or ""
            ).strip()
        except Exception:
            color_text = ""
        try:
            length_text = (
                row.locator('td[id$="p-148242406"] span[title]').first.get_attribute("title") or ""
            ).strip()
        except Exception:
            length_text = ""
        normalized_length = length_text.replace("cm", "").strip()
        qty_value = str(length_stock_map.get(normalized_length, 0))
        row_price = sku_price
        if color_length_stock_map and color_text:
            qty_value = str(color_length_stock_map.get((color_text, normalized_length), 0))
        elif multi_color_mode and row_id_attr and "TEST" in color_text.upper():
            try:
                row_index = int(row_id_attr.split("-", 1)[0])
                qty_value = str(row_index + 1)
            except Exception:
                pass
        if multi_color_mode and color_text:
            match = re.search(r"TEST(\d{2})$", color_text, re.IGNORECASE)
            if match:
                suffix = match.group(1)
                row_price = suffix * 2
        if row_price:
            try:
                price_input = price_cell.locator("input").first
                if price_input.count() > 0:
                    if not _input_matches(price_input, row_price):
                        _fill_cell_input(price_input, row_price)
            except Exception:
                pass
        try:
            stock_input = row.locator('td[id$="skuStock"] input').first
            if stock_input.count() > 0:
                if not _input_matches(stock_input, qty_value):
                    _fill_cell_input(stock_input, qty_value)
        except Exception:
            pass
        filled_rows += 1
    return filled_rows


def _fill_virtualized_sku_table(page, sku_price: str, lengths: list[str], length_stock_map: dict[str, int], multi_color_mode: bool = False, color_length_stock_map: dict[tuple[str, str], int] | None = None):
    scroll_wrap = page.locator(".ver-scroll-wrap").first
    if scroll_wrap.count() == 0:
        _fill_visible_sku_rows(page, sku_price, lengths, length_stock_map, multi_color_mode=multi_color_mode, color_length_stock_map=color_length_stock_map)
        return

    try:
        scroll_wrap.scroll_into_view_if_needed()
        page.wait_for_timeout(200)
    except Exception:
        pass

    expected_rows = max(1, page.locator("tr.sku-table-row").count())
    if multi_color_mode and lengths:
        try:
            color_count = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]').count()
            expected_rows = max(expected_rows, color_count * len(lengths))
        except Exception:
            pass

    seen_ids = set()
    stagnant_rounds = 0
    last_seen_count = 0
    for _ in range(80):
        visible_rows = page.locator("tr.sku-table-row")
        row_count = visible_rows.count()
        for idx in range(row_count):
            row = visible_rows.nth(idx)
            try:
                row_id_attr = row.locator('td[id$="skuPrice"]').first.get_attribute("id") or ""
            except Exception:
                row_id_attr = ""
            if row_id_attr:
                seen_ids.add(row_id_attr)
        _fill_visible_sku_rows(page, sku_price, lengths, length_stock_map, multi_color_mode=multi_color_mode, color_length_stock_map=color_length_stock_map)
        if len(seen_ids) >= expected_rows:
            break
        if len(seen_ids) == last_seen_count:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
            last_seen_count = len(seen_ids)
        if stagnant_rounds >= 3:
            break
        try:
            page.evaluate(
                """() => {
                    const wrap = document.querySelector('.ver-scroll-wrap');
                    if (wrap) {
                      wrap.scrollTop = Math.min(wrap.scrollTop + 420, wrap.scrollHeight);
                    }
                }"""
            )
        except Exception:
            pass
        page.wait_for_timeout(350)

    try:
        page.evaluate(
            """() => {
                const wrap = document.querySelector('.ver-scroll-wrap');
                if (wrap) {
                  wrap.scrollTop = 0;
                }
            }"""
        )
        page.wait_for_timeout(400)
    except Exception:
        pass


def _try_fill_snowboard_sales_once(page, payload: dict) -> bool:
    if str(payload.get("category") or "").strip() != "滑雪板":
        return False
    model = str(payload.get("suggested_snowboard_model") or "").strip()
    stock_text = str(payload.get("stock") or "").strip()
    models, lengths, color_length_stock_map = derive_snowboard_variant_data(model, stock_text)
    if not lengths:
        lengths = [str(x).strip() for x in (payload.get("suggested_snowboard_lengths") or []) if str(x).strip()]
    length_stock_map = derive_length_stock_map(stock_text)
    sku_price = str(payload.get("price_cny") or "").strip()
    if not model and not lengths:
        return False
    try:
        if page.locator("#struct-p-148242406 .fake-input-wrapper").count() == 0:
            return False

        try:
            sales_block = page.locator("#struct-p-1627207").first
            if sales_block.count() > 0:
                sales_block.scroll_into_view_if_needed()
                page.wait_for_timeout(200)
        except Exception:
            pass

        color_inputs = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]')
        for idx, color_value in enumerate(models):
            color_inputs = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]')
            if color_inputs.count() <= idx:
                add_btn = page.locator("#struct-p-1627207 button.add").first
                before_count = color_inputs.count()
                if add_btn.count() > 0:
                    try:
                        add_btn.click(force=True)
                    except Exception:
                        try:
                            add_btn.evaluate("(el) => el.click()")
                        except Exception:
                            pass
                    for _ in range(12):
                        page.wait_for_timeout(250)
                        color_inputs = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]')
                        if color_inputs.count() > before_count:
                            break
            if color_inputs.count() <= idx:
                break
            color_input = color_inputs.nth(idx)
            try:
                color_input.scroll_into_view_if_needed()
                page.wait_for_timeout(120)
            except Exception:
                pass
            current = (color_input.input_value() or "").strip()
            if current != color_value:
                _fill_input_with_events(color_input, color_value)
                _blur_preserve_scroll(page, color_input)
                page.wait_for_timeout(300)
            if idx < len(models) - 1:
                color_inputs = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]')
                if color_inputs.count() <= idx + 1:
                    add_btn = page.locator("#struct-p-1627207 button.add").first
                    before_count = color_inputs.count()
                    if add_btn.count() > 0:
                        try:
                            if add_btn.is_enabled():
                                add_btn.click(force=True)
                                for _ in range(6):
                                    page.wait_for_timeout(250)
                                    color_inputs = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]')
                                    if color_inputs.count() > before_count:
                                        break
                            if color_inputs.count() <= before_count:
                                add_btn.evaluate("(el) => el.click()")
                                for _ in range(6):
                                    page.wait_for_timeout(250)
                                    color_inputs = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]')
                                    if color_inputs.count() > before_count:
                                        break
                            if color_inputs.count() <= before_count:
                                add_icon = add_btn.locator("i.next-icon-add").first
                                if add_icon.count() > 0:
                                    add_icon.click(force=True)
                                    for _ in range(6):
                                        page.wait_for_timeout(250)
                                        color_inputs = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]')
                                        if color_inputs.count() > before_count:
                                            break
                        except Exception:
                            try:
                                add_btn.evaluate("(el) => el.click()")
                            except Exception:
                                pass
                        for _ in range(12):
                            page.wait_for_timeout(250)
                            color_inputs = page.locator('#struct-p-1627207 input[placeholder="主色(必选)"]')
                            if color_inputs.count() > before_count:
                                break

        stable_rounds = 0
        last_length_count = -1
        for _ in range(40):
            try:
                current_length_count = page.locator("#struct-p-148242406 .fake-input-wrapper").count()
            except Exception:
                current_length_count = 0
            if current_length_count == last_length_count and current_length_count > 0:
                stable_rounds += 1
            else:
                stable_rounds = 0
                last_length_count = current_length_count
            if stable_rounds >= 3:
                break
            page.wait_for_timeout(300)

        target_length_count = max(1, len(lengths))
        _debug(f"length target_count={target_length_count} values={lengths}")

        def _ensure_length_wrapper_count(target_count: int) -> bool:
            for _ in range(6):
                current_count = page.locator("#struct-p-148242406 .fake-input-wrapper").count()
                if current_count >= target_count:
                    return True
                add_btn = page.locator("#struct-p-148242406 button.add").first
                add_icon = add_btn.locator("i.next-icon-add").first if add_btn.count() > 0 else page.locator("")
                if add_btn.count() == 0:
                    _debug("length add button missing")
                    return False
                try:
                    add_btn.scroll_into_view_if_needed()
                except Exception:
                    pass
                for target in (add_btn, add_icon, add_btn):
                    try:
                        if target.count() == 0:
                            continue
                        if target is add_btn:
                            try:
                                if add_btn.is_enabled():
                                    add_btn.click(force=True)
                                else:
                                    add_btn.evaluate("(el) => el.click()")
                            except Exception:
                                add_btn.evaluate("(el) => el.click()")
                        else:
                            target.click(force=True)
                    except Exception:
                        continue
                    for _ in range(20):
                        page.wait_for_timeout(250)
                        current_count = page.locator("#struct-p-148242406 .fake-input-wrapper").count()
                        if current_count >= target_count:
                            _debug(f"length wrappers increased={current_count}")
                            return True
                page.wait_for_timeout(300)
            return page.locator("#struct-p-148242406 .fake-input-wrapper").count() >= target_count

        for idx, length in enumerate(lengths):
            fake_wrappers = page.locator("#struct-p-148242406 .fake-input-wrapper")
            _debug(f"length idx={idx} value={length} wrappers={fake_wrappers.count()}")
            if fake_wrappers.count() <= idx:
                for _ in range(20):
                    page.wait_for_timeout(250)
                    fake_wrappers = page.locator("#struct-p-148242406 .fake-input-wrapper")
                    if fake_wrappers.count() > idx:
                        break
                if fake_wrappers.count() <= idx and idx > 0:
                    _ensure_length_wrapper_count(idx + 1)
                    fake_wrappers = page.locator("#struct-p-148242406 .fake-input-wrapper")
                if fake_wrappers.count() <= idx:
                    _debug(f"length idx={idx} wrapper missing after wait/add wrappers={fake_wrappers.count()}")
                    break
            wrapper = fake_wrappers.nth(idx)
            try:
                wrapper.scroll_into_view_if_needed()
                page.wait_for_timeout(120)
            except Exception:
                pass
            row_input = wrapper.locator('input[placeholder="规格"]').first
            expected_row_value = f"{length}cm"
            def _length_written() -> bool:
                try:
                    row_value = (row_input.input_value() or "").strip()
                except Exception:
                    row_value = ""
                try:
                    wrapper_text = (wrapper.inner_text() or "").strip()
                except Exception:
                    wrapper_text = ""
                return row_value == expected_row_value or expected_row_value in wrapper_text
            if not _length_written():
                row_value_ok = False
                for _attempt in range(3):
                    _debug(f"length idx={idx} attempt={_attempt + 1} expected={expected_row_value}")
                    opened_editor = False
                    click_targets = [
                        wrapper.locator(".overlay").last,
                        wrapper.locator(".fake-input").last,
                        wrapper,
                        row_input,
                    ]
                    for click_target in click_targets:
                        try:
                            if click_target.count() == 0:
                                continue
                            click_target.click(force=True)
                        except Exception:
                            try:
                                click_target.evaluate("(el) => el.click()")
                            except Exception:
                                continue
                        page.wait_for_timeout(350)
                        probe = page.locator('input[placeholder="必须输入整数"]:visible').last
                        if probe.count() > 0:
                            opened_editor = True
                            break
                    if not opened_editor:
                        try:
                            wrapper.click(force=True, position={"x": 20, "y": 20})
                        except Exception:
                            pass
                        page.wait_for_timeout(350)
                    overlay_input = page.locator(
                        '#struct-p-148242406 input[placeholder="必须输入整数"]:visible'
                    ).last
                    if overlay_input.count() == 0:
                        overlay_input = page.locator(
                            '#struct-p-148242406 input[placeholder="必须输入整数"]'
                        ).last
                    if overlay_input.count() == 0:
                        overlay_input = page.locator(
                            '#struct-p-148242406 span.next-input.next-medium.fusion-input input:visible'
                        ).last
                    if overlay_input.count() == 0:
                        overlay_input = page.locator('input[placeholder="必须输入整数"]:visible').last
                    if overlay_input.count() == 0:
                        overlay_input = page.locator('input[placeholder="必须输入整数"]').last
                    _debug(f"length idx={idx} visible_overlay_count={overlay_input.count()}")
                    if overlay_input.count() == 0:
                        page.wait_for_timeout(500)
                        continue
                    try:
                        overlay_input.click(force=True)
                    except Exception:
                        pass
                    try:
                        overlay_input.press("Meta+A")
                    except Exception:
                        pass
                    try:
                        overlay_input.fill("")
                    except Exception:
                        pass
                    overlay_input.type(str(length), delay=30)
                    try:
                        overlay_input.press("Enter")
                    except Exception:
                        pass
                    _confirm_snowboard_length(page, overlay_input)
                    for _ in range(20):
                        page.wait_for_timeout(200)
                        if _length_written():
                            _debug(f"length idx={idx} wrote_ok={expected_row_value}")
                            row_value_ok = True
                            break
                    if row_value_ok:
                        break
                if not row_value_ok:
                    try:
                        wrapper_text = (wrapper.inner_text() or "").strip()
                    except Exception:
                        wrapper_text = ""
                    _debug(f"length idx={idx} failed expected={expected_row_value} wrapper_text={wrapper_text}")
                    break
            if idx < len(lengths) - 1:
                rows_before_add = page.locator("#struct-p-148242406 .fake-input-wrapper").count()
                _debug(f"length idx={idx} add_next rows_before_add={rows_before_add}")
                if _ensure_length_wrapper_count(rows_before_add + 1):
                    current_count = page.locator("#struct-p-148242406 .fake-input-wrapper").count()
                    _debug(f"length idx={idx} add_next increased_to={current_count}")
                else:
                    _debug(f"length idx={idx} add_next failed current_count={page.locator('#struct-p-148242406 .fake-input-wrapper').count()}")
                    break
            else:
                _confirm_snowboard_length(page, row_input)
                page.wait_for_timeout(300)
        _fill_virtualized_sku_table(
            page,
            sku_price,
            lengths,
            length_stock_map,
            multi_color_mode=len(models) > 1,
            color_length_stock_map=color_length_stock_map,
        )
        return True
    except Exception:
        return False


def _try_upload_main_images(page, payload: dict) -> bool:
    image_paths = [str(item).strip() for item in (payload.get("image_paths") or []) if str(item).strip()]
    image_paths = [item for item in image_paths if Path(item).exists()][:5]
    if not image_paths:
        return False
    try:
        for _upload_attempt in range(3):
            for _ in range(12):
                if page.locator("#struct-mainImagesGroup").count() > 0:
                    break
                page.wait_for_timeout(500)
            if page.locator("#struct-mainImagesGroup").count() == 0:
                continue

            if page.locator("iframe#mainImagesGroup").count() == 0:
                open_selectors = [
                    "#struct-mainImagesGroup .empty-container",
                    "#struct-mainImagesGroup .image-empty",
                    "#struct-mainImagesGroup .upload-text",
                ]
                for selector in open_selectors:
                    try:
                        trigger = page.locator(selector).first
                        if trigger.count() == 0:
                            continue
                        trigger.click(force=True)
                        page.wait_for_timeout(1500)
                        if page.locator("iframe#mainImagesGroup").count() > 0:
                            break
                    except Exception:
                        continue
            if page.locator("iframe#mainImagesGroup").count() == 0:
                page.wait_for_timeout(1000)
                continue

            frame = page.frame_locator("iframe#mainImagesGroup")
            uploaded_in_frame = False
            cards = frame.locator(".PicList_PicturesShow_main-show__QVvZn")
            cards_before_upload = 0
            try:
                cards_before_upload = cards.count()
            except Exception:
                cards_before_upload = 0

            upload_btn_selectors = [
                'text=本地上传',
                'button:has-text("本地上传")',
            ]
            for selector in upload_btn_selectors:
                try:
                    upload_btn = frame.locator(selector).first
                    if upload_btn.count() == 0:
                        continue
                    with page.expect_file_chooser(timeout=4000) as fc_info:
                        upload_btn.click(force=True)
                    fc_info.value.set_files(image_paths)
                    page.wait_for_timeout(1200)
                    uploaded_in_frame = True
                    break
                except Exception:
                    continue

            if not uploaded_in_frame:
                frame_input_selectors = [
                    'input[type="file"]',
                    'input[type="file"][accept*="image"]',
                ]
                for selector in frame_input_selectors:
                    try:
                        file_inputs = frame.locator(selector)
                        if file_inputs.count() == 0:
                            continue
                        file_inputs.first.set_input_files(image_paths)
                        page.wait_for_timeout(1200)
                        uploaded_in_frame = True
                        break
                    except Exception:
                        continue

            upload_success = False
            cards_after_upload = cards_before_upload
            if uploaded_in_frame:
                for _ in range(50):
                    try:
                        success_text = (
                            frame.locator(".UploadPanel_actions__lavTs").inner_text(timeout=500) or ""
                        ).strip()
                        match = re.search(r"(\d+)\s*个文件上传成功", success_text)
                        if match and int(match.group(1)) >= len(image_paths):
                            upload_success = True
                    except Exception:
                        pass
                    try:
                        cards_after_upload = max(cards_after_upload, cards.count())
                    except Exception:
                        pass
                    if upload_success:
                        break
                    page.wait_for_timeout(500)

            if not uploaded_in_frame or not upload_success:
                page.wait_for_timeout(1200)
                continue

            complete_selectors = [
                'text=完成',
                'button:has-text("完成")',
                '.next-btn:has-text("完成")',
            ]
            cards_before_complete = cards_after_upload
            for selector in complete_selectors:
                try:
                    complete_btn = frame.locator(selector).first
                    if complete_btn.count() == 0:
                        continue
                    complete_btn.click(force=True)
                    page.wait_for_timeout(1000)
                    break
                except Exception:
                    continue

            stable_rounds = 0
            last_card_count = -1
            for _ in range(50):
                try:
                    current_count = cards.count()
                except Exception:
                    current_count = -1
                if current_count == last_card_count and current_count >= cards_before_complete:
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                    last_card_count = current_count
                if stable_rounds >= 3:
                    break
                page.wait_for_timeout(400)

            cards_after_upload = 0
            for _ in range(24):
                try:
                    cards_after_upload = cards.count()
                except Exception:
                    cards_after_upload = 0
                if cards_after_upload >= len(image_paths):
                    break
                page.wait_for_timeout(500)
            if cards_after_upload <= 0:
                page.wait_for_timeout(1200)
                continue

            selected_count = 0
            target_count = min(len(image_paths), cards_after_upload)
            for _select_round in range(2):
                selected_count = 0
                for idx in range(target_count):
                    card = cards.nth(idx)
                    click_targets = [
                        card.locator(".PicList_pic_imgBox__c0HXw img").first,
                        card.locator(".PicList_pic_imgBox__c0HXw").first,
                        card.locator(".PicList_pic_background__pGTdV > label").first,
                        card.locator(".PicList_pic_background__pGTdV").first,
                        card.locator('input.next-checkbox-input').first,
                    ]
                    clicked = False
                    for target in click_targets:
                        try:
                            if target.count() == 0:
                                continue
                            target.click(force=True)
                            page.wait_for_timeout(500)
                            clicked = True
                            break
                        except Exception:
                            continue
                    if clicked:
                        selected_count += 1
                if selected_count >= target_count:
                    break
                page.wait_for_timeout(800)
            if selected_count >= target_count and target_count > 0:
                page.wait_for_timeout(1500)
                return True

        input_selectors = [
            '#struct-mainImagesGroup input[type="file"]',
            '#sell-field-mainImagesGroup input[type="file"]',
            'input[type="file"][accept*="image"]',
            'input[type="file"]',
        ]
        for selector in input_selectors:
            try:
                file_inputs = page.locator(selector)
                if file_inputs.count() > 0:
                    file_inputs.first.set_input_files(image_paths)
                    page.wait_for_timeout(1500)
                    return True
            except Exception:
                continue

        upload_trigger_selectors = [
            "#struct-mainImagesGroup .empty-container",
            "#struct-mainImagesGroup .image-empty",
            "#struct-mainImagesGroup .upload-text",
        ]
        for selector in upload_trigger_selectors:
            try:
                trigger = page.locator(selector).first
                if trigger.count() == 0:
                    continue
                with page.expect_file_chooser(timeout=3000) as fc_info:
                    trigger.click(force=True)
                fc_info.value.set_files(image_paths)
                page.wait_for_timeout(1500)
                return True
            except Exception:
                continue
    except Exception:
        return False
    return False


def _apply_page_fill_and_overlay(page, payload: dict):
    try:
        _inject_assistant_overlay(page, payload)
    except Exception:
        pass
    try:
        _install_dom_fill_agent(page, payload)
    except Exception:
        pass
    try:
        _best_effort_fill_basic_fields(page, payload)
    except Exception:
        pass


def _try_complete_category_and_brand(page, payload: dict) -> bool:
    category_keyword = str(payload.get("suggested_category_keyword") or "").strip()
    brand_keyword = str(payload.get("suggested_brand_keyword") or "").strip()
    if not category_keyword:
        return False
    try:
        def brand_ready() -> bool:
            try:
                return page.locator('text="品牌"').count() > 0 or page.locator('input[placeholder="请选择"]').count() > 0
            except Exception:
                return False

        def selected_brand_matches() -> bool:
            if not brand_keyword:
                return True
            try:
                selected_brand_value = (
                    page.locator('.sell-catProp-item-select .next-select-values').first.inner_text(timeout=500) or ""
                ).strip()
                return selected_brand_value.lower() == brand_keyword.lower()
            except Exception:
                return False

        if not brand_ready():
            search_input = page.locator('input[placeholder*="可输入产品名称"]').first
            if search_input.count() == 0:
                return False

            current_value = (search_input.input_value() or "").strip()
            if current_value != category_keyword:
                search_input.click(force=True)
                search_input.fill("")
                search_input.type(category_keyword, delay=20)
                page.wait_for_timeout(300)

            search_btn = page.locator('button:has-text("搜索")').first
            if search_btn.count() > 0:
                search_btn.click(force=True)
                page.wait_for_timeout(1200)

        category_candidates = [
            page.locator(f'.sell-component-general-category-result-cate-path span:has-text("{category_keyword}")').last,
            page.locator(f'.sell-component-general-category-result-cate-path:has-text("{category_keyword}")').first,
            page.locator(".sell-component-general-category-result-cate-path").first,
        ]
        clicked_category = False
        if brand_ready():
            clicked_category = True
        else:
            for _ in range(6):
                if brand_ready():
                    clicked_category = True
                    break
                for target in category_candidates:
                    try:
                        if target.count() == 0:
                            continue
                        target.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    try:
                        if target.count() == 0:
                            continue
                        target.click(force=True)
                        page.wait_for_timeout(500)
                    except Exception:
                        pass
                    if not brand_ready():
                        try:
                            if target.count() > 0:
                                target.evaluate("(el) => el.click()")
                                page.wait_for_timeout(500)
                        except Exception:
                            pass
                    if brand_ready():
                        clicked_category = True
                        break
                if clicked_category:
                    break
                page.wait_for_timeout(400)

        brand_selected = selected_brand_matches()
        if brand_keyword and not brand_selected:
            brand_triggers = [
                page.locator('.sell-catProp-item-select input[placeholder="请选择"]').first,
                page.locator('.sell-catProp-item-select .next-select-inner').first,
                page.locator('.sell-catProp-item-select').first,
            ]
            for _ in range(5):
                if selected_brand_matches():
                    brand_selected = True
                    break
                brand_opened = False
                for trigger in brand_triggers:
                    try:
                        if trigger.count() == 0:
                            continue
                        trigger.click(force=True)
                        page.wait_for_timeout(500)
                        if page.locator(".sell-o-select-options").count() > 0:
                            brand_opened = True
                            break
                    except Exception:
                        continue
                if not brand_opened:
                    page.wait_for_timeout(400)
                    continue

                brand_search_candidates = [
                    page.locator(".sell-o-select-options .options-search input").first,
                    page.locator('.sell-o-select-options input[autocomplete="off"]').first,
                ]
                for brand_search in brand_search_candidates:
                    try:
                        if brand_search.count() == 0:
                            continue
                        brand_search.click(force=True)
                        brand_search.fill("")
                        brand_search.type(brand_keyword, delay=40)
                        page.wait_for_timeout(900)
                        break
                    except Exception:
                        continue

                brand_options = [
                    page.locator(f'.sell-o-select-options .options-item[title="{brand_keyword}"]').first,
                    page.locator(f'.sell-o-select-options .options-item[title="{brand_keyword.title()}"]').first,
                    page.locator(f'.sell-o-select-options .options-item[title="{brand_keyword.upper()}"]').first,
                    page.locator(f'.sell-o-select-options .options-item:has(.info-content:text-is("{brand_keyword}"))').first,
                    page.locator(f'.sell-o-select-options .options-item:has(.info-content:text-is("{brand_keyword.title()}"))').first,
                    page.locator(f'.sell-o-select-options .options-item:has-text("{brand_keyword}")').first,
                ]
                clicked_brand = False
                for option in brand_options:
                    try:
                        if option.count() == 0:
                            continue
                        option.click(force=True)
                        page.wait_for_timeout(700)
                        clicked_brand = True
                        break
                    except Exception:
                        continue
                if not clicked_brand:
                    page.wait_for_timeout(500)
                    continue
                for _ in range(6):
                    if selected_brand_matches():
                        brand_selected = True
                        break
                    page.wait_for_timeout(300)
                if brand_selected:
                    break

        next_btn = page.locator('button:has-text("确认，下一步")').first
        if next_btn.count() == 0:
            next_btn = page.locator('button:has-text("下一步")').first
        if next_btn.count() > 0 and (not brand_keyword or brand_selected):
            next_btn.click(force=True)
            page.wait_for_timeout(1500)
            return True
        return clicked_category or brand_selected
    except Exception:
        return False


def _stop_dom_fill_agent(page):
    try:
        page.evaluate(
            """
            () => {
              if (window.__TB_DOM_FILL_TIMER__) {
                clearInterval(window.__TB_DOM_FILL_TIMER__);
                window.__TB_DOM_FILL_TIMER__ = null;
              }
              window.__TB_SNOWBOARD_FILL_DONE__ = true;
              window.__TB_SNOWBOARD_FILL_RUNNING__ = false;
            }
            """
        )
    except Exception:
        pass


def _retry_fill_basic_fields(page, payload: dict, attempts: int = 4):
    for _ in range(attempts):
        _best_effort_fill_basic_fields(page, payload)
        try:
            title = truncate_weighted_text(str(payload.get("title") or "").strip(), 60)
            short_title = truncate_weighted_text(str(payload.get("title") or "").strip(), 30)
            total_stock = str(compute_total_stock(str(payload.get("stock") or "").strip()))
            title_ok = False
            short_ok = False
            stock_ok = False
            if page.locator("#sell-field-title input").count() > 0:
                title_ok = (page.locator("#sell-field-title input").first.input_value() or "") == title
            if page.locator("#sell-field-shopping_title input").count() > 0:
                short_ok = (page.locator("#sell-field-shopping_title input").first.input_value() or "") == short_title
            if page.locator("#struct-quantity input").count() > 0:
                stock_ok = (page.locator("#struct-quantity input").first.input_value() or "") == total_stock
            if title_ok and short_ok and stock_ok:
                return True
        except Exception:
            pass
        page.wait_for_timeout(400)
    return False


def _open_browser(shop_row: dict, payload: dict | None = None, payloads: list[dict] | None = None):
    publish_url = str(shop_row.get("publish_url") or "").strip() or DEFAULT_TAOBAO_PUBLISH_URL
    login_url = str(shop_row.get("login_url") or "").strip() or DEFAULT_TAOBAO_LOGIN_URL

    with sync_playwright() as p:
        browser, context, attached = connect_or_launch_context(p, shop_row)
        page = context.new_page()
        page.goto(login_url if not payload and not payloads else publish_url, wait_until="domcontentloaded", timeout=60000)
        if payload:
            _inject_assistant_overlay(page, payload)
        extra_payloads = payloads or []
        for item in extra_payloads:
            extra_page = context.new_page()
            extra_page.goto(publish_url, wait_until="domcontentloaded", timeout=60000)
            _inject_assistant_overlay(extra_page, item)
        deadline = time.time() + 60
        publish_page_applied = False
        main_images_uploaded = False
        snowboard_attempted = False
        while time.time() < deadline:
            if payload:
                try:
                    if page.locator('input[placeholder*="可输入产品名称"]').count() > 0 and page.locator("#sell-field-title input").count() == 0:
                        _try_complete_category_and_brand(page, payload)
                except Exception:
                    pass
                if not publish_page_applied:
                    try:
                        if page.locator("#sell-field-title input").count() > 0:
                            _apply_page_fill_and_overlay(page, payload)
                            _retry_fill_basic_fields(page, payload)
                            publish_page_applied = True
                    except Exception:
                        pass
                if publish_page_applied and not main_images_uploaded:
                    try:
                        main_images_uploaded = _try_upload_main_images(page, payload)
                    except Exception:
                        pass
                if publish_page_applied and not snowboard_attempted:
                    is_snowboard = str(payload.get("category") or "").strip() == "滑雪板"
                    if is_snowboard:
                        try:
                            if page.locator("#struct-p-148242406 .fake-input-wrapper").count() > 0:
                                filled = _try_fill_snowboard_sales_once(page, payload)
                                if filled:
                                    _stop_dom_fill_agent(page)
                                    snowboard_attempted = True
                                    break
                        except Exception:
                            pass
            page.wait_for_timeout(500)
        try:
            if not page.is_closed():
                page.bring_to_front()
                page.wait_for_timeout(1000 * 27 * 60)
        except Exception:
            pass
        try:
            if attached:
                if browser:
                    browser.close()
            else:
                context.close()
        except Exception:
            pass


def launch_login_browser(shop_row: dict):
    thread = threading.Thread(target=_open_browser, args=(shop_row, None), daemon=True)
    thread.start()


def launch_publish_assistant(shop_row: dict, payload: dict):
    thread = threading.Thread(target=_open_browser, args=(shop_row, payload), daemon=True)
    thread.start()


def launch_publish_assistants(shop_row: dict, payloads: list[dict]):
    if not payloads:
        return
    first = payloads[0]
    rest = payloads[1:]
    thread = threading.Thread(target=_open_browser, args=(shop_row, first, rest), daemon=True)
    thread.start()
