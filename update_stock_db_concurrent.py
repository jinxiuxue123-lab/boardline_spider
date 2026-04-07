import asyncio
import os
import random
import re
import threading
import time
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

import requests
from playwright.async_api import async_playwright

from discount_rules import calculate_latest_discount_price, load_discount_rules
from db_utils import (
    get_latest_update,
    upsert_product_update,
    insert_change_log,
    get_connection,
    update_product_image_info,
    delete_product_by_id,
)
from pricing_rules import calculate_cny_pricing, load_pricing_rules
from xianyu_open.auto_attributes import apply_auto_attributes_for_product
from daily_run_tracker import get_env_run_id, get_env_step_key, update_step_progress

PROGRESS_FILE = "stock_db_progress_concurrent.txt"
FAILED_FILE = "failed_stock_urls.txt"

TEST_LIMIT = None      # 测试时抓 100，正式跑全部时改成 None
CONCURRENCY = 1      # 详情页先单线程，优先避免触发 boardline 详情限流
BATCH_SIZE = 30      # 每批处理多少个商品后写一次进度
MAX_FAILED_RETRY_ROUNDS = 2
DEBUG_STOCK = True   # 是否打印库存解析结果
BASE_URL = "http://www.boardline.co.kr"
SOURCE_NAME = "boardline"
DISCOUNT_RULES = load_discount_rules()
PRICING_RULES = load_pricing_rules()
PLAYWRIGHT_HEADLESS = (os.getenv("PLAYWRIGHT_HEADLESS", "1").strip().lower() not in ("0", "false", "no"))
DETAIL_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE_URL,
}
DETAIL_SESSION = requests.Session()
DETAIL_SESSION.headers.update(DETAIL_REQUEST_HEADERS)
DETAIL_DEBUG_DIR = Path("logs/boardline_detail_debug")
DETAIL_REQUEST_MIN_INTERVAL_SECONDS = float(os.getenv("BOARDLINE_DETAIL_REQUEST_MIN_INTERVAL", "1.2"))
DETAIL_REQUEST_RETRY_COUNT = int(os.getenv("BOARDLINE_DETAIL_REQUEST_RETRY_COUNT", "3"))
DETAIL_REQUEST_BACKOFF_SECONDS = float(os.getenv("BOARDLINE_DETAIL_REQUEST_BACKOFF_SECONDS", "2.5"))
DETAIL_REQUEST_SLEEP_MIN_SECONDS = float(os.getenv("BOARDLINE_DETAIL_REQUEST_SLEEP_MIN", "2.5"))
DETAIL_REQUEST_SLEEP_MAX_SECONDS = float(os.getenv("BOARDLINE_DETAIL_REQUEST_SLEEP_MAX", "3.75"))
DETAIL_REQUEST_LOCK = threading.Lock()
DETAIL_LAST_REQUEST_AT = 0.0


def clean_name(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"[가-힣]", "", text)
    season_match = re.search(r"\d{2}/\d{2}", text)
    if season_match:
        # 如果清掉韩文后前面残留了零散数字，优先从雪季前缀开始保留。
        text = text[season_match.start():]
    return " ".join(text.split())


def clean_option_text(text, preserve_numeric_parens=False):
    if not text:
        return ""
    text = text.strip()
    # 只清理类似 "1.选项名" / "1. 选项名" 这种序号前缀，避免把鞋码 7.5 误清成 5
    text = re.sub(r"^\d+\.\s*", "", text)
    if not preserve_numeric_parens:
        text = re.sub(r"\(\s*[+\-]?\d[\d,\s]*\)", "", text)
    text = re.sub(r"\(\)", "", text)
    text = clean_name(text)
    return text.strip(" /-")


def is_placeholder_option(text, value):
    text = (text or "").strip()
    value = (value or "").strip()

    if text == "":
        return True

    keywords = ["선택", "옵션", "색상", "사이즈", "SIZE", "COLOR"]
    for k in keywords:
        if k.lower() in text.lower():
            return True

    if value == "":
        return True

    return False


def parse_positive_stock_count(stock_cnt):
    stock_text = (stock_cnt or "").strip()
    match = re.search(r"\d+", stock_text)
    if not match:
        return None
    qty = int(match.group())
    if qty <= 0:
        return None
    return qty


def read_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            try:
                return int(f.read().strip())
            except Exception:
                return 0
    return 0


def write_progress(i):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        f.write(str(i))


def clear_file(path):
    with open(path, "w", encoding="utf-8") as f:
        f.write("")


def append_failed(url):
    with open(FAILED_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")


def clear_failed_file():
    with open(FAILED_FILE, "w", encoding="utf-8") as f:
        f.write("")


def guess_ext(url):
    path = urlparse(url).path.lower()
    if path.endswith(".png"):
        return ".png"
    if path.endswith(".webp"):
        return ".webp"
    return ".jpg"


def download_image(image_url, branduid):
    if not image_url:
        return None

    ext = guess_ext(image_url)

    save_dir = Path(f"data/images/{SOURCE_NAME}/{branduid}")
    save_dir.mkdir(parents=True, exist_ok=True)

    save_path = save_dir / f"main{ext}"

    if save_path.exists() and save_path.stat().st_size > 0:
        return str(save_path)

    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": BASE_URL,
        }

        resp = requests.get(image_url, headers=headers, timeout=20)
        resp.raise_for_status()

        with open(save_path, "wb") as f:
            f.write(resp.content)

        return str(save_path)
    except Exception as e:
        print(f"详情图下载失败: {image_url} | {e}")
        return None


def decode_boardline_html(response):
    try:
        return response.content.decode("euc-kr")
    except UnicodeDecodeError:
        return response.content.decode("euc-kr", errors="ignore")


def has_valid_local_image(local_image_path):
    if not local_image_path:
        return False

    path = Path(local_image_path)
    return path.exists() and path.is_file() and path.stat().st_size > 0


def get_active_products(limit=None):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
    SELECT id, branduid, name, url, category, image_url, local_image_path, COALESCE(detail_image_fetched, 0)
    FROM products
    WHERE source='boardline' AND status='active'
    ORDER BY id
    """

    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows


def compare_and_log(
    product_id,
    old_row,
    price,
    original_price,
    latest_discount_price,
    price_cny,
    original_price_cny,
    shipping_fee_cny,
    final_price_cny,
    exchange_rate,
    profit_rate,
    stock,
):
    old_price = ""
    old_original = ""
    old_stock = ""
    old_latest_discount = ""
    old_price_cny = ""
    old_original_price_cny = ""
    old_shipping_fee_cny = ""
    old_final_price_cny = ""
    old_exchange_rate = ""
    old_profit_rate = ""

    if old_row:
        old_price = old_row[0] or ""
        old_original = old_row[1] or ""
        old_stock = old_row[2] or ""
        if len(old_row) > 3:
            old_latest_discount = old_row[3] or ""
        if len(old_row) > 4:
            old_price_cny = old_row[4] or ""
        if len(old_row) > 5:
            old_original_price_cny = old_row[5] or ""
        if len(old_row) > 6:
            old_shipping_fee_cny = old_row[6] or ""
        if len(old_row) > 7:
            old_final_price_cny = old_row[7] or ""
        if len(old_row) > 8:
            old_exchange_rate = old_row[8] or ""
        if len(old_row) > 9:
            old_profit_rate = old_row[9] or ""

    if old_price != (price or ""):
        insert_change_log(product_id, "price", old_price, price or "")

    if old_original != (original_price or ""):
        insert_change_log(product_id, "original_price", old_original, original_price or "")

    if old_latest_discount != (latest_discount_price or ""):
        insert_change_log(
            product_id,
            "latest_discount_price",
            old_latest_discount,
            latest_discount_price or "",
        )

    if old_price_cny != (price_cny or ""):
        insert_change_log(product_id, "price_cny", old_price_cny, price_cny or "")

    if old_original_price_cny != (original_price_cny or ""):
        insert_change_log(
            product_id,
            "original_price_cny",
            old_original_price_cny,
            original_price_cny or "",
        )

    if old_shipping_fee_cny != (shipping_fee_cny or ""):
        insert_change_log(
            product_id,
            "shipping_fee_cny",
            old_shipping_fee_cny,
            shipping_fee_cny or "",
        )

    if old_final_price_cny != (final_price_cny or ""):
        insert_change_log(
            product_id,
            "final_price_cny",
            old_final_price_cny,
            final_price_cny or "",
        )

    if old_exchange_rate != (exchange_rate or ""):
        insert_change_log(
            product_id,
            "exchange_rate",
            old_exchange_rate,
            exchange_rate or "",
        )

    if old_profit_rate != (profit_rate or ""):
        insert_change_log(
            product_id,
            "profit_rate",
            old_profit_rate,
            profit_rate or "",
        )

    if old_stock != (stock or ""):
        insert_change_log(product_id, "stock", old_stock, stock or "")


def strip_tags(text):
    text = re.sub(r"<[^>]+>", " ", text or "", flags=re.S)
    return " ".join(unescape(text).split()).strip()


class FirstClassTextParser(HTMLParser):
    def __init__(self, target_classes):
        super().__init__()
        self.target_classes = set(target_classes)
        self.capture_depth = 0
        self.parts = []
        self.done = False

    def handle_starttag(self, tag, attrs):
        if self.done:
            return
        attrs_dict = dict(attrs)
        classes = set((attrs_dict.get("class") or "").split())
        if self.capture_depth > 0:
            self.capture_depth += 1
            return
        if self.target_classes.intersection(classes):
            self.capture_depth = 1

    def handle_endtag(self, tag):
        if self.done:
            return
        if self.capture_depth > 0:
            self.capture_depth -= 1
            if self.capture_depth == 0 and self.parts:
                self.done = True

    def handle_data(self, data):
        if self.done:
            return
        if self.capture_depth > 0:
            self.parts.append(data)


def extract_first_text_by_class(html_text, class_name):
    parser = FirstClassTextParser({class_name})
    parser.feed(html_text or "")
    return " ".join("".join(parser.parts).split()).strip()


def extract_title_text(html_text):
    parser = FirstClassTextParser({"tit-prd"})
    parser.feed(html_text or "")
    return clean_name("".join(parser.parts))


def extract_detail_image_url_from_html(html_text):
    patterns = [
        r'<img[^>]+class="[^"]*\bdetail_image\b[^"]*"[^>]+src="([^"]+)"',
        r'<img[^>]+src="([^"]+)"[^>]+class="[^"]*\bdetail_image\b[^"]*"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text or "", flags=re.I | re.S)
        if match:
            src = (match.group(1) or "").strip()
            if src.startswith("/"):
                return BASE_URL + src
            return src
    return ""


def extract_select_html(html_text, select_id):
    pattern = rf'<select[^>]+id="{re.escape(select_id)}"[^>]*>(.*?)</select>'
    match = re.search(pattern, html_text or "", flags=re.I | re.S)
    return match.group(1) if match else ""


def parse_attrs(attr_text):
    attrs = {}
    for key, value in re.findall(r'([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*"([^"]*)"', attr_text or ""):
        attrs[key.lower()] = value
    for key, value in re.findall(r"([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*'([^']*)'", attr_text or ""):
        attrs[key.lower()] = value
    return attrs


def extract_option_items(select_html):
    items = []
    for attr_text, inner_html in re.findall(r"<option([^>]*)>(.*?)</option>", select_html or "", flags=re.I | re.S):
        attrs = parse_attrs(attr_text)
        items.append(
            {
                "text": strip_tags(inner_html),
                "value": attrs.get("value", ""),
                "stock_cnt": attrs.get("stock_cnt", ""),
            }
        )
    return items


def parse_stock_single_from_options(options, preserve_numeric_parens=False):
    valid_options = []
    for opt in options:
        text = (opt.get("text") or "").strip()
        value = (opt.get("value") or "").strip()
        stock_cnt = opt.get("stock_cnt") or ""

        if is_placeholder_option(text, value):
            continue

        qty = parse_positive_stock_count(stock_cnt)
        if qty is None:
            continue

        text = clean_option_text(text, preserve_numeric_parens=preserve_numeric_parens)
        valid_options.append((text, str(qty)))

    if len(valid_options) == 1:
        text, stock_cnt = valid_options[0]
        if len(text) > 25:
            return [f"ONE SIZE:{stock_cnt}"]

    return [f"{text}:{stock_cnt}" for text, stock_cnt in valid_options]


def parse_stock_from_script_data(html_text, preserve_numeric_parens=False):
    html_text = html_text or ""
    stock_map = {}
    pattern = re.compile(
        r'["\']option_name["\']\s*:\s*["\']([^"\']+)["\'].*?["\']stock_cnt["\']\s*:\s*["\']?(\d+)["\']?',
        flags=re.I | re.S,
    )
    matches = pattern.findall(html_text)
    if not matches:
        return []

    for raw_name, raw_qty in matches:
        qty = parse_positive_stock_count(raw_qty)
        if qty is None:
            continue

        option_name = clean_option_text(raw_name, preserve_numeric_parens=preserve_numeric_parens)
        if not option_name:
            continue

        parts = [part.strip() for part in re.split(r"\s*[,/]\s*", option_name) if part.strip()]
        if len(parts) >= 2:
            color_name = parts[0]
            size_name = "/".join(parts[1:])
            stock_map.setdefault(color_name, []).append(f"{size_name}:{qty}")
        else:
            stock_map.setdefault("__single__", []).append(f"{option_name}:{qty}")

    if not stock_map:
        return []

    if set(stock_map.keys()) == {"__single__"}:
        return stock_map["__single__"]

    result = []
    for color_name, sizes in stock_map.items():
        if color_name == "__single__":
            result.extend(sizes)
        else:
            result.append(f"{color_name}({','.join(sizes)})")
    return result


def parse_stockinfo_map(html_text):
    match = re.search(r"var\s+stockInfo\s*=\s*(\{.*?\})\s*;", html_text or "", flags=re.I | re.S)
    if not match:
        return {}

    stock_map = {}
    for key, value in re.findall(r'"([^"]+)"\s*:\s*(-?\d+)', match.group(1)):
        try:
            stock_map[key] = int(value)
        except ValueError:
            continue
    return stock_map


def parse_stock_two_level_from_options(select0_options, select1_options, stockinfo_map, preserve_numeric_parens=False):
    if not select0_options or not select1_options or not stockinfo_map:
        return []

    stock_map = {}
    valid_first = []
    valid_second = []

    for opt in select0_options:
        if is_placeholder_option(opt.get("text"), opt.get("value")):
            continue
        valid_first.append(opt)

    for opt in select1_options:
        if is_placeholder_option(opt.get("text"), opt.get("value")):
            continue
        valid_second.append(opt)

    for first_index, first_opt in enumerate(valid_first):
        first_name = clean_option_text(first_opt.get("text") or "", preserve_numeric_parens=preserve_numeric_parens)
        if not first_name:
            continue

        sizes = []
        for second_index, second_opt in enumerate(valid_second):
            qty = stockinfo_map.get(f"{first_index},{second_index}")
            if qty is None or qty <= 0:
                continue

            second_name = clean_option_text(second_opt.get("text") or "", preserve_numeric_parens=preserve_numeric_parens)
            if not second_name:
                continue

            sizes.append(f"{second_name}:{qty}")

        if sizes:
            stock_map[first_name] = sizes

    if not stock_map:
        return []

    result = []
    for first_name, sizes in stock_map.items():
        result.append(f"{first_name}({','.join(sizes)})")
    return result


def html_has_soldout_marker(html_text):
    lowered = (html_text or "").lower()
    return any(marker in lowered for marker in ("품절", "sold out", "soldout"))


def ensure_product_image_from_url(
    product_id,
    branduid,
    current_image_url,
    current_local_image_path,
    detail_image_fetched,
    detail_image_url,
):
    if int(detail_image_fetched or 0) == 1 and has_valid_local_image(current_local_image_path):
        return current_local_image_path

    final_image_url = (detail_image_url or current_image_url or "").strip()
    if not final_image_url:
        return current_local_image_path

    if final_image_url.startswith("/"):
        final_image_url = BASE_URL + final_image_url

    local_image_path = download_image(final_image_url, branduid)
    if local_image_path:
        update_product_image_info(product_id, final_image_url, local_image_path, detail_image_fetched=1)
        return local_image_path

    if final_image_url != (current_image_url or ""):
        update_product_image_info(product_id, final_image_url, None, detail_image_fetched=0)
    return current_local_image_path


def save_detail_debug_html(branduid, html_text, reason):
    try:
        DETAIL_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        safe_reason = re.sub(r"[^A-Za-z0-9_-]+", "_", reason or "unknown").strip("_") or "unknown"
        debug_path = DETAIL_DEBUG_DIR / f"{branduid}_{safe_reason}.html"
        if not debug_path.exists():
            debug_path.write_text(html_text or "", encoding="utf-8")
            print(f"已保存详情调试HTML: {debug_path}")
    except Exception as e:
        print(f"保存详情调试HTML失败: {branduid} | {e}")


def summarize_script_signals(html_text):
    signals = []
    lowered = (html_text or "").lower()
    for keyword in (
        "stock_cnt",
        "mk_p_s_0",
        "mk_p_s_1",
        "option",
        "var ",
        "function",
    ):
        if keyword in lowered:
            signals.append(keyword)
    return ",".join(signals)


def is_detail_rate_limited(html_text):
    lowered = (html_text or "").lower()
    markers = [
        "초단위로 페이지를 너무 많이 요청",
        "서버보호차원에서 차단",
        "특정ip",
        "로봇에 의한 페이지수집",
    ]
    return any(marker.lower() in lowered for marker in markers)


def is_product_not_found_page(html_text):
    lowered = (html_text or "").lower()
    markers = [
        "존재하지 않는 상품입니다",
        "존재하지않는 상품입니다",
    ]
    return any(marker.lower() in lowered for marker in markers)


def throttle_detail_request():
    global DETAIL_LAST_REQUEST_AT
    with DETAIL_REQUEST_LOCK:
        random_sleep = random.uniform(DETAIL_REQUEST_SLEEP_MIN_SECONDS, DETAIL_REQUEST_SLEEP_MAX_SECONDS)
        time.sleep(random_sleep)
        now = time.monotonic()
        wait_seconds = DETAIL_REQUEST_MIN_INTERVAL_SECONDS - (now - DETAIL_LAST_REQUEST_AT)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        DETAIL_LAST_REQUEST_AT = time.monotonic()


def fetch_detail_html_with_retry(url, branduid):
    last_html = ""
    last_error = None
    for attempt in range(1, DETAIL_REQUEST_RETRY_COUNT + 1):
        throttle_detail_request()
        try:
            response = DETAIL_SESSION.get(url, timeout=20)
            response.raise_for_status()
            html_text = decode_boardline_html(response)
            last_html = html_text
            if is_product_not_found_page(html_text):
                raise RuntimeError("product_not_found")
            if not is_detail_rate_limited(html_text):
                return html_text

            print(f"详情页请求被限流，等待后重试: {branduid} | {attempt}/{DETAIL_REQUEST_RETRY_COUNT}")
            last_error = RuntimeError("detail_rate_limited")
        except Exception as e:
            last_error = e

        if attempt < DETAIL_REQUEST_RETRY_COUNT:
            sleep_seconds = DETAIL_REQUEST_BACKOFF_SECONDS * attempt
            time.sleep(sleep_seconds)

    if last_html and is_detail_rate_limited(last_html):
        save_detail_debug_html(branduid, last_html, "rate_limited")
        raise RuntimeError("detail_rate_limited")

    if last_html and is_product_not_found_page(last_html):
        save_detail_debug_html(branduid, last_html, "product_not_found")
        raise RuntimeError("product_not_found")

    if last_error:
        raise last_error
    raise RuntimeError("detail_request_failed")


def fetch_detail_via_requests_sync(product_tuple):
    product_id, branduid, name, url, category, image_url, local_image_path, detail_image_fetched = product_tuple

    html_text = fetch_detail_html_with_retry(url, branduid)

    title = extract_title_text(html_text)
    resolved_name = clean_name(title) if title else (name or "")

    price = extract_first_text_by_class(html_text, "price")
    original_price = extract_first_text_by_class(html_text, "consumer")
    detail_image_url = extract_detail_image_url_from_html(html_text)

    if not price and not original_price:
        save_detail_debug_html(branduid, html_text, "missing_price")
        return {
            "ok": False,
            "fallback": True,
            "reason": f"静态价格未解析到|signals={summarize_script_signals(html_text)}",
        }

    preserve_numeric_parens = "滑雪鞋" in (category or "")
    select0_html = extract_select_html(html_text, "MK_p_s_0")
    select1_html = extract_select_html(html_text, "MK_p_s_1")
    select0_options = extract_option_items(select0_html)
    select1_options = extract_option_items(select1_html)
    stockinfo_map = parse_stockinfo_map(html_text)
    script_stock_items = parse_stock_from_script_data(
        html_text,
        preserve_numeric_parens=preserve_numeric_parens,
    )

    stock_items = []
    used_requests = False
    if "滑雪板" in (category or ""):
        stock_items = parse_stock_single_from_options(
            select0_options,
            preserve_numeric_parens=preserve_numeric_parens,
        )
        used_requests = True
    elif select0_options and select1_options and stockinfo_map:
        stock_items = parse_stock_two_level_from_options(
            select0_options,
            select1_options,
            stockinfo_map,
            preserve_numeric_parens=preserve_numeric_parens,
        )
        used_requests = True
    elif script_stock_items:
        stock_items = script_stock_items
        used_requests = True
    elif select0_options and not select1_options:
        stock_items = parse_stock_single_from_options(
            select0_options,
            preserve_numeric_parens=preserve_numeric_parens,
        )
        used_requests = True
    elif not select0_options and not select1_options:
        stock_items = [] if html_has_soldout_marker(html_text) else ["ONE SIZE:1"]
        used_requests = True

    if not used_requests:
        save_detail_debug_html(branduid, html_text, "dynamic_options")
        return {
            "ok": False,
            "fallback": True,
            "reason": f"需要动态解析联动库存|signals={summarize_script_signals(html_text)}",
        }

    local_image_path = ensure_product_image_from_url(
        product_id,
        branduid,
        image_url,
        local_image_path,
        detail_image_fetched,
        detail_image_url,
    )

    stock = " | ".join(stock_items)
    latest_discount_price = calculate_latest_discount_price(
        original_price,
        resolved_name,
        category,
        DISCOUNT_RULES,
    )
    cny_pricing = calculate_cny_pricing(
        category,
        price,
        original_price,
        latest_discount_price,
        PRICING_RULES,
    )

    return {
        "ok": True,
        "used_requests": True,
        "product_id": product_id,
        "branduid": branduid,
        "category": category,
        "url": url,
        "name": resolved_name,
        "price": price,
        "original_price": original_price,
        "latest_discount_price": latest_discount_price,
        "price_cny": cny_pricing["price_cny"],
        "original_price_cny": cny_pricing["original_price_cny"],
        "shipping_fee_cny": cny_pricing["shipping_fee_cny"],
        "final_price_cny": cny_pricing["final_price_cny"],
        "exchange_rate": cny_pricing["exchange_rate"],
        "profit_rate": cny_pricing["profit_rate"],
        "stock": stock,
    }


async def parse_stock_single(page, preserve_numeric_parens=False):
    stock_list = []
    options = await page.query_selector_all("#MK_p_s_0 option")

    valid_options = []

    for opt in options:
        text = (await opt.inner_text()).strip()
        value = await opt.get_attribute("value")
        stock_cnt = await opt.get_attribute("stock_cnt")

        if is_placeholder_option(text, value):
            continue

        qty = parse_positive_stock_count(stock_cnt)
        if qty is None:
            continue

        text = clean_option_text(text, preserve_numeric_parens=preserve_numeric_parens)
        valid_options.append((text, str(qty)))

    # 只有一个有效选项，并且文字特别长，视为单一规格商品
    if len(valid_options) == 1:
        text, stock_cnt = valid_options[0]
        if len(text) > 25:
            return [f"ONE SIZE:{stock_cnt}"]

    for text, stock_cnt in valid_options:
        stock_list.append(f"{text}:{stock_cnt}")

    return stock_list


async def parse_stock_two_level(page, preserve_numeric_parens=False):
    stock_map = {}

    select0 = await page.query_selector("#MK_p_s_0")
    select1 = await page.query_selector("#MK_p_s_1")

    if not select0 or not select1:
        return []

    colors = await page.query_selector_all("#MK_p_s_0 option")

    for color in colors:
        color_text = (await color.inner_text()).strip()
        color_value = await color.get_attribute("value")

        if is_placeholder_option(color_text, color_value):
            continue

        color_name = clean_option_text(color_text, preserve_numeric_parens=preserve_numeric_parens)

        try:
            await page.select_option("#MK_p_s_0", value=color_value)
            await page.wait_for_timeout(500)

            sizes = await page.query_selector_all("#MK_p_s_1 option")

            for size in sizes:
                size_text = (await size.inner_text()).strip()
                size_value = await size.get_attribute("value")
                stock_cnt = await size.get_attribute("stock_cnt")

                if is_placeholder_option(size_text, size_value):
                    continue

                qty = parse_positive_stock_count(stock_cnt)
                if qty is None:
                    continue

                size_name = clean_option_text(size_text, preserve_numeric_parens=preserve_numeric_parens)

                if color_name not in stock_map:
                    stock_map[color_name] = []

                stock_map[color_name].append(f"{size_name}:{qty}")

        except Exception as e:
            print("联动库存解析失败:", color_name, e)

    result = []
    for color, sizes in stock_map.items():
        result.append(f"{color}({','.join(sizes)})")

    return result


async def parse_snowboard_stock(page):
    """
    滑雪板类目专用：
    只解析 MK_p_s_0
    """
    stock_list = []
    options = await page.query_selector_all("#MK_p_s_0 option")

    for opt in options:
        text = (await opt.inner_text()).strip()
        value = await opt.get_attribute("value")
        stock_cnt = await opt.get_attribute("stock_cnt")

        if is_placeholder_option(text, value):
            continue

        qty = parse_positive_stock_count(stock_cnt)
        if qty is None:
            continue

        text = clean_option_text(text)
        stock_list.append(f"{text}:{qty}")

    return stock_list


async def has_soldout_marker(page):
    markers = [
        "품절",
        "SOLD OUT",
        "sold out",
        "soldout",
    ]
    body_text = ""
    try:
        body = await page.query_selector("body")
        if body:
            body_text = ((await body.inner_text()) or "").strip()
    except Exception:
        body_text = ""

    lowered = body_text.lower()
    for marker in markers:
        if marker.lower() in lowered:
            return True

    soldout_selectors = [
        ".soldout",
        ".sold-out",
        ".xans-product-soldout",
        "img[alt*='품절']",
        "img[alt*='SOLD OUT']",
    ]
    for selector in soldout_selectors:
        try:
            if await page.locator(selector).count() > 0:
                return True
        except Exception:
            continue

    return False


async def parse_stock_by_category(page, category):
    """
    如果是滑雪板，只解析 MK_p_s_0
    其他类目按原逻辑
    """
    category = (category or "").strip()
    preserve_numeric_parens = "滑雪鞋" in category

    if "滑雪板" in category:
        stock_items = await parse_snowboard_stock(page)
    else:
        select0 = await page.query_selector("#MK_p_s_0")
        select1 = await page.query_selector("#MK_p_s_1")

        if select0 and select1:
            stock_items = await parse_stock_two_level(page, preserve_numeric_parens=preserve_numeric_parens)
        elif select0:
            stock_items = await parse_stock_single(page, preserve_numeric_parens=preserve_numeric_parens)
        else:
            stock_items = []

    if stock_items:
        return stock_items

    if await has_soldout_marker(page):
        return []

    return ["ONE SIZE:1"]


async def ensure_product_image(page, product_id, branduid, current_image_url, current_local_image_path, detail_image_fetched):
    if int(detail_image_fetched or 0) == 1 and has_valid_local_image(current_local_image_path):
        return current_local_image_path

    detail_image_url = ""

    img_el = await page.query_selector("#zoom_image img.detail_image")
    if not img_el:
        img_el = await page.query_selector("img.detail_image")
    if img_el:
        detail_image_url = (await img_el.get_attribute("src")) or detail_image_url

    if not detail_image_url:
        detail_image_url = current_image_url or ""

    if detail_image_url.startswith("/"):
        detail_image_url = BASE_URL + detail_image_url

    if not detail_image_url:
        return current_local_image_path

    local_image_path = download_image(detail_image_url, branduid)
    if local_image_path:
        update_product_image_info(product_id, detail_image_url, local_image_path, detail_image_fetched=1)
        print(f"详情页补图成功: {branduid}")
        return local_image_path

    if detail_image_url != (current_image_url or ""):
        update_product_image_info(product_id, detail_image_url, None, detail_image_fetched=0)

    return current_local_image_path


async def fetch_one(browser, product_tuple, sem, idx, total):
    product_id, branduid, name, url, category, image_url, local_image_path, detail_image_fetched = product_tuple

    async with sem:
        try:
            print(f"更新 {idx}/{total}: {branduid} [{category}]")

            try:
                request_result = await asyncio.to_thread(fetch_detail_via_requests_sync, product_tuple)
            except Exception as request_error:
                reason = str(request_error)
                if "product_not_found" in reason:
                    return {
                        "ok": False,
                        "skip_delete": True,
                        "product_id": product_id,
                        "branduid": branduid,
                        "category": category,
                        "url": url,
                        "error": "商品不存在，已从数据库删除",
                    }
                if "detail_rate_limited" in reason:
                    reason = "详情页requests被限流"
                request_result = {
                    "ok": False,
                    "fallback": True,
                    "reason": f"requests解析失败: {reason}",
                }

            if request_result.get("ok"):
                apply_auto_attributes_for_product(product_id, category, name, request_result["stock"])
                log_detail_result(request_result, "requests")
                return request_result

            if DEBUG_STOCK:
                print(f"回退浏览器 -> {branduid} | {category} | {request_result.get('reason', 'unknown')}")

            page = await browser.new_page()

            await page.goto(url, timeout=60000)
            await page.wait_for_selector("h3.cboth.tit-prd", timeout=10000)
            await page.wait_for_timeout(800)

            price_el = await page.query_selector(".price")
            price = (await price_el.inner_text()).strip() if price_el else ""

            original_el = await page.query_selector(".consumer")
            original_price = (await original_el.inner_text()).strip() if original_el else ""
            latest_discount_price = calculate_latest_discount_price(
                original_price,
                name,
                category,
                DISCOUNT_RULES,
            )
            cny_pricing = calculate_cny_pricing(
                category,
                price,
                original_price,
                latest_discount_price,
                PRICING_RULES,
            )

            local_image_path = await ensure_product_image(
                page,
                product_id,
                branduid,
                image_url,
                local_image_path,
                detail_image_fetched,
            )

            stock_items = await parse_stock_by_category(page, category)
            stock = " | ".join(stock_items)

            apply_auto_attributes_for_product(product_id, category, name, stock)

            log_detail_result(
                {
                    "branduid": branduid,
                    "price": price,
                    "original_price": original_price,
                    "stock": stock,
                },
                "playwright",
            )

            return {
                "ok": True,
                "product_id": product_id,
                "branduid": branduid,
                "category": category,
                "url": url,
                "price": price,
                "original_price": original_price,
                "latest_discount_price": latest_discount_price,
                "price_cny": cny_pricing["price_cny"],
                "original_price_cny": cny_pricing["original_price_cny"],
                "shipping_fee_cny": cny_pricing["shipping_fee_cny"],
                "final_price_cny": cny_pricing["final_price_cny"],
                "exchange_rate": cny_pricing["exchange_rate"],
                "profit_rate": cny_pricing["profit_rate"],
                "stock": stock,
            }

        except Exception as e:
            return {
                "ok": False,
                "product_id": product_id,
                "branduid": branduid,
                "category": category,
                "url": url,
                "error": str(e),
            }

        finally:
            if "page" in locals():
                await page.close()


async def process_batch(browser, batch_products, sem, start_no, total):
    tasks = []
    for offset, product in enumerate(batch_products):
        idx = start_no + offset + 1
        tasks.append(fetch_one(browser, product, sem, idx, total))
    return await asyncio.gather(*tasks)


def persist_failed_urls(failed_products):
    clear_failed_file()
    for product in failed_products:
        append_failed(product[3])


def handle_success_result(result):
    old_row = get_latest_update(result["product_id"])

    compare_and_log(
        result["product_id"],
        old_row,
        result["price"],
        result["original_price"],
        result["latest_discount_price"],
        result["price_cny"],
        result["original_price_cny"],
        result["shipping_fee_cny"],
        result["final_price_cny"],
        result["exchange_rate"],
        result["profit_rate"],
        result["stock"],
    )

    upsert_product_update(
        result["product_id"],
        result["price"],
        result["original_price"],
        result["stock"],
        result["latest_discount_price"],
        result["price_cny"],
        result["original_price_cny"],
        result["shipping_fee_cny"],
        result["final_price_cny"],
        result["exchange_rate"],
        result["profit_rate"],
    )


def handle_deleted_result(result):
    delete_product_by_id(result["product_id"])
    print(f"商品不存在，已删除: SKU={result['branduid']} | 分类={result['category']}")


def log_detail_result(result, method):
    print(
        "详情结果 -> "
        f"SKU={result['branduid']} | "
        f"方式={method} | "
        f"价格={result['price'] or '-'} | "
        f"原价={result['original_price'] or '-'} | "
        f"库存={result['stock'] or '-'}"
    )


async def main():
    if os.path.exists(FAILED_FILE):
        os.remove(FAILED_FILE)

    limit = TEST_LIMIT if TEST_LIMIT else None
    products = get_active_products(limit)

    total = len(products)
    start = read_progress()
    run_id = get_env_run_id()
    step_key = get_env_step_key("boardline_detail_update")

    if total <= 0:
        print("待更新商品数: 0")
        print("没有可更新商品")
        clear_file(PROGRESS_FILE)
        if run_id:
            update_step_progress(run_id, step_key, current=0, total=0, message="没有可更新商品")
        return

    if start >= total:
        print(f"检测到库存进度断点已到末尾: {start}/{total}，本轮自动从头开始")
        start = 0
        clear_file(PROGRESS_FILE)

    print("待更新商品数:", total)
    print("从第", start + 1, "个商品开始")
    print("并发数:", CONCURRENCY)
    print("批次大小:", BATCH_SIZE)
    print("折扣规则数:", len(DISCOUNT_RULES))
    print("人民币定价规则数:", len(PRICING_RULES))
    if run_id:
        update_step_progress(
            run_id,
            step_key,
            current=start,
            total=total,
            message=f"详情库存更新中 | 从第 {start + 1} 个商品开始",
        )

    sem = asyncio.Semaphore(CONCURRENCY)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)

        i = start
        failed_products = []
        success_count = 0

        while i < total:
            batch_products = products[i:i + BATCH_SIZE]
            results = await process_batch(browser, batch_products, sem, i, total)

            # 批次完成后统一写数据库，避免 SQLite 并发写冲突
            for product, result in zip(batch_products, results):
                if result["ok"]:
                    handle_success_result(result)
                    success_count += 1
                elif result.get("skip_delete"):
                    handle_deleted_result(result)
                else:
                    print("失败:", result["url"])
                    print(result["error"])
                    failed_products.append(product)

            i += len(batch_products)
            write_progress(i)
            print(f"批次完成，当前进度: {i}/{total}")
            if run_id:
                update_step_progress(run_id, step_key, current=i, total=total, message=f"详情库存更新中 | {i}/{total}")

        retry_round = 0
        while failed_products and retry_round < MAX_FAILED_RETRY_ROUNDS:
            retry_round += 1
            print(f"\n开始详情失败自动重试，第 {retry_round}/{MAX_FAILED_RETRY_ROUNDS} 轮")
            retry_targets = failed_products
            failed_products = []

            j = 0
            retry_total = len(retry_targets)
            while j < retry_total:
                retry_batch = retry_targets[j:j + BATCH_SIZE]
                results = await process_batch(browser, retry_batch, sem, j, retry_total)

                for product, result in zip(retry_batch, results):
                    if result["ok"]:
                        handle_success_result(result)
                        success_count += 1
                    elif result.get("skip_delete"):
                        handle_deleted_result(result)
                    else:
                        print("重试仍失败:", result["url"])
                        print(result["error"])
                        failed_products.append(product)

                j += len(retry_batch)
                print(f"重试进度: {j}/{retry_total}")
                if run_id:
                    update_step_progress(
                        run_id,
                        step_key,
                        current=total - len(failed_products),
                        total=total,
                        message=f"详情失败重试中 | 第 {retry_round} 轮 | {j}/{retry_total}",
                    )

        await browser.close()

    persist_failed_urls(failed_products)

    print("库存并发更新完成")
    print("成功更新数:", success_count)
    print("最终失败数:", len(failed_products))
    print("失败详情文件:", FAILED_FILE)
    if run_id:
        update_step_progress(
            run_id,
            step_key,
            current=total,
            total=total,
            message=f"详情库存更新完成 | 成功 {success_count} | 失败 {len(failed_products)}",
        )


if __name__ == "__main__":
    asyncio.run(main())
