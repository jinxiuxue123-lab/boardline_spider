from datetime import datetime
import argparse
import json
from pathlib import Path
import random
import re
import sqlite3
import time
from html import unescape
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests

from discount_rules import find_matching_discount_rule, load_discount_rules
from db_utils import (
    get_latest_update,
    get_product_by_branduid,
    insert_change_log,
    insert_product,
    update_product_basic,
    upsert_product_update,
)

BASE_URL = "https://one8.co.kr"
START_URL = "https://one8.co.kr/product/list.html?cate_no=6455"
SOURCE_NAME = "one8"
CATEGORY_FILE = "categories_one8.xlsx"
TEST_LIMIT = None
OPTION_REQUEST_RETRIES = 3
PROGRESS_FILE = "one8_list_progress.json"
DB_FILE = "products.db"
EXPORT_FILE = "one8_products.xlsx"
DISCOUNT_RULES_FILE = "one8_discount_rules.xlsx"
PAGE_SLEEP_MIN_SECONDS = 2.0
PAGE_SLEEP_MAX_SECONDS = 4.0
CATEGORY_SLEEP_MIN_SECONDS = 5.0
CATEGORY_SLEEP_MAX_SECONDS = 8.0
PAGE_REQUEST_TIMEOUT_SECONDS = 20
PAGE_REQUEST_RETRIES = 2
DETAIL_REQUEST_TIMEOUT_SECONDS = 20


class CategoryPageLoadError(Exception):
    pass


def sleep_between_pages() -> None:
    delay = random.uniform(PAGE_SLEEP_MIN_SECONDS, PAGE_SLEEP_MAX_SECONDS)
    print(f"页面间隔休眠: {delay:.1f}s")
    time.sleep(delay)


def sleep_between_categories() -> None:
    delay = random.uniform(CATEGORY_SLEEP_MIN_SECONDS, CATEGORY_SLEEP_MAX_SECONDS)
    print(f"分类间隔休眠: {delay:.1f}s")
    time.sleep(delay)


def upsert_one8_product_update_with_change_log(
    *,
    product_id: int,
    price: str,
    original_price: str,
    latest_discount_price: str,
    stock: str,
    price_cny: str = "",
    original_price_cny: str = "",
    shipping_fee_cny: str = "",
    final_price_cny: str = "",
    exchange_rate: str = "",
    profit_rate: str = "",
) -> None:
    old_row = get_latest_update(product_id)
    old_stock = ""
    if old_row:
        old_stock = str(old_row[2] or "")

    upsert_product_update(
        product_id=product_id,
        price=price,
        original_price=original_price,
        latest_discount_price=latest_discount_price,
        price_cny=price_cny,
        original_price_cny=original_price_cny,
        shipping_fee_cny=shipping_fee_cny,
        final_price_cny=final_price_cny,
        exchange_rate=exchange_rate,
        profit_rate=profit_rate,
        stock=stock,
    )

    if old_stock != (stock or ""):
        insert_change_log(product_id, "stock", old_stock, stock or "")


def export_one8_inventory_excel() -> int:
    conn = sqlite3.connect(DB_FILE)
    sql = """
    SELECT
        p.id,
        p.source,
        p.branduid,
        p.category,
        p.name,
        p.color,
        p.url,
        p.image_url,
        p.local_image_path,
        p.image_downloaded,
        p.status,
        p.first_seen,
        p.last_seen,
        pu.price,
        pu.original_price,
        pu.latest_discount_price,
        pu.stock,
        pu.price_cny,
        pu.original_price_cny,
        pu.shipping_fee_cny,
        pu.final_price_cny,
        pu.exchange_rate,
        pu.profit_rate,
        pu.updated_at AS update_time
    FROM products p
    LEFT JOIN product_updates pu ON pu.product_id = p.id
    WHERE p.source = ?
    ORDER BY p.category, p.id
    """
    df = pd.read_sql_query(sql, conn, params=(SOURCE_NAME,))
    conn.close()
    discount_rules = load_discount_rules(DISCOUNT_RULES_FILE)
    df["applied_discount"] = df.apply(
        lambda row: format_applied_discount_text(
            find_matching_discount_rule(
                str(row.get("name") or ""),
                str(row.get("category") or ""),
                discount_rules,
            )
        ),
        axis=1,
    )
    df.to_excel(EXPORT_FILE, index=False)
    return len(df)


def ensure_products_color_column() -> None:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(products)")
    product_columns = {row[1] for row in cursor.fetchall()}
    if "color" not in product_columns:
        cursor.execute("ALTER TABLE products ADD COLUMN color TEXT")
        conn.commit()
    conn.close()


def format_applied_discount_text(rule: dict | None) -> str:
    if not rule:
        return ""

    discount_type = str(rule.get("discount_type") or "").lower()
    discount_value = rule.get("discount_value")
    keyword = clean_text(str(rule.get("keyword") or ""))
    note = clean_text(str(rule.get("note") or ""))

    if discount_type == "rate":
        try:
            rate = float(discount_value)
        except (TypeError, ValueError):
            rate_text = ""
        else:
            if rate <= 1:
                rate_text = f"{rate * 10:g}折"
            else:
                rate_text = f"{rate:g}%"
    elif discount_type == "amount":
        try:
            amount = int(round(float(discount_value)))
        except (TypeError, ValueError):
            rate_text = ""
        else:
            rate_text = f"减{amount:,}韩元"
    else:
        rate_text = ""

    parts = [part for part in [keyword, rate_text, note] if part]
    return " | ".join(parts)


def guess_ext(url: str) -> str:
    path = urlparse(url).path.lower()
    if path.endswith(".png"):
        return ".png"
    if path.endswith(".webp"):
        return ".webp"
    if path.endswith(".gif"):
        return ".gif"
    return ".jpg"


def download_image(image_url: str, branduid: str) -> str | None:
    if not image_url:
        return None
    if "/mov/" in image_url.lower():
        print(f"跳过视频封面图: {branduid} | {image_url}")
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
        save_path.write_bytes(resp.content)
        print(f"下载成功: {branduid}")
        return str(save_path)
    except Exception as e:
        print(f"下载失败: {image_url} | {e}")
        return None


def read_progress() -> dict:
    path = Path(PROGRESS_FILE)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_progress(progress: dict) -> None:
    Path(PROGRESS_FILE).write_text(
        json.dumps(progress, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def clear_progress() -> None:
    path = Path(PROGRESS_FILE)
    if path.exists():
        path.unlink()


def update_category_progress(category_name: str, next_page: int) -> None:
    progress = read_progress()
    progress[category_name] = {"next_page": max(1, int(next_page))}
    write_progress(progress)


def clear_category_progress(category_name: str) -> None:
    progress = read_progress()
    progress.pop(category_name, None)
    write_progress(progress)


def clean_option_text(text: str) -> str:
    text = clean_text(text)
    text = re.sub(r"\[[^\]]*\]", "", text).strip()
    text = re.sub(r"\(\s*[+\-]?\d[\d,\s]*원?\s*\)", "", text).strip()
    return clean_text(text)


def is_placeholder_option(text: str, value: str) -> bool:
    text = clean_text(text)
    value = clean_text(value)
    if not text or not value:
        return True
    keywords = ["선택", "옵션", "사이즈", "색상", "SIZE", "COLOR"]
    return any(k.lower() in text.lower() for k in keywords)


def parse_stock_count(text: str) -> int | None:
    text = clean_text(text)
    soldout_keywords = ["품절", "sold out", "soldout", "일시품절"]
    if any(k.lower() in text.lower() for k in soldout_keywords):
        return 0
    match = re.search(r"재고\s*[:：]?\s*(\d+)", text)
    if match:
        return int(match.group(1))
    match = re.search(r"\((\d+)\)", text)
    if match:
        return int(match.group(1))
    return None


def display_option_value(option_value: str) -> str:
    option_value = clean_text(option_value)
    if not option_value:
        return ""
    if ":" in option_value:
        return option_value.split(":", 1)[1].strip()
    option_value = option_value.lstrip("*").strip()
    option_value = re.sub(r"\([^)]*[가-힣][^)]*\)", "", option_value)
    option_value = re.sub(r"[가-힣]+", "", option_value)
    option_value = clean_text(option_value)
    return option_value


def normalize_primary_option_label(option_value: str) -> str:
    text = display_option_value(option_value)
    if not text:
        return ""
    match = re.match(r"^([A-Za-z]*\d+(?:\.\d+)?[A-Za-z]*)", text)
    if match:
        return match.group(1)
    return text


def decode_escaped_json_text(raw_text: str) -> str:
    return bytes(raw_text or "", "utf-8").decode("unicode_escape")


def strip_html_tags(text: str) -> str:
    return clean_text(re.sub(r"<[^>]+>", " ", text or ""))


def guess_bundle_component_label(product_name: str, index: int) -> str:
    upper_name = (product_name or "").upper()
    has_top = any(keyword in upper_name for keyword in ("JACKET", "ANORAK", "PARKA", "HOODIE", "FLEECE", "CREW", "TOP"))
    has_bottom = any(keyword in upper_name for keyword in ("PANT", "PANTS", "BIB", "TROUSER"))
    if has_top and has_bottom:
        return "上衣" if index == 1 else "裤子"
    if has_bottom:
        return "裤子"
    if has_top:
        return "上衣"
    return f"组件{index}"


def parse_option_stock_json_block(raw_json: str) -> list[str]:
    decoded_json = raw_json
    stock_data = None
    last_error = None
    for _ in range(3):
        try:
            stock_data = json.loads(decoded_json)
            break
        except Exception as e:
            last_error = e
            decoded_json = decode_escaped_json_text(decoded_json)
    if stock_data is None:
        raise last_error or ValueError("option_stock_data 解码失败")
    option_rows: list[tuple[str, int | str]] = []
    has_any_known_stock = False
    has_any_positive_stock = False
    for item in stock_data.values():
        option_value = clean_text(str(item.get("option_value") or ""))
        if not option_value:
            continue
        stock_number = item.get("stock_number")
        if stock_number is None:
            stock_number = "UNKNOWN"
        elif isinstance(stock_number, int):
            has_any_known_stock = True
            if stock_number > 0:
                has_any_positive_stock = True
        label = normalize_primary_option_label(option_value)
        option_rows.append((label, stock_number))

    grouped: dict[str, int | str] = {}
    for label, stock_number in option_rows:
        if label not in grouped:
            grouped[label] = stock_number
            continue
        existing = grouped[label]
        if isinstance(existing, int) and isinstance(stock_number, int):
            grouped[label] = max(existing, stock_number)
        elif existing == "UNKNOWN" and stock_number != "UNKNOWN":
            grouped[label] = stock_number

    rows: list[str] = []
    for label, stock_number in grouped.items():
        if stock_number == 0:
            continue
        rows.append(f"{label}:{stock_number}")
    if not rows and has_any_known_stock and not has_any_positive_stock:
        return ["售罄"]
    return rows


def fetch_option_stock(product_no: str, cate_no: str) -> str:
    if not product_no or not cate_no:
        return ""
    option_url = f"{BASE_URL}/product/basket_option.html?product_no={product_no}&sActionType=basket&cate_no={cate_no}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"{BASE_URL}/product/list.html?cate_no={cate_no}",
        "Connection": "close",
    }
    html = ""
    last_error = None
    for attempt in range(1, OPTION_REQUEST_RETRIES + 1):
        try:
            resp = requests.get(option_url, headers=headers, timeout=20)
            resp.raise_for_status()
            html = resp.text
            break
        except Exception as e:
            last_error = e
            if attempt < OPTION_REQUEST_RETRIES:
                print(f"选项接口重试 {attempt}/{OPTION_REQUEST_RETRIES - 1}: product_no={product_no} cate_no={cate_no}")
                continue
    if not html:
        print(f"选项接口请求失败: product_no={product_no} cate_no={cate_no} | {last_error}")
        return ""

    option_rows: list[str] = []

    component_matches = list(
        re.finditer(
            r'product_name\\":\\"(.*?)\\",\\"has_option\\":\\"T\\".*?option_stock_data\\":\\"(.*?)\\",\\"stock_manage\\":true',
            html,
            re.S,
        )
    )
    if component_matches:
        component_sections: list[str] = []
        all_components_sold_out = True
        for index, match in enumerate(component_matches, start=1):
            try:
                product_name = strip_html_tags(decode_escaped_json_text(match.group(1)))
                component_label = guess_bundle_component_label(product_name, index)
                component_rows = parse_option_stock_json_block(match.group(2))
                if component_rows:
                    if component_rows == ["售罄"]:
                        component_sections.append(f"{component_label}:\n售罄")
                    else:
                        all_components_sold_out = False
                        component_sections.append(f"{component_label}:\n" + " | ".join(component_rows))
            except Exception as e:
                print(f"套装选项库存JSON解析失败: product_no={product_no} cate_no={cate_no} | {e}")
        if component_sections:
            if all_components_sold_out:
                return "售罄"
            return "\n".join(component_sections)

    single_stock_match = re.search(r"var\s+single_option_stock_data\s*=\s*'(.+?)';", html, re.S)
    if single_stock_match:
        try:
            decoded_json = single_stock_match.group(1)
            stock_data = None
            last_error = None
            for _ in range(3):
                try:
                    stock_data = json.loads(decoded_json)
                    break
                except Exception as e:
                    last_error = e
                    decoded_json = decode_escaped_json_text(decoded_json)
            if stock_data is None:
                raise last_error or ValueError("single_option_stock_data 解码失败")
            stock_number = stock_data.get("stock_number")
            if stock_number is None:
                stock_number = "UNKNOWN"
            if stock_number == 0:
                return "售罄"
            return f"单规格:{stock_number}"
        except Exception as e:
            print(f"单规格库存JSON解析失败: product_no={product_no} cate_no={cate_no} | {e}")

    stock_json_match = re.search(r"var\s+option_stock_data\s*=\s*'(.+?)';", html, re.S)
    if stock_json_match:
        try:
            option_rows = parse_option_stock_json_block(stock_json_match.group(1))
        except Exception as e:
            print(f"选项库存JSON解析失败: product_no={product_no} cate_no={cate_no} | {e}")

    if not option_rows:
        pattern = re.compile(r"<option[^>]*value=\"([^\"]*)\"[^>]*>(.*?)</option>", re.I | re.S)
        for value, label_html in pattern.findall(html):
            label = re.sub(r"<[^>]+>", "", label_html)
            label = clean_option_text(label)
            if is_placeholder_option(label, value):
                continue
            label = display_option_value(label)
            stock_count = parse_stock_count(label_html)
            if stock_count is None:
                option_rows.append(f"{label}:UNKNOWN")
            else:
                option_rows.append(f"{label}:{stock_count}")

    deduped: list[str] = []
    seen: set[str] = set()
    for row in option_rows:
        if row in seen:
            continue
        seen.add(row)
        deduped.append(row)
    return " | ".join(deduped)


def normalize_product_url(href: str) -> tuple[str | None, str | None]:
    if not href:
        return None, None
    full_url = urljoin(BASE_URL, href)
    parsed = urlparse(full_url)
    query = parse_qs(parsed.query)
    goods_no = (query.get("goodsNo") or [""])[0].strip()
    if not goods_no:
        match = re.search(r"/product/.+?/(\d+)/category/", parsed.path)
        if match:
            goods_no = match.group(1)
    if not goods_no:
        return None, None
    if "goodsNo" in query:
        clean_query = urlencode({"goodsNo": goods_no})
        clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, clean_query, ""))
    else:
        clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))
    return goods_no, clean_url


def clean_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def parse_price_value(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    digits = "".join(ch for ch in value if ch.isdigit())
    if not digits:
        return ""
    return f"{int(digits):,}원"


class One8ListParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.products = []
        self._normal_section_depth = 0
        self._in_item = False
        self._item_depth = 0
        self._current = None
        self._capture_eng_name = False
        self._capture_name = False
        self._capture_discount_note = False
        self._eng_name_parts = []
        self._name_parts = []
        self._discount_note_parts = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        class_attr = attrs_dict.get("class", "")
        class_list = class_attr.split()

        if tag == "div" and "xans-product-listnormal" in class_list:
            self._normal_section_depth = 1
        elif self._normal_section_depth > 0:
            self._normal_section_depth += 1

        if self._normal_section_depth > 0 and tag == "li" and (attrs_dict.get("id") or "").startswith("anchorBoxId_"):
            self._in_item = True
            self._item_depth = 1
            self._current = {
                "href": "",
                "name": "",
                "image_url": "",
                "original_price": "",
                "discount_price": "",
                "discount_note": "",
                "text_parts": [],
            }
            self._eng_name_parts = []
            self._name_parts = []
            self._discount_note_parts = []
            self._capture_eng_name = False
            self._capture_name = False
            self._capture_discount_note = False
            return

        if not self._in_item or self._current is None:
            return

        if tag == "li":
            self._item_depth += 1

        if tag == "a":
            href = (attrs_dict.get("href") or "").strip()
            if "/product/" in href and not self._current["href"]:
                self._current["href"] = href
        elif tag == "img":
            if "video_img" in class_list:
                return
            image_url = (
                attrs_dict.get("ec-data-src")
                or attrs_dict.get("data-src")
                or attrs_dict.get("src")
                or ""
            ).strip()
            if image_url and "/mov/" in image_url.lower():
                image_url = ""
            if image_url and not self._current["image_url"]:
                self._current["image_url"] = image_url
        elif tag == "div" and "description" in class_list:
            self._current["original_price"] = parse_price_value(attrs_dict.get("ec-data-custom", ""))
            self._current["discount_price"] = parse_price_value(attrs_dict.get("ec-data-price", ""))
        elif tag == "span" and "eng_name" in class_list:
            self._capture_eng_name = True
            self._eng_name_parts = []
        elif tag == "div" and "name" in class_list:
            self._capture_name = True
            self._name_parts = []
        elif tag == "div" and "custom_pro_txt" in class_list:
            self._capture_discount_note = True
            self._discount_note_parts = []

    def handle_endtag(self, tag):
        if not self._in_item and self._normal_section_depth > 0:
            self._normal_section_depth -= 1
            if self._normal_section_depth < 0:
                self._normal_section_depth = 0

        if not self._in_item or self._current is None:
            return

        if tag == "span" and self._capture_eng_name:
            self._capture_eng_name = False
            text = clean_text("".join(self._eng_name_parts))
            if text:
                self._current["name"] = text
            self._eng_name_parts = []
            return

        if tag == "div" and self._capture_name:
            self._capture_name = False
            text = clean_text("".join(self._name_parts))
            if text and not self._current["name"]:
                self._current["name"] = text
            self._name_parts = []
            return

        if tag == "div" and self._capture_discount_note:
            self._capture_discount_note = False
            self._current["discount_note"] = clean_text("".join(self._discount_note_parts))
            self._discount_note_parts = []
            return

        if tag == "li":
            self._item_depth -= 1
            if self._item_depth == 0:
                self._in_item = False
                full_text = clean_text(" ".join(self._current["text_parts"]))
                self._current["sold_out"] = "품절" in full_text
                if self._current.get("href"):
                    self.products.append(self._current)
                self._current = None

    def handle_data(self, data):
        if not self._in_item or self._current is None:
            return
        text = clean_text(data)
        if text:
            self._current["text_parts"].append(text)
        if self._capture_eng_name:
            self._eng_name_parts.append(data)
        if self._capture_name:
            self._name_parts.append(data)
        if self._capture_discount_note:
            self._discount_note_parts.append(data)


def make_page_url(url: str, page_num: int) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["page"] = [str(page_num)]
    new_query = urlencode(query, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def fetch_category_html(session: requests.Session, current_url: str) -> str:
    response = session.get(
        current_url,
        headers={"User-Agent": "Mozilla/5.0", "Referer": BASE_URL},
        timeout=PAGE_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.text


def fetch_product_color(session: requests.Session, product_url: str) -> str:
    if not product_url:
        return ""
    response = session.get(
        product_url,
        headers={"User-Agent": "Mozilla/5.0", "Referer": BASE_URL},
        timeout=DETAIL_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    html = response.text
    match = re.search(
        r'<li[^>]*>\s*<span[^>]*class="title"[^>]*>\s*제품 스펙\s*</span>\s*<span[^>]*>(.*?)</span>\s*</li>',
        html,
        re.I | re.S,
    )
    if not match:
        return ""
    raw_value = match.group(1)
    raw_value = re.sub(r"<span[^>]*class=\"displaynone\"[^>]*>.*?</span>", " ", raw_value, flags=re.I | re.S)
    raw_value = re.sub(r"<[^>]+>", " ", raw_value)
    return clean_text(unescape(raw_value))


def extract_product_items_from_html(html: str):
    parser = One8ListParser()
    parser.feed(html or "")
    return parser.products


def extract_product_from_item(item, cate_no: str):
    href = clean_text(item.get("href") or "")
    if "/display/2/" in href:
        return None
    branduid, product_url = normalize_product_url(href)
    if not branduid or not product_url:
        return None

    name = clean_text(item.get("name") or "")
    image_url = clean_text(item.get("image_url") or "")
    if image_url:
        image_url = urljoin(BASE_URL, image_url)

    return {
        "branduid": branduid,
        "cate_no": cate_no,
        "url": product_url,
        "name": name,
        "color": "",
        "image_url": image_url,
        "original_price": clean_text(item.get("original_price") or ""),
        "discount_price": clean_text(item.get("discount_price") or ""),
        "discount_note": clean_text(item.get("discount_note") or ""),
        "sold_out": bool(item.get("sold_out")),
    }


def crawl_category(category_url: str, category_name: str, remaining_limit=None, start_page: int = 1) -> int:
    page_num = start_page
    total_count = 0
    cate_no = (parse_qs(urlparse(category_url).query).get("cate_no") or [""])[0].strip()
    stop_category = False
    consecutive_sold_out = 0
    session = requests.Session()
    color_cache: dict[str, str] = {}

    while True:
        if remaining_limit is not None and total_count >= remaining_limit:
            break

        current_url = make_page_url(category_url, page_num)
        print(f"\n抓取分类 [{category_name}] 第 {page_num} 页")
        if page_num > start_page:
            sleep_between_pages()
        try:
            print(f"打开页面: {current_url}")
            html = fetch_category_html(session, current_url)
        except Exception as e:
            print(f"页面加载异常，重试一次: {current_url} | {e}")
            try:
                sleep_between_pages()
                print(f"重新打开页面: {current_url}")
                html = fetch_category_html(session, current_url)
            except Exception as retry_error:
                raise CategoryPageLoadError(f"{current_url} | {retry_error}") from retry_error
        product_items = extract_product_items_from_html(html)
        print(f"调试: prdList商品项={len(product_items)} | url={current_url}")
        if not product_items:
            break

        page_count = 0
        page_inserted = 0
        page_updated = 0
        page_skipped = 0
        page_failed = 0
        for item in product_items:
            if remaining_limit is not None and total_count >= remaining_limit:
                break
            if item.get("sold_out"):
                page_skipped += 1
                consecutive_sold_out += 1
                if consecutive_sold_out >= 5:
                    print(f"连续检测到 5 个售罄商品，停止当前分类抓取: {category_name} | page={page_num}")
                    stop_category = True
                    break
                continue
            consecutive_sold_out = 0
            product = extract_product_from_item(item, cate_no)
            if not product:
                href_debug = clean_text(item.get("href") or "")
                print(f"跳过商品: href={href_debug}")
                page_skipped += 1
                continue
            try:
                if product["url"] not in color_cache:
                    try:
                        color_cache[product["url"]] = fetch_product_color(session, product["url"])
                    except Exception as color_error:
                        print(f"颜色抓取失败: {product['branduid']} | {color_error}")
                        color_cache[product["url"]] = ""
                product["color"] = color_cache.get(product["url"], "")
                local_image_path = download_image(product["image_url"], product["branduid"])
                today = datetime.now().strftime("%Y-%m-%d")
                existing = get_product_by_branduid(SOURCE_NAME, product["branduid"])
                if existing is None:
                    insert_product(
                        source=SOURCE_NAME,
                        branduid=product["branduid"],
                        category=category_name,
                        name=product["name"],
                        color=product["color"],
                        url=product["url"],
                        image_url=product["image_url"],
                        local_image_path=local_image_path,
                        image_downloaded=1 if local_image_path else 0,
                        first_seen=today,
                        last_seen=today,
                    )
                    page_inserted += 1
                else:
                    update_product_basic(
                        source=SOURCE_NAME,
                        branduid=product["branduid"],
                        category=category_name,
                        name=product["name"],
                        color=product["color"],
                        url=product["url"],
                        image_url=product["image_url"],
                        local_image_path=local_image_path,
                        image_downloaded=1 if local_image_path else 0,
                        last_seen=today,
                    )
                    product_id = existing[0]
                    page_updated += 1

                if existing is None:
                    inserted = get_product_by_branduid(SOURCE_NAME, product["branduid"])
                    product_id = inserted[0] if inserted else None

                if product_id:
                    stock_text = fetch_option_stock(product["branduid"], product["cate_no"]) or product["discount_note"]
                    upsert_one8_product_update_with_change_log(
                        product_id=product_id,
                        price=product["discount_price"],
                        original_price=product["original_price"],
                        latest_discount_price=product["discount_price"],
                        stock=stock_text,
                    )
                page_count += 1
                total_count += 1
            except Exception as e:
                page_failed += 1
                print(f"处理失败: branduid={product['branduid']} | {e}")

        print(
            f"本页商品数: {page_count} | 新增: {page_inserted} | 更新: {page_updated} | 跳过: {page_skipped} | 失败: {page_failed}"
        )
        if page_count > 0 and not stop_category:
            update_category_progress(category_name, page_num + 1)
        if page_count == 0 or stop_category:
            break
        page_num += 1

    print(f"分类 [{category_name}] 完成，总数: {total_count}")
    return total_count


def main():
    parser = argparse.ArgumentParser(description="抓取 one8 分类商品")
    parser.add_argument("--headed", action="store_true", help="打开浏览器窗口运行，默认无头模式")
    args = parser.parse_args()
    ensure_products_color_column()

    total_processed = 0
    progress = read_progress()
    completed_all = True
    categories_df = pd.read_excel(CATEGORY_FILE)
    progress_start_name = next(iter(progress.keys()), "")
    start_index = 0
    if progress_start_name:
        for idx, row in categories_df.iterrows():
            category_name = clean_text(str(row.get("name") or ""))
            if category_name == progress_start_name:
                start_index = idx
                break
    for idx, row in categories_df.iterrows():
        if idx < start_index:
            continue
        if TEST_LIMIT is not None and total_processed >= TEST_LIMIT:
            break
        category_name = clean_text(str(row.get("name") or ""))
        category_url = clean_text(str(row.get("url") or ""))
        if not category_name or not category_url:
            continue
        print(f"\n开始分类: {category_name} | 当前累计: {total_processed}")
        category_progress = progress.get(category_name, {})
        start_page = int(category_progress.get("next_page", 1) or 1)
        remaining_limit = None
        if TEST_LIMIT is not None:
            remaining_limit = TEST_LIMIT - total_processed
        try:
            category_total = crawl_category(
                category_url,
                category_name,
                remaining_limit=remaining_limit,
                start_page=start_page,
            )
        except CategoryPageLoadError as e:
            print(f"分类页面连续失败，已保留断点，停止本轮抓取: {category_name} | {e}")
            completed_all = False
            break
        except Exception:
            raise
        total_processed += category_total
        print(f"完成分类: {category_name} | 分类新增处理: {category_total} | 累计: {total_processed}")
        clear_category_progress(category_name)
        if idx + 1 < len(categories_df):
            sleep_between_categories()
        if idx + 1 < len(categories_df):
            next_row = categories_df.iloc[idx + 1]
            next_name = clean_text(str(next_row.get("name") or ""))
            if next_name:
                update_category_progress(next_name, 1)

    if completed_all:
        clear_progress()
        exported_count = export_one8_inventory_excel()
        print(f"已导出 one8 库存表: {EXPORT_FILE} | 条数: {exported_count}")
    print("\n全部完成")


if __name__ == "__main__":
    main()
