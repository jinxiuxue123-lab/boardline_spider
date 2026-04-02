import pandas as pd
import re
import requests
import random
import time
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from pathlib import Path
import os

from db_utils import (
    get_product_by_branduid,
    insert_product,
    update_product_basic
)
from xianyu_open.auto_attributes import apply_auto_attributes_for_product

BASE_URL = "http://www.boardline.co.kr"
SOURCE_NAME = "boardline"
TEST_LIMIT = None
FAILED_PAGES_FILE = "failed_list_pages.txt"
MAX_PAGE_RETRIES = 3
PAGE_REQUEST_TIMEOUT_SECONDS = 15
PAGE_SLEEP_MIN_SECONDS = float(os.getenv("BOARDLINE_PAGE_SLEEP_MIN", "2.5"))
PAGE_SLEEP_MAX_SECONDS = float(os.getenv("BOARDLINE_PAGE_SLEEP_MAX", "5.0"))
CATEGORY_SLEEP_MIN_SECONDS = float(os.getenv("BOARDLINE_CATEGORY_SLEEP_MIN", "6"))
CATEGORY_SLEEP_MAX_SECONDS = float(os.getenv("BOARDLINE_CATEGORY_SLEEP_MAX", "10"))
PAGE_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE_URL,
}


# ==========================
# 图片下载
# ==========================
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

    # 已存在就不重复下载
    if save_path.exists() and save_path.stat().st_size > 0:
        return str(save_path)

    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": BASE_URL
        }

        r = requests.get(image_url, headers=headers, timeout=20)
        r.raise_for_status()

        with open(save_path, "wb") as f:
            f.write(r.content)

        print(f"下载成功: {branduid}")
        return str(save_path)

    except Exception as e:
        print(f"下载失败: {image_url} | {e}")
        return None


# ==========================
# 工具函数
# ==========================
def extract_branduid(url):
    m = re.search(r"branduid=(\d+)", url)
    if m:
        return m.group(1)
    return None


def clean_name(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"[가-힣]", "", text)
    season_match = re.search(r"\d{2}/\d{2}", text)
    if season_match:
        # 如果韩文标题前半段里夹着零散数字，且后面已经有标准雪季前缀，
        # 直接截到雪季前缀开始，避免留下类似 "2 25/26 ..." 的脏标题。
        text = text[season_match.start():]
    return " ".join(text.split())


def decode_boardline_html(response):
    try:
        return response.content.decode("euc-kr")
    except UnicodeDecodeError:
        return response.content.decode("euc-kr", errors="ignore")


def make_page_url(url, page_num):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["page"] = [str(page_num)]
    new_query = urlencode(query, doseq=True)

    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))


class BoardlineListParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.products = []
        self._in_td = False
        self._td_depth = 0
        self._current = None
        self._capture_name = False
        self._name_parts = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        classes = attrs_dict.get("class", "")
        class_list = classes.split()
        if tag == "td":
            self._in_td = True
            self._td_depth += 1
            if self._td_depth == 1:
                self._current = {"href": "", "name": "", "image_url": ""}
                self._name_parts = []
                self._capture_name = False
            return

        if not self._in_td or not self._current:
            return

        if tag == "a":
            href = attrs_dict.get("href", "")
            if "shopdetail.html" in href and "branduid=" in href and not self._current["href"]:
                self._current["href"] = href
        elif tag == "img":
            src = attrs_dict.get("src", "")
            if src and not self._current["image_url"]:
                self._current["image_url"] = src
        elif tag == "li" and "dsc" in class_list:
            self._capture_name = True

    def handle_endtag(self, tag):
        if tag == "li" and self._capture_name:
            self._capture_name = False
            if self._current is not None:
                self._current["name"] = clean_name("".join(self._name_parts))
            self._name_parts = []
            return

        if tag == "td" and self._in_td:
            self._td_depth -= 1
            if self._td_depth == 0:
                self._in_td = False
                if self._current and self._current.get("href"):
                    self.products.append(self._current)
                self._current = None

    def handle_data(self, data):
        if self._capture_name:
            self._name_parts.append(data)


def extract_main_product_list_html(html_text):
    html_text = html_text or ""

    content_wrap_match = re.search(
        r'<div id="contentWrap".*</body>',
        html_text,
        flags=re.I | re.S,
    )
    search_html = content_wrap_match.group(0) if content_wrap_match else html_text

    list_matches = re.findall(
        r'<div class="prd-list[^"]*".*?</table>',
        search_html,
        flags=re.I | re.S,
    )
    if list_matches:
        return list_matches[-1]

    return search_html


def extract_product_cards(html_text):
    parser = BoardlineListParser()
    parser.feed(extract_main_product_list_html(html_text))
    return parser.products


def clear_failed_pages_file():
    Path(FAILED_PAGES_FILE).write_text("", encoding="utf-8")


def append_failed_page(category_name, page_num, url, error):
    with open(FAILED_PAGES_FILE, "a", encoding="utf-8") as f:
        f.write(f"{category_name}\tpage={page_num}\t{url}\t{error}\n")


def load_failed_pages():
    path = Path(FAILED_PAGES_FILE)
    if not path.exists() or path.stat().st_size == 0:
        return []

    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(line)
    return items


def is_rate_limited(response, body_text):
    if response.status_code == 302:
        return True
    body_text = body_text or ""
    return "47.80.63.228" in body_text and "페이지를 너무 많이 요청" in body_text


def sleep_between_pages():
    delay = random.uniform(PAGE_SLEEP_MIN_SECONDS, PAGE_SLEEP_MAX_SECONDS)
    print(f"页面间隔休眠: {delay:.1f}s")
    time.sleep(delay)


def sleep_between_categories():
    delay = random.uniform(CATEGORY_SLEEP_MIN_SECONDS, CATEGORY_SLEEP_MAX_SECONDS)
    print(f"分类间隔休眠: {delay:.1f}s")
    time.sleep(delay)


def open_page_with_retries(session, category_name, current_url, page_num):
    last_error = None

    for attempt in range(1, MAX_PAGE_RETRIES + 1):
        try:
            print(f"打开页面尝试 {attempt}/{MAX_PAGE_RETRIES}: {current_url}")
            response = session.get(
                current_url,
                headers=PAGE_REQUEST_HEADERS,
                timeout=PAGE_REQUEST_TIMEOUT_SECONDS,
                allow_redirects=False,
            )
            html_text = decode_boardline_html(response)
            if is_rate_limited(response, html_text):
                raise RuntimeError("目标站点触发限流/封禁提示页")
            response.raise_for_status()
            return html_text
        except Exception as e:
            last_error = str(e)
            print(f"页面失败（第 {attempt} 次）: {e}")
            if "限流/封禁" in last_error:
                break

    append_failed_page(category_name, page_num, current_url, last_error or "unknown error")
    print(f"页面最终失败，已记录: {category_name} 第 {page_num} 页")
    return ""


# ==========================
# 抓分类
# ==========================
def crawl_category(category_url, category_name, remaining_limit=None):
    page_num = 1
    total_count = 0
    session = requests.Session()

    while True:
        if remaining_limit is not None and total_count >= remaining_limit:
            break

        current_url = make_page_url(category_url, page_num)
        print(f"\n抓取分类 [{category_name}] 第 {page_num} 页")

        if page_num > 1:
            sleep_between_pages()

        html_text = open_page_with_retries(session, category_name, current_url, page_num)
        if not html_text:
            page_num += 1
            continue

        products = extract_product_cards(html_text)
        if len(products) == 0:
            break

        page_count = 0

        for item in products:
            if remaining_limit is not None and total_count >= remaining_limit:
                break

            href = unescape((item.get("href") or "").strip())
            if not href:
                continue

            product_url = BASE_URL + href if href.startswith("/") else href
            branduid = extract_branduid(product_url)

            if not branduid:
                continue

            product_url = f"{BASE_URL}/shop/shopdetail.html?branduid={branduid}"

            name = clean_name(item.get("name") or "")

            image_url = unescape((item.get("image_url") or "").strip())

            if image_url and image_url.startswith("/"):
                image_url = BASE_URL + image_url

            # 下载图片
            local_image_path = download_image(image_url, branduid)

            today = datetime.now().strftime("%Y-%m-%d")

            product = get_product_by_branduid(SOURCE_NAME, branduid)

            if product is None:
                insert_product(
                    source=SOURCE_NAME,
                    branduid=branduid,
                    category=category_name,
                    name=name,
                    color="",
                    url=product_url,
                    image_url=image_url,
                    local_image_path=local_image_path,
                    image_downloaded=1 if local_image_path else 0,
                    first_seen=today,
                    last_seen=today
                )
                inserted = get_product_by_branduid(SOURCE_NAME, branduid)
                if inserted:
                    apply_auto_attributes_for_product(inserted[0], category_name, name)
            else:
                update_product_basic(
                    source=SOURCE_NAME,
                    branduid=branduid,
                    category=category_name,
                    name=name,
                    color="",
                    url=product_url,
                    image_url=image_url,
                    local_image_path=local_image_path,
                    image_downloaded=1 if local_image_path else 0,
                    last_seen=today
                )
                apply_auto_attributes_for_product(product[0], category_name, name)

            page_count += 1
            total_count += 1

        print(f"本页商品数: {page_count}")

        if page_count == 0:
            break

        page_num += 1

    print(f"分类 [{category_name}] 完成，总数: {total_count}")
    return total_count


# ==========================
# 主函数
# ==========================
def main():
    categories_df = pd.read_excel("categories.xlsx")
    total_processed = 0
    clear_failed_pages_file()

    for _, row in categories_df.iterrows():
        if TEST_LIMIT is not None and total_processed >= TEST_LIMIT:
            break

        category_name = str(row["name"]).strip()
        category_url = str(row["url"]).strip()

        if not category_url:
            continue

        print(f"\n开始分类: {category_name} | 当前累计: {total_processed}")

        remaining_limit = None
        if TEST_LIMIT is not None:
            remaining_limit = TEST_LIMIT - total_processed

        category_total = crawl_category(
            category_url,
            category_name,
            remaining_limit=remaining_limit,
        )
        total_processed += category_total
        print(f"完成分类: {category_name} | 分类新增处理: {category_total} | 累计: {total_processed}")
        if pd.notna(row["url"]):
            sleep_between_categories()

    print("\n全部完成")

    failed_pages = load_failed_pages()
    if failed_pages:
        print("\n失败分页汇总:")
        for item in failed_pages:
            print(item)
        print(f"失败分页已写入: {FAILED_PAGES_FILE}")
    else:
        print("\n没有失败分页")


if __name__ == "__main__":
    main()
