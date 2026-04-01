from playwright.sync_api import sync_playwright
import pandas as pd
import re
import requests
from datetime import datetime
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
PLAYWRIGHT_HEADLESS = (os.getenv("PLAYWRIGHT_HEADLESS", "1").strip().lower() not in ("0", "false", "no"))
PAGE_GOTO_TIMEOUT_MS = 30000
BLOCKED_RESOURCE_TYPES = {"image", "media", "font"}
BLOCKED_URL_KEYWORDS = (
    "google-analytics",
    "googletagmanager",
    "doubleclick",
    "facebook.net",
    "analytics",
    "gtag/js",
)


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


def get_product_cells(page):
    content_wrap = page.query_selector("#contentWrap")
    if not content_wrap:
        return []

    cells = []
    for cell in content_wrap.query_selector_all("td"):
        link_el = cell.query_selector("div.thumb a[href*='branduid=']")
        if link_el:
            cells.append(cell)

    return cells


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


def should_block_request(request) -> bool:
    if request.resource_type in BLOCKED_RESOURCE_TYPES:
        return True
    lowered_url = request.url.lower()
    return any(keyword in lowered_url for keyword in BLOCKED_URL_KEYWORDS)


def open_page_with_retries(page, category_name, current_url, page_num):
    last_error = None

    for attempt in range(1, MAX_PAGE_RETRIES + 1):
        try:
            print(f"打开页面尝试 {attempt}/{MAX_PAGE_RETRIES}: {current_url}")
            page.goto(current_url, timeout=PAGE_GOTO_TIMEOUT_MS, wait_until="domcontentloaded")
            page.wait_for_timeout(1500)
            return True
        except Exception as e:
            last_error = str(e)
            print(f"页面失败（第 {attempt} 次）: {e}")
            page.wait_for_timeout(2000)

    append_failed_page(category_name, page_num, current_url, last_error or "unknown error")
    print(f"页面最终失败，已记录: {category_name} 第 {page_num} 页")
    return False


# ==========================
# 抓分类
# ==========================
def crawl_category(page, category_url, category_name, remaining_limit=None):
    page_num = 1
    total_count = 0

    while True:
        if remaining_limit is not None and total_count >= remaining_limit:
            break

        current_url = make_page_url(category_url, page_num)
        print(f"\n抓取分类 [{category_name}] 第 {page_num} 页")

        opened = open_page_with_retries(page, category_name, current_url, page_num)
        if not opened:
            page_num += 1
            continue

        cells = get_product_cells(page)
        if len(cells) == 0:
            break

        page_count = 0

        for cell in cells:
            if remaining_limit is not None and total_count >= remaining_limit:
                break

            link_el = cell.query_selector("div.thumb a")
            if not link_el:
                continue

            href = link_el.get_attribute("href")
            if not href:
                continue

            product_url = BASE_URL + href if href.startswith("/") else href
            branduid = extract_branduid(product_url)

            if not branduid:
                continue

            product_url = f"{BASE_URL}/shop/shopdetail.html?branduid={branduid}"

            # 商品名称
            name_el = cell.query_selector("li.dsc")
            name = name_el.inner_text().strip() if name_el else ""
            name = clean_name(name)

            # 图片
            img_el = cell.query_selector("div.thumb img")
            image_url = img_el.get_attribute("src") if img_el else ""

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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        page = browser.new_page()
        page.route("**/*", lambda route: route.abort() if should_block_request(route.request) else route.continue_())

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
                page,
                category_url,
                category_name,
                remaining_limit=remaining_limit,
            )
            total_processed += category_total
            print(f"完成分类: {category_name} | 分类新增处理: {category_total} | 累计: {total_processed}")

        browser.close()

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
