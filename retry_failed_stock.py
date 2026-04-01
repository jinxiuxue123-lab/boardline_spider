from playwright.sync_api import sync_playwright
import os
import re
import time
import random

from db_utils import (
    get_connection,
    get_latest_update,
    upsert_product_update,
    insert_change_log,
)

FAILED_FILE = "failed_stock_urls.txt"


def clean_name(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"[가-힣]", "", text)
    return " ".join(text.split())


def clean_option_text(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"^\d+\.\s+", "", text)
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


def parse_stock_single(page):
    options = page.query_selector_all("#MK_p_s_0 option")
    valid_options = []

    for opt in options:
        text = opt.inner_text().strip()
        value = opt.get_attribute("value")
        stock_cnt = opt.get_attribute("stock_cnt")

        if is_placeholder_option(text, value):
            continue

        qty = parse_positive_stock_count(stock_cnt)
        if qty is None:
            continue

        text = clean_option_text(text)
        valid_options.append((text, str(qty)))

    if len(valid_options) == 1:
        text, stock_cnt = valid_options[0]
        if len(text) > 25:
            return [f"ONE SIZE:{stock_cnt}"]

    stock_list = []
    for text, stock_cnt in valid_options:
        stock_list.append(f"{text}:{stock_cnt}")

    return stock_list


def parse_stock_two_level(page):
    stock_map = {}

    select0 = page.query_selector("#MK_p_s_0")
    select1 = page.query_selector("#MK_p_s_1")

    if not select0 or not select1:
        return []

    colors = page.query_selector_all("#MK_p_s_0 option")

    for color in colors:
        color_text = color.inner_text().strip()
        color_value = color.get_attribute("value")

        if is_placeholder_option(color_text, color_value):
            continue

        color_name = clean_option_text(color_text)

        try:
            page.select_option("#MK_p_s_0", value=color_value)
            page.wait_for_timeout(700)

            sizes = page.query_selector_all("#MK_p_s_1 option")

            for size in sizes:
                size_text = size.inner_text().strip()
                size_value = size.get_attribute("value")
                stock_cnt = size.get_attribute("stock_cnt")

                if is_placeholder_option(size_text, size_value):
                    continue

                qty = parse_positive_stock_count(stock_cnt)
                if qty is None:
                    continue

                size_name = clean_option_text(size_text)

                if color_name not in stock_map:
                    stock_map[color_name] = []

                stock_map[color_name].append(f"{size_name}:{qty}")

        except Exception as e:
            print("联动库存解析失败:", color_name)
            print(e)

    result = []
    for color, sizes in stock_map.items():
        result.append(f"{color}({','.join(sizes)})")

    return result


def parse_snowboard_stock(page):
    stock_list = []
    options = page.query_selector_all("#MK_p_s_0 option")

    for opt in options:
        text = opt.inner_text().strip()
        value = opt.get_attribute("value")
        stock_cnt = opt.get_attribute("stock_cnt")

        if is_placeholder_option(text, value):
            continue

        qty = parse_positive_stock_count(stock_cnt)
        if qty is None:
            continue

        text = clean_option_text(text)
        stock_list.append(f"{text}:{qty}")

    return stock_list


def has_soldout_marker(page):
    markers = ["품절", "SOLD OUT", "sold out", "soldout"]
    body_text = ""
    try:
        body = page.query_selector("body")
        if body:
            body_text = (body.inner_text() or "").strip()
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
            if page.locator(selector).count() > 0:
                return True
        except Exception:
            continue
    return False


def parse_stock(page, category=""):
    select0 = page.query_selector("#MK_p_s_0")
    select1 = page.query_selector("#MK_p_s_1")

    category = (category or "").strip()
    if "滑雪板" in category:
        r = parse_snowboard_stock(page)
        if r:
            return r
    else:
        if select0 and select1:
            r = parse_stock_two_level(page)
            if r:
                return r

        if select0:
            r = parse_stock_single(page)
            if r:
                return r

    if has_soldout_marker(page):
        return []

    return ["ONE SIZE:1"]


def compare_and_log(product_id, old_row, price, original_price, stock):
    old_price = ""
    old_original = ""
    old_stock = ""

    if old_row:
        old_price = old_row[0] or ""
        old_original = old_row[1] or ""
        old_stock = old_row[2] or ""

    if old_price != (price or ""):
        insert_change_log(product_id, "price", old_price, price or "")

    if old_original != (original_price or ""):
        insert_change_log(product_id, "original_price", old_original, original_price or "")

    if old_stock != (stock or ""):
        insert_change_log(product_id, "stock", old_stock, stock or "")


def get_product_by_url(url):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, branduid, name, url, category
        FROM products
        WHERE url = ?
        LIMIT 1
    """, (url,))

    row = cur.fetchone()
    conn.close()
    return row


def main():
    if not os.path.exists(FAILED_FILE):
        print("没有失败文件:", FAILED_FILE)
        return

    with open(FAILED_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        print("失败文件为空")
        return

    print("待重试商品数:", len(urls))

    success_urls = []
    still_failed = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        for i, url in enumerate(urls, start=1):
            try:
                print(f"重试 {i}/{len(urls)}: {url}")

                product = get_product_by_url(url)
                if not product:
                    print("数据库中找不到该商品:", url)
                    still_failed.append(url)
                    continue

                product_id, branduid, name, db_url, category = product

                page.goto(url, timeout=60000)
                page.wait_for_selector("h3.cboth.tit-prd", timeout=10000)
                time.sleep(random.uniform(1, 2))

                price_el = page.query_selector(".price")
                price = price_el.inner_text().strip() if price_el else ""

                original_el = page.query_selector(".consumer")
                original_price = original_el.inner_text().strip() if original_el else ""

                stock_items = parse_stock(page, category)
                stock = " | ".join(stock_items)

                old_row = get_latest_update(product_id)

                compare_and_log(
                    product_id,
                    old_row,
                    price,
                    original_price,
                    stock
                )

                upsert_product_update(
                    product_id,
                    price,
                    original_price,
                    stock
                )

                success_urls.append(url)

            except Exception as e:
                print("重试仍失败:", url)
                print(e)
                still_failed.append(url)

        browser.close()

    # 回写失败文件，只保留仍失败的
    with open(FAILED_FILE, "w", encoding="utf-8") as f:
        for url in still_failed:
            f.write(url + "\n")

    print("重试完成")
    print("补抓成功:", len(success_urls))
    print("仍失败:", len(still_failed))


if __name__ == "__main__":
    main()
