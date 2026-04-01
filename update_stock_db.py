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

PROGRESS_FILE = "stock_db_progress.txt"
FAILED_FILE = "failed_stock_urls.txt"

TEST_LIMIT = None   # 测试用，全部抓取时改成 None


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
    text = re.sub(r"^\d+\.", "", text)
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


def parse_stock_single(page):
    stock_list = []
    options = page.query_selector_all("#MK_p_s_0 option")

    for opt in options:
        text = opt.inner_text().strip()
        value = opt.get_attribute("value")
        stock_cnt = opt.get_attribute("stock_cnt")

        if is_placeholder_option(text, value):
            continue

        if not stock_cnt:
            continue

        text = clean_option_text(text)
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
            page.wait_for_timeout(500)

            sizes = page.query_selector_all("#MK_p_s_1 option")

            for size in sizes:

                size_text = size.inner_text().strip()
                size_value = size.get_attribute("value")
                stock_cnt = size.get_attribute("stock_cnt")

                if is_placeholder_option(size_text, size_value):
                    continue

                if not stock_cnt:
                    continue

                size_name = clean_option_text(size_text)

                if color_name not in stock_map:
                    stock_map[color_name] = []

                stock_map[color_name].append(f"{size_name}:{stock_cnt}")

        except Exception as e:
            print("联动库存解析失败:", color_name)
            print(e)

    result = []

    for color, sizes in stock_map.items():
        result.append(f"{color}({','.join(sizes)})")

    return result


def parse_stock(page):

    select0 = page.query_selector("#MK_p_s_0")
    select1 = page.query_selector("#MK_p_s_1")

    if select0 and select1:
        r = parse_stock_two_level(page)
        if r:
            return r

    if select0:
        r = parse_stock_single(page)
        if r:
            return r

    return ["DEFAULT:1"]


def read_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except:
                return 0
    return 0


def write_progress(i):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(i))


def append_failed(url):
    with open(FAILED_FILE, "a") as f:
        f.write(url + "\n")


def get_products(limit=None):

    conn = get_connection()
    cur = conn.cursor()

    sql = """
    SELECT id, branduid, name, url
    FROM products
    WHERE source='boardline' AND status='active'
    ORDER BY id
    """

    if limit:
        sql += f" LIMIT {limit}"

    cur.execute(sql)
    rows = cur.fetchall()

    conn.close()
    return rows


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


def main():

    if os.path.exists(FAILED_FILE):
        os.remove(FAILED_FILE)

    limit = TEST_LIMIT if TEST_LIMIT else None

    products = get_products(limit)

    total = len(products)
    start = read_progress()

    print("待更新商品数:", total)
    print("从第", start + 1, "个商品开始")

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        for i in range(start, total):

            product_id, branduid, name, url = products[i]

            try:

                print(f"更新 {i+1}/{total} :", branduid)

                page.goto(url, timeout=60000)
                page.wait_for_selector("h3.cboth.tit-prd", timeout=10000)

                time.sleep(random.uniform(0.6,1.2))

                price_el = page.query_selector(".price")
                price = price_el.inner_text().strip() if price_el else ""

                original_el = page.query_selector(".consumer")
                original_price = original_el.inner_text().strip() if original_el else ""

                stock_items = parse_stock(page)
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

                write_progress(i+1)

            except Exception as e:

                print("失败:", url)
                print(e)

                append_failed(url)
                write_progress(i+1)

        browser.close()

    print("库存更新完成")


if __name__ == "__main__":
    main()