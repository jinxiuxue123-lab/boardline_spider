import os

from playwright.sync_api import sync_playwright
import pandas as pd
import re
from db_utils import get_product_by_branduid

BASE_URL = "http://www.boardline.co.kr"
CHECK_TOP_N = 20
PLAYWRIGHT_HEADLESS = (os.getenv("PLAYWRIGHT_HEADLESS", "1").strip().lower() not in ("0", "false", "no"))


def extract_branduid(url):
    m = re.search(r"branduid=(\d+)", url)
    if m:
        return m.group(1)
    return None


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


def check_category_first_page(page, category_name, category_url):
    print(f"\n检查分类: {category_name}")
    print(f"分类地址: {category_url}")

    page.goto(category_url, timeout=60000)
    page.wait_for_timeout(3000)

    cells = get_product_cells(page)
    checked = 0
    found_new = False

    for cell in cells:
        if checked >= CHECK_TOP_N:
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

        checked += 1

        product = get_product_by_branduid("boardline", branduid)
        if product is None:
            print(f"发现新增商品: {branduid}")
            found_new = True

    print(f"已检查前 {checked} 个商品")
    return found_new


def main():
    categories_df = pd.read_excel("categories.xlsx")
    categories_with_new = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        page = browser.new_page()

        for _, row in categories_df.iterrows():
            category_name = str(row["name"]).strip() if pd.notna(row["name"]) else ""
            category_url = str(row["url"]).strip() if pd.notna(row["url"]) else ""

            if not category_url:
                continue

            has_new = check_category_first_page(page, category_name, category_url)
            if has_new:
                categories_with_new.append(category_name)

        browser.close()

    print("\n巡检完成")
    if categories_with_new:
        print("发现新增商品的分类:")
        for c in categories_with_new:
            print("-", c)
    else:
        print("没有发现新增商品")


if __name__ == "__main__":
    main()
