import re
from html import unescape
from urllib.parse import urljoin

import pandas as pd
import requests

from db_utils import get_product_by_branduid

BASE_URL = "http://www.boardline.co.kr"
CHECK_TOP_N = 20
PAGE_REQUEST_TIMEOUT_SECONDS = 15
PAGE_REQUEST_RETRIES = 2
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE_URL,
    "Connection": "close",
}


def extract_branduid(url: str) -> str | None:
    m = re.search(r"branduid=(\d+)", url or "")
    if m:
        return m.group(1)
    return None


def fetch_category_html(category_name: str, category_url: str) -> str:
    last_error = None
    for attempt in range(1, PAGE_REQUEST_RETRIES + 1):
        try:
            response = requests.get(
                category_url,
                headers=REQUEST_HEADERS,
                timeout=PAGE_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.text
        except Exception as exc:
            last_error = exc
            print(f"分类首屏请求失败，重试 {attempt}/{PAGE_REQUEST_RETRIES}: {category_name} | {exc}")
    raise RuntimeError(f"{category_name} 首屏请求失败: {last_error}")


def extract_first_page_branduids(html: str) -> list[str]:
    html = html or ""

    # 优先只取正式商品列表区域，避免抓到顶部导航/推荐区。
    block_match = re.search(
        r'<div id="contentWrap".*?<div class="prd-list.*?</table>',
        html,
        flags=re.I | re.S,
    )
    if not block_match:
        block_match = re.search(
            r'<div id="contentWrapper".*?<div class="prd-list.*?</table>',
            html,
            flags=re.I | re.S,
        )

    target_html = block_match.group(0) if block_match else html
    href_matches = re.findall(
        r'href="([^"]*shopdetail\.html\?[^"]*branduid=\d+[^"]*)"',
        target_html,
        flags=re.I,
    )
    branduids: list[str] = []
    seen: set[str] = set()
    for href in href_matches:
        href = unescape(href.strip())
        full_url = urljoin(BASE_URL, href)
        branduid = extract_branduid(full_url)
        if not branduid or branduid in seen:
            continue
        seen.add(branduid)
        branduids.append(branduid)
        if len(branduids) >= CHECK_TOP_N:
            break
    return branduids


def check_category_first_page(category_name: str, category_url: str) -> bool:
    print(f"\n检查分类: {category_name}")
    print(f"分类地址: {category_url}")

    html = fetch_category_html(category_name, category_url)
    branduids = extract_first_page_branduids(html)
    checked = 0
    found_new = False

    for branduid in branduids:
        checked += 1
        product = get_product_by_branduid("boardline", branduid)
        if product is None:
            print(f"发现新增商品: {branduid}")
            found_new = True

    print(f"已检查前 {checked} 个商品")
    return found_new


def main() -> None:
    categories_df = pd.read_excel("categories.xlsx")
    categories_with_new: list[str] = []

    for _, row in categories_df.iterrows():
        category_name = str(row["name"]).strip() if pd.notna(row["name"]) else ""
        category_url = str(row["url"]).strip() if pd.notna(row["url"]) else ""
        if not category_url:
            continue
        has_new = check_category_first_page(category_name, category_url)
        if has_new:
            categories_with_new.append(category_name)

    print("\n巡检完成")
    if categories_with_new:
        print("发现新增商品的分类:")
        for category_name in categories_with_new:
            print("-", category_name)
    else:
        print("没有发现新增商品")


if __name__ == "__main__":
    main()
