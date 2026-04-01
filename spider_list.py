from playwright.sync_api import sync_playwright
import pandas as pd
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

BASE_URL = "http://www.boardline.co.kr"

# 读取分类文件
categories_df = pd.read_excel("categories.xlsx")

products = []
seen_branduids = set()


def make_page_url(url, page_num):
    """
    给分类 URL 自动加上 page 参数
    """
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


def normalize_product_url(href):
    """
    规范化商品链接，只保留 branduid
    返回:
        branduid, clean_url
    """
    if not href:
        return None, None

    if href.startswith("/"):
        href = BASE_URL + href

    parsed = urlparse(href)
    query = parse_qs(parsed.query)

    branduid_list = query.get("branduid", [])
    if not branduid_list:
        return None, None

    branduid = branduid_list[0]
    clean_url = f"{BASE_URL}/shop/shopdetail.html?branduid={branduid}"

    return branduid, clean_url


def clean_name(text):
    """
    去掉商品标题中的韩文，只保留英文、数字和常见符号
    """
    if not text:
        return ""

    text = text.strip()

    # 去掉韩文
    text = re.sub(r"[가-힣]", "", text)

    # 去掉多余空格
    text = " ".join(text.split())

    return text


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


with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    for _, cat_row in categories_df.iterrows():

        category_name = str(cat_row["name"]).strip() if pd.notna(cat_row["name"]) else ""
        category_url = str(cat_row["url"]).strip() if pd.notna(cat_row["url"]) else ""

        if not category_url:
            continue

        print(f"\n开始抓分类: {category_name}")
        print(f"分类地址: {category_url}")

        page_num = 1

        while True:
            current_url = make_page_url(category_url, page_num)
            print(f"  抓第 {page_num} 页: {current_url}")

            try:
                page.goto(current_url, timeout=60000)
                page.wait_for_timeout(3000)
            except Exception as e:
                print("  页面打开失败，停止当前分类")
                print(e)
                break

            cells = get_product_cells(page)
            if len(cells) == 0:
                print("  没有找到商品单元格，停止当前分类")
                break

            page_new_count = 0

            for td in cells:
                link_el = td.query_selector(".thumb a")
                img_el = td.query_selector(".thumb img")
                name_el = td.query_selector(".dsc")

                if not link_el:
                    continue

                href = link_el.get_attribute("href")
                branduid, product_url = normalize_product_url(href)

                if not branduid or not product_url:
                    continue

                # 用 branduid 去重
                if branduid in seen_branduids:
                    continue

                seen_branduids.add(branduid)

                image_url = ""
                if img_el:
                    image_url = img_el.get_attribute("src") or ""
                    if image_url.startswith("/"):
                        image_url = BASE_URL + image_url

                name_text = ""
                if name_el:
                    name_text = clean_name(name_el.inner_text())

                products.append({
                    "branduid": branduid,
                    "category": category_name,
                    "name": name_text,
                    "image": image_url,
                    "link": product_url
                })

                page_new_count += 1

            print(f"  本页新增商品数: {page_new_count}")

            # 当前页没有新商品，就停止当前分类
            if page_new_count == 0:
                print("  本页没有新商品，停止当前分类")
                break

            page_num += 1

    browser.close()

# 保存 Excel
df = pd.DataFrame(products)
df.drop_duplicates(subset=["branduid"], inplace=True)
df.to_excel("product_links.xlsx", index=False)

print("\n全部抓取完成")
print("总商品数量:", len(df))
