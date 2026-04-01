from playwright.sync_api import sync_playwright
import pandas as pd
import os
import re
import time
import random

INPUT_FILE = "product_links.xlsx"
OUTPUT_FILE = "products_raw.xlsx"
PROGRESS_FILE = "progress_raw.txt"
FAILED_FILE = "failed_urls_raw.txt"

SAVE_EVERY = 100
TEST_LIMIT = None   # 测试时填 50，正式抓全部时改成 None


def clean_name(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"[가-힣]", "", text)
    text = " ".join(text.split())
    return text


def clean_option_text(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"^\d+\.", "", text)
    text = re.sub(r"\(\)", "", text)
    text = clean_name(text)
    text = text.strip(" /-")
    return text


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
    option_els = page.query_selector_all("#MK_p_s_0 option")

    for opt in option_els:
        text = opt.inner_text().strip()
        value = opt.get_attribute("value")
        stock_cnt = opt.get_attribute("stock_cnt")

        if is_placeholder_option(text, value):
            continue

        if stock_cnt is None or stock_cnt == "":
            continue

        text = clean_option_text(text)
        stock_list.append(f"{text}:{stock_cnt}")

    return stock_list


def parse_stock_two_level(page):
    stock_map = {}

    select_0 = page.query_selector("#MK_p_s_0")
    select_1 = page.query_selector("#MK_p_s_1")

    if not select_0 or not select_1:
        return []

    color_options = page.query_selector_all("#MK_p_s_0 option")

    for color_opt in color_options:
        color_text = color_opt.inner_text().strip()
        color_value = color_opt.get_attribute("value")

        if is_placeholder_option(color_text, color_value):
            continue

        color_clean = clean_option_text(color_text)

        try:
            page.select_option("#MK_p_s_0", value=color_value)
            page.wait_for_timeout(500)

            size_options = page.query_selector_all("#MK_p_s_1 option")

            for size_opt in size_options:
                size_text = size_opt.inner_text().strip()
                size_value = size_opt.get_attribute("value")
                stock_cnt = size_opt.get_attribute("stock_cnt")

                if is_placeholder_option(size_text, size_value):
                    continue

                if stock_cnt is None or stock_cnt == "":
                    continue

                size_clean = clean_option_text(size_text)

                if color_clean not in stock_map:
                    stock_map[color_clean] = []

                stock_map[color_clean].append(f"{size_clean}:{stock_cnt}")

        except Exception as e:
            print("联动库存解析失败:", color_clean)
            print(e)
            continue

    result = []
    for color, sizes in stock_map.items():
        result.append(f"{color}({','.join(sizes)})")

    return result


def parse_stock(page):
    select_0 = page.query_selector("#MK_p_s_0")
    select_1 = page.query_selector("#MK_p_s_1")

    if select_0 and select_1:
        result = parse_stock_two_level(page)
        if result:
            return result

    if select_0:
        result = parse_stock_single(page)
        if result:
            return result

    return ["DEFAULT:1"]


def read_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return int(f.read().strip())
        except Exception:
            return 0
    return 0


def write_progress(index_value):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        f.write(str(index_value))


def clear_file(path):
    with open(path, "w", encoding="utf-8") as f:
        f.write("")


def append_failed_url(url):
    with open(FAILED_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")


def save_batch(items, first_batch=False):
    df_batch = pd.DataFrame(items)

    if first_batch and not os.path.exists(OUTPUT_FILE):
        df_batch.to_excel(OUTPUT_FILE, index=False)
    else:
        if os.path.exists(OUTPUT_FILE):
            old_df = pd.read_excel(OUTPUT_FILE)
            new_df = pd.concat([old_df, df_batch], ignore_index=True)
            new_df.to_excel(OUTPUT_FILE, index=False)
        else:
            df_batch.to_excel(OUTPUT_FILE, index=False)


# 初始化失败文件
clear_file(FAILED_FILE)

# 读取商品列表
df = pd.read_excel(INPUT_FILE)

if TEST_LIMIT:
    df = df.head(TEST_LIMIT)

start_index = read_progress()
total_count = len(df)

print(f"总商品数: {total_count}")
print(f"从第 {start_index + 1} 个商品开始继续抓取")
print(f"每 {SAVE_EVERY} 个商品保存一次")

buffer_items = []
first_batch = not os.path.exists(OUTPUT_FILE)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    for index in range(start_index, total_count):
        row = df.iloc[index]
        url = str(row["link"]).strip()

        try:
            print(f"正在抓取第 {index + 1}/{total_count} 个商品: {url}")

            page.goto(url, timeout=60000)
            page.wait_for_selector("h3.cboth.tit-prd", timeout=10000)

            time.sleep(random.uniform(0.6, 1.2))

            # 商品标题
            title_el = page.query_selector("h3.cboth.tit-prd")
            name = title_el.inner_text().strip() if title_el else ""
            name = clean_name(name)

            # 售价
            price_el = page.query_selector(".price")
            price_text = price_el.inner_text().strip() if price_el else ""

            # 原价
            consumer_el = page.query_selector(".consumer")
            consumer_price = consumer_el.inner_text().strip() if consumer_el else ""

            # 主图URL
            img_el = page.query_selector("img.detail_image")
            image_url = img_el.get_attribute("src") if img_el else ""

            if image_url and image_url.startswith("/"):
                image_url = "http://www.boardline.co.kr" + image_url

            # 库存
            stock_items = parse_stock(page)
            stock_text = " | ".join(stock_items)

            buffer_items.append({
                "url": url,
                "name": name,
                "price": price_text,
                "original_price": consumer_price,
                "stock": stock_text,
                "image_url": image_url
            })

            write_progress(index + 1)

            if len(buffer_items) >= SAVE_EVERY:
                print(f"开始保存 {len(buffer_items)} 条数据...")
                save_batch(buffer_items, first_batch=first_batch)
                first_batch = False
                buffer_items = []
                print("保存完成")

        except Exception as e:
            print("抓取失败:", url)
            print(e)
            append_failed_url(url)
            write_progress(index + 1)
            continue

    browser.close()

# 保存最后一批
if buffer_items:
    print(f"开始保存最后 {len(buffer_items)} 条数据...")
    save_batch(buffer_items, first_batch=first_batch)
    print("最后一批保存完成")

print("全部抓取完成")
print(f"结果文件: {OUTPUT_FILE}")
print(f"失败商品文件: {FAILED_FILE}")