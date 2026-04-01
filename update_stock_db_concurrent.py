import asyncio
import os
import re
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
)
from pricing_rules import calculate_cny_pricing, load_pricing_rules
from xianyu_open.auto_attributes import apply_auto_attributes_for_product

PROGRESS_FILE = "stock_db_progress_concurrent.txt"
FAILED_FILE = "failed_stock_urls.txt"

TEST_LIMIT = None      # 测试时抓 100，正式跑全部时改成 None
CONCURRENCY = 3      # 先用 3，稳定后再考虑 5
BATCH_SIZE = 30      # 每批处理多少个商品后写一次进度
MAX_FAILED_RETRY_ROUNDS = 2
DEBUG_STOCK = True   # 是否打印库存解析结果
BASE_URL = "http://www.boardline.co.kr"
SOURCE_NAME = "boardline"
DISCOUNT_RULES = load_discount_rules()
PRICING_RULES = load_pricing_rules()
PLAYWRIGHT_HEADLESS = (os.getenv("PLAYWRIGHT_HEADLESS", "1").strip().lower() not in ("0", "false", "no"))


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
    # 只清理类似 "1. 选项名" 这种序号前缀，避免把鞋码 7.5 误清成 5
    text = re.sub(r"^\d+\.\s+", "", text)
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
        page = await browser.new_page()
        try:
            print(f"更新 {idx}/{total}: {branduid} [{category}]")

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

            if DEBUG_STOCK:
                print(f"库存结果 -> {branduid} | {category} | {stock}")

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


async def main():
    if os.path.exists(FAILED_FILE):
        os.remove(FAILED_FILE)

    limit = TEST_LIMIT if TEST_LIMIT else None
    products = get_active_products(limit)

    total = len(products)
    start = read_progress()

    if total <= 0:
        print("待更新商品数: 0")
        print("没有可更新商品")
        clear_file(PROGRESS_FILE)
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
                else:
                    print("失败:", result["url"])
                    print(result["error"])
                    failed_products.append(product)

            i += len(batch_products)
            write_progress(i)
            print(f"批次完成，当前进度: {i}/{total}")

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
                    else:
                        print("重试仍失败:", result["url"])
                        print(result["error"])
                        failed_products.append(product)

                j += len(retry_batch)
                print(f"重试进度: {j}/{retry_total}")

        await browser.close()

    persist_failed_urls(failed_products)

    print("库存并发更新完成")
    print("成功更新数:", success_count)
    print("最终失败数:", len(failed_products))
    print("失败详情文件:", FAILED_FILE)


if __name__ == "__main__":
    asyncio.run(main())
