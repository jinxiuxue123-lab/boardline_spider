import asyncio
from pathlib import Path

from update_stock_db_concurrent import (
    CONCURRENCY,
    DEBUG_STOCK,
    compare_and_log,
    fetch_one,
    get_connection,
    get_latest_update,
    process_batch,
    upsert_product_update,
)
from playwright.async_api import async_playwright


def has_valid_local_image(local_image_path):
    if not local_image_path:
        return False

    path = Path(local_image_path)
    return path.exists() and path.is_file() and path.stat().st_size > 0


def get_repair_products(limit=None):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            p.id,
            p.branduid,
            p.name,
            p.url,
            p.category,
            p.image_url,
            p.local_image_path,
            u.price,
            u.original_price,
            u.stock
        FROM products p
        LEFT JOIN product_updates u
          ON p.id = u.product_id
        WHERE p.source = 'boardline'
          AND p.status = 'active'
        ORDER BY p.id
    """)

    rows = cur.fetchall()
    conn.close()

    repair_rows = []
    for row in rows:
        (
            product_id,
            branduid,
            name,
            url,
            category,
            image_url,
            local_image_path,
            price,
            original_price,
            stock,
        ) = row

        missing_update = not (price or "").strip() or not (stock or "").strip()
        missing_image = not has_valid_local_image(local_image_path)

        if missing_update or missing_image:
            repair_rows.append((
                product_id,
                branduid,
                name,
                url,
                category,
                image_url,
                local_image_path,
            ))

    if limit is not None:
        return repair_rows[:limit]

    return repair_rows


async def main():
    products = get_repair_products()
    total = len(products)

    print("待补齐商品数:", total)
    print("并发数:", CONCURRENCY)

    if total == 0:
        print("没有需要补齐的商品")
        return

    sem = asyncio.Semaphore(CONCURRENCY)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        results = await process_batch(browser, products, sem, 0, total)

        repaired_count = 0
        failed_count = 0

        for result in results:
            if result["ok"]:
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
                repaired_count += 1
            else:
                print("补齐失败:", result["url"])
                print(result["error"])
                failed_count += 1

        await browser.close()

    print("补齐完成")
    print("成功数:", repaired_count)
    print("失败数:", failed_count)


if __name__ == "__main__":
    asyncio.run(main())
