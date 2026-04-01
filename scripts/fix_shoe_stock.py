import asyncio
import sys
from pathlib import Path

from playwright.async_api import async_playwright

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from update_stock_db_concurrent import (
    BATCH_SIZE,
    CONCURRENCY,
    MAX_FAILED_RETRY_ROUNDS,
    fetch_one,
    get_active_products,
    handle_success_result,
    persist_failed_urls,
    process_batch,
)


TARGET_CATEGORY = "滑雪鞋"


async def main():
    all_products = get_active_products()
    products = [row for row in all_products if (row[4] or "").strip() == TARGET_CATEGORY]

    total = len(products)
    print("开始修复滑雪鞋库存")
    print("目标商品数:", total)
    print("并发数:", CONCURRENCY)
    print("批次大小:", BATCH_SIZE)

    if total == 0:
        print("没有找到需要修复的滑雪鞋商品")
        return

    sem = asyncio.Semaphore(CONCURRENCY)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        i = 0
        failed_products = []
        success_count = 0

        while i < total:
            batch_products = products[i:i + BATCH_SIZE]
            results = await process_batch(browser, batch_products, sem, i, total)

            for product, result in zip(batch_products, results):
                if result["ok"]:
                    handle_success_result(result)
                    success_count += 1
                else:
                    print("失败:", result["url"])
                    print(result["error"])
                    failed_products.append(product)

            i += len(batch_products)
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

    print("滑雪鞋库存修复完成")
    print("成功更新数:", success_count)
    print("最终失败数:", len(failed_products))


if __name__ == "__main__":
    asyncio.run(main())
