import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_utils import update_product_image_info
from spider_one8_list_db import (
    download_image,
    fetch_option_stock,
    upsert_one8_product_update_with_change_log,
)


DB_FILE = str(ROOT_DIR / "products.db")
LEGACY_DISCOUNT_STOCKS = {"10%", "20%", "25%", "30%", "31%", "35%", "40%", "50%", "60%"}


def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def load_candidate_rows(limit: int | None = None, branduids: list[str] | None = None) -> list[sqlite3.Row]:
    conn = get_conn()
    where = ["p.source = 'one8'"]
    params: list = []

    if branduids:
        placeholders = ",".join("?" for _ in branduids)
        where.append(f"p.branduid IN ({placeholders})")
        params.extend(branduids)
    else:
        where.append(
            "("
            "trim(coalesce(p.local_image_path,'')) = '' "
            "OR coalesce(p.image_downloaded, 0) != 1 "
            "OR trim(coalesce(u.stock,'')) = '' "
            "OR coalesce(u.stock,'') IN ({})"
            ")".format(",".join("?" for _ in LEGACY_DISCOUNT_STOCKS))
        )
        params.extend(sorted(LEGACY_DISCOUNT_STOCKS))

    sql = f"""
    SELECT
        p.id,
        p.branduid,
        p.category,
        p.name,
        p.url,
        p.image_url,
        p.local_image_path,
        p.image_downloaded,
        u.price,
        u.original_price,
        u.latest_discount_price,
        u.stock
    FROM products p
    LEFT JOIN product_updates u ON u.product_id = p.id
    WHERE {' AND '.join(where)}
    ORDER BY p.category, p.id
    """
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def infer_cate_no_from_url(url: str) -> str:
    marker = "/category/"
    if marker not in url:
        return ""
    text = url.split(marker, 1)[1]
    return text.split("/", 1)[0].strip()


def repair_row(row: sqlite3.Row) -> dict:
    product_id = int(row["id"])
    branduid = str(row["branduid"] or "").strip()
    image_url = str(row["image_url"] or "").strip()
    cate_no = infer_cate_no_from_url(str(row["url"] or ""))

    fixed_image = False
    fixed_stock = False

    local_image_path = str(row["local_image_path"] or "").strip()
    image_downloaded = int(row["image_downloaded"] or 0)
    if image_url and (not local_image_path or image_downloaded != 1):
        new_local_image_path = download_image(image_url, branduid)
        if new_local_image_path:
            update_product_image_info(product_id, image_url, new_local_image_path)
            fixed_image = True

    current_stock = str(row["stock"] or "").strip()
    if not current_stock or current_stock in LEGACY_DISCOUNT_STOCKS:
        stock = fetch_option_stock(branduid, cate_no)
        if stock:
            upsert_one8_product_update_with_change_log(
                product_id=product_id,
                price=str(row["price"] or ""),
                original_price=str(row["original_price"] or ""),
                latest_discount_price=str(row["latest_discount_price"] or row["price"] or ""),
                stock=stock,
            )
            fixed_stock = True

    return {
        "product_id": product_id,
        "branduid": branduid,
        "fixed_image": fixed_image,
        "fixed_stock": fixed_stock,
    }


def main():
    parser = argparse.ArgumentParser(description="补抓 one8 失败尾巴：缺图、空库存、旧折扣库存")
    parser.add_argument("--limit", type=int, default=None, help="只处理前 N 条")
    parser.add_argument("--branduid", action="append", default=[], help="只处理指定 branduid，可重复传多次")
    args = parser.parse_args()

    rows = load_candidate_rows(limit=args.limit, branduids=args.branduid or None)
    print(f"待补抓商品数: {len(rows)}")
    image_fixed_count = 0
    stock_fixed_count = 0

    for index, row in enumerate(rows, start=1):
        result = repair_row(row)
        image_fixed_count += 1 if result["fixed_image"] else 0
        stock_fixed_count += 1 if result["fixed_stock"] else 0
        print(
            f"[{index}/{len(rows)}] branduid={result['branduid']} "
            f"image={'Y' if result['fixed_image'] else '-'} "
            f"stock={'Y' if result['fixed_stock'] else '-'}"
        )

    print(
        f"补抓完成 | 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"图片修复: {image_fixed_count} | 库存修复: {stock_fixed_count}"
    )


if __name__ == "__main__":
    main()
