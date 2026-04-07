import json
import sqlite3
from pathlib import Path

from xianyu_open.stock_utils import parse_total_stock


DB_FILE = "products.db"
OUTPUT_DIR = Path("web")
OUTPUT_DATA_FILE = OUTPUT_DIR / "catalog-data.js"


def normalize_image_path(local_image_path: str | None, image_url: str | None) -> str:
    local_image_path = (local_image_path or "").strip()
    if local_image_path:
        return f"../{local_image_path.lstrip('./')}"
    return (image_url or "").strip()


def fetch_hot_metrics(conn: sqlite3.Connection) -> dict[int, dict]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            c.product_id,
            c.old_value,
            c.new_value
        FROM change_logs c
        JOIN products p
          ON p.id = c.product_id
        WHERE c.field_name = 'stock'
          AND p.source IN ('boardline', 'one8')
        ORDER BY c.change_time ASC, c.id ASC
        """
    ).fetchall()

    metrics: dict[int, dict] = {}
    for row in rows:
        product_id = int(row["product_id"])
        old_total = parse_total_stock(row["old_value"])
        new_total = parse_total_stock(row["new_value"])
        if new_total >= old_total:
            continue
        sold_units = old_total - new_total
        if sold_units <= 0:
            continue
        item = metrics.setdefault(product_id, {"drop_events": 0, "sold_units": 0, "raw_score": 0})
        item["drop_events"] += 1
        item["sold_units"] += sold_units

    for item in metrics.values():
        item["raw_score"] = int(item["sold_units"]) * 10 + int(item["drop_events"]) * 5

    ranked = sorted(
        ((product_id, item) for product_id, item in metrics.items() if int(item["raw_score"]) > 0),
        key=lambda pair: (-int(pair[1]["raw_score"]), -int(pair[1]["sold_units"]), -int(pair[1]["drop_events"]), int(pair[0])),
    )
    max_raw = int(ranked[0][1]["raw_score"]) if ranked else 0

    rank = 1
    for product_id, item in ranked:
        raw_score = int(item["raw_score"])
        item["hot_index"] = int(round(raw_score * 100 / max_raw)) if max_raw > 0 else 0
        item["hot_rank"] = rank
        rank += 1

    return metrics


def fetch_products() -> list[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    hot_metrics = fetch_hot_metrics(conn)

    cur.execute("""
        SELECT
            p.id,
            p.branduid,
            p.category,
            p.name,
            p.url,
            p.image_url,
            p.local_image_path,
            p.status,
            u.price,
            u.original_price,
            u.latest_discount_price,
            u.price_cny,
            u.original_price_cny,
            u.shipping_fee_cny,
            u.final_price_cny,
            u.stock,
            u.updated_at
        FROM products p
        LEFT JOIN product_updates u
          ON p.id = u.product_id
        WHERE p.source = 'boardline'
        ORDER BY
            p.category ASC,
            p.id ASC
    """)

    rows = cur.fetchall()
    conn.close()

    products = []
    for row in rows:
        metric = hot_metrics.get(int(row["id"]), {})
        products.append({
            "id": row["id"],
            "branduid": row["branduid"],
            "category": row["category"] or "未分类",
            "name": row["name"] or "",
            "url": row["url"] or "",
            "image": normalize_image_path(row["local_image_path"], row["image_url"]),
            "status": row["status"] or "",
            "price": row["price"] or "",
            "original_price": row["original_price"] or "",
            "latest_discount_price": row["latest_discount_price"] or "",
            "price_cny": row["price_cny"] or "",
            "original_price_cny": row["original_price_cny"] or "",
            "shipping_fee_cny": row["shipping_fee_cny"] or "",
            "final_price_cny": row["final_price_cny"] or "",
            "stock": row["stock"] or "",
            "updated_at": row["updated_at"] or "",
            "hot_index": int(metric.get("hot_index") or 0),
            "hot_rank": int(metric.get("hot_rank") or 0),
            "stock_drop_events": int(metric.get("drop_events") or 0),
            "stock_sold_units": int(metric.get("sold_units") or 0),
        })

    return products


def build_payload(products: list[dict]) -> dict:
    categories = {}
    for product in products:
        category = product["category"]
        categories[category] = categories.get(category, 0) + 1

    ordered_categories = [
        {"name": "全部", "count": len(products)}
    ]
    ordered_categories.extend(
        {"name": name, "count": count}
        for name, count in sorted(categories.items(), key=lambda item: item[0])
    )

    return {
        "categories": ordered_categories,
        "products": products,
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    products = fetch_products()
    payload = build_payload(products)

    OUTPUT_DATA_FILE.write_text(
        "window.CATALOG_DATA = " + json.dumps(payload, ensure_ascii=False, indent=2) + ";\n",
        encoding="utf-8",
    )

    print(f"商品数: {len(products)}")
    print(f"已生成数据文件: {OUTPUT_DATA_FILE}")


if __name__ == "__main__":
    main()
