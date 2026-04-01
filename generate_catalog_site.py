import json
import sqlite3
from pathlib import Path


DB_FILE = "products.db"
OUTPUT_DIR = Path("web")
OUTPUT_DATA_FILE = OUTPUT_DIR / "catalog-data.js"


def normalize_image_path(local_image_path: str | None, image_url: str | None) -> str:
    local_image_path = (local_image_path or "").strip()
    if local_image_path:
        return f"../{local_image_path.lstrip('./')}"
    return (image_url or "").strip()


def fetch_products() -> list[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

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
