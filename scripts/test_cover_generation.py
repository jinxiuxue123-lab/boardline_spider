import sqlite3
from services.cover_image_service import create_cover

DB_PATH = "products.db"


def get_test_products(limit=3):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT t.product_id, p.name, p.local_image_path, t.ai_title
        FROM xianyu_publish_tasks t
        JOIN products p ON t.product_id = p.id
        WHERE p.local_image_path IS NOT NULL
          AND TRIM(p.local_image_path) != ''
          AND t.ai_title IS NOT NULL
          AND TRIM(t.ai_title) != ''
        ORDER BY t.id DESC
        LIMIT ?
    """, (limit,))

    rows = cursor.fetchall()
    conn.close()
    return rows


def make_brand(name: str) -> str:
    name = (name or "").upper()

    for brand in [
        "SMITH", "BURTON", "YONEX", "GRAY", "NITRO",
        "PLAYBOY", "DIMITO", "JONES", "SALOMON", "K2"
    ]:
        if brand in name:
            return brand

    return "SNOW"


def make_subtitle(name: str) -> str:
    name = (name or "").upper()

    if "GOGGLE" in name or "4D MAG" in name or "SMITH" in name:
        return "大视野滑雪镜"
    if "BAG" in name:
        return "雪具收纳方便"
    if "BOOT" in name:
        return "雪鞋滑行适用"
    if "BOARD" in name or "CUSTOM" in name or "GRAY" in name or "YONEX" in name:
        return "雪季滑雪装备"

    return "雪季装备上新"


def main():
    products = get_test_products(limit=3)

    if not products:
        print("没有可测试的数据，请先确认 xianyu_publish_tasks 里已经有 ai_title。")
        return

    for product_id, original_name, image_path, ai_title in products:
        output_path = f"data/images/cover/{product_id}.jpg"
        subtitle = make_subtitle(original_name)
        brand = make_brand(original_name)

        result = create_cover(
            input_image_path=image_path,
            output_path=output_path,
            title=ai_title,
            subtitle=subtitle,
            brand_text=brand,
        )

        print(f"已生成: {result}")


if __name__ == "__main__":
    main()