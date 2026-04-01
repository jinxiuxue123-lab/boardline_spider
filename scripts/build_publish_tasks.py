import sqlite3
import time
from services.material_ai_service import build_material

DB_PATH = "products.db"


# ==========================
# 只取“未生成任务”的商品
# ==========================
def get_products(limit=2):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT p.id, p.name, p.local_image_path
        FROM products p
        LEFT JOIN xianyu_publish_tasks t
          ON p.id = t.product_id
        WHERE p.local_image_path IS NOT NULL
          AND TRIM(p.local_image_path) != ''
          AND t.product_id IS NULL
        ORDER BY p.id ASC
        LIMIT ?
    """, (limit,))

    rows = cursor.fetchall()
    conn.close()
    return rows


# ==========================
# 插入任务（防重复）
# ==========================
def insert_task(product_id, title, desc):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 防止重复插入
    cursor.execute("""
        SELECT 1 FROM xianyu_publish_tasks
        WHERE product_id = ?
    """, (product_id,))

    exists = cursor.fetchone()
    if exists:
        print(f"跳过（已存在）: {product_id}")
        conn.close()
        return

    cursor.execute("""
        INSERT INTO xianyu_publish_tasks
        (product_id, ai_title, ai_description, status)
        VALUES (?, ?, ?, 'pending')
    """, (product_id, title, desc))

    conn.commit()
    conn.close()


# ==========================
# 主流程
# ==========================
def main():
    products = get_products(2)

    if not products:
        print("没有需要处理的新商品 ✅")
        return

    for i, p in enumerate(products, start=1):
        product = {
            "id": p[0],
            "name": p[1],
            "image": p[2]
        }

        print(f"生成: {product['name']}")

        try:
            material = build_material(product)

            insert_task(
                product["id"],
                material["title"],
                material["description"]
            )

        except Exception as e:
            print(f"❌ 失败: {product['name']} | {e}")

        # 限速（免费额度必备）
        if i < len(products):
            print("等待 20 秒（防限流）...")
            time.sleep(20)

    print("完成")


if __name__ == "__main__":
    main()