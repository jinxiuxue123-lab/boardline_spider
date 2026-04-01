import sqlite3
from typing import Optional

DB_PATH = "products.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


# ==========================
# 查询商品
# ==========================
def get_product_by_branduid(source: str, branduid: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, source, branduid, category, name, url,
               image_url, local_image_path, image_downloaded, detail_image_fetched,
               status, last_seen, missing_days
        FROM products
        WHERE source = ? AND branduid = ?
    """, (source, branduid))

    row = cursor.fetchone()
    conn.close()
    return row


# ==========================
# 插入商品（新增）
# ==========================
def insert_product(
    source: str,
    branduid: str,
    category: str,
    name: str,
    url: str,
    image_url: str,
    local_image_path: Optional[str] = None,
    image_downloaded: int = 0,
    first_seen: Optional[str] = None,
    last_seen: Optional[str] = None,
):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO products (
            source,
            branduid,
            category,
            name,
            url,
            image_url,
            local_image_path,
            image_downloaded,
            status,
            first_seen,
            last_seen,
            missing_days
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, 0)
    """, (
        source,
        branduid,
        category,
        name,
        url,
        image_url,
        local_image_path,
        image_downloaded,
        first_seen,
        last_seen
    ))

    conn.commit()
    conn.close()


# ==========================
# 更新商品基础信息（关键修复点）
# ==========================
def update_product_basic(
    source: str,
    branduid: str,
    category: str,
    name: str,
    url: str,
    image_url: str,
    local_image_path: Optional[str] = None,
    image_downloaded: int = 0,
    last_seen: Optional[str] = None,
):
    conn = get_connection()
    cursor = conn.cursor()

    # 🔥 关键修复：只有成功下载时才更新
    if local_image_path:
        cursor.execute("""
            UPDATE products
            SET category = ?,
                name = ?,
                url = ?,
                image_url = ?,
                local_image_path = ?,
                image_downloaded = 1,
                status = 'active',
                missing_days = 0,
                last_seen = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE source = ? AND branduid = ?
        """, (
            category,
            name,
            url,
            image_url,
            local_image_path,
            last_seen,
            source,
            branduid
        ))
    else:
        # 没下载成功就不覆盖原数据
        cursor.execute("""
            UPDATE products
            SET category = ?,
                name = ?,
                url = ?,
                image_url = ?,
                status = 'active',
                missing_days = 0,
                last_seen = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE source = ? AND branduid = ?
        """, (
            category,
            name,
            url,
            image_url,
            last_seen,
            source,
            branduid
        ))

    conn.commit()
    conn.close()


# ==========================
# 更新商品图片信息
# ==========================
def update_product_image_info(
    product_id: int,
    image_url: str,
    local_image_path: Optional[str] = None,
    detail_image_fetched: Optional[int] = None,
):
    conn = get_connection()
    cursor = conn.cursor()

    if local_image_path:
        if detail_image_fetched is None:
            cursor.execute("""
                UPDATE products
                SET image_url = ?,
                    local_image_path = ?,
                    image_downloaded = 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                image_url,
                local_image_path,
                product_id,
            ))
        else:
            cursor.execute("""
                UPDATE products
                SET image_url = ?,
                    local_image_path = ?,
                    image_downloaded = 1,
                    detail_image_fetched = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                image_url,
                local_image_path,
                detail_image_fetched,
                product_id,
            ))
    else:
        if detail_image_fetched is None:
            cursor.execute("""
                UPDATE products
                SET image_url = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                image_url,
                product_id,
            ))
        else:
            cursor.execute("""
                UPDATE products
                SET image_url = ?,
                    detail_image_fetched = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                image_url,
                detail_image_fetched,
                product_id,
            ))

    conn.commit()
    conn.close()


# ==========================
# 库存更新
# ==========================
def upsert_product_update(
    product_id: int,
    price: str,
    original_price: str,
    stock: str,
    latest_discount_price: str = "",
    price_cny: str = "",
    original_price_cny: str = "",
    shipping_fee_cny: str = "",
    final_price_cny: str = "",
    exchange_rate: str = "",
    profit_rate: str = "",
):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO product_updates (
            product_id,
            price,
            original_price,
            latest_discount_price,
            price_cny,
            original_price_cny,
            shipping_fee_cny,
            final_price_cny,
            exchange_rate,
            profit_rate,
            stock
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            price = excluded.price,
            original_price = excluded.original_price,
            latest_discount_price = excluded.latest_discount_price,
            price_cny = excluded.price_cny,
            original_price_cny = excluded.original_price_cny,
            shipping_fee_cny = excluded.shipping_fee_cny,
            final_price_cny = excluded.final_price_cny,
            exchange_rate = excluded.exchange_rate,
            profit_rate = excluded.profit_rate,
            stock = excluded.stock,
            updated_at = CURRENT_TIMESTAMP
    """, (
        product_id,
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
    ))

    conn.commit()
    conn.close()


def get_latest_update(product_id: int):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            price,
            original_price,
            stock,
            latest_discount_price,
            price_cny,
            original_price_cny,
            shipping_fee_cny,
            final_price_cny,
            exchange_rate,
            profit_rate
        FROM product_updates
        WHERE product_id = ?
    """, (product_id,))

    row = cursor.fetchone()
    conn.close()
    return row


# ==========================
# 变化日志
# ==========================
def insert_change_log(product_id: int, field_name: str, old_value: str, new_value: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO change_logs (product_id, field_name, old_value, new_value)
        VALUES (?, ?, ?, ?)
    """, (product_id, field_name, old_value, new_value))

    conn.commit()
    conn.close()


# ==========================
# 下架逻辑
# ==========================
def increment_missing_days_for_not_seen_today(source: str, today: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE products
        SET missing_days = missing_days + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE source = ?
          AND status = 'active'
          AND (last_seen IS NULL OR last_seen != ?)
    """, (source, today))

    conn.commit()
    conn.close()


def reset_missing_days_for_seen_today(source: str, today: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE products
        SET missing_days = 0,
            status = 'active',
            updated_at = CURRENT_TIMESTAMP
        WHERE source = ?
          AND last_seen = ?
    """, (source, today))

    conn.commit()
    conn.close()


def mark_inactive_products(days_threshold: int = 3, source: Optional[str] = None):
    conn = get_connection()
    cursor = conn.cursor()

    if source:
        cursor.execute("""
            UPDATE products
            SET status = 'inactive',
                updated_at = CURRENT_TIMESTAMP
            WHERE source = ?
              AND missing_days >= ?
        """, (source, days_threshold))
    else:
        cursor.execute("""
            UPDATE products
            SET status = 'inactive',
                updated_at = CURRENT_TIMESTAMP
            WHERE missing_days >= ?
        """, (days_threshold,))

    conn.commit()
    conn.close()
