import sqlite3
from typing import Iterable


DB_FILE = "products.db"

ONE8_GROUPABLE_CATEGORIES = {
    "固定器",
    "滑雪鞋",
    "滑雪服",
    "手套",
    "滑雪镜",
    "滑雪头盔",
    "滑雪帽衫和中间层",
}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_product_group_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS product_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            category TEXT NOT NULL,
            group_key TEXT NOT NULL UNIQUE,
            group_name TEXT NOT NULL,
            spec_axis TEXT DEFAULT 'color',
            item_count INTEGER DEFAULT 0,
            color_summary TEXT DEFAULT '',
            cover_product_id INTEGER,
            publish_mode TEXT DEFAULT 'group',
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS product_group_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL UNIQUE,
            color_value TEXT DEFAULT '',
            sort_order INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(group_id) REFERENCES product_groups(id),
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_product_groups_source_category
        ON product_groups(source, category)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_product_group_members_group
        ON product_group_members(group_id, sort_order, product_id)
        """
    )
    conn.commit()
    conn.close()


def ensure_xianyu_group_task_support() -> None:
    conn = get_connection()
    cur = conn.cursor()
    columns = {row["name"] for row in cur.execute("PRAGMA table_info(xianyu_publish_tasks)").fetchall()}
    extra_columns = {
        "publish_mode": "TEXT DEFAULT 'single'",
        "group_id": "INTEGER",
        "group_member_product_ids": "TEXT DEFAULT ''",
        "cover_product_id": "INTEGER",
        "selected_group_images_json": "TEXT DEFAULT ''",
    }
    for name, col_type in extra_columns.items():
        if name not in columns:
            cur.execute(f"ALTER TABLE xianyu_publish_tasks ADD COLUMN {name} {col_type}")
    conn.commit()
    conn.close()


def build_group_key(source: str, category: str, name: str) -> str:
    return f"{str(source or '').strip().lower()}::{str(category or '').strip()}::{str(name or '').strip()}"


def _normalize_color_items(rows: Iterable[sqlite3.Row]) -> tuple[list[sqlite3.Row], list[str]]:
    ordered_rows = []
    seen_colors = []
    color_seen = set()
    for row in rows:
        color_value = str(row["color"] or "").strip()
        if not color_value:
            continue
        ordered_rows.append(row)
        if color_value not in color_seen:
            color_seen.add(color_value)
            seen_colors.append(color_value)
    return ordered_rows, seen_colors


def refresh_one8_product_groups() -> dict:
    ensure_product_group_tables()
    conn = get_connection()
    cur = conn.cursor()
    existing_group_ids = {
        str(row["group_key"] or ""): int(row["id"])
        for row in cur.execute(
            """
            SELECT id, group_key
            FROM product_groups
            WHERE source = 'one8'
            """
        ).fetchall()
    }

    rows = cur.execute(
        f"""
        SELECT
            id AS product_id,
            source,
            category,
            name,
            COALESCE(color, '') AS color
        FROM products
        WHERE status = 'active'
          AND source = 'one8'
          AND category IN ({",".join("?" for _ in ONE8_GROUPABLE_CATEGORIES)})
        ORDER BY category, name, id
        """,
        tuple(sorted(ONE8_GROUPABLE_CATEGORIES)),
    ).fetchall()

    grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in rows:
        key = (str(row["category"] or "").strip(), str(row["name"] or "").strip())
        if not key[0] or not key[1]:
            continue
        grouped.setdefault(key, []).append(row)

    valid_groups = []
    for (category, name), group_rows in grouped.items():
        normalized_rows, colors = _normalize_color_items(group_rows)
        if len(normalized_rows) < 2:
            continue
        if len(colors) < 2:
            continue
        valid_groups.append(
            {
                "source": "one8",
                "category": category,
                "name": name,
                "group_key": build_group_key("one8", category, name),
                "color_summary": " | ".join(colors),
                "rows": normalized_rows,
                "colors": colors,
            }
        )

    cur.execute(
        """
        DELETE FROM product_group_members
        WHERE group_id IN (SELECT id FROM product_groups WHERE source = 'one8')
        """
    )
    cur.execute("DELETE FROM product_groups WHERE source = 'one8'")

    member_count = 0
    for item in valid_groups:
        cover_product_id = int(item["rows"][0]["product_id"])
        existing_id = existing_group_ids.get(item["group_key"])
        if existing_id:
            cur.execute(
                """
                INSERT INTO product_groups (
                    id,
                    source,
                    category,
                    group_key,
                    group_name,
                    spec_axis,
                    item_count,
                    color_summary,
                    cover_product_id,
                    publish_mode,
                    status
                )
                VALUES (?, ?, ?, ?, ?, 'color', ?, ?, ?, 'group', 'active')
                """,
                (
                    existing_id,
                    item["source"],
                    item["category"],
                    item["group_key"],
                    item["name"],
                    len(item["rows"]),
                    item["color_summary"],
                    cover_product_id,
                ),
            )
            group_id = int(existing_id)
        else:
            cur.execute(
                """
                INSERT INTO product_groups (
                    source,
                    category,
                    group_key,
                    group_name,
                    spec_axis,
                    item_count,
                    color_summary,
                    cover_product_id,
                    publish_mode,
                    status
                )
                VALUES (?, ?, ?, ?, 'color', ?, ?, ?, 'group', 'active')
                """,
                (
                    item["source"],
                    item["category"],
                    item["group_key"],
                    item["name"],
                    len(item["rows"]),
                    item["color_summary"],
                    cover_product_id,
                ),
            )
            group_id = int(cur.lastrowid)
        for sort_order, row in enumerate(item["rows"], start=1):
            cur.execute(
                """
                INSERT INTO product_group_members (
                    group_id,
                    product_id,
                    color_value,
                    sort_order
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    group_id,
                    int(row["product_id"]),
                    str(row["color"] or "").strip(),
                    sort_order,
                ),
            )
            member_count += 1

    conn.commit()
    conn.close()
    return {
        "group_count": len(valid_groups),
        "member_count": member_count,
    }


def load_one8_product_groups(limit: int = 20) -> list[sqlite3.Row]:
    ensure_product_group_tables()
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            g.id,
            g.category,
            g.group_name,
            g.item_count,
            g.color_summary,
            g.cover_product_id
        FROM product_groups g
        WHERE g.source = 'one8'
        ORDER BY g.category, g.group_name, g.id
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    conn.close()
    return rows


def find_group_by_member_ids(member_product_ids: list[int]) -> sqlite3.Row | None:
    ensure_product_group_tables()
    cleaned_ids = [int(pid) for pid in member_product_ids if int(pid) > 0]
    if not cleaned_ids:
        return None
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            g.*,
            GROUP_CONCAT(m.product_id, ',') AS member_ids,
            COUNT(*) AS member_count
        FROM product_groups g
        JOIN product_group_members m
          ON m.group_id = g.id
        WHERE g.source = 'one8'
        GROUP BY g.id
        ORDER BY g.id
        """
    ).fetchall()
    conn.close()
    wanted = ",".join(str(pid) for pid in sorted(cleaned_ids))
    for row in rows:
        actual_ids = [int(pid) for pid in str(row["member_ids"] or "").split(",") if str(pid).strip().isdigit()]
        actual = ",".join(str(pid) for pid in sorted(actual_ids))
        if actual == wanted:
            return row
    return None


def find_group_by_member_ids_relaxed(member_product_ids: list[int]) -> sqlite3.Row | None:
    exact = find_group_by_member_ids(member_product_ids)
    if exact:
        return exact
    cleaned_ids = [int(pid) for pid in member_product_ids if int(pid) > 0]
    if not cleaned_ids:
        return None
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT id AS product_id, COALESCE(color, '') AS color
        FROM products
        WHERE id IN ({})
        ORDER BY id
        """.format(",".join("?" for _ in cleaned_ids)),
        tuple(cleaned_ids),
    ).fetchall()
    conn.close()
    colored_ids = [int(row["product_id"]) for row in rows if str(row["color"] or "").strip()]
    if len(colored_ids) < 2:
        return None
    return find_group_by_member_ids(colored_ids)
