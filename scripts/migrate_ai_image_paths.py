#!/usr/bin/env python3
import argparse
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "products.db"
DEFAULT_AI_DIR = PROJECT_ROOT / "data" / "ai_generated"


def candidate_paths(original_path: str, ai_dir: Path) -> list[Path]:
    raw_path = str(original_path or "").strip()
    if not raw_path:
        return []

    original = Path(raw_path)
    candidates: list[Path] = []

    if original.name:
        candidates.append((ai_dir / original.name).resolve())

    normalized = raw_path.replace("\\", "/")
    marker = "data/ai_generated/"
    if marker in normalized:
        suffix = normalized.split(marker, 1)[1].lstrip("/")
        candidates.append((ai_dir / suffix).resolve())

    unique: list[Path] = []
    seen = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def main() -> int:
    parser = argparse.ArgumentParser(description="迁移 AI 图历史绝对路径到当前项目目录")
    parser.add_argument("--apply", action="store_true", help="实际写回数据库；默认仅预览")
    parser.add_argument("--db", default=str(DB_PATH), help="SQLite 数据库路径")
    parser.add_argument("--ai-dir", default=str(DEFAULT_AI_DIR), help="当前 AI 图片目录")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    ai_dir = Path(args.ai_dir).expanduser().resolve()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT id, product_id, account_name, asset_type, ai_main_image_path
        FROM xianyu_product_ai_images
        WHERE TRIM(COALESCE(ai_main_image_path, '')) != ''
        ORDER BY id
        """
    ).fetchall()

    updates: list[tuple[str, int]] = []
    unresolved: list[tuple[int, str]] = []
    for row in rows:
        image_id = int(row["id"])
        current_path = str(row["ai_main_image_path"] or "").strip()
        current_file = Path(current_path)
        if current_file.exists():
            continue

        resolved_target = None
        for candidate in candidate_paths(current_path, ai_dir):
            if candidate.exists():
                resolved_target = candidate
                break

        if resolved_target is None:
            unresolved.append((image_id, current_path))
            continue

        updates.append((str(resolved_target), image_id))

    print(f"扫描记录: {len(rows)}")
    print(f"可迁移: {len(updates)}")
    print(f"未匹配: {len(unresolved)}")

    for new_path, image_id in updates[:20]:
        print(f"[迁移] id={image_id} -> {new_path}")
    if len(updates) > 20:
        print(f"... 还有 {len(updates) - 20} 条可迁移")

    for image_id, old_path in unresolved[:20]:
        print(f"[未匹配] id={image_id} -> {old_path}")
    if len(unresolved) > 20:
        print(f"... 还有 {len(unresolved) - 20} 条未匹配")

    if args.apply and updates:
        conn.executemany(
            """
            UPDATE xianyu_product_ai_images
            SET ai_main_image_path = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            updates,
        )
        conn.commit()
        print(f"已写回 {len(updates)} 条记录")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
