import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from product_grouping import load_one8_product_groups, refresh_one8_product_groups


def main():
    result = refresh_one8_product_groups()
    print(f"one8 商品组刷新完成 | 组数: {result['group_count']} | 成员数: {result['member_count']}")
    sample_rows = load_one8_product_groups(limit=10)
    if not sample_rows:
        print("当前没有可用的 one8 商品组。")
        return
    print("示例商品组:")
    for row in sample_rows:
        print(
            f"- [{row['category']}] {row['group_name']} | 组内{int(row['item_count'] or 0)}款 | {row['color_summary']}"
        )


if __name__ == "__main__":
    main()
