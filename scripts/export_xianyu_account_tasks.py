import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open.reporting import (
    load_account_product_pool,
    load_account_summary,
    load_batches,
    load_task_details,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-name", default="", help="可选，只导出某个账号")
    parser.add_argument("--include-product-pool", action="store_true", help="当指定账号时，额外导出账号商品池和上传标记")
    args = parser.parse_args()

    summary_df = load_account_summary(args.account_name)
    tasks_df = load_task_details(args.account_name)
    batches_df = load_batches(args.account_name)

    suffix = args.account_name.strip() or "all_accounts"
    safe_suffix = suffix.replace("/", "_").replace(" ", "_")
    output_path = Path(f"xianyu_account_tasks_{safe_suffix}.xlsx")

    with pd.ExcelWriter(output_path) as writer:
        summary_df.to_excel(writer, sheet_name="account_summary", index=False)
        batches_df.to_excel(writer, sheet_name="batches", index=False)
        tasks_df.to_excel(writer, sheet_name="tasks", index=False)

        if args.include_product_pool and args.account_name.strip():
            product_pool_df = load_account_product_pool(args.account_name.strip())
            product_pool_df.to_excel(writer, sheet_name="product_pool", index=False)

    print(f"导出完成: {output_path}")
    print(f"任务数量: {len(tasks_df)}")


if __name__ == "__main__":
    main()
