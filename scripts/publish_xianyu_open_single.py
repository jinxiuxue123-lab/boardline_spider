import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open import XianyuOpenClient, update_batch_counts, update_task_meta
from xianyu_open.payload_builder import (
    build_create_payload,
    build_publish_payload,
    get_category_mapping,
    get_publish_task,
    load_publish_defaults,
)

DB_FILE = "products.db"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", type=int, required=True, help="xianyu_publish_tasks.id")
    parser.add_argument("--execute", action="store_true", help="实际调用开放平台接口")
    parser.add_argument("--skip-publish", action="store_true", help="仅创建商品，不调用上架接口")
    args = parser.parse_args()

    task = get_publish_task(args.task_id)
    defaults = load_publish_defaults()
    category_mapping = get_category_mapping(task["category"])
    create_payload = build_create_payload(task, defaults, category_mapping)

    update_task_meta(
        args.task_id,
        channel_cat_id=category_mapping["channel_cat_id"],
        channel_cat_name=category_mapping["channel_cat_name"],
        publish_payload_json=json.dumps(create_payload, ensure_ascii=False),
        status="payload_ready",
        publish_status="payload_ready",
    )

    print("已生成创建 payload")
    print(json.dumps(create_payload, ensure_ascii=False, indent=2))

    if not args.execute:
        print("\n当前为预览模式，未实际调用接口。")
        print("如需实际创建并上架，请添加 --execute")
        return

    client = XianyuOpenClient(
        app_key=(task.get("app_key") or "").strip() or None,
        app_secret=(task.get("app_secret") or "").strip() or None,
    )

    try:
        create_resp = client.post("/api/open/product/create", create_payload)
        print("\n创建接口返回:")
        print(json.dumps(create_resp, ensure_ascii=False, indent=2))

        data = create_resp.get("data") or {}
        third_product_id = data.get("product_id") or data.get("id") or ""
        if not third_product_id:
            raise RuntimeError(f"创建接口返回缺少 product_id: {create_resp}")

        publish_resp = {"skipped": True}
        status = "created"
        publish_status = "created"

        if not args.skip_publish:
            publish_payload = build_publish_payload(
                third_product_id,
                task.get("account_user_name") or defaults.get("user_name", ""),
                defaults.get("callback_url", ""),
            )
            publish_resp = client.post("/api/open/product/publish", publish_payload)

            print("\n上架接口返回:")
            print(json.dumps(publish_resp, ensure_ascii=False, indent=2))
            status = "submitted"
            publish_status = "submitted"

        update_task_meta(
            args.task_id,
            third_product_id=str(third_product_id),
            status=status,
            publish_status=publish_status,
            task_result=json.dumps(
                {
                    "create_resp": create_resp,
                    "publish_resp": publish_resp,
                },
                ensure_ascii=False,
            ),
            last_error="",
            err_code="",
            err_msg="",
        )
        if task.get("batch_id"):
            update_batch_counts(int(task["batch_id"]))
    except Exception as e:
        update_task_meta(
            args.task_id,
            status="failed",
            publish_status="failed",
            last_error=str(e),
            err_msg=str(e),
        )
        if task.get("batch_id"):
            update_batch_counts(int(task["batch_id"]))
        raise


if __name__ == "__main__":
    main()
