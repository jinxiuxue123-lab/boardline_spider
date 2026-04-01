import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open.batch_runner import execute_batch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", type=int, required=True, help="xianyu_publish_batches.id")
    parser.add_argument("--execute", action="store_true", help="实际调用开放平台接口")
    parser.add_argument("--skip-publish", action="store_true", help="仅创建商品，不调用上架接口")
    parser.add_argument("--limit", type=int, default=0, help="限制本次处理数量，默认全批次")
    args = parser.parse_args()

    result = execute_batch(args.batch_id, args.execute, args.skip_publish, args.limit or None)
    for failure in result.get("failures", []):
        print(f"任务失败: {failure['task_id']} | {failure['error']}")
    print(result["message"])
    print(f"成功数: {result['success_count']}")
    print(f"失败数: {result['failed_count']}")


if __name__ == "__main__":
    main()
