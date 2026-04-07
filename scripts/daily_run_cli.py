import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from daily_run_tracker import (
    ensure_daily_run_tables,
    finish_step,
    get_env_run_id,
    start_run,
    start_step,
    update_run,
    update_step_progress,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="日更运行状态记录 CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("ensure")

    p = sub.add_parser("start-run")
    p.add_argument("--run-type", required=True)
    p.add_argument("--trigger-mode", default="manual")
    p.add_argument("--log-file", default="")
    p.add_argument("--note", default="")

    p = sub.add_parser("update-run")
    p.add_argument("--run-id", type=int, default=0)
    p.add_argument("--status", default=None)
    p.add_argument("--log-file", default=None)
    p.add_argument("--note", default=None)

    p = sub.add_parser("finish-run")
    p.add_argument("--run-id", type=int, default=0)
    p.add_argument("--status", default="success")
    p.add_argument("--note", default="")

    p = sub.add_parser("start-step")
    p.add_argument("--run-id", type=int, default=0)
    p.add_argument("--step-key", required=True)
    p.add_argument("--step-name", required=True)
    p.add_argument("--message", default="")

    p = sub.add_parser("finish-step")
    p.add_argument("--run-id", type=int, default=0)
    p.add_argument("--step-key", required=True)
    p.add_argument("--status", default="success")
    p.add_argument("--message", default="")

    p = sub.add_parser("progress")
    p.add_argument("--run-id", type=int, default=0)
    p.add_argument("--step-key", required=True)
    p.add_argument("--current", type=int, default=None)
    p.add_argument("--total", type=int, default=None)
    p.add_argument("--message", default=None)
    p.add_argument("--log-excerpt", default=None)

    args = parser.parse_args()

    if args.command == "ensure":
        ensure_daily_run_tables()
        return
    run_id = getattr(args, "run_id", 0) or get_env_run_id()
    if args.command == "start-run":
        print(start_run(args.run_type, args.trigger_mode, args.log_file, args.note))
        return
    if args.command == "update-run":
        update_run(run_id, status=args.status, log_file=args.log_file, note=args.note)
        return
    if args.command == "finish-run":
        update_run(run_id, status=args.status, note=args.note)
        return
    if args.command == "start-step":
        start_step(run_id, args.step_key, args.step_name, args.message)
        return
    if args.command == "finish-step":
        finish_step(run_id, args.step_key, args.status, args.message)
        return
    if args.command == "progress":
        update_step_progress(
            run_id,
            args.step_key,
            current=args.current,
            total=args.total,
            message=args.message,
            log_excerpt=args.log_excerpt,
        )
        return


if __name__ == "__main__":
    main()
