import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import time

from .client import XianyuOpenClient
from .delete_product import build_delete_payload
from .payload_builder import (
    build_create_payload,
    build_publish_payload,
    get_category_mapping,
    get_publish_task,
    load_account_shipping_regions,
    load_publish_defaults,
)
from .task_ops import update_batch_counts, update_task_meta


DB_FILE = "products.db"
MAX_BATCH_CREATE_SIZE = 10
LOG_DIR = Path("data/xianyu_batch_logs")


def _parse_publish_time(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _format_publish_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _ensure_min_publish_time(value: datetime, min_lead_seconds: int = 660) -> datetime:
    minimum = datetime.now() + timedelta(seconds=min_lead_seconds)
    if value < minimum:
        return minimum
    return value


def get_batch_tasks(
    batch_id: int,
    limit: int | None = None,
    failed_only: bool = False,
    created_only: bool = False,
    publish_retry_only: bool = False,
) -> list[int]:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    if failed_only:
        sql = """
            SELECT id
            FROM xianyu_publish_tasks
            WHERE batch_id = ?
              AND COALESCE(third_product_id, '') = ''
              AND status IN ('failed', 'payload_ready', 'pending')
            ORDER BY id
        """
        params = [batch_id]
    elif created_only:
        sql = """
            SELECT id
            FROM xianyu_publish_tasks
            WHERE batch_id = ?
              AND COALESCE(third_product_id, '') != ''
              AND status = 'created'
              AND COALESCE(publish_status, '') = 'created'
            ORDER BY id
        """
        params = [batch_id]
    elif publish_retry_only:
        sql = """
            SELECT id
            FROM xianyu_publish_tasks
            WHERE batch_id = ?
              AND status NOT IN ('published', 'success')
              AND (
                COALESCE(third_product_id, '') != ''
                OR status IN ('failed', 'publish_failed', 'created', 'submitted')
              )
            ORDER BY id
        """
        params = [batch_id]
    else:
        sql = """
            SELECT id
            FROM xianyu_publish_tasks
            WHERE batch_id = ?
              AND status NOT IN ('published', 'success')
            ORDER BY id
        """
        params = [batch_id]
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    cur.execute(sql, params)
    rows = [row[0] for row in cur.fetchall()]
    conn.close()
    return rows


def chunked(seq: list, size: int) -> list[list]:
    return [seq[index:index + size] for index in range(0, len(seq), size)]


def write_batch_create_log(batch_id: int, item_ids: list[int], payload: dict, response=None, error: str = "") -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        file_path = LOG_DIR / f"batch_{batch_id}_items_{item_ids[0]}_{item_ids[-1]}_{timestamp}.json"
        file_path.write_text(
            json.dumps(
                {
                    "batch_id": batch_id,
                    "task_ids": item_ids,
                    "payload_summary": {
                        "count": len(item_ids),
                        "item_keys": [str(item.get("item_key") or "") for item in payload.get("product_data") or []],
                    },
                    "response": response,
                    "error": error,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception:
        pass


def extract_task_outer_id(task: dict) -> str:
    payload_text = str(task.get("publish_payload_json") or "").strip()
    if payload_text:
        try:
            payload = json.loads(payload_text)
        except Exception:
            payload = {}
        outer_id = str(payload.get("outer_id") or "").strip()
        if outer_id:
            return outer_id
    return str(task.get("branduid") or task.get("product_id") or "").strip()


def infer_local_status_from_remote(product_status) -> tuple[str, str]:
    try:
        status_code = int(product_status)
    except Exception:
        status_code = 0
    if status_code == 22:
        return "published", "published"
    if status_code == 21:
        return "submitted", "submitted"
    if status_code in (23, 31, 33, 36):
        return "created", "created"
    return "created", "created"


def is_publish_account_mismatch_error(error: Exception | str) -> bool:
    text = str(error or "")
    return "当前闲鱼号与商品闲鱼号不匹配" in text


def _parse_db_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def get_batch_remote_window(batch_id: int) -> tuple[int | None, int]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT MIN(created_at) AS first_created_at
        FROM xianyu_publish_tasks
        WHERE batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    conn.close()
    first_created_at = _parse_db_timestamp(row["first_created_at"] if row else "")
    if not first_created_at:
        return None, int(datetime.now().timestamp())
    # Leave a buffer before the first local create attempt to tolerate clock drift.
    start_ts = max(0, int((first_created_at - timedelta(minutes=10)).timestamp()))
    end_ts = int(datetime.now().timestamp())
    return start_ts, end_ts


def fetch_remote_product_map(
    task: dict,
    lookback_days: int = 3,
    max_pages: int = 20,
    update_time_range: tuple[int, int] | None = None,
) -> dict[str, dict]:
    client = XianyuOpenClient(
        app_key=(task.get("app_key") or "").strip() or None,
        app_secret=(task.get("app_secret") or "").strip() or None,
    )
    now_ts = int(datetime.now().timestamp())
    start_ts = int((datetime.now() - timedelta(days=lookback_days)).timestamp())
    if update_time_range:
        start_ts, now_ts = update_time_range
    page_no = 1
    page_size = 100
    remote_map: dict[str, dict] = {}
    while page_no <= max_pages:
        resp = client.post(
            "/api/open/product/list",
            {
                "update_time": [start_ts, now_ts],
                "page_no": page_no,
                "page_size": page_size,
            },
        )
        data = resp.get("data") or {}
        items = data.get("list") or []
        for row in items:
            outer_id = str(row.get("outer_id") or "").strip()
            if not outer_id:
                continue
            existing = remote_map.get(outer_id)
            current_ts = int(row.get("update_time") or row.get("create_time") or 0)
            existing_ts = int((existing or {}).get("update_time") or (existing or {}).get("create_time") or 0)
            if existing is None or current_ts >= existing_ts:
                remote_map[outer_id] = row
        count = int(data.get("count") or 0)
        if not items or page_no * page_size >= count:
            break
        page_no += 1
    return remote_map


def fetch_remote_product_map_by_status(
    task: dict,
    target_status: int,
    lookback_days: int = 3,
    max_pages: int = 20,
    update_time_range: tuple[int, int] | None = None,
) -> dict[str, dict]:
    client = XianyuOpenClient(
        app_key=(task.get("app_key") or "").strip() or None,
        app_secret=(task.get("app_secret") or "").strip() or None,
    )
    now_ts = int(datetime.now().timestamp())
    start_ts = int((datetime.now() - timedelta(days=lookback_days)).timestamp())
    if update_time_range:
        start_ts, now_ts = update_time_range
    page_no = 1
    page_size = 100
    remote_map: dict[str, dict] = {}
    while page_no <= max_pages:
        resp = client.post(
            "/api/open/product/list",
            {
                "update_time": [start_ts, now_ts],
                "product_status": target_status,
                "page_no": page_no,
                "page_size": page_size,
            },
        )
        data = resp.get("data") or {}
        items = data.get("list") or []
        for row in items:
            outer_id = str(row.get("outer_id") or "").strip()
            if not outer_id:
                continue
            existing = remote_map.get(outer_id)
            current_ts = int(row.get("update_time") or row.get("create_time") or 0)
            existing_ts = int((existing or {}).get("update_time") or (existing or {}).get("create_time") or 0)
            if existing is None or current_ts >= existing_ts:
                remote_map[outer_id] = row
        count = int(data.get("count") or 0)
        if not items or page_no * page_size >= count:
            break
        page_no += 1
    return remote_map


def reconcile_remote_created_tasks(batch_id: int, candidate_task_ids: list[int], use_batch_window: bool = False) -> dict:
    if not candidate_task_ids:
        return {"matched_count": 0, "matched_task_ids": []}

    tasks = [get_publish_task(task_id) for task_id in candidate_task_ids]
    if not tasks:
        return {"matched_count": 0, "matched_task_ids": []}

    remote_window = get_batch_remote_window(batch_id) if use_batch_window else None
    remote_map = fetch_remote_product_map(tasks[0], update_time_range=remote_window)
    matched_task_ids: list[int] = []
    for task in tasks:
        outer_id = extract_task_outer_id(task)
        remote = remote_map.get(outer_id)
        if not remote:
            continue
        third_product_id = str(remote.get("product_id") or "").strip()
        if not third_product_id:
            continue
        status, publish_status = infer_local_status_from_remote(remote.get("product_status"))
        update_task_meta(
            int(task["id"]),
            third_product_id=third_product_id,
            status=status,
            publish_status=publish_status,
            callback_status="remote_synced",
            last_error="",
            err_msg="",
            task_result=json.dumps(
                {"remote_list_sync": remote},
                ensure_ascii=False,
            ),
        )
        matched_task_ids.append(int(task["id"]))
    return {"matched_count": len(matched_task_ids), "matched_task_ids": matched_task_ids}


def get_remote_pending_publish_task_ids(batch_id: int, limit: int | None = None) -> dict:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id
        FROM xianyu_publish_tasks
        WHERE batch_id = ?
        ORDER BY id
        """,
        (batch_id,),
    ).fetchall()
    conn.close()
    if not rows:
        return {"matched_count": 0, "matched_task_ids": []}

    task_ids = [int(row["id"]) for row in rows]
    if limit:
        task_ids = task_ids[:limit]
    tasks = [get_publish_task(task_id) for task_id in task_ids]
    if not tasks:
        return {"matched_count": 0, "matched_task_ids": []}

    remote_map = fetch_remote_product_map_by_status(
        tasks[0],
        target_status=21,
        update_time_range=get_batch_remote_window(batch_id),
    )
    matched_task_ids: list[int] = []
    for task in tasks:
        outer_id = extract_task_outer_id(task)
        remote = remote_map.get(outer_id)
        if not remote:
            continue
        try:
            status_code = int(remote.get("product_status") or 0)
        except Exception:
            status_code = 0
        if status_code != 21:
            continue
        remote_product_id = str(remote.get("product_id") or "").strip()
        if not remote_product_id:
            continue
        update_task_meta(
            int(task["id"]),
            third_product_id=remote_product_id,
            status="created",
            publish_status="created",
            callback_status="remote_synced",
            last_error="",
            err_code="",
            err_msg="",
            task_result=json.dumps(
                {"remote_pending_sync": remote},
                ensure_ascii=False,
            ),
        )
        matched_task_ids.append(int(task["id"]))
    return {"matched_count": len(matched_task_ids), "matched_task_ids": matched_task_ids}


def prepare_create_task(task_id: int, upload_watermark: bool = False, shipping_region_override: dict | None = None) -> dict:
    task = get_publish_task(task_id)
    defaults = load_publish_defaults()
    category_mapping = get_category_mapping(task["category"])
    create_payload = build_create_payload(
        task,
        defaults,
        category_mapping,
        upload_watermark=upload_watermark,
        shipping_region_override=shipping_region_override,
    )

    update_task_meta(
        task_id,
        channel_cat_id=category_mapping["channel_cat_id"],
        channel_cat_name=category_mapping["channel_cat_name"],
        publish_payload_json=json.dumps(create_payload, ensure_ascii=False),
        status="payload_ready",
        publish_status="payload_ready",
        callback_status="",
        last_error="",
        err_code="",
        err_msg="",
    )
    return {
        "task_id": task_id,
        "task": task,
        "defaults": defaults,
        "category_mapping": category_mapping,
        "create_payload": create_payload,
    }


def execute_batch_create_chunk(
    batch_id: int,
    items: list[dict],
    skip_publish: bool,
    publish_time_map: dict[int, str],
) -> tuple[int, int, list[dict]]:
    if not items:
        return 0, 0, []

    first_task = items[0]["task"]
    client = XianyuOpenClient(
        app_key=(first_task.get("app_key") or "").strip() or None,
        app_secret=(first_task.get("app_secret") or "").strip() or None,
    )

    payload = {
        "product_data": [
            {"item_key": str(item["task_id"]), **item["create_payload"]}
            for item in items
        ]
    }
    item_ids = [int(item["task_id"]) for item in items]
    try:
        batch_resp = client.post("/api/open/product/batchCreate", payload)
        write_batch_create_log(batch_id, item_ids, payload, response=batch_resp)
    except Exception as e:
        write_batch_create_log(batch_id, item_ids, payload, error=str(e))
        raise
    data = batch_resp.get("data") or {}
    success_rows = data.get("success") or []
    error_rows = data.get("error") or []
    success_map = {str(row.get("item_key") or ""): row for row in success_rows}
    error_map = {str(row.get("item_key") or ""): row for row in error_rows}

    success_count = 0
    failed_count = 0
    failures: list[dict] = []

    for item in items:
        task_id = int(item["task_id"])
        key = str(task_id)
        task = item["task"]
        defaults = item["defaults"]

        if key in error_map:
            error_msg = str(error_map[key].get("msg") or "批量创建失败").strip()
            failed_count += 1
            update_task_meta(
                task_id,
                status="failed",
                publish_status="failed",
                last_error=error_msg,
                err_msg=error_msg,
                task_result=json.dumps(
                    {"batch_create_resp": error_map[key]},
                    ensure_ascii=False,
                ),
            )
            failures.append({"task_id": task_id, "error": error_msg})
            continue

        success_row = success_map.get(key)
        if not success_row:
            error_msg = "批量创建接口返回缺少当前任务结果"
            failed_count += 1
            update_task_meta(
                task_id,
                status="failed",
                publish_status="failed",
                last_error=error_msg,
                err_msg=error_msg,
                task_result=json.dumps(
                    {"batch_create_resp": batch_resp},
                    ensure_ascii=False,
                ),
            )
            failures.append({"task_id": task_id, "error": error_msg})
            continue

        third_product_id = success_row.get("product_id") or ""
        if not third_product_id:
            error_msg = f"批量创建接口返回缺少 product_id: {success_row}"
            failed_count += 1
            update_task_meta(
                task_id,
                status="failed",
                publish_status="failed",
                last_error=error_msg,
                err_msg=error_msg,
                task_result=json.dumps(
                    {"batch_create_resp": success_row},
                    ensure_ascii=False,
                ),
            )
            failures.append({"task_id": task_id, "error": error_msg})
            continue

        publish_resp = {"skipped": True}
        status = "created"
        publish_status = "created"
        try:
            if not skip_publish:
                user_name = (task.get("account_user_name") or "").strip()
                if not user_name:
                    raise ValueError("账号缺少 user_name，无法调用上架接口")
                publish_resp = client.post(
                    "/api/open/product/publish",
                    build_publish_payload(
                        str(third_product_id),
                        user_name,
                        defaults.get("callback_url", ""),
                        publish_time_map.get(task_id, ""),
                    ),
                )
                status = "submitted"
                publish_status = "submitted"

            update_task_meta(
                task_id,
                third_product_id=str(third_product_id),
                status=status,
                publish_status=publish_status,
                callback_status="",
                task_result=json.dumps(
                    {
                        "batch_create_resp": success_row,
                        "publish_resp": publish_resp,
                        "publish_request": {"specify_publish_time": publish_time_map.get(task_id, "")},
                    },
                    ensure_ascii=False,
                ),
                last_error="",
                err_code="",
                err_msg="",
            )
            success_count += 1
        except Exception as e:
            failed_count += 1
            mismatch = is_publish_account_mismatch_error(e)
            update_task_meta(
                task_id,
                third_product_id="" if mismatch else str(third_product_id),
                status="failed",
                publish_status="failed",
                last_error=str(e),
                err_msg=str(e),
                task_result=json.dumps(
                    {"batch_create_resp": success_row},
                    ensure_ascii=False,
                ),
            )
            failures.append({"task_id": task_id, "error": str(e)})

    return success_count, failed_count, failures


def run_task(
    task_id: int,
    execute: bool,
    skip_publish: bool,
    upload_watermark: bool = False,
    specify_publish_time: str = "",
    shipping_region_override: dict | None = None,
) -> tuple[bool, str]:
    task = get_publish_task(task_id)
    defaults = load_publish_defaults()
    existing_product_id = (task.get("third_product_id") or "").strip()

    if existing_product_id:
        if not execute:
            return True, "created"
        if skip_publish:
            return True, "created"

        user_name = (task.get("account_user_name") or "").strip()
        if not user_name:
            raise ValueError("账号缺少 user_name，无法调用上架接口")

        client = XianyuOpenClient(
            app_key=(task.get("app_key") or "").strip() or None,
            app_secret=(task.get("app_secret") or "").strip() or None,
        )
        try:
            publish_resp = client.post(
                "/api/open/product/publish",
                build_publish_payload(existing_product_id, user_name, defaults.get("callback_url", ""), specify_publish_time),
            )
            update_task_meta(
                task_id,
                status="submitted",
                publish_status="submitted",
                callback_status="",
                task_result=json.dumps(
                    {
                        "publish_resp": publish_resp,
                        "publish_request": {"specify_publish_time": specify_publish_time},
                    },
                    ensure_ascii=False,
                ),
                last_error="",
                err_code="",
                err_msg="",
            )
            return True, "submitted"
        except Exception as e:
            mismatch = is_publish_account_mismatch_error(e)
            update_task_meta(
                task_id,
                third_product_id="" if mismatch else existing_product_id,
                status="failed",
                publish_status="failed",
                last_error=str(e),
                err_msg=str(e),
            )
            raise

    category_mapping = get_category_mapping(task["category"])
    create_payload = build_create_payload(
        task,
        defaults,
        category_mapping,
        upload_watermark=upload_watermark,
        shipping_region_override=shipping_region_override,
    )

    update_task_meta(
        task_id,
        channel_cat_id=category_mapping["channel_cat_id"],
        channel_cat_name=category_mapping["channel_cat_name"],
        publish_payload_json=json.dumps(create_payload, ensure_ascii=False),
        status="payload_ready",
        publish_status="payload_ready",
        callback_status="",
        last_error="",
        err_code="",
        err_msg="",
    )

    if not execute:
        return True, "payload_ready"

    client = XianyuOpenClient(
        app_key=(task.get("app_key") or "").strip() or None,
        app_secret=(task.get("app_secret") or "").strip() or None,
    )

    create_resp = client.post("/api/open/product/create", create_payload)
    data = create_resp.get("data") or {}
    third_product_id = data.get("product_id") or data.get("id") or ""
    if not third_product_id:
        raise RuntimeError(f"创建接口返回缺少 product_id: {create_resp}")

    publish_resp = {"skipped": True}
    status = "created"
    publish_status = "created"
    try:
        if not skip_publish:
            user_name = (task.get("account_user_name") or "").strip()
            if not user_name:
                raise ValueError("账号缺少 user_name，无法调用上架接口")

            publish_resp = client.post(
                "/api/open/product/publish",
                build_publish_payload(third_product_id, user_name, defaults.get("callback_url", ""), specify_publish_time),
            )
            status = "submitted"
            publish_status = "submitted"

        update_task_meta(
            task_id,
            third_product_id=str(third_product_id),
            status=status,
            publish_status=publish_status,
            callback_status="",
            task_result=json.dumps(
                {
                    "create_resp": create_resp,
                    "publish_resp": publish_resp,
                    "publish_request": {"specify_publish_time": specify_publish_time},
                },
                ensure_ascii=False,
            ),
            last_error="",
            err_code="",
            err_msg="",
        )
        return True, status
    except Exception as e:
        mismatch = is_publish_account_mismatch_error(e)
        update_task_meta(
            task_id,
            third_product_id="" if mismatch else str(third_product_id),
            status="failed",
            publish_status="failed",
            last_error=str(e),
            err_msg=str(e),
            task_result=json.dumps(
                {"create_resp": create_resp},
                ensure_ascii=False,
            ),
        )
        raise


def execute_batch(
    batch_id: int,
    execute: bool,
    skip_publish: bool,
    limit: int | None = None,
    failed_only: bool = False,
    created_only: bool = False,
    publish_retry_only: bool = False,
    recreate_pending_only: bool = False,
    upload_watermark: bool = False,
    specify_publish_time: str = "",
    auto_stagger_publish: bool = False,
    progress_callback=None,
) -> dict:
    timer_started = time.perf_counter()
    timing = {
        "load_tasks_seconds": 0.0,
        "precheck_seconds": 0.0,
        "prepare_and_run_seconds": 0.0,
        "batch_create_seconds": 0.0,
        "reconcile_seconds": 0.0,
        "total_seconds": 0.0,
    }

    def report_progress(message: str) -> None:
        if progress_callback:
            try:
                progress_callback(message)
            except Exception:
                pass

    stage_started = time.perf_counter()
    if execute and recreate_pending_only:
        pending_remote = get_remote_pending_publish_task_ids(batch_id, limit=limit)
        task_ids = pending_remote.get("matched_task_ids") or []
    else:
        task_ids = get_batch_tasks(
            batch_id,
            limit,
            failed_only=failed_only,
            created_only=created_only,
            publish_retry_only=publish_retry_only,
        )
    timing["load_tasks_seconds"] = round(time.perf_counter() - stage_started, 2)

    stage_started = time.perf_counter()
    if execute and publish_retry_only and task_ids:
        reconcile_remote_created_tasks(batch_id, task_ids, use_batch_window=True)
        task_ids = get_batch_tasks(batch_id, limit, created_only=True)
    timing["precheck_seconds"] = round(time.perf_counter() - stage_started, 2)
    if not task_ids:
        timing["total_seconds"] = round(time.perf_counter() - timer_started, 2)
        return {"success_count": 0, "failed_count": 0, "message": "该批次没有可处理任务", "total_count": 0}

    report_progress(f"已加载 {len(task_ids)} 个任务，正在准备执行。")
    success_count = 0
    failed_count = 0
    failures = []
    stagger_enabled = execute and (not skip_publish) and auto_stagger_publish and len(task_ids) > 1
    stagger_base = _ensure_min_publish_time(_parse_publish_time(specify_publish_time) or datetime.now())
    publish_time_map = {}
    shipping_region_map: dict[int, dict] = {}
    shipping_regions = []
    shipping_group_size = 0
    if execute and task_ids:
        first_task = get_publish_task(task_ids[0])
        shipping_regions, shipping_group_size = load_account_shipping_regions(first_task)
    for index, task_id in enumerate(task_ids):
        task_publish_time = specify_publish_time
        if stagger_enabled:
            task_publish_time = _format_publish_time(stagger_base + timedelta(minutes=index))
        publish_time_map[task_id] = task_publish_time
        if shipping_regions:
            region_index = (index // shipping_group_size) % len(shipping_regions)
            shipping_region_map[task_id] = shipping_regions[region_index]

    pending_batch_create = []

    stage_started = time.perf_counter()
    for index, task_id in enumerate(task_ids, start=1):
        try:
            if execute:
                task = get_publish_task(task_id)
                existing_product_id = (task.get("third_product_id") or "").strip()
                if not existing_product_id:
                    report_progress(f"正在准备创建任务 {index}/{len(task_ids)}，任务ID {task_id}。")
                    prepared = prepare_create_task(
                        task_id,
                        upload_watermark=upload_watermark,
                        shipping_region_override=shipping_region_map.get(task_id),
                    )
                    pending_batch_create.append(prepared)
                    continue
            report_progress(f"正在处理任务 {index}/{len(task_ids)}，任务ID {task_id}。")
            ok, status = run_task(
                task_id,
                execute,
                skip_publish,
                upload_watermark=upload_watermark,
                specify_publish_time=publish_time_map.get(task_id, ""),
                shipping_region_override=shipping_region_map.get(task_id),
            )
            if ok:
                success_count += 1
        except Exception as e:
            failed_count += 1
            update_task_meta(
                task_id,
                status="failed",
                publish_status="failed",
                last_error=str(e),
                err_msg=str(e),
            )
            failures.append({"task_id": task_id, "error": str(e)})
    timing["prepare_and_run_seconds"] = round(time.perf_counter() - stage_started, 2)

    if execute and pending_batch_create:
        stage_started = time.perf_counter()
        total_groups = len(chunked(pending_batch_create, MAX_BATCH_CREATE_SIZE))
        for group_index, group in enumerate(chunked(pending_batch_create, MAX_BATCH_CREATE_SIZE), start=1):
            group_task_ids = [int(item["task_id"]) for item in group]
            report_progress(
                f"正在执行批量创建第 {group_index}/{total_groups} 批，任务ID {group_task_ids[0]}-{group_task_ids[-1]}。"
            )
            try:
                group_success, group_failed, group_failures = execute_batch_create_chunk(
                    batch_id=batch_id,
                    items=group,
                    skip_publish=skip_publish,
                    publish_time_map=publish_time_map,
                )
                success_count += group_success
                failed_count += group_failed
                failures.extend(group_failures)
                report_progress(
                    f"批量创建第 {group_index}/{total_groups} 批完成，成功 {group_success}，失败 {group_failed}。"
                )
            except Exception as e:
                group_error = str(e)
                for item in group:
                    task_id = int(item["task_id"])
                    failed_count += 1
                    update_task_meta(
                        task_id,
                        status="failed",
                        publish_status="failed",
                        last_error=group_error,
                        err_msg=group_error,
                    )
                    failures.append({"task_id": task_id, "error": group_error})
                report_progress(
                    f"批量创建第 {group_index}/{total_groups} 批失败：{group_error}"
                )
        timing["batch_create_seconds"] = round(time.perf_counter() - stage_started, 2)

    if execute and failures:
        stage_started = time.perf_counter()
        report_progress(f"正在对 {len(failures)} 个失败任务做远端结果校正。")
        reconciled = reconcile_remote_created_tasks(
            batch_id,
            [int(item["task_id"]) for item in failures if item.get("task_id")],
            use_batch_window=failed_only,
        )
        matched_ids = set(reconciled.get("matched_task_ids") or [])
        if matched_ids:
            matched_count = len(matched_ids)
            success_count += matched_count
            failed_count = max(0, failed_count - matched_count)
            failures = [item for item in failures if int(item.get("task_id") or 0) not in matched_ids]
        timing["reconcile_seconds"] = round(time.perf_counter() - stage_started, 2)

    update_batch_counts(batch_id)
    report_progress("批次执行已完成，正在汇总最终结果。")
    timing["total_seconds"] = round(time.perf_counter() - timer_started, 2)
    return {
        "success_count": success_count,
        "failed_count": failed_count,
        "failures": failures,
        "total_count": len(task_ids),
        "message": "批次处理完成",
        "timing": timing,
    }
