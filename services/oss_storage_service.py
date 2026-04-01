import importlib.util
import os
import time
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _normalize_oss_endpoint(endpoint: str) -> str:
    endpoint = (endpoint or "").strip().rstrip("/")
    if not endpoint:
        return ""
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    return f"https://{endpoint}"


def _oss_connect_timeout() -> int:
    value = _env("ALIYUN_OSS_CONNECT_TIMEOUT")
    try:
        return max(30, int(value)) if value else 180
    except ValueError:
        return 180


def is_oss_configured() -> bool:
    return all(
        [
            _env("ALIYUN_OSS_ACCESS_KEY_ID"),
            _env("ALIYUN_OSS_ACCESS_KEY_SECRET"),
            _env("ALIYUN_OSS_BUCKET"),
            _env("ALIYUN_OSS_ENDPOINT"),
        ]
    )


def _require_oss2():
    if importlib.util.find_spec("oss2") is None:
        raise RuntimeError(
            "已检测到 OSS 配置，但当前环境未安装 oss2。"
            "请先执行: python3 -m pip install oss2"
        )
    import oss2

    return oss2


def get_oss_public_base_url() -> str:
    custom_base = _env("XIANYU_IMAGE_CDN_BASE_URL").rstrip("/")
    if custom_base:
        return custom_base
    bucket = _env("ALIYUN_OSS_BUCKET")
    endpoint = _env("ALIYUN_OSS_ENDPOINT")
    if bucket and endpoint:
        endpoint_host = endpoint.replace("https://", "").replace("http://", "")
        return f"https://{bucket}.{endpoint_host}"
    return ""


def _object_key_for(local_path: Path) -> str:
    try:
        rel = local_path.resolve().relative_to(DATA_DIR.resolve())
        return rel.as_posix()
    except Exception:
        return f"uploads/{local_path.name}"


@lru_cache(maxsize=2048)
def _upload_cached(local_path_text: str, mtime_ns: int, size: int) -> str:
    oss2 = _require_oss2()

    access_key_id = _env("ALIYUN_OSS_ACCESS_KEY_ID")
    access_key_secret = _env("ALIYUN_OSS_ACCESS_KEY_SECRET")
    bucket_name = _env("ALIYUN_OSS_BUCKET")
    endpoint = _normalize_oss_endpoint(_env("ALIYUN_OSS_ENDPOINT"))
    if not all([access_key_id, access_key_secret, bucket_name, endpoint]):
        raise RuntimeError("OSS 环境变量不完整，无法上传图片")

    local_path = Path(local_path_text)
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name, connect_timeout=_oss_connect_timeout())
    object_key = _object_key_for(local_path)
    headers = {"Cache-Control": "public, max-age=31536000"}
    last_error = None
    for attempt in range(1, 3):
        try:
            bucket.put_object_from_file(object_key, str(local_path), headers=headers)
            last_error = None
            break
        except Exception as e:
            last_error = e
            if attempt >= 2:
                raise
            time.sleep(attempt)
    if last_error is not None:
        raise last_error

    public_base = get_oss_public_base_url().rstrip("/")
    if not public_base:
        raise RuntimeError("无法生成 OSS 公网地址，请检查 OSS 配置")
    return f"{public_base}/{quote(object_key)}"


def upload_local_file_to_oss(local_image_path: str) -> str:
    path = Path(local_image_path)
    if not path.exists():
        raise FileNotFoundError(f"本地图片不存在: {local_image_path}")
    stat = path.stat()
    return _upload_cached(str(path.resolve()), int(stat.st_mtime_ns), int(stat.st_size))
