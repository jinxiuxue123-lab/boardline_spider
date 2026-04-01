import os
import hashlib
from pathlib import Path
from urllib.parse import urlencode

from PIL import Image

from services.product_image_ai_service import build_watermarked_upload_variant
from services.oss_storage_service import is_oss_configured, upload_local_file_to_oss

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
PUBLISH_VARIANT_DIR = DATA_DIR / "publish_variants"


def build_hosted_image_url(local_image_path: str, watermark_text: str = "") -> str:
    if watermark_text and local_image_path:
        local_image_path = build_watermarked_upload_variant(local_image_path, watermark_text)

    if local_image_path and is_oss_configured():
        return upload_local_file_to_oss(local_image_path)

    base_url = (os.getenv("XIANYU_IMAGE_CDN_BASE_URL", "") or "").rstrip("/")
    path = Path(local_image_path)
    try:
        rel_path = path.resolve().relative_to(DATA_DIR.resolve())
    except Exception:
        rel_path = None

    if base_url and rel_path is not None:
        return f"{base_url}/{rel_path.as_posix()}"

    public_media_base = (os.getenv("PUBLIC_MEDIA_BASE_URL", "") or "").rstrip("/")
    if public_media_base:
        query = urlencode({"file_path": str(path)})
        return f"{public_media_base}/media/local?{query}"

    return ""


def build_standard_png_variant(local_image_path: str) -> str:
    source_path = Path(local_image_path)
    if not source_path.exists():
        raise FileNotFoundError(f"本地图片不存在: {local_image_path}")

    stat = source_path.stat()
    source_hash = hashlib.md5(str(source_path.resolve()).encode("utf-8")).hexdigest()[:6]
    time_token = format(int(stat.st_mtime_ns), "x")[-9:]
    out_name = f"p{source_hash}{time_token}.png"
    PUBLISH_VARIANT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PUBLISH_VARIANT_DIR / out_name
    if out_path.exists():
        return str(out_path)

    image = Image.open(source_path)
    if image.mode not in ("RGB", "RGBA"):
        image = image.convert("RGBA")

    image.save(out_path, format="PNG", optimize=True)
    return str(out_path)
