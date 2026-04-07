from math import ceil
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


OUTPUT_ROOT = Path("data/group_assets")


def _get_font(size: int, bold: bool = False):
    candidates = []
    if bold:
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/Library/Fonts/Arial Bold.ttf",
        ]
    else:
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/Library/Fonts/Arial.ttf",
        ]
    for font_path in candidates:
        path = Path(font_path)
        if path.exists():
            return ImageFont.truetype(str(path), size)
    return ImageFont.load_default()


def _fit_cover(image_path: str, size: tuple[int, int]) -> Image.Image:
    image = Image.open(image_path).convert("RGB")
    target_w, target_h = size
    src_w, src_h = image.size
    src_ratio = src_w / max(1, src_h)
    target_ratio = target_w / max(1, target_h)
    if src_ratio > target_ratio:
        new_h = target_h
        new_w = int(new_h * src_ratio)
    else:
        new_w = target_w
        new_h = int(new_w / max(src_ratio, 1e-6))
    image = image.resize((new_w, new_h), Image.LANCZOS)
    left = max(0, (new_w - target_w) // 2)
    top = max(0, (new_h - target_h) // 2)
    return image.crop((left, top, left + target_w, top + target_h))


def _truncate(text: str, limit: int) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def build_group_cover_image(
    *,
    source: str,
    group_id: int,
    group_name: str,
    category: str,
    items: list[dict],
    output_name: str = "cover.jpg",
) -> str:
    valid_items = []
    for item in items:
        image_path = str(item.get("image_path") or item.get("local_image_path") or "").strip()
        if image_path and Path(image_path).exists():
            valid_items.append({
                "image_path": image_path,
                "color": str(item.get("color") or "").strip(),
            })
    if not valid_items:
        raise ValueError("组商品缺少可用本地图，无法生成组主图")

    visible_items = valid_items[:4]
    count = len(visible_items)
    cols = 2 if count > 1 else 1
    rows = ceil(count / cols)
    canvas_w, canvas_h = 1080, 1080
    header_h, footer_h = 110, 170
    gap = 18
    content_h = canvas_h - header_h - footer_h - gap * 3
    cell_w = (canvas_w - gap * (cols + 1)) // cols
    cell_h = (content_h - gap * (rows + 1)) // max(1, rows)

    base = Image.new("RGB", (canvas_w, canvas_h), (248, 248, 245))
    draw = ImageDraw.Draw(base)
    draw.rectangle([(0, 0), (canvas_w, header_h)], fill=(20, 20, 20))
    draw.rectangle([(0, canvas_h - footer_h), (canvas_w, canvas_h)], fill=(245, 243, 238))

    title_font = _get_font(42, bold=True)
    subtitle_font = _get_font(24, bold=False)
    badge_font = _get_font(24, bold=True)
    color_font = _get_font(22, bold=True)

    title = _truncate(group_name, 38)
    subtitle = _truncate(f"{category} · 组内{len(items)}款", 40)
    draw.text((36, 26), title, fill=(255, 255, 255), font=title_font)
    draw.text((38, 72), subtitle, fill=(220, 220, 220), font=subtitle_font)

    for idx, item in enumerate(visible_items):
        row = idx // cols
        col = idx % cols
        x = gap + col * (cell_w + gap)
        y = header_h + gap + row * (cell_h + gap)
        tile = _fit_cover(item["image_path"], (cell_w, cell_h))
        base.paste(tile, (x, y))
        label = _truncate(item.get("color") or f"款式{idx + 1}", 16)
        label_box = (x + 18, y + cell_h - 54, x + min(cell_w - 18, 18 + max(120, len(label) * 22)), y + cell_h - 16)
        overlay = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
        overlay_draw = ImageDraw.Draw(overlay)
        overlay_draw.rounded_rectangle(label_box, radius=18, fill=(255, 255, 255, 232))
        overlay_draw.text((label_box[0] + 14, label_box[1] + 8), label, fill=(25, 25, 25), font=color_font)
        base = Image.alpha_composite(base.convert("RGBA"), overlay).convert("RGB")

    color_labels = [_truncate(item.get("color") or "", 12) for item in valid_items if str(item.get("color") or "").strip()]
    color_text = " / ".join(color_labels[:4])
    if len(color_labels) > 4:
        color_text += f" 等{len(color_labels)}色"
    draw = ImageDraw.Draw(base)
    draw.text((36, canvas_h - 132), "多色可选", fill=(30, 30, 30), font=badge_font)
    draw.text((36, canvas_h - 88), _truncate(color_text, 46), fill=(70, 70, 70), font=subtitle_font)

    output_dir = OUTPUT_ROOT / source / f"group_{group_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_name
    base.save(output_path, quality=92)
    return str(output_path)
