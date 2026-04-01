from pathlib import Path
from PIL import Image, ImageDraw, ImageFont


def get_font(size: int, bold: bool = False):
    """
    Mac 字体优先级
    """
    candidates = []
    if bold:
        candidates = [
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/System/Library/Fonts/Supplemental/Helvetica.ttc",
            "/Library/Fonts/Arial Bold.ttf",
        ]
    else:
        candidates = [
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/System/Library/Fonts/Supplemental/Helvetica.ttc",
            "/Library/Fonts/Arial.ttf",
        ]

    for font_path in candidates:
        if Path(font_path).exists():
            return ImageFont.truetype(font_path, size)

    return ImageFont.load_default()


def fit_text(draw, text, max_width, start_size=60, min_size=24, bold=False):
    for size in range(start_size, min_size - 1, -2):
        font = get_font(size, bold=bold)
        bbox = draw.textbbox((0, 0), text, font=font)
        width = bbox[2] - bbox[0]
        if width <= max_width:
            return font
    return get_font(min_size, bold=bold)


def truncate_text(text: str, max_len: int) -> str:
    text = (text or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def split_title(title: str, max_chars_per_line=18):
    """
    简单按长度切成两行
    """
    title = (title or "").strip()
    if len(title) <= max_chars_per_line:
        return [title]

    parts = []
    while len(title) > max_chars_per_line:
        parts.append(title[:max_chars_per_line])
        title = title[max_chars_per_line:]
    if title:
        parts.append(title)

    return parts[:2]


def build_cover_canvas(input_image_path: str, size=(1080, 1080)):
    input_path = Path(input_image_path)
    if not input_path.exists():
        raise FileNotFoundError(f"找不到图片: {input_image_path}")

    img = Image.open(input_path).convert("RGB")

    target_w, target_h = size
    src_w, src_h = img.size
    src_ratio = src_w / src_h
    target_ratio = target_w / target_h

    if src_ratio > target_ratio:
        new_h = target_h
        new_w = int(new_h * src_ratio)
    else:
        new_w = target_w
        new_h = int(new_w / src_ratio)

    img = img.resize((new_w, new_h), Image.LANCZOS)

    left = (new_w - target_w) // 2
    top = (new_h - target_h) // 2
    img = img.crop((left, top, left + target_w, top + target_h))

    return img.convert("RGBA")


def draw_gradient_panel(base: Image.Image):
    """
    底部渐变信息区
    """
    w, h = base.size
    gradient = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    gd = ImageDraw.Draw(gradient)

    # 多层渐变叠加，做柔和底部面板
    for i in range(300):
        alpha = int(180 * (i / 300))
        y = h - 300 + i
        gd.line([(0, y), (w, y)], fill=(10, 10, 10, alpha))

    # 再叠一层底部轻微模糊感的纯色区
    gd.rounded_rectangle(
        [(30, h - 255), (w - 30, h - 35)],
        radius=32,
        fill=(18, 18, 18, 115)
    )

    return Image.alpha_composite(base, gradient)


def draw_brand_badge(draw: ImageDraw.ImageDraw, brand_text: str):
    badge_x1, badge_y1, badge_x2, badge_y2 = 42, 42, 250, 108
    draw.rounded_rectangle(
        [(badge_x1, badge_y1), (badge_x2, badge_y2)],
        radius=18,
        fill=(255, 255, 255)
    )

    brand_font = fit_text(draw, brand_text, max_width=170, start_size=32, min_size=20, bold=True)
    draw.text((65, 58), brand_text, fill=(20, 20, 20), font=brand_font)


def draw_subtitle_badge(draw: ImageDraw.ImageDraw, subtitle: str, canvas_w: int, canvas_h: int):
    subtitle = truncate_text(subtitle, 18)
    font = fit_text(draw, subtitle, max_width=420, start_size=32, min_size=20)

    bbox = draw.textbbox((0, 0), subtitle, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]

    pad_x = 24
    pad_y = 14

    x1 = 55
    y1 = canvas_h - 155
    x2 = x1 + text_w + pad_x * 2
    y2 = y1 + text_h + pad_y * 2

    draw.rounded_rectangle(
        [(x1, y1), (x2, y2)],
        radius=24,
        fill=(255, 255, 255, 235)
    )
    draw.text((x1 + pad_x, y1 + pad_y - 2), subtitle, fill=(35, 35, 35), font=font)


def create_cover(
    input_image_path: str,
    output_path: str,
    title: str,
    subtitle: str,
    brand_text: str = "SNOW",
    size=(1080, 1080),
):
    title = truncate_text(title, 34)
    subtitle = truncate_text(subtitle, 20)
    brand_text = truncate_text(brand_text.upper(), 12)

    base = build_cover_canvas(input_image_path, size=size)
    base = draw_gradient_panel(base)

    draw = ImageDraw.Draw(base)
    canvas_w, canvas_h = base.size

    # 左上角品牌标签
    draw_brand_badge(draw, brand_text)

    # 标题最多两行
    title_lines = split_title(title, max_chars_per_line=16)

    y_start = canvas_h - 255
    line_gap = 16

    title_fonts = []
    for line in title_lines:
        font = fit_text(draw, line, max_width=920, start_size=58, min_size=28, bold=True)
        title_fonts.append(font)

    current_y = y_start
    for idx, line in enumerate(title_lines):
        font = title_fonts[idx]
        draw.text((55, current_y), line, fill=(255, 255, 255), font=font)
        bbox = draw.textbbox((0, 0), line, font=font)
        line_h = bbox[3] - bbox[1]
        current_y += line_h + line_gap

    # 卖点白色标签
    draw_subtitle_badge(draw, subtitle, canvas_w, canvas_h)

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    base.convert("RGB").save(out_path, quality=95)

    return str(out_path)