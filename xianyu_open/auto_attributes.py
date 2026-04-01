import json
import re
import sqlite3


DB_FILE = "products.db"

SNOWBOARD_BRANDS = [
    "DIMITO",
    "BURTON",
    "YONEX",
    "GRAY",
    "RIDE",
    "NITRO",
    "NOVEMBER",
    "OGASAKA",
    "SALOMON",
    "CAPITA",
    "BATALEON",
    "CRABGRAB",
    "BSRABBIT",
    "UNION",
    "K2",
]

BINDING_BRAND_ALIASES = [
    ("BURTON", ["BURTON"]),
    ("Union", ["UNION", "UNION BINDING COMPANY"]),
    ("FLUX", ["FLUX"]),
    ("Bataleon", ["BATALEON"]),
    ("SALOMON/萨洛蒙", ["SALOMON"]),
    ("NITRO", ["NITRO"]),
    ("Ride", ["RIDE"]),
    ("SP（运动户外）", ["SP", "SP BINDING"]),
    ("DRAKE", ["DRAKE"]),
    ("Flow", ["FLOW"]),
    ("NIDECKER", ["NIDECKER"]),
    ("K2", ["K2"]),
    ("NOW", ["NOW"]),
    ("YONEX/尤尼克斯", ["YONEX"]),
    ("BENT METAL", ["BENT METAL"]),
    ("HEAD/海德", ["HEAD"]),
    ("Rossignol/金鸡", ["ROSSIGNOL"]),
    ("ATOMIC", ["ATOMIC"]),
    ("GNU", ["GNU"]),
    ("ARBOR", ["ARBOR"]),
    ("CLEW", ["CLEW"]),
    ("FIX", ["FIX"]),
    ("rome sds", ["ROME", "ROME SDS"]),
    ("ROXY", ["ROXY"]),
    ("other/其他", []),
]

GOGGLE_BRAND_ALIASES = [
    ("Anon", ["ANON"]),
    ("Oakley/欧克利", ["OAKLEY"]),
]

SNOWBOARD_BOOT_BRAND_ALIASES = [
    ("Ride", ["RIDE"]),
    ("NITRO", ["NITRO"]),
    ("burton", ["BURTON"]),
    ("DEELUXE", ["DEELUXE"]),
    ("SALOMON/萨洛蒙", ["SALOMON"]),
    ("Rossignol/金鸡", ["ROSSIGNOL"]),
    ("thirtytwo", ["THIRTYTWO", "32"]),
    ("HEAD/海德", ["HEAD"]),
    ("ATOMIC", ["ATOMIC"]),
    ("DC", ["DC", "DC SHOES"]),
    ("VANS", ["VANS"]),
    ("K2", ["K2"]),
    ("NORTHWAVE", ["NORTHWAVE"]),
    ("FLUX", ["FLUX"]),
    ("LASTARTS", ["LASTARTS"]),
    ("rome sds", ["ROME", "ROME SDS"]),
    ("other/其他", []),
]

SNOWBOARD_LENGTH_RANGES = [
    ("100cm及以下", 0, 100),
    ("101-110cm", 101, 110),
    ("111-120cm", 111, 120),
    ("121-130cm", 121, 130),
    ("131-140cm", 131, 140),
    ("141-150cm", 141, 150),
    ("151-160cm", 151, 160),
    ("161-170cm", 161, 170),
    ("171-180cm", 171, 180),
    ("180cm以上", 181, 999),
]


def normalize_text(text: str) -> str:
    text = (text or "").strip().upper()
    text = re.sub(r"[^A-Z0-9\u4e00-\u9fff/]+", "", text)
    return text


def detect_snowboard_brand(name: str) -> str:
    normalized_name = normalize_text(name)
    for brand in SNOWBOARD_BRANDS:
        if normalize_text(brand) in normalized_name:
            return brand
    return "other/其他"


def detect_binding_brand(name: str) -> str:
    normalized_name = normalize_text(name)
    for target_value, aliases in BINDING_BRAND_ALIASES:
        for alias in aliases:
            alias_norm = normalize_text(alias)
            if alias_norm and alias_norm in normalized_name:
                return target_value
    return "other/其他"


def detect_binding_gender(name: str) -> str:
    normalized_name = normalize_text(name)
    if any(token in normalized_name for token in ("WMS", "WOMENS", "WOMEN", "LADIES", "WOMAN")):
        return "女"
    if any(token in normalized_name for token in ("MENS", "MEN")):
        return "男"
    return "中性"


def detect_binding_entry_type(name: str) -> str:
    normalized_name = normalize_text(name)
    if any(token in normalized_name for token in ("STEPON", "SUPERMATIC", "FASE")):
        return "快穿"
    return "传统式"


def detect_goggle_brand(name: str) -> str:
    normalized_name = normalize_text(name)
    for target_value, aliases in GOGGLE_BRAND_ALIASES:
        for alias in aliases:
            alias_norm = normalize_text(alias)
            if alias_norm and alias_norm in normalized_name:
                return target_value
    return ""


def detect_snowboard_boot_brand(name: str) -> str:
    normalized_name = normalize_text(name)
    for target_value, aliases in SNOWBOARD_BOOT_BRAND_ALIASES:
        for alias in aliases:
            alias_norm = normalize_text(alias)
            if alias_norm and alias_norm in normalized_name:
                return target_value
    return "other/其他"


def build_brand_aliases_from_items(items: list) -> list[tuple[str, str]]:
    aliases = []
    for item in items or []:
        value_name = str(item.get("value_name") or "").strip()
        if not value_name:
            continue
        candidates = {value_name}
        if "/" in value_name:
            for part in value_name.split("/"):
                part = part.strip()
                if part:
                    candidates.add(part)
        for part in re.split(r"[（）()]+", value_name):
            part = part.strip()
            if part:
                candidates.add(part)
        for candidate in candidates:
            normalized = normalize_text(candidate)
            if len(normalized) >= 3:
                aliases.append((normalized, value_name))
    return aliases


def detect_brand_from_property_options(name: str, property_map: dict) -> str:
    brand_prop = property_map.get("品牌") or {}
    aliases = build_brand_aliases_from_items(brand_prop.get("items") or [])
    normalized_name = normalize_text(name)
    best_match = None
    for alias_norm, value_name in aliases:
        if not alias_norm:
            continue
        pos = normalized_name.find(alias_norm)
        if pos < 0:
            continue
        candidate = (pos, -len(alias_norm), value_name)
        if best_match is None or candidate < best_match:
            best_match = candidate
    if best_match:
        return best_match[2]
    return "other/其他"


def resolve_snowboard_brand(name: str, property_map: dict) -> str:
    matched = detect_brand_from_property_options(name, property_map)
    if matched and matched != "other/其他":
        return matched

    detected = detect_snowboard_brand(name)
    if not detected or detected == "other/其他":
        return "other/其他"

    brand_prop = property_map.get("品牌") or {}
    value_id, value_name = find_matching_option(brand_prop.get("items") or [], detected)
    if value_id:
        return value_name
    return "other/其他"


def detect_target_audience(name: str) -> str:
    normalized_name = normalize_text(name)
    if any(token in normalized_name for token in ("YOUTH", "KIDS", "KID", "JR", "JUNIOR", "CHILD")):
        return "儿童"
    if any(token in normalized_name for token in ("WMS", "WOMENS", "WOMEN", "LADIES", "WOMAN", "GIRLS", "GIRL")):
        return "女"
    if any(token in normalized_name for token in ("MENS", "MEN", "BOYS", "BOY")):
        return "男"
    return "中性"


def load_property_map(category: str) -> dict:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT property_id, property_name, raw_json
        FROM xianyu_category_properties
        WHERE source = 'boardline'
          AND source_category = ?
    """, (category,)).fetchall()
    conn.close()

    result = {}
    for row in rows:
        raw_json = row["raw_json"] or "{}"
        try:
            raw = json.loads(raw_json)
        except Exception:
            raw = {}
        result[row["property_name"]] = {
            "property_id": row["property_id"] or "",
            "property_name": row["property_name"] or "",
            "items": raw.get("items") or [],
        }
    return result


def find_matching_option(items: list, desired_value: str) -> tuple[str, str]:
    desired_norm = normalize_text(desired_value)
    for item in items:
        value_name = str(item.get("value_name") or "").strip()
        value_id = str(item.get("value_id") or "").strip()
        option_norm = normalize_text(value_name)
        if option_norm == desired_norm:
            return value_id, value_name

    for item in items:
        value_name = str(item.get("value_name") or "").strip()
        value_id = str(item.get("value_id") or "").strip()
        option_norm = normalize_text(value_name)
        if desired_norm and (desired_norm in option_norm or option_norm in desired_norm):
            return value_id, value_name

    return "", desired_value


def extract_snowboard_lengths(name: str, stock: str = "") -> list[int]:
    candidates = []
    combined = f"{name or ''} {stock or ''}"
    for raw in re.findall(r"(?<!\d)(\d{3})(?!\d)", combined):
        value = int(raw)
        if 90 <= value <= 220:
            candidates.append(value)
    return sorted(set(candidates))


def detect_length_range(name: str, stock: str = "") -> str:
    lengths = extract_snowboard_lengths(name, stock)
    if not lengths:
        return ""

    count = len(lengths)
    if count % 2 == 1:
        pivot = lengths[count // 2]
    else:
        pivot = round((lengths[count // 2 - 1] + lengths[count // 2]) / 2)

    for label, start, end in SNOWBOARD_LENGTH_RANGES:
        if start <= pivot <= end:
            return label
    return ""


def upsert_property_value(
    conn,
    product_id: int,
    category: str,
    property_id: str,
    property_name: str,
    value_id: str,
    value_name: str,
) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO xianyu_product_property_values (
            product_id,
            source_category,
            property_id,
            property_name,
            value_id,
            value_name,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(product_id, property_id) DO UPDATE SET
            source_category = excluded.source_category,
            property_name = excluded.property_name,
            value_id = excluded.value_id,
            value_name = excluded.value_name,
            updated_at = CURRENT_TIMESTAMP
    """, (
        product_id,
        category,
        property_id,
        property_name,
        value_id,
        value_name,
    ))


def delete_property_value(conn, product_id: int, property_id: str) -> None:
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM xianyu_product_property_values
        WHERE product_id = ?
          AND property_id = ?
    """, (product_id, property_id))


def upsert_publish_meta(conn, product_id: int, stuff_status: str, note: str = "") -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO xianyu_product_publish_meta (
            product_id,
            stuff_status,
            note,
            updated_at
        )
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(product_id) DO UPDATE SET
            stuff_status = excluded.stuff_status,
            note = excluded.note,
            updated_at = CURRENT_TIMESTAMP
    """, (product_id, stuff_status, note))


def apply_property_values(conn, product_id: int, category: str, property_map: dict, desired_values: dict) -> None:
    for property_name, desired_value in desired_values.items():
        prop = property_map.get(property_name)
        if not prop:
            continue
        value_id, value_name = find_matching_option(prop.get("items") or [], desired_value)
        upsert_property_value(
            conn,
            product_id=product_id,
            category=category,
            property_id=prop["property_id"],
            property_name=prop["property_name"],
            value_id=value_id,
            value_name=value_name,
        )


def apply_auto_attributes_for_product(product_id: int, category: str, name: str, stock: str = "") -> bool:
    if category not in ("滑雪板", "固定器", "滑雪镜", "滑雪鞋", "滑雪服", "滑雪头盔", "手套", "儿童装备", "滑雪帽衫和中间层", "帽子护脸", "袜子以及周边配件"):
        return False

    property_map = load_property_map(category)

    conn = sqlite3.connect(DB_FILE)
    try:
        if category == "滑雪板":
            if not property_map:
                return False
            brand = resolve_snowboard_brand(name, property_map)
            length_range = detect_length_range(name, stock)
            desired_values = {
                "品牌": brand,
                "成色": "全新",
                "滑雪板类型": "单板",
                "适用对象": "通用",
            }
            apply_property_values(conn, product_id, category, property_map, desired_values)

            length_prop = property_map.get("长度")
            if length_prop:
                if length_range:
                    value_id, value_name = find_matching_option(length_prop.get("items") or [], length_range)
                    upsert_property_value(
                        conn,
                        product_id=product_id,
                        category=category,
                        property_id=length_prop["property_id"],
                        property_name=length_prop["property_name"],
                        value_id=value_id,
                        value_name=value_name,
                    )
                else:
                    delete_property_value(conn, product_id, length_prop["property_id"])

            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:滑雪板规则")
        elif category == "固定器":
            if not property_map:
                return False
            desired_values = {
                "品牌": detect_binding_brand(name),
                "成色": "全新",
                "滑雪板类型": "单板",
                "适用性别": detect_binding_gender(name),
                "固定器穿脱方式": detect_binding_entry_type(name),
            }
            apply_property_values(conn, product_id, category, property_map, desired_values)
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:固定器规则")
        elif category == "滑雪镜":
            if not property_map:
                return False
            desired_values = {
                "成色": "全新",
            }
            brand = detect_goggle_brand(name)
            if brand:
                desired_values["品牌"] = brand
            apply_property_values(conn, product_id, category, property_map, desired_values)
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:滑雪镜规则")
        elif category == "滑雪鞋":
            if property_map:
                desired_values = {
                    "品牌": detect_snowboard_boot_brand(name),
                    "成色": "全新",
                    "适用对象": detect_binding_gender(name),
                }
                apply_property_values(conn, product_id, category, property_map, desired_values)
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:滑雪鞋规则")
        elif category == "滑雪服":
            if not property_map:
                return False
            desired_values = {
                "品牌": detect_brand_from_property_options(name, property_map),
                "成色": "全新",
                "适用对象": detect_target_audience(name),
            }
            apply_property_values(conn, product_id, category, property_map, desired_values)
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:滑雪服规则")
        elif category == "滑雪头盔":
            if not property_map:
                return False
            desired_values = {
                "品牌": detect_brand_from_property_options(name, property_map),
                "成色": "全新",
                "适用性别": detect_binding_gender(name),
            }
            apply_property_values(conn, product_id, category, property_map, desired_values)
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:滑雪头盔规则")
        elif category == "手套":
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:手套规则")
        elif category == "儿童装备":
            if not property_map:
                return False
            desired_values = {
                "品牌": detect_brand_from_property_options(name, property_map),
                "成色": "全新",
            }
            apply_property_values(conn, product_id, category, property_map, desired_values)
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:儿童装备规则")
        elif category == "滑雪帽衫和中间层":
            if not property_map:
                return False
            desired_values = {
                "品牌": detect_brand_from_property_options(name, property_map),
                "成色": "全新",
                "适用对象": detect_target_audience(name),
            }
            apply_property_values(conn, product_id, category, property_map, desired_values)
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:滑雪帽衫和中间层规则")
        elif category == "帽子护脸":
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:帽子护脸规则")
        elif category == "袜子以及周边配件":
            upsert_publish_meta(conn, product_id=product_id, stuff_status="100", note="auto:袜子以及周边配件规则")

        conn.commit()
        return True
    finally:
        conn.close()
