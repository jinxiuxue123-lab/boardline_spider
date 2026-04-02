import re
from pathlib import Path

import pandas as pd


RULES_FILE = "discount_rules.xlsx"


def cell_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_match_text(text: str) -> str:
    text = (text or "").upper().strip()
    text = re.sub(r"(?<=\d)\s*[/\-]\s*(?=\d)", "", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return " ".join(text.split())


def keyword_tokens(text: str) -> list[str]:
    normalized = normalize_match_text(text)
    if not normalized:
        return []
    return [token for token in normalized.split(" ") if token]


def parse_price_value(price_text: str) -> int | None:
    if not price_text:
        return None

    digits = re.sub(r"[^\d]", "", str(price_text))
    if not digits:
        return None

    return int(digits)


def format_price_value(price: int | None) -> str:
    if price is None:
        return ""
    return f"{price:,}"


def load_discount_rules(file_path: str = RULES_FILE) -> list[dict]:
    path = Path(file_path)
    if not path.exists():
        return []

    df = pd.read_excel(path)
    if df.empty:
        return []

    rules = []
    for _, row in df.iterrows():
        keyword = cell_text(row.get("keyword", ""))
        category = cell_text(row.get("category", ""))
        if not keyword and not category:
            continue

        enabled_raw = row.get("enabled", 1)
        enabled = cell_text(enabled_raw).lower() not in {"0", "false", "no", ""}
        if not enabled:
            continue

        discount_type = cell_text(row.get("discount_type", "rate")).lower()
        discount_value_raw = row.get("discount_value", "")

        try:
            discount_value = float(discount_value_raw)
        except (TypeError, ValueError):
            continue

        try:
            priority = int(row.get("priority", 0))
        except (TypeError, ValueError):
            priority = 0

        rules.append({
            "category": category,
            "category_normalized": category.strip(),
            "keyword": keyword,
            "keyword_normalized": normalize_match_text(keyword),
            "keyword_tokens": keyword_tokens(keyword),
            "discount_type": discount_type,
            "discount_value": discount_value,
            "priority": priority,
            "note": cell_text(row.get("note", "")),
        })

    rules.sort(key=lambda item: item["priority"], reverse=True)
    return rules


def find_matching_discount_rule(product_name: str, category: str, rules: list[dict]) -> dict | None:
    normalized_name = normalize_match_text(product_name)
    name_tokens = set(keyword_tokens(product_name))
    normalized_category = str(category or "").strip()

    category_only_match = None
    for rule in rules:
        rule_category = str(rule.get("category_normalized") or "").strip()
        if rule_category and rule_category != normalized_category:
            continue
        keyword = rule.get("keyword_normalized", "")
        rule_tokens = rule.get("keyword_tokens") or []
        if rule_tokens and all(token in name_tokens for token in rule_tokens):
            return rule
        if keyword and keyword in normalized_name:
            return rule
        if not keyword and category_only_match is None:
            category_only_match = rule

    return category_only_match


def calculate_latest_discount_price(
    original_price_text: str,
    product_name: str,
    category: str,
    rules: list[dict],
) -> str:
    original_price = parse_price_value(original_price_text)
    if original_price is None:
        return ""

    rule = find_matching_discount_rule(product_name, category, rules)
    if not rule:
        return ""

    discount_type = rule["discount_type"]
    discount_value = rule["discount_value"]

    if discount_type == "rate":
        rate = discount_value / 100 if discount_value > 1 else discount_value
        latest_price = round(original_price * rate)
    elif discount_type == "amount":
        latest_price = round(original_price - discount_value)
    else:
        return ""

    latest_price = max(int(latest_price), 0)
    return format_price_value(latest_price)
