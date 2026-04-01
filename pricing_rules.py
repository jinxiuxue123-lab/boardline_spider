import re
from pathlib import Path

import pandas as pd


RULES_FILE = "pricing_rules.xlsx"
DEFAULT_EXCHANGE_RATE = 0.0052
DEFAULT_PROFIT_RATE = 0.10


def cell_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def parse_price_value(price_text: str) -> int | None:
    if not price_text:
        return None

    digits = re.sub(r"[^\d]", "", str(price_text))
    if not digits:
        return None

    return int(digits)


def format_money_text(value: float | int | None) -> str:
    if value is None:
        return ""
    rounded = int(round(float(value)))
    return f"{rounded:,}"


def load_pricing_rules(file_path: str = RULES_FILE) -> dict[str, dict]:
    path = Path(file_path)
    if not path.exists():
        return {}

    df = pd.read_excel(path)
    if df.empty:
        return {}

    rules = {}
    for _, row in df.iterrows():
        category = cell_text(row.get("category", ""))
        if not category:
            continue

        enabled_raw = row.get("enabled", 1)
        enabled = cell_text(enabled_raw).lower() not in {"0", "false", "no", ""}
        if not enabled:
            continue

        try:
            exchange_rate = float(row.get("exchange_rate", DEFAULT_EXCHANGE_RATE))
        except (TypeError, ValueError):
            exchange_rate = DEFAULT_EXCHANGE_RATE

        try:
            shipping_fee_cny = float(row.get("shipping_fee_cny", 0))
        except (TypeError, ValueError):
            shipping_fee_cny = 0.0

        try:
            profit_rate = float(row.get("profit_rate", DEFAULT_PROFIT_RATE))
        except (TypeError, ValueError):
            profit_rate = DEFAULT_PROFIT_RATE

        if profit_rate > 1:
            profit_rate = profit_rate / 100

        rules[category] = {
            "exchange_rate": exchange_rate,
            "shipping_fee_cny": shipping_fee_cny,
            "profit_rate": profit_rate,
            "note": cell_text(row.get("note", "")),
        }

    return rules


def calculate_cny_pricing(
    category: str,
    price_text: str,
    original_price_text: str,
    latest_discount_price_text: str,
    rules: dict[str, dict],
) -> dict[str, str]:
    rule = rules.get((category or "").strip())
    if not rule:
        return {
            "price_cny": "",
            "original_price_cny": "",
            "shipping_fee_cny": "",
            "final_price_cny": "",
            "exchange_rate": "",
            "profit_rate": "",
        }

    base_krw = parse_price_value(latest_discount_price_text) or parse_price_value(price_text)
    if base_krw is None:
        return {
            "price_cny": "",
            "original_price_cny": "",
            "shipping_fee_cny": format_money_text(rule["shipping_fee_cny"]),
            "final_price_cny": "",
            "exchange_rate": str(rule["exchange_rate"]),
            "profit_rate": str(rule["profit_rate"]),
        }

    exchange_rate = rule["exchange_rate"]
    shipping_fee_cny = rule["shipping_fee_cny"]
    profit_rate = rule["profit_rate"]
    original_base_krw = parse_price_value(original_price_text)

    price_cny_value = base_krw * exchange_rate
    final_price_cny_value = (price_cny_value + shipping_fee_cny) * (1 + profit_rate)
    original_price_cny_value = None
    if original_base_krw is not None:
        original_price_cny_value = (original_base_krw * exchange_rate + shipping_fee_cny) * (1 + profit_rate)

    return {
        "price_cny": format_money_text(price_cny_value),
        "original_price_cny": format_money_text(original_price_cny_value),
        "shipping_fee_cny": format_money_text(shipping_fee_cny),
        "final_price_cny": format_money_text(final_price_cny_value),
        "exchange_rate": str(exchange_rate),
        "profit_rate": str(profit_rate),
    }
