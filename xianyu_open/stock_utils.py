import re


def parse_total_stock(stock_text: str | None) -> int:
    stock_text = (stock_text or "").strip()
    if not stock_text:
        return 0

    matches = [int(match) for match in re.findall(r":\s*(\d+)", stock_text)]
    if matches:
        return sum(matches)

    first_number = re.search(r"\d+", stock_text)
    if first_number:
        return int(first_number.group())

    return 0
