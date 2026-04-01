from playwright.sync_api import sync_playwright
import re

# 在这里填你要测试的商品
TEST_URL = "http://www.boardline.co.kr/shop/shopdetail.html?branduid=1090576"


def clean_option_text(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"^\d+\.", "", text)
    text = re.sub(r"\(\)", "", text)
    return text.strip()


def parse_stock_single(page):
    print("检测到单层库存结构")

    stock_list = []
    options = page.query_selector_all("#MK_p_s_0 option")

    print("option数量:", len(options))

    for opt in options:

        text = opt.inner_text().strip()
        value = opt.get_attribute("value")
        stock_cnt = opt.get_attribute("stock_cnt")

        print("option:", text, "value:", value, "stock:", stock_cnt)

        if not value or value == "":
            continue

        if not stock_cnt:
            continue

        text = clean_option_text(text)
        stock_list.append(f"{text}:{stock_cnt}")

    return stock_list


def parse_stock_two_level(page):

    print("检测到双层联动库存结构")

    stock_map = {}

    colors = page.query_selector_all("#MK_p_s_0 option")

    for color in colors:

        color_text = color.inner_text().strip()
        color_value = color.get_attribute("value")

        if not color_value:
            continue

        color_name = clean_option_text(color_text)

        print("选择颜色:", color_name)

        page.select_option("#MK_p_s_0", value=color_value)
        page.wait_for_timeout(800)

        sizes = page.query_selector_all("#MK_p_s_1 option")

        for size in sizes:

            size_text = size.inner_text().strip()
            size_value = size.get_attribute("value")
            stock_cnt = size.get_attribute("stock_cnt")

            print("  尺码:", size_text, "stock:", stock_cnt)

            if not size_value:
                continue

            if not stock_cnt:
                continue

            size_name = clean_option_text(size_text)

            if color_name not in stock_map:
                stock_map[color_name] = []

            stock_map[color_name].append(f"{size_name}:{stock_cnt}")

    result = []

    for color, sizes in stock_map.items():
        result.append(f"{color}({','.join(sizes)})")

    return result


def parse_stock(page):

    select0 = page.query_selector("#MK_p_s_0")
    select1 = page.query_selector("#MK_p_s_1")

    print("select0:", bool(select0))
    print("select1:", bool(select1))

    if select0 and select1:
        r = parse_stock_two_level(page)
        if r:
            return r

    if select0:
        r = parse_stock_single(page)
        if r:
            return r

    return ["DEFAULT:1"]


def main():

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        print("打开商品:", TEST_URL)

        page.goto(TEST_URL)
        page.wait_for_selector("h3.cboth.tit-prd")

        stock_items = parse_stock(page)

        print("\n最终库存解析结果:")
        print(" | ".join(stock_items))

        browser.close()


if __name__ == "__main__":
    main()