from playwright.sync_api import sync_playwright
import pandas as pd

products = []

base_url = "http://www.boardline.co.kr/shop/shopbrand.html?type=Y&xcode=058&sort=&page="

page_num = 1

with sync_playwright() as p:

    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    while True:

        url = base_url + str(page_num)
        print("抓取:", url)

        page.goto(url)
        page.wait_for_timeout(4000)

        # 商品容器
        container = page.query_selector(".prd-list.pdt20")

        if not container:
            print("没有商品容器，停止")
            break

        # 判断容器内部是否有内容
        inner_html = container.inner_html().strip()

        if inner_html == "":
            print("商品列表为空，停止翻页")
            break

        rows = page.query_selector_all("tbody tr")

        print("找到行:", len(rows))

        for row in rows:

            tds = row.query_selector_all("td")

            for td in tds:

                link = td.query_selector(".thumb a")
                img = td.query_selector(".thumb img")
                name = td.query_selector(".dsc")

                if link:

                    href = link.get_attribute("href")

                    name_text = name.inner_text().strip() if name else ""
                    img_url = img.get_attribute("src") if img else ""

                    products.append({
                        "name": name_text,
                        "image": img_url,
                        "link": "http://www.boardline.co.kr" + href
                    })

        page_num += 1

    browser.close()

df = pd.DataFrame(products)

df.drop_duplicates(inplace=True)

df.to_excel("boards.xlsx", index=False)

print("总商品:", len(df))