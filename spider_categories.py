from playwright.sync_api import sync_playwright
import pandas as pd

BASE_URL = "http://www.boardline.co.kr"

categories = []

with sync_playwright() as p:

    browser = p.chromium.launch(headless=False)

    page = browser.new_page()

    page.goto(BASE_URL)

    page.wait_for_selector("#menu")

    links = page.query_selector_all("#menu li a")

    for link in links:

        name = link.inner_text().strip()

        href = link.get_attribute("href")

        if href:

            if href.startswith("/"):
                href = BASE_URL + href

            categories.append({
                "name": name,
                "url": href
            })

    browser.close()

df = pd.DataFrame(categories)

df = df.drop_duplicates()

df.to_excel("categories.xlsx", index=False)

print("抓取分类数量:", len(df))