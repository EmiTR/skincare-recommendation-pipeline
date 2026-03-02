import csv
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Manual inputs
BASE_URL = "https://www.flaconi.de/beste-gesichtscreme/"
TOTAL_PAGES = 34
PAGE_SIZE = 24
OUTPUT_FILE = "gesichtscreme.csv"

FIELDNAMES = [
    "brand",
    "series",
    "product_type",
    "price",
    "uvp_price",
    "base_price",
    "url",
]


def build_driver() -> webdriver.Chrome:
    options = Options()
    # Run in non-headless mode so the site is less likely to detect a bot.

    # Switch to headless by uncommenting the line below if you want background execution.Successful run was made without this option, the cookie banner was manually clicked.
    # options.add_argument("--headless=new")

    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    # Patch navigator.webdriver to false
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    return driver

# This part was meant for the cookiebanner, which was not tested.

def accept_cookies(driver: webdriver.Chrome):
    """Click the cookie consent button if it appears."""
    try:
        btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Alle akzeptieren')]"))
        )
        btn.click()
        print("  ✓ Cookie banner accepted.")
        time.sleep(1.5)
    except Exception:
        print("  – No cookie banner found (or already accepted).")


def scroll_page(driver: webdriver.Chrome):
    """Scroll slowly to the bottom so lazy-loaded products are rendered."""
    total_height = driver.execute_script("return document.body.scrollHeight")
    scroll_step = 400
    current = 0
    while current < total_height:
        driver.execute_script(f"window.scrollTo(0, {current});")
        current += scroll_step
        time.sleep(0.15)
    # Scroll back to top
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.5)


def parse_products(soup: BeautifulSoup) -> list[dict]:
    products = []

    section = soup.find("div", {"data-qa-block": "product-section"})
    if not section:
        print("  ⚠ Product section not found on this page.")
        return products

    cards = section.find_all("a", {"data-nc": "card"})

    for card in cards:
        def get_text(qa_block):
            el = card.find(attrs={"data-qa-block": qa_block})
            return el.get_text(strip=True) if el else ""

        product = {
            "brand":        get_text("product_brand"),
            "series":       get_text("product_series"),
            "product_type": get_text("product_type"),
            "price":        get_text("product_price"),
            "uvp_price":    get_text("product_rrpprice"),
            "base_price":   get_text("product_baseprice"),
            "url":          "https://www.flaconi.de" + card.get("href", ""),
        }
        products.append(product)

    return products


def main():
    driver = build_driver()
    all_products = []
    cookie_accepted = False

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter=";")
        writer.writeheader()

        for page_num in range(TOTAL_PAGES):
            offset = page_num * PAGE_SIZE
            url = BASE_URL if offset == 0 else f"{BASE_URL}?offset={offset}"
            print(f"\nScraping page {page_num + 1}/{TOTAL_PAGES} → {url}")

            driver.get(url)

            # Accept cookies only on the first page
            if not cookie_accepted:
                accept_cookies(driver)
                cookie_accepted = True

            # Wait until at least one product card is present
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-nc='card']"))
                )
            except Exception:
                print("  ⚠ Timed out waiting for products. Skipping page.")
                continue

            # Scroll to trigger any lazy-loaded images/content
            scroll_page(driver)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            products = parse_products(soup)
            print(f"  → Found {len(products)} products")

            writer.writerows(products)
            f.flush()
            all_products.extend(products)

            # Random delay between pages (3–6 seconds) to mimic human behaviour
            delay = random.uniform(3, 6)
            print(f"  ⏳ Waiting {delay:.1f}s before next page...")
            time.sleep(delay)

    driver.quit()
    print(f"\n✅ Done! {len(all_products)} products saved to '{OUTPUT_FILE}'")


if __name__ == "__main__":
    main()