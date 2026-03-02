"""
Flaconi Product Scraper (Selenium version)
==========================================
Extracts product name, brand, and ingredients from Flaconi product pages.

Requirements:
    pip install selenium beautifulsoup4 pandas webdriver-manager

Usage:
    python flaconi_scraper.py --input urls.csv --output results.csv
"""

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------------------------------------------------------
# Logging setup — prints to terminal AND writes errors to file
# ---------------------------------------------------------------------------
logger = logging.getLogger("flaconi")
logger.setLevel(logging.DEBUG)

# Terminal handler: shows INFO and above
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(ch)

# File handler: only ERROR and above go to errors.log
fh = logging.FileHandler("errors.log", mode="a", encoding="utf-8")
fh.setLevel(logging.ERROR)
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(fh)


# ---------------------------------------------------------------------------
# Driver setup
# ---------------------------------------------------------------------------

def build_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=de-DE")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    logger.info("🚀 Launching Chrome...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    logger.info("✅ Chrome ready.\n")
    return driver


# ---------------------------------------------------------------------------
# Cookie consent handler
# ---------------------------------------------------------------------------

def dismiss_cookie_banner(driver: webdriver.Chrome):
    """Try to click the 'Accept all cookies' button if it appears."""
    # Flaconi uses a OneTrust / custom consent banner — try common selectors
    selectors = [
        "button#onetrust-accept-btn-handler",
        "button[data-testid='uc-accept-all-button']",
        "button.accept-all-cookies",
        # Generic fallback: any button whose text contains "Akzeptieren" or "Accept"
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'akzeptieren')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'alle akzeptieren')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept all')]",
    ]
    for sel in selectors:
        try:
            if sel.startswith("//"):
                btn = driver.find_element(By.XPATH, sel)
            else:
                btn = driver.find_element(By.CSS_SELECTOR, sel)
            btn.click()
            logger.info("  🍪 Cookie banner dismissed.")
            time.sleep(1)
            return
        except NoSuchElementException:
            continue


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def parse_product(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    brand, product_name, ingredients = '', '', ''
    
    # Brand and product name (unchanged)
    h1 = soup.find('h1', attrs={'data-nc': 'typography-next'}) or soup.find('h1')
    if h1:
        brandtag = h1.find('a', attrs={'data-qa-block': 'product_brand_name'})
        if brandtag: brand = brandtag.get_text(strip=True)
        nametag = h1.find('span', attrs={'data-qa-block': 'product_name'})
        if nametag: product_name = nametag.get_text(strip=True)
    
    # Extract from exact Inhaltsstoffe block ID
    inhalts_block = soup.find('div', id='764163da-b008-4e51-ae78-632f8fd81dbf')
    if inhalts_block:
        ing_span = inhalts_block.find('span', class_='pdp-product-info-details')
        if ing_span:
            ingredients = ing_span.get_text(separator=', ', strip=True)
            logger.info(f"✅ Inhaltsstoffe from ID block: {ingredients[:100]}...")
    
    return {'url': url, 'brand': brand, 'product_name': product_name, 'ingredients': ingredients}

# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

COOKIE_DISMISSED = False  # dismiss once per session, not every page

def scrape_with_selenium(urls: list[str], delay: float = 2.0) -> list[dict]:
    global COOKIE_DISMISSED
    results = []
    driver = build_driver()

    try:
        for i, url in enumerate(urls, 1):
            logger.info(f"[{i}/{len(urls)}] Loading: {url}")
            try:
                driver.get(url)
                logger.info(f"  ⏳ Page loaded, waiting for content...")

                # Dismiss cookie banner on first page only
                if not COOKIE_DISMISSED:
                    time.sleep(2)  # give banner time to appear
                    dismiss_cookie_banner(driver)
                    COOKIE_DISMISSED = True

                # Wait for h1 (product title)
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "h1[data-nc='typography-next']")
                        )
                    )
                except TimeoutException:
                    logger.warning(f"  ⚠️  Timed out waiting for h1 — trying to parse whatever loaded.")

                # Wait for ingredients (shorter timeout — may not always exist)
                try:
                    WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "span.pdp-product-info-details")
                        )
                    )
                except TimeoutException:
                    logger.warning(f"  ⚠️  Ingredients element not found — will parse what's available.")

                html = driver.page_source
                row = parse_product(html, url)

                if row["brand"] or row["product_name"] or row["ingredients"]:
                    logger.info(f"  ✅ {row['brand']} | {row['product_name']}")
                else:
                    logger.warning(f"  ⚠️  No data extracted — page may have changed or blocked.")
                    logging.getLogger("flaconi").error(f"No data extracted: {url}")

                results.append(row)

            except WebDriverException as exc:
                logger.error(f"  ❌ WebDriver error: {exc}")
                results.append({"url": url, "brand": "", "product_name": "", "ingredients": ""})

            except Exception as exc:
                logger.error(f"  ❌ Unexpected error: {exc}")
                results.append({"url": url, "brand": "", "product_name": "", "ingredients": ""})

            # Save a checkpoint every 25 URLs so you don't lose progress
            if i % 25 == 0:
                checkpoint_path = "results_checkpoint.csv"
                pd.DataFrame(results).to_csv(checkpoint_path, index=False, encoding="utf-8-sig")
                logger.info(f"\n💾 Checkpoint saved ({i} URLs done) → {checkpoint_path}\n")

            time.sleep(delay)

    finally:
        driver.quit()
        logger.info("🏁 Browser closed.")

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape Flaconi product pages using Selenium.")
    parser.add_argument("--input",      default="gesichtscreme.csv", help="Input CSV with a 'url' column (default: gesichtscreme.csv).")
    parser.add_argument("--output",     default="ingredients.csv",       help="Output CSV path (default: ingredients.csv).")
    parser.add_argument("--delay",      type=float, default=2.0,     help="Delay between requests in seconds (default: 2).")
    parser.add_argument("--url-column", default="url",               help="Name of the URL column (default: 'url').")
    parser.add_argument("--sep",        default=";",                 help="CSV column separator (default: ';').")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}")
        return

    df_input = pd.read_csv(input_path, sep=args.sep)
    if args.url_column not in df_input.columns:
        print(f"ERROR: Column '{args.url_column}' not found. Available: {', '.join(df_input.columns)}")
        return

    urls = df_input[args.url_column].dropna().str.strip().tolist()
    urls = [u for u in urls if u]
    logger.info(f"📋 Loaded {len(urls)} URLs from '{args.input}'.\n")

    results = scrape_with_selenium(urls, delay=args.delay)

    df_out = pd.DataFrame(results, columns=["url", "brand", "product_name", "ingredients"])
    df_out.to_csv(args.output, index=False, encoding="utf-8-sig")

    success = df_out[(df_out["brand"] != "") | (df_out["product_name"] != "") | (df_out["ingredients"] != "")]
    failed  = df_out[(df_out["brand"] == "") & (df_out["product_name"] == "") & (df_out["ingredients"] == "")]

    logger.info(f"\n✅ Done! Saved to '{args.output}'.")
    logger.info(f"   Successful : {len(success)} / {len(results)}")
    if len(failed):
        logger.info(f"   Failed     : {len(failed)} — see errors.log for details")


if __name__ == "__main__":
    main()
