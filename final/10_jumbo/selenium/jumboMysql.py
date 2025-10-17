#!/usr/bin/env python3
"""
Scraper for the Jumbo Argentina grocery section using Selenium.

This script automates a headless Chrome browser to visit the listing
pages under ``https://www.jumbo.com.ar/almacen?page=…``.  It collects
basic product information from each listing card and then opens each
product's detail page to extract richer data such as SKU, price, unit
price, and specifications.  Collected records are exported to an
Excel file and optionally a CSV file.

Requirements
------------
This scraper depends on Selenium and a compatible WebDriver.  The
``webdriver_manager`` package can automatically download an
appropriate ChromeDriver binary.  You will also need pandas and
openpyxl for tabular export.

Install dependencies via pip:

    pip install selenium webdriver-manager pandas openpyxl

To run the scraper:

    python jumbo_selenium_scraper.py --start 1 --max-pages 20 --delay 1 \
           --out Jumbo_Almacen.xlsx --csv Jumbo_Almacen.csv

The script will iterate pages starting from ``start`` up to
``start + max_pages - 1``, or stop earlier if it encounters a page
without product cards.

Notes
-----
* This script uses multiple CSS selectors for robustness against
  minor frontend changes in the VTEX store implementation used by
  Jumbo.  If you find missing data, adjust the selectors accordingly.
* Selenium performs real browser automation.  Running the scraper
  against many pages too quickly could trigger rate limits or
  anti-bot measures.  Use a reasonable delay (``--delay``) between
  requests to emulate human browsing.
* If you run the script with ``--headless false`` you can observe
  the browser while it scrapes, which is helpful for debugging.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


BASE_URL = "https://www.jumbo.com.ar"
LISTING_URL_TEMPLATE = BASE_URL + "/almacen?page={page}"


@dataclass
class ProductRecord:
    """Container for product data collected from both listing and detail pages."""

    # Basic fields from the listing card
    listing_page: int
    product_position: Optional[str] = None
    product_id: Optional[str] = None
    listing_brand: Optional[str] = None
    listing_name: Optional[str] = None
    listing_price_text: Optional[str] = None
    listing_price_value: Optional[float] = None
    product_url: Optional[str] = None
    image_url: Optional[str] = None

    # Detailed fields from the product page
    detail_name: Optional[str] = None
    detail_brand: Optional[str] = None
    sku: Optional[str] = None
    detail_price_text: Optional[str] = None
    detail_price_value: Optional[float] = None
    unit_price_text: Optional[str] = None
    iva_text: Optional[str] = None
    specs: Optional[str] = None
    image_url_detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "listing_page": self.listing_page,
            "product_position": self.product_position,
            "product_id": self.product_id,
            "listing_brand": self.listing_brand,
            "listing_name": self.listing_name,
            "listing_price_text": self.listing_price_text,
            "listing_price_value": self.listing_price_value,
            "detail_name": self.detail_name,
            "detail_brand": self.detail_brand,
            "sku": self.sku,
            "detail_price_text": self.detail_price_text,
            "detail_price_value": self.detail_price_value,
            "unit_price_text": self.unit_price_text,
            "iva_text": self.iva_text,
            "specs": self.specs,
            "product_url": self.product_url,
            "image_url": self.image_url,
            "image_url_detail": self.image_url_detail,
        }


def parse_price_string(text: str) -> Optional[float]:
    """Convert price strings like '$2.550' into floats (2550.0)."""
    if not text:
        return None
    m = re.search(r"\$\s*([\d\.,]+)", text)
    if not m:
        return None
    number = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(number)
    except ValueError:
        return None


def setup_driver(headless: bool = True):
    options = Options()
    options.add_argument("--start-maximized")
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # 🚀 Nueva forma compatible con Selenium 4.10+
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver


def extract_listing_products(driver: webdriver.Chrome, wait: WebDriverWait) -> List[Dict[str, str]]:
    """
    Read product cards on the current listing page and return a list of
    dictionaries containing basic information.  This function assumes
    the driver has already navigated to the listing page.
    """
    products = []

    # Wait for at least one card to appear or time out
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'div[data-af-element="search-result"]')
            )
        )
    except TimeoutException:
        # No products found on this page
        return products

    # Find all card containers (VTEX uses many classes; data-af-element is stable)
    cards = driver.find_elements(By.CSS_SELECTOR, 'div[data-af-element="search-result"]')
    if not cards:
        # Fallback: use a broader class name used by Jumbo
        cards = driver.find_elements(
            By.CSS_SELECTOR,
            "div.jumboargentinaio-cmedia-integration-cencosud-1-x-galleryItem",
        )

    for idx, card in enumerate(cards, 1):
        record: Dict[str, Optional[str]] = {}
        # Basic identifiers
        record["product_id"] = card.get_attribute("data-af-product-id")
        record["product_position"] = card.get_attribute("data-af-product-position")

        # Product URL
        try:
            anchor = card.find_element(
                By.CSS_SELECTOR, "a.vtex-product-summary-2-x-clearLink"
            )
            href = anchor.get_attribute("href")
            record["product_url"] = href
        except NoSuchElementException:
            record["product_url"] = None

        # Image URL
        try:
            img = card.find_element(By.CSS_SELECTOR, "img.vtex-product-summary-2-x-image")
            record["image_url"] = img.get_attribute("src")
        except NoSuchElementException:
            record["image_url"] = None

        # Brand
        brand = None
        for sel in [
            ".vtex-product-summary-2-x-productBrandName",
            ".vtex-product-summary-2-x-brandName",
        ]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                brand = el.text.strip()
                if brand:
                    break
            except NoSuchElementException:
                continue
        record["listing_brand"] = brand

        # Name
        name = None
        for sel in [
            ".vtex-product-summary-2-x-productNameContainer",
            ".vtex-product-summary-2-x-nameWrapper",
            ".vtex-product-summary-2-x-nameContainer",
        ]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                name = el.text.strip()
                if name:
                    break
            except NoSuchElementException:
                continue
        record["listing_name"] = name

        # Price
        price_text = None
        for sel in [
            ".vtex-price-format-gallery",
            ".vtex-price-format",
            ".vtex-store-components-3-x-sellingPrice",
            ".vtex-price",
        ]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                text = el.text.strip()
                if text:
                    price_text = text
                    break
            except NoSuchElementException:
                continue
        record["listing_price_text"] = price_text
        record["listing_price_value"] = parse_price_string(price_text) if price_text else None

        # 🔊 imprime lo que se extrajo de la tarjeta de listado
        print(f"[LISTING #{idx}] {json.dumps(record, ensure_ascii=False)}", flush=True)

        products.append(record)

    return products


def extract_product_detail(driver: webdriver.Chrome, wait: WebDriverWait) -> Dict[str, Optional[str]]:
    """
    Parse details from an open product page.  Assumes the driver
    already navigated to the product's URL.
    """
    detail: Dict[str, Optional[str]] = {}

    # Name
    name_selectors = [
        ".vtex-store-components-3-x-productNameContainer",
        "h1.vtex-store-components-3-x-productNameContainer",
        "h1",
    ]
    name = None
    for sel in name_selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            name = el.text.strip()
            if name:
                break
        except NoSuchElementException:
            continue
    detail["detail_name"] = name

    # Brand
    brand = None
    for sel in [
        ".vtex-store-components-3-x-productBrandName",
        ".vtex-product-summary-2-x-productBrandName",
    ]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            brand = el.text.strip()
            if brand:
                break
        except NoSuchElementException:
            continue
    detail["detail_brand"] = brand

    # SKU
    sku = None
    try:
        sku_el = driver.find_element(
            By.CSS_SELECTOR, ".vtex-product-identifier-0-x-product-identifier__value"
        )
        sku = sku_el.text.strip()
    except NoSuchElementException:
        # fallback: parse from page text
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text
            m = re.search(r"SKU\s*[:\-]?\s*([A-Za-z0-9_.-]+)", body_text)
            if m:
                sku = m.group(1)
        except NoSuchElementException:
            pass
    detail["sku"] = sku

    # Main price text
    price_selectors = [
        ".vtex-price-format-gallery",
        ".vtex-price__container .vtex-price",
        ".vtex-store-components-3-x-sellingPrice",
        ".vtex-price__price",
        ".vtex-price",
    ]
    price_text = None
    for sel in price_selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            text = el.text.strip()
            if text:
                price_text = text
                break
        except NoSuchElementException:
            continue
    detail["detail_price_text"] = price_text
    detail["detail_price_value"] = parse_price_string(price_text) if price_text else None

    # Unit price and IVA text
    unit_price_text = None
    iva_text = None
    try:
        unit = driver.find_element(By.CSS_SELECTOR, ".vtex-custom-unit-price")
        unit_price_text = unit.text.strip()
        try:
            iva = unit.find_element(By.CSS_SELECTOR, ".vtex-paragraph--impuesto")
            iva_text = iva.text.strip()
        except NoSuchElementException:
            iva_text = None
    except NoSuchElementException:
        pass
    detail["unit_price_text"] = unit_price_text
    detail["iva_text"] = iva_text

    # Specifications list
    specs_str = None
    try:
        specs_ul = driver.find_element(By.CSS_SELECTOR, "#custom-product-specs ul.product-specs")
        lis = specs_ul.find_elements(By.CSS_SELECTOR, "li.product-spec")
        pairs = []
        for li in lis:
            text = li.text.strip()
            if ":" in text:
                k, v = [part.strip() for part in text.split(":", 1)]
                pairs.append(f"{k}: {v}")
            elif text:
                pairs.append(text)
        specs_str = " | ".join(pairs) if pairs else None
    except NoSuchElementException:
        specs_str = None
    detail["specs"] = specs_str

    # Main image (large)
    img_url_detail = None
    for sel in [
        "img.vtex-store-components-3-x-productImageTag--main",
        "img.vtex-store-components-3-x-productImageTag",
    ]:
        try:
            im = driver.find_element(By.CSS_SELECTOR, sel)
            src = im.get_attribute("src")
            if src:
                img_url_detail = src.strip()
                break
        except NoSuchElementException:
            continue
    detail["image_url_detail"] = img_url_detail

    # 🔊 imprime lo que se extrajo de la ficha de producto
    print(f"[DETAIL] {json.dumps(detail, ensure_ascii=False)}", flush=True)

    return detail


def scrape_jumbo(
    start_page: int = 1,
    max_pages: int = 50,
    delay: float = 1.0,
    out_xlsx: str = "Jumbo_Almacen_Selenium.xlsx",
    out_csv: Optional[str] = None,
    headless: bool = True,
) -> pd.DataFrame:
    """
    Main routine to drive the scraper.  It iterates over listing pages,
    collects product data and exports it to the specified files.
    """
    driver = setup_driver(headless=headless)
    wait = WebDriverWait(driver, 20)
    records: List[ProductRecord] = []

    try:
        for page_num in range(start_page, start_page + max_pages):
            listing_url = LISTING_URL_TEMPLATE.format(page=page_num)
            print(f"\n[PAGE] Visiting listing page {page_num}: {listing_url}", flush=True)
            driver.get(listing_url)

            cards = extract_listing_products(driver, wait)
            if not cards:
                print("[PAGE] No more products found; stopping.", flush=True)
                break
            print(f"[PAGE] Found {len(cards)} products on page {page_num}", flush=True)

            # Iterate through product cards
            for idx, card in enumerate(cards, 1):
                rec = ProductRecord(listing_page=page_num)
                rec.product_position = card.get("product_position")
                rec.product_id = card.get("product_id")
                rec.listing_brand = card.get("listing_brand")
                rec.listing_name = card.get("listing_name")
                rec.listing_price_text = card.get("listing_price_text")
                rec.listing_price_value = card.get("listing_price_value")
                rec.product_url = card.get("product_url")
                rec.image_url = card.get("image_url")

                print(f"[PRODUCT #{idx}] Listing:", json.dumps({
                    "product_position": rec.product_position,
                    "product_id": rec.product_id,
                    "listing_brand": rec.listing_brand,
                    "listing_name": rec.listing_name,
                    "listing_price_text": rec.listing_price_text,
                    "listing_price_value": rec.listing_price_value,
                    "product_url": rec.product_url,
                    "image_url": rec.image_url,
                }, ensure_ascii=False), flush=True)

                # Go to product detail page if URL is present
                if rec.product_url:
                    try:
                        print(f"  -> Opening product URL: {rec.product_url}", flush=True)
                        # Open in same tab (navigation) to avoid too many tabs
                        driver.get(rec.product_url)
                        # Wait for product title to load (best-effort)
                        try:
                            wait.until(
                                EC.presence_of_element_located(
                                    (
                                        By.CSS_SELECTOR,
                                        ".vtex-store-components-3-x-productNameContainer",
                                    )
                                )
                            )
                        except TimeoutException:
                            pass
                        detail = extract_product_detail(driver, wait)
                        rec.detail_name = detail.get("detail_name")
                        rec.detail_brand = detail.get("detail_brand")
                        rec.sku = detail.get("sku")
                        rec.detail_price_text = detail.get("detail_price_text")
                        rec.detail_price_value = detail.get("detail_price_value")
                        rec.unit_price_text = detail.get("unit_price_text")
                        rec.iva_text = detail.get("iva_text")
                        rec.specs = detail.get("specs")
                        rec.image_url_detail = detail.get("image_url_detail")
                    except Exception as e:
                        print(f"  !! Error scraping product detail: {e}", flush=True)
                else:
                    print(f"  !! Skipping product with missing URL (position {idx})", flush=True)

                # 🔊 imprime el registro final combinado
                print(f"[PRODUCT #{idx}] Final:", json.dumps(rec.to_dict(), ensure_ascii=False), flush=True)

                records.append(rec)
                # Sleep between products
                time.sleep(delay)

            # Sleep between pages
            time.sleep(delay)

    finally:
        driver.quit()

    # Convert to DataFrame
    df = pd.DataFrame([rec.to_dict() for rec in records])
    # Suggest column order
    columns = [
        "listing_page",
        "product_position",
        "product_id",
        "listing_brand",
        "listing_name",
        "listing_price_text",
        "listing_price_value",
        "detail_name",
        "detail_brand",
        "sku",
        "detail_price_text",
        "detail_price_value",
        "unit_price_text",
        "iva_text",
        "specs",
        "product_url",
        "image_url",
        "image_url_detail",
    ]
    # Keep unknown columns at the end
    final_cols = [c for c in columns if c in df.columns] + [
        c for c in df.columns if c not in columns
    ]
    df = df.reindex(columns=final_cols)

    # Export results
    print(f"\n[EXPORT] Writing Excel file to {out_xlsx}", flush=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Productos")
    if out_csv:
        print(f"[EXPORT] Writing CSV file to {out_csv}", flush=True)
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print(f"[DONE] Total products scraped: {len(df)}", flush=True)
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Jumbo Argentina Almacen using Selenium."
    )
    parser.add_argument(
        "--start",
        type=int,
        default=1,
        help="First listing page to scrape (default: 1)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=50,
        help="Maximum number of listing pages to scrape (default: 50)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between requests (default: 1.0)",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="Jumbo_Almacen_Selenium.xlsx",
        help="Output Excel filename (default: Jumbo_Almacen_Selenium.xlsx)",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="",
        help="Optional output CSV filename",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome in headless mode (default: off)",
    )

    args = parser.parse_args()
    scrape_jumbo(
        start_page=args.start,
        max_pages=args.max_pages,
        delay=args.delay,
        out_xlsx=args.out,
        out_csv=args.csv or None,
        headless=args.headless,
    )


if __name__ == "__main__":
    main()
