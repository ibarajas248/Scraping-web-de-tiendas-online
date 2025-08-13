# -*- coding: utf-8 -*-
# Scraper SuperMami – páginas 51 a 100 con Selenium (Oracle Commerce/Endeca)

import re
import time
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# ---------- Config editable ----------
BASE_CATEG_URL = (
    "https://supermami.com.ar/super/categoria"
    "?_dyncharset=utf-8&Dy=1&Nty=1&minAutoSuggestInputLength=3"
    "&autoSuggestServiceUrl=%2Fassembler%3FassemblerContentCollection%3D%2Fcontent%2FShared%2FAuto-Suggest+Panels%26format%3Djson"
    "&searchUrl=%2Fsuper&containerClass=search_rubricator&defaultImage=%2Fimages%2Fno_image_auto_suggest.png"
    "&rightNowEnabled=false&Ntt="
)
NRPP = 72                 # productos por página (36/72/120 suelen funcionar)
PAGE_START = 51           # <-- pedido: de 51
PAGE_END   = 100          # <-- a 100
WAIT_SEC = 20
HUMAN_SLEEP = 0.25
OUT_CSV = "supermami_p51-100.csv"
OUT_XLSX = "supermami_p51-100.xlsx"

# ---------- Helpers ----------
PRICE_RX = re.compile(r"(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:,\d{2}))")

def with_params(url: str, **extra):
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in extra.items():
        q[str(k)] = [str(v)]
    query = urlencode({k: v[0] for k, v in q.items()}, doseq=False)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, query, u.fragment))

def norm_price(txt):
    if not txt:
        return None
    m = PRICE_RX.search(txt.replace("\xa0", " ").strip())
    if not m:
        return None
    val = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(val)
    except:
        return None

def get_text(el):
    return (el.get_text(strip=True) if el else "").strip()

def first(sel_results):
    return sel_results[0] if sel_results else None

def extract_products_from_dom(html, base_page_url):
    soup = BeautifulSoup(html, "html.parser")

    # Probar varios layouts comunes de Oracle Commerce
    grids_to_try = [
        "ul.products li", "li.product", "div.product", "div.product-tile",
        "div.grid-tile", "div.product_item", "div.productItem", "div.tile-content"
    ]
    cards = []
    for css in grids_to_try:
        found = soup.select(css)
        if len(found) > len(cards):
            cards = found
    if not cards:
        return []

    items = []
    for card in cards:
        # Nombre y URL
        name_el = first(card.select("a.product_name")) or first(card.select("a.productName")) \
                  or first(card.select("a.name")) or first(card.select("h3 a")) \
                  or first(card.select("h2 a")) or first(card.select("a"))
        name = get_text(name_el) or get_text(first(card.select(".product_name, .productName, .name, h3, h2, .title")))
        url = None
        if name_el and name_el.has_attr("href"):
            href = name_el["href"].strip()
            if href.startswith("//"):
                url = "https:" + href
            elif href.startswith("/"):
                url = f"https://{urlparse(base_page_url).netloc}{href}"
            elif href.startswith("http"):
                url = href

        # Imagen
        img_el = first(card.select("img"))
        img = None
        if img_el:
            for attr in ["data-src", "data-original", "src", "data-image"]:
                if img_el.has_attr(attr) and img_el.get(attr):
                    val = img_el[attr].strip()
                    if val.startswith("//"):
                        img = "https:" + val
                    elif val.startswith("/"):
                        img = f"https://{urlparse(base_page_url).netloc}{val}"
                    else:
                        img = val
                    break

        # Precio
        price_txt = None
        for css in [".price .value", ".price .amount", ".price", ".product_price",
                    ".current_price", ".sale_price", ".our_price", "[class*='price']"]:
            el = first(card.select(css))
            if el and el.get_text(strip=True):
                price_txt = el.get_text(" ", strip=True)
                break
        price = norm_price(price_txt)

        # PPU (si aparece)
        unit_txt = get_text(first(card.select(".unit, .unit-price, .price-per-unit, .precioXUnidad, .unidad")))
        unit_price = norm_price(unit_txt)

        # Marca / SKU si hay
        brand = get_text(first(card.select(".brand, .marca, [itemprop='brand']")))
        sku = None
        for attr in ["data-sku", "data-productid", "data-id", "data-sku-id"]:
            if card.has_attr(attr):
                sku = card.get(attr)
                break

        # Filtrar ruido
        if not name and not url and not price:
            continue

        items.append({
            "nombre": name or None,
            "marca": brand or None,
            "precio": price,
            "precio_texto": price_txt,
            "precio_por_unidad": unit_price,
            "ppu_texto": unit_txt or None,
            "sku": sku,
            "url": url,
            "imagen": img
        })
    return items

def make_driver():
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return driver

def wait_grid(driver):
    try:
        WebDriverWait(driver, WAIT_SEC).until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "li.product, div.product, div.product-tile, div.grid-tile, [class*='product'] [class*='price']"
            ))
        )
    except TimeoutException:
        pass
    time.sleep(0.6)

# ---------- Main ----------
def run_range(page_start: int, page_end: int, nrpp: int = NRPP):
    driver = make_driver()
    all_rows = []
    try:
        for page in range(page_start, page_end + 1):
            offset = (page - 1) * nrpp
            url = with_params(BASE_CATEG_URL, Nrpp=nrpp, No=offset)
            driver.get(url)
            wait_grid(driver)
            html = driver.page_source
            rows = extract_products_from_dom(html, url)

            # reintento corto por si hay lazy load
            if not rows:
                time.sleep(1.0)
                html = driver.page_source
                rows = extract_products_from_dom(html, url)

            print(f"[p{page}] {len(rows)} productos (offset={offset})")
            all_rows.extend(rows)
            time.sleep(HUMAN_SLEEP)
    finally:
        driver.quit()

    if not all_rows:
        print("No se extrajo nada en el rango solicitado.")
        return

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url", "nombre"], keep="first")
    cols = ["nombre", "marca", "precio", "precio_texto", "precio_por_unidad", "ppu_texto", "sku", "url", "imagen"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].reset_index(drop=True)

    print(f"Total productos en páginas {page_start}-{page_end}: {len(df)}")
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    try:
        df.to_excel(OUT_XLSX, index=False)
    except Exception as e:
        print(f"Advertencia al guardar XLSX: {e}")

if __name__ == "__main__":
    run_range(PAGE_START, PAGE_END, NRPP)
