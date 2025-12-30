#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
from typing import Optional, List, Dict

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


# =========================
# CONFIG
# =========================
BASE_LISTING = "https://www.jumbo.com.ar/55707?evento=jumboofertas&map=productClusterIds&page={page}"
MAX_PAGES = 2000  # por si querés limitar duro; el script corta cuando no hay items
OUT_XLSX = "Jumbo_Lacteos.xlsx"

HEADLESS = True
SLEEP_BETWEEN_PRODUCTS = (0.2, 0.6)  # min,max
SLEEP_BETWEEN_PAGES = (0.4, 0.9)     # min,max

TIMEOUT = 20

PRICE_RE = re.compile(r"(\d[\d\.\,]*)")


# =========================
# UTILS
# =========================
def jitter(a: float, b: float) -> None:
    import random
    time.sleep(random.uniform(a, b))


def normalize_price_to_varchar(raw: Optional[str]) -> Optional[str]:
    """
    "$11.766,67" -> "11766.67"
    "$2.100"     -> "2100.00"
    """
    if not raw:
        return None
    s = raw.strip()
    m = PRICE_RE.search(s)
    if not m:
        return None
    num = m.group(1)
    # Caso AR: miles con "." y decimales con ","
    num = num.replace(".", "").replace(",", ".")
    try:
        val = float(num)
        return f"{val:.2f}"
    except ValueError:
        return None


def safe_text(el) -> Optional[str]:
    if not el:
        return None
    t = el.text.strip()
    return t if t else None


def first_text(driver, xpaths: List[str], timeout=4) -> Optional[str]:
    """
    Prueba una lista de XPaths y devuelve el primer texto no vacío.
    """
    for xp in xpaths:
        try:
            el = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.XPATH, xp))
            )
            txt = safe_text(el)
            if txt:
                return txt
        except TimeoutException:
            continue
        except WebDriverException:
            continue
    return None


def all_texts(driver, xpaths: List[str], timeout=2) -> List[str]:
    out = []
    for xp in xpaths:
        try:
            els = WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((By.XPATH, xp))
            )
            for e in els:
                t = safe_text(e)
                if t:
                    out.append(t)
        except TimeoutException:
            continue
        except WebDriverException:
            continue
    # dedupe manteniendo orden
    seen = set()
    dedup = []
    for t in out:
        if t not in seen:
            seen.add(t)
            dedup.append(t)
    return dedup


# =========================
# SELENIUM SETUP
# =========================
def build_driver() -> webdriver.Chrome:
    chrome_options = webdriver.ChromeOptions()
    if HEADLESS:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1400,900")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--lang=es-AR")

    # Reduce ruido
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


# =========================
# SCRAPING LOGIC
# =========================
def collect_product_urls_from_listing(driver, page_url: str) -> List[str]:
    """
    En una página /lacteos?page=N:
    - espera items: div[data-af-element="search-result"]
    - toma href del <a> hacia /p
    """
    driver.get(page_url)

    # Espera un poco por carga
    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-af-element="search-result"]'))
        )
    except TimeoutException:
        return []

    cards = driver.find_elements(By.CSS_SELECTOR, 'div[data-af-element="search-result"]')
    if not cards:
        return []

    urls = []
    for c in cards:
        try:
            a = c.find_element(By.CSS_SELECTOR, "a[href*='/p']")
            href = a.get_attribute("href")
            if href and "/p" in href:
                urls.append(href.split("?")[0])
        except Exception:
            continue

    # dedupe
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def parse_product_page(driver, url: str) -> Dict[str, Optional[str]]:
    """
    Abre el producto y extrae:
    - marca
    - nombre
    - sku
    - precio_base (regular)
    - precio_oferta
    - tipo_oferta (varios)
    """
    driver.get(url)

    # Espera algo estable del PDP (SKU o título)
    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, "//span[contains(@class,'vtex-product-identifier') and contains(@class,'__value')]"))
        )
    except TimeoutException:
        # si no está el SKU, intenta por nombre
        pass

    # SKU
    sku = first_text(driver, [
        "//span[contains(@class,'vtex-product-identifier') and contains(@class,'__value')]",
        "//span[contains(@class,'product-identifier__value')]"
    ], timeout=4)

    # Nombre (VTEX suele tener h1)
    nombre = first_text(driver, [
        "//h1",
        "//h1[contains(@class,'vtex-store-components')]",
        "//span[contains(@class,'vtex-store-components') and contains(@class,'productName')]"
    ], timeout=4)

    # Marca (en PDP suele aparecer como brand)
    marca = first_text(driver, [
        "//*[contains(@class,'productBrand')][1]",
        "//*[contains(@class,'brand')][1]",
        "//a[contains(@href,'/marca')][1]"
    ], timeout=2)

    # Precios:
    # - oferta: suele ser el precio grande/actual (vtex-price-format-gallery)
    # - base: suele ser el regular (a veces aparece como otro div cercano)
    # Como las clases cambian, usamos contains()
    precio_oferta_raw = first_text(driver, [
        # precio "actual"
        "(//div[contains(@class,'vtex-price-format-gallery')])[1]",
        "(//span[contains(@class,'vtex-price-format-gallery')])[1]",
    ], timeout=3)

    # precio base/regular (a veces aparece como otro div cerca con clase distinta)
    # intentos:
    precio_base_raw = first_text(driver, [
        # En muchos casos este es el "regular" tachado o secundario
        "(//div[contains(@class,'2t-mVs')])[1]",  # tu clase observada (puede variar)
        "(//div[contains(@class,'regular') or contains(@class,'listPrice') or contains(@class,'ListPrice')])[1]",
        "(//span[contains(@class,'regular') or contains(@class,'listPrice') or contains(@class,'ListPrice')])[1]",
        # fallback: tomar el 2do precio gallery si hay dos
        "(//div[contains(@class,'vtex-price-format-gallery')])[2]"
    ], timeout=2)

    precio_oferta = normalize_price_to_varchar(precio_oferta_raw)
    precio_base = normalize_price_to_varchar(precio_base_raw)

    # Tipos de oferta: pueden ser varios (3x2, 2x1, -70%, etc.)
    # Capturamos spans con textos típicos:
    promos = all_texts(driver, [
        # clases que viste (pueden variar)
        "//span[contains(@class,'Aq2AAEui')]",     # 3x2 / 2x1
        "//span[contains(@class,'3Hc7_')]",        # -70%
        # fallback: cualquier span visible que tenga x o %
        "//span[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'x') and string-length(normalize-space(.))<=6]",
        "//span[contains(normalize-space(.),'%') and string-length(normalize-space(.))<=6]",
    ], timeout=2)

    # filtra textos raros
    promos_clean = []
    for p in promos:
        p2 = p.replace(" ", "").strip()
        if not p2:
            continue
        if re.fullmatch(r"\d+x\d+|\-\d+%|\d+%", p2) or ("x" in p2 and len(p2) <= 6) or ("%" in p2 and len(p2) <= 6):
            promos_clean.append(p2)

    tipo_oferta = " | ".join(dict.fromkeys(promos_clean)) if promos_clean else None

    # Reglas simples:
    # - Si solo hay un precio y no hay base, set base=oferta
    if precio_oferta and not precio_base:
        precio_base = precio_oferta

    return {
        "url": url,
        "sku": sku,
        "marca": marca,
        "nombre": nombre,
        "precio_base": precio_base,
        "precio_oferta": precio_oferta,
        "tipo_oferta": tipo_oferta,
    }


def scrape_jumbo_lacteos():
    driver = build_driver()
    data = []
    try:
        for page in range(1, MAX_PAGES + 1):
            page_url = BASE_LISTING.format(page=page)
            print(f"📄 Listing page {page}: {page_url}")

            urls = collect_product_urls_from_listing(driver, page_url)
            if not urls:
                print("✅ No se encontraron más productos. Fin.")
                break

            print(f"   ↳ productos detectados: {len(urls)}")

            for i, u in enumerate(urls, start=1):
                try:
                    jitter(*SLEEP_BETWEEN_PRODUCTS)
                    row = parse_product_page(driver, u)
                    print(f"   ✅ [{i}/{len(urls)}] {row.get('sku')} | {row.get('precio_oferta')} | {row.get('nombre')}")
                    data.append(row)
                except Exception as e:
                    print(f"   ❌ Error en producto {u}: {e}")
                    data.append({
                        "url": u, "sku": None, "marca": None, "nombre": None,
                        "precio_base": None, "precio_oferta": None, "tipo_oferta": None
                    })

            jitter(*SLEEP_BETWEEN_PAGES)

    finally:
        driver.quit()

    df = pd.DataFrame(data)

    # Dedupe por url o sku (preferible url)
    if not df.empty:
        df = df.drop_duplicates(subset=["url"], keep="first")

    df.to_excel(OUT_XLSX, index=False)
    print(f"✅ Excel generado: {OUT_XLSX} | filas: {len(df)}")


if __name__ == "__main__":
    scrape_jumbo_lacteos()
