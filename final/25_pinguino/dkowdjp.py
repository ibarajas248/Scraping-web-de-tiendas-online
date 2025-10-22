#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jumbo (VTEX) — Scraper Selenium que recorre TODAS las hojas de 'Almacén'
haciendo scroll infinito / 'Mostrar más' hasta agotar productos, y luego
abre cada producto para extraer detalles (SKU, precios, specs).

- Sin límite de ~408: recorre todas las subcategorías hoja.
- Selenium-only para render y detalle; requests se usa SOLO para obtener
  la lista de hojas (más robusto que tratar de abrir todos los menús).
- Si prefieres 0 requests, se puede hacer un scraper del menú con Selenium.

Requisitos:
  pip install selenium webdriver-manager pandas openpyxl requests
"""

from __future__ import annotations
import argparse, json, re, time, math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ========= Constantes sitio =========
BASE_URL = "https://www.jumbo.com.ar"
API_TREE = f"{BASE_URL}/api/catalog_system/pub/category/tree/{{depth}}"

@dataclass
class ProductRecord:
    listing_page: int
    product_position: Optional[str] = None
    product_id: Optional[str] = None
    listing_brand: Optional[str] = None
    listing_name: Optional[str] = None
    listing_price_text: Optional[str] = None
    listing_price_value: Optional[float] = None
    product_url: Optional[str] = None
    image_url: Optional[str] = None
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

# ========= Utilidades =========
def parse_price_string(text: str) -> Optional[float]:
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

def setup_driver(headless: bool = True) -> webdriver.Chrome:
    options = ChromeOptions()
    options.add_argument("--start-maximized")
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver

def _http() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) JumboSeleniumAll/1.0",
        "Accept": "application/json, text/plain, */*"
    })
    return s

def get_leaf_categories_almacen(depth: int = 3) -> List[Dict[str,str]]:
    """
    Devuelve hojas bajo 'almacen' con su ruta navegable: /almacen/... ?map=c,c,...
    """
    s = _http()
    r = s.get(API_TREE.format(depth=depth), timeout=30)
    r.raise_for_status()
    tree = r.json()
    leaves = []

    def walk(node, slugs, maps):
        slug = node.get("url","").strip("/").split("/")[-1]
        if not slug:
            return
        new_slugs = slugs + [slug]
        new_maps  = maps  + ["c"]
        children = node.get("children") or []
        if not children:
            leaves.append({
                "path": "/".join(new_slugs),
                "map": ",".join(new_maps),
                "name": node.get("name","(sin nombre)")
            })
        else:
            for ch in children:
                walk(ch, new_slugs, new_maps)

    for root in tree:
        walk(root, [], [])

    leaves = [lv for lv in leaves if lv["path"].split("/")[0] == "almacen"]
    # Quitamos la propia raíz "almacen" si aparece como hoja
    leaves = [lv for lv in leaves if lv["path"] != "almacen"]
    # Orden alfabético por nombre para reproducibilidad
    leaves.sort(key=lambda x: x["name"].lower())
    return leaves

# ========= Scroll infinito / “Mostrar más” =========
def load_all_cards_in_category(driver: webdriver.Chrome, wait: WebDriverWait, delay: float = 0.6, max_idle_rounds: int = 6) -> None:
    """
    Hace scroll hasta el fondo, intenta click en 'Mostrar más' si aparece,
    y repite hasta que el conteo de tarjetas no aumente por varios ciclos.
    """
    last_count = 0
    idle = 0
    while True:
        # Conteo actual de tarjetas
        cards = driver.find_elements(By.CSS_SELECTOR, 'div[data-af-element="search-result"], div.jumboargentinaio-cmedia-integration-cencosud-1-x-galleryItem')
        count = len(cards)

        # Intentar click en “Mostrar más” si existe
        try:
            btns = driver.find_elements(By.CSS_SELECTOR, "button.vtex-search-result-3-x-buttonShowMore, button.vtex-button")
            clicked = False
            for b in btns:
                label = (b.text or "").strip().lower()
                if "mostrar más" in label or "ver más" in label or "cargar más" in label:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({behavior:'auto', block:'center'});", b)
                        time.sleep(0.2)
                        b.click()
                        clicked = True
                        time.sleep(delay)
                        break
                    except WebDriverException:
                        continue
            if clicked:
                continue
        except Exception:
            pass

        # Scroll al fondo
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(delay)

        # Espera algún elemento de producto (por si tarda)
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-af-element="search-result"]')))
        except TimeoutException:
            pass

        # Nuevo conteo
        cards = driver.find_elements(By.CSS_SELECTOR, 'div[data-af-element="search-result"], div.jumboargentinaio-cmedia-integration-cencosud-1-x-galleryItem')
        new_count = len(cards)

        if new_count <= last_count:
            idle += 1
        else:
            idle = 0
            last_count = new_count

        # Si no crece después de varios ciclos, paramos
        if idle >= max_idle_rounds:
            break

def extract_listing_cards(driver: webdriver.Chrome) -> List[Dict[str, Optional[str]]]:
    products = []
    cards = driver.find_elements(By.CSS_SELECTOR, 'div[data-af-element="search-result"], div.jumboargentinaio-cmedia-integration-cencosud-1-x-galleryItem')
    for idx, card in enumerate(cards, 1):
        rec: Dict[str, Optional[str]] = {}
        rec["product_id"] = card.get_attribute("data-af-product-id")
        rec["product_position"] = card.get_attribute("data-af-product-position")
        # URL
        href = None
        for sel in ["a.vtex-product-summary-2-x-clearLink", "a"]:
            try:
                a = card.find_element(By.CSS_SELECTOR, sel)
                cand = a.get_attribute("href")
                if cand and "/p" in cand:
                    href = cand
                    break
            except NoSuchElementException:
                continue
        rec["product_url"] = href
        # Imagen
        try:
            img = card.find_element(By.CSS_SELECTOR, "img.vtex-product-summary-2-x-image, img.vtex-product-summary-2-x-imageNormal, img")
            rec["image_url"] = img.get_attribute("src")
        except NoSuchElementException:
            rec["image_url"] = None
        # Marca
        brand = None
        for sel in [".vtex-product-summary-2-x-productBrandName", ".vtex-product-summary-2-x-brandName"]:
            try:
                t = card.find_element(By.CSS_SELECTOR, sel).text.strip()
                if t:
                    brand = t
                    break
            except NoSuchElementException:
                continue
        rec["listing_brand"] = brand
        # Nombre
        name = None
        for sel in [".vtex-product-summary-2-x-productNameContainer", ".vtex-product-summary-2-x-nameWrapper", ".vtex-product-summary-2-x-nameContainer"]:
            try:
                t = card.find_element(By.CSS_SELECTOR, sel).text.strip()
                if t:
                    name = t
                    break
            except NoSuchElementException:
                continue
        rec["listing_name"] = name
        # Precio (texto)
        price_text = None
        for sel in [".vtex-price-format-gallery", ".vtex-price-format", ".vtex-store-components-3-x-sellingPrice", ".vtex-price", ".vtex-price__price"]:
            try:
                t = card.find_element(By.CSS_SELECTOR, sel).text.strip()
                if t:
                    price_text = t
                    break
            except NoSuchElementException:
                continue
        rec["listing_price_text"] = price_text
        rec["listing_price_value"] = parse_price_string(price_text) if price_text else None

        products.append(rec)
    return products

def extract_product_detail(driver: webdriver.Chrome) -> Dict[str, Optional[str]]:
    detail: Dict[str, Optional[str]] = {}
    # Nombre
    name = None
    for sel in [".vtex-store-components-3-x-productNameContainer", "h1.vtex-store-components-3-x-productNameContainer", "h1"]:
        try:
            t = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
            if t:
                name = t
                break
        except NoSuchElementException:
            continue
    detail["detail_name"] = name
    # Marca
    brand = None
    for sel in [".vtex-store-components-3-x-productBrandName", ".vtex-product-summary-2-x-productBrandName"]:
        try:
            t = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
            if t:
                brand = t
                break
        except NoSuchElementException:
            continue
    detail["detail_brand"] = brand
    # SKU
    sku = None
    try:
        sku_el = driver.find_element(By.CSS_SELECTOR, ".vtex-product-identifier-0-x-product-identifier__value")
        sku = sku_el.text.strip()
    except NoSuchElementException:
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text
            m = re.search(r"SKU\s*[:\-]?\s*([A-Za-z0-9_.-]+)", body_text)
            if m:
                sku = m.group(1)
        except NoSuchElementException:
            pass
    detail["sku"] = sku
    # Precio principal
    price_text = None
    for sel in [".vtex-price-format-gallery", ".vtex-price__container .vtex-price", ".vtex-store-components-3-x-sellingPrice", ".vtex-price__price", ".vtex-price"]:
        try:
            t = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
            if t:
                price_text = t
                break
        except NoSuchElementException:
            continue
    detail["detail_price_text"] = price_text
    detail["detail_price_value"] = parse_price_string(price_text) if price_text else None
    # Unit price & IVA
    unit_price_text, iva_text = None, None
    try:
        unit = driver.find_element(By.CSS_SELECTOR, ".vtex-custom-unit-price")
        unit_price_text = unit.text.strip()
        try:
            iva = unit.find_element(By.CSS_SELECTOR, ".vtex-paragraph--impuesto")
            iva_text = iva.text.strip()
        except NoSuchElementException:
            pass
    except NoSuchElementException:
        pass
    detail["unit_price_text"] = unit_price_text
    detail["iva_text"] = iva_text
    # Especificaciones
    specs_str = None
    try:
        lis = driver.find_elements(By.CSS_SELECTOR, "#custom-product-specs ul.product-specs li.product-spec")
        pairs = []
        for li in lis:
            txt = li.text.strip()
            if ":" in txt:
                k, v = [p.strip() for p in txt.split(":", 1)]
                pairs.append(f"{k}: {v}")
            elif txt:
                pairs.append(txt)
        specs_str = " | ".join(pairs) if pairs else None
    except NoSuchElementException:
        pass
    detail["specs"] = specs_str
    # Imagen grande
    img_url_detail = None
    for sel in ["img.vtex-store-components-3-x-productImageTag--main", "img.vtex-store-components-3-x-productImageTag"]:
        try:
            src = driver.find_element(By.CSS_SELECTOR, sel).get_attribute("src")
            if src:
                img_url_detail = src.strip()
                break
        except NoSuchElementException:
            continue
    detail["image_url_detail"] = img_url_detail
    return detail

def scrape_jumbo_all(
    delay: float = 0.6,
    out_xlsx: str = "Jumbo_Almacen_Selenium_FULL.xlsx",
    out_csv: Optional[str] = None,
    headless: bool = True,
    open_details: bool = True,
    max_products_per_leaf: Optional[int] = None
) -> pd.DataFrame:
    """
    Recorre todas las hojas bajo 'Almacén', hace scroll infinito y colecta TODOS
    los productos visibles en cada hoja. Luego (opcional) abre cada producto.
    """
    driver = setup_driver(headless=headless)
    wait = WebDriverWait(driver, 20)
    records: List[ProductRecord] = []
    seen_pairs: Set[Tuple[str,str]] = set()  # (product_id, product_url)

    try:
        leaves = get_leaf_categories_almacen(depth=3)
        print(f"[TREE] Hojas 'Almacén': {len(leaves)}")
        for li, leaf in enumerate(leaves, 1):
            path = leaf["path"]
            map_str = leaf["map"]
            cat_url = f"{BASE_URL}/{path}?map={map_str}&O=OrderByNameASC"
            print(f"\n[CATEGORY {li}/{len(leaves)}] {leaf['name']} → {cat_url}")

            driver.get(cat_url)
            # Asegurar que carguen resultados
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.vtex-search-result-3-x-gallery")))
            except TimeoutException:
                print("  !! Sin galería visible, se omite.")
                continue

            # Scroll infinito + “mostrar más”
            load_all_cards_in_category(driver, wait, delay=delay, max_idle_rounds=6)

            # Extraer todas las cards del DOM cargado
            cards = extract_listing_cards(driver)
            print(f"  · Tarjetas listadas: {len(cards)}")

            # Guardar hrefs para no perderlos al navegar
            hrefs: List[Tuple[Dict[str, Optional[str]], str]] = []
            for c in cards:
                href = c.get("product_url")
                pid  = c.get("product_id") or ""
                if not href:
                    continue
                key = (pid, href)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                hrefs.append((c, href))
                if max_products_per_leaf and len(hrefs) >= max_products_per_leaf:
                    break

            print(f"  · Productos únicos a visitar: {len(hrefs)}")

            # Visitar detalle si está activo
            for idx, (card, href) in enumerate(hrefs, 1):
                rec = ProductRecord(listing_page=0)
                rec.product_position = card.get("product_position")
                rec.product_id = card.get("product_id")
                rec.listing_brand = card.get("listing_brand")
                rec.listing_name = card.get("listing_name")
                rec.listing_price_text = card.get("listing_price_text")
                rec.listing_price_value = card.get("listing_price_value")
                rec.product_url = href
                rec.image_url = card.get("image_url")

                if open_details:
                    try:
                        driver.get(href)
                        # Espera título o contenedor
                        try:
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".vtex-store-components-3-x-productNameContainer")))
                        except TimeoutException:
                            pass
                        detail = extract_product_detail(driver)
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
                        print(f"  !! Error detalle ({idx}/{len(hrefs)}): {e}")
                records.append(rec)
                time.sleep(delay)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # DataFrame + export
    df = pd.DataFrame([r.to_dict() for r in records]).drop_duplicates(subset=["product_id","product_url","sku"], keep="first")
    print(f"\n[EXPORT] Excel → {out_xlsx}  (filas: {len(df)})")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Productos")
    if out_csv:
        print(f"[EXPORT] CSV → {out_csv}")
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[DONE] Total productos: {len(df)}")
    return df

# ========= CLI =========
def main():
    parser = argparse.ArgumentParser(description="Scraper Selenium Jumbo — recorre TODO Almacén con scroll infinito.")
    parser.add_argument("--delay", type=float, default=0.6, help="Delay (s) entre acciones Selenium (default 0.6)")
    parser.add_argument("--out", type=str, default="Jumbo_Almacen_Selenium_FULL.xlsx", help="Archivo Excel salida")
    parser.add_argument("--csv", type=str, default="", help="Archivo CSV opcional")
    parser.add_argument("--headless", action="store_true", help="Headless Chrome")
    parser.add_argument("--no-detail", action="store_true", help="No abrir detalle, solo listado")
    parser.add_argument("--max-per-leaf", type=int, default=0, help="Máximo productos por categoría (0 = sin límite)")
    args = parser.parse_args()

    scrape_jumbo_all(
        delay=args.delay,
        out_xlsx=args.out,
        out_csv=args.csv or None,
        headless=args.headless,
        open_details=(not args.no_detail),
        max_products_per_leaf=(args.max_per_leaf or None),
    )

if __name__ == "__main__":
    main()
