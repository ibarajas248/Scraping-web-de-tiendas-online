#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper La Anónima (categoría -> detalle)
- Selenium para navegar, clicks y scroll (render dinámico)
- BeautifulSoup para parseo
- Exporta a CSV

Requisitos:
  pip install selenium beautifulsoup4 pandas webdriver-manager lxml

Notas:
- No hay EAN visible; se extrae "Cod." (sku/sku tienda) y otros metadatos.
- Hace scroll para asegurar carga de imágenes/DOM y toma hrefs antes de visitar.
- Si quieres limitar páginas, ajusta MAX_PAGES.
"""

import re
import time
import html
from typing import List, Dict, Optional
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 🔧 cambio clave para Selenium 4+: usar Service
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


BASE = "https://supermercado.laanonimaonline.com"
START = f"{BASE}/almacen/n1_512/pag/1/"

# ---------- Ajustes ----------
HEADLESS = True          # ponlo en False si quieres ver el navegador
PAGE_LOAD_TIMEOUT = 25
IMPLICIT_WAIT = 2
SCROLL_PAUSES = [300, 600, 900]  # px; hará scroll incremental
SLEEP_BETWEEN_PAGES = 1.2
SLEEP_BETWEEN_PRODUCTS = 0.8
MAX_PAGES: Optional[int] = None   # None = intentar todas las páginas. Para probar: 1
OUT_CSV = "laanonima_detalle.csv"


def setup_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        # Usa el nuevo headless para Chrome reciente
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,1800")
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    # ✅ Forma correcta en Selenium 4+
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.implicitly_wait(IMPLICIT_WAIT)
    return driver


def wait_dom(driver, css: str, timeout: int = 15):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css))
    )


def try_click_cookies(driver):
    """Si aparece banner de cookies, intenta cerrarlo (no siempre está)."""
    try:
        # Ajusta si ves otro selector real en la página
        candidates = [
            "button#onetrust-accept-btn-handler",
            "button[aria-label='Aceptar']",
            "button.cookie-accept",
            "div.cookie a.btn, div.cookie button",
        ]
        for sel in candidates:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            if elems:
                elems[0].click()
                time.sleep(0.2)
                break
    except Exception:
        pass


def smooth_scroll(driver):
    """Desplaza parcialmente para forzar carga vaga."""
    for y in SCROLL_PAUSES:
        driver.execute_script(f"window.scrollTo(0, {y});")
        time.sleep(0.3)
    # Bajar hasta el final
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(0.4)


def get_list_links_from_page(driver) -> List[str]:
    """Desde una página de categoría, devuelve hrefs absolutos de cada producto."""
    html_source = driver.page_source
    soup = BeautifulSoup(html_source, "lxml")

    # Cada producto tiene div.producto.item ... y dentro un <a> con href "/almacen/.../art_XXXXX/"
    links = []
    for card in soup.select("div.producto.item a[href*='/almacen/'][href*='/art_']"):
        href = card.get("href") or ""
        if not href:
            continue
        full = urljoin(BASE, href)
        # Evitar duplicados y thumbs de galería
        if "/art_" in full and full not in links:
            links.append(full)
    return links


def get_next_page_url(current_url: str, soup: BeautifulSoup) -> Optional[str]:
    """
    Intenta detectar la siguiente página:
      - Botón de paginación con 'Siguiente' o un enlace con /pag/{n}/
    """
    current_match = re.search(r"/pag/(\d+)/", current_url)
    if not current_match:
        return None
    current_page = int(current_match.group(1))

    # Intento 1: un link explícito con /pag/{current+1}/
    next_guess = f"/pag/{current_page + 1}/"
    a = soup.select_one(f"a[href*='{next_guess}']")
    if a and a.has_attr("href"):
        return urljoin(BASE, a["href"])

    # Intento 2: construir por patrón; verificaremos en la siguiente iteración
    guess = re.sub(r"/pag/\d+/", f"/pag/{current_page + 1}/", current_url)
    return guess


def parse_price_text(txt: str) -> Optional[float]:
    """
    Convierte textos tipo '$ 6.600,00' o '6.600' a float (6600.0)
    """
    if not txt:
        return None
    t = txt.strip()
    t = t.replace("$", "").replace(" ", "")
    # formato AR: punto como miles, coma como decimal
    t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def text_or_none(node) -> Optional[str]:
    if not node:
        return None
    return re.sub(r"\s+", " ", node.get_text(strip=True)) or None


def parse_detail(html_source: str, url: str) -> Dict:
    soup = BeautifulSoup(html_source, "lxml")

    # Nombre
    nombre = text_or_none(soup.select_one("h1.titulo_producto.principal"))

    # Código/SKU (div.codigo: "Cod. 3115185")
    cod_txt = text_or_none(soup.select_one("div.codigo"))
    sku = None
    if cod_txt:
        m = re.search(r"(\d+)", cod_txt)
        if m:
            sku = m.group(1)

    # Inputs ocultos: sku, id_item, marca, precios
    sku_hidden = soup.select_one("input[id^='sku_item_imetrics_'][value]")
    if sku_hidden and not sku:
        sku = sku_hidden.get("value")

    id_item = None
    id_item_hidden = soup.select_one("input#id_item[value], input[id^='id_item_'][value]")
    if id_item_hidden:
        id_item = id_item_hidden.get("value")

    # Marca
    marca_hidden = soup.select_one("input[id^='brand_item_imetrics_'][value]")
    marca = marca_hidden.get("value") if marca_hidden else None

    # Categorías (viene con entidades HTML y ' &gt; ')
    cat_hidden = soup.select_one("input[id^='categorias_item_imetrics_'][value]")
    categorias = None
    if cat_hidden:
        categorias = html.unescape(cat_hidden.get("value") or "")
        categorias = categorias.replace("  ", " ").strip()

    # Descripción
    descripcion = text_or_none(soup.select_one("div.descripcion div.texto"))

    # Precios (lista y plus)
    # En detalle se ven:
    #  - ".precio_complemento .precio.destacado" para precio principal
    #  - ".precio-plus .precio b" para precio plus
    precio_lista = None
    precio_plus = None

    price_block = soup.select_one(".precio_complemento .precio.destacado")
    if price_block:
        precio_lista = parse_price_text(price_block.get_text(" ", strip=True))

    plus_block = soup.select_one(".precio-plus .precio b")
    if plus_block:
        precio_plus = parse_price_text(plus_block.get_text(" ", strip=True))

    # Imágenes (principal y galería)
    imagenes = []
    for im in soup.select("#img_producto img[src], #galeria_img img[src]"):
        src = im.get("src")
        if src and src not in imagenes:
            imagenes.append(src)

    return {
        "url": url,
        "nombre": nombre,
        "sku": sku,
        "id_item": id_item,
        "marca": marca,
        "categorias": categorias,
        "precio_lista": precio_lista,
        "precio_plus": precio_plus,
        "descripcion": descripcion,
        "imagenes": " | ".join(imagenes) if imagenes else None,
    }


def grab_category(driver, start_url: str) -> pd.DataFrame:
    all_rows: List[Dict] = []
    visited_detail_urls = set()

    page_url = start_url
    page_idx = 0

    while True:
        page_idx += 1
        if MAX_PAGES is not None and page_idx > MAX_PAGES:
            break

        try:
            driver.get(page_url)
        except (TimeoutException, WebDriverException):
            # último intento de recargar
            try:
                driver.get(page_url)
            except Exception:
                print(f"⚠️ No se pudo cargar: {page_url}")
                break

        # Cookies si aparecen
        try_click_cookies(driver)

        # Esperar algo representativo de listado
        try:
            wait_dom(driver, "div.producto.item")
        except TimeoutException:
            print(f"⚠️ Sin productos visibles en: {page_url}")
            break

        smooth_scroll(driver)
        time.sleep(0.3)

        links = get_list_links_from_page(driver)
        print(f"🔗 P{page_idx} {len(links)} productos - {page_url}")

        if not links:
            break

        # Visitar cada detalle
        for href in links:
            if href in visited_detail_urls:
                continue
            visited_detail_urls.add(href)

            try:
                driver.get(href)
                wait_dom(driver, "h1.titulo_producto.principal")
                time.sleep(0.25)
                row = parse_detail(driver.page_source, href)
                all_rows.append(row)
                print(f"  ✔ {row.get('nombre') or ''} [{row.get('sku') or ''}]")
                time.sleep(SLEEP_BETWEEN_PRODUCTS)
            except TimeoutException:
                print(f"  ⚠️ Timeout detalle: {href}")
            except Exception as e:
                print(f"  ⚠️ Error detalle: {href} -> {e}")

        # Intentar siguiente página
        soup = BeautifulSoup(driver.page_source, "lxml")
        next_url = get_next_page_url(page_url, soup)

        if not next_url or next_url == page_url:
            break

        page_url = next_url
        time.sleep(SLEEP_BETWEEN_PAGES)

    return pd.DataFrame(all_rows)


def main():
    driver = setup_driver()
    try:
        df = grab_category(driver, START)
        if df.empty:
            print("No se extrajeron registros.")
        else:
            # Orden y salida
            cols = [
                "sku", "id_item", "nombre", "marca", "categorias",
                "precio_lista", "precio_plus", "descripcion", "imagenes", "url"
            ]
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
            df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
            print(f"✅ Listo: {OUT_CSV} ({len(df)} filas)")
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
