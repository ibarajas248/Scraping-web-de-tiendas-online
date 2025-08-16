#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Comodín (home) -> Detalle por producto
- Selenium para render e infinite scroll
- BeautifulSoup para parsear HTML
- Exporta a Excel

Instalar:
  pip install selenium bs4 pandas webdriver-manager

Ejecutar:
  python comodin_scraper.py
"""

import re
import time
import math
import pandas as pd
from typing import List, Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


BASE_URL = "https://www.comodinencasa.com.ar/"


def make_driver(headless: bool = True) -> Chrome:
    opts = Options()
    if headless:
        # En algunos sitios headless puede bloquear; si ves pocos resultados,
        # prueba headless=False
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument("--lang=es-AR")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(5)
    return driver


def wait_for_any_product(driver: Chrome, timeout: int = 20):
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.product a.product-header"))
    )


def infinite_scroll_collect_product_links(driver: Chrome, max_idle_rounds: int = 3, pause: float = 1.2) -> List[str]:
    """
    Scrollea hasta el fondo cargando más productos.
    Corta cuando no crece la cantidad de cards por 'max_idle_rounds'.
    Devuelve la lista de URLs únicas de productos (enlaces que terminan en '/p').
    """
    seen = set()

    def current_cards() -> List[str]:
        # card estructura: div.product dentro de col-md-3...
        anchors = driver.find_elements(By.CSS_SELECTOR, "div.product a.product-header")
        links = []
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                # En el home, el a.product-header apunta al detalle (termina en /p)
                if href and href.startswith("http") and href.rstrip("/").endswith("/p"):
                    links.append(href.split("?")[0])
            except Exception:
                continue
        return links

    idle = 0
    last_count = 0

    while True:
        # recuenta
        links_now = current_cards()
        for lk in links_now:
            seen.add(lk)

        # scroll al fondo
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)

        # esperar que agregue algo o confirmar estancamiento
        cards_count = len(current_cards())
        if cards_count <= last_count:
            idle += 1
        else:
            idle = 0
        last_count = cards_count

        # freno si llevo varios ciclos sin crecer
        if idle >= max_idle_rounds:
            break

    return sorted(seen)


_price_clean_re = re.compile(r"[^\d,\.]")


def parse_price(text: str) -> Optional[float]:
    """
    Convierte precio argentino a float.
    Ej: "$ 3.599,00" -> 3599.00
    """
    if not text:
        return None
    t = _price_clean_re.sub("", text)
    # Heurística ar-ES: punto separador de miles, coma decimal
    # 3.599,00 -> 3599.00
    # 459 -> 459
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t and "." not in t:
        t = t.replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None


def soup_select_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    el = soup.select_one(selector)
    if not el:
        return None
    return el.get_text(strip=True)


def extract_product_detail(driver: Chrome, url: str) -> Dict:
    """
    En la página de detalle, extrae:
      - brand (small gris)
      - name (h2)
      - offer price (p.offer-price)
      - regular price (si aparece en el bloque de precio)
      - availability (badge item-available)
      - product code (span.product-code)
      - image url principal (.image-gallery-image)
    """
    # Abrir en pestaña nueva para mantener el home cargado
    driver.execute_script("window.open(arguments[0], '_blank');", url)
    driver.switch_to.window(driver.window_handles[-1])

    # Espera por elementos del detalle
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".shop-detail-right"))
        )
        time.sleep(0.8)  # pequeño respiro para que monte el carrusel de imágenes
    except Exception:
        pass

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    brand = soup_select_text(soup, ".shop-detail-right small")  # suele ser el primero, gris
    name = soup_select_text(soup, ".shop-detail-right .header h2") or soup_select_text(soup, "h2")

    # precio oferta
    offer_raw = soup_select_text(soup, ".shop-detail-right .offer-price")
    price_offer = parse_price(offer_raw) if offer_raw else None

    # precio regular: a veces sólo en tarjeta; si no, buscar spans cercanos
    # alternativa: en el home lo traen dentro .offer-price .regular-price
    regular_raw = None
    # intentar leer un "precio tachado" si está presente en detalle (algunas plantillas no lo muestran)
    reg_el = soup.select_one(".shop-detail-right .regular-price")
    if reg_el:
        regular_raw = reg_el.get_text(strip=True)
    price_regular = parse_price(regular_raw) if regular_raw else None

    availability = None
    avail_badge = soup.select_one(".item-available")  # badge "Disponible"
    if avail_badge:
        availability = avail_badge.get_text(strip=True)

    product_code = None
    pc = soup.select_one(".product-code")
    if pc:
        txt = pc.get_text(" ", strip=True)
        # suele venir como "| Código:- 4638"
        m = re.search(r"C[oó]digo[:\-\s]*([A-Za-z0-9\-_.]+)", txt, re.I)
        if m:
            product_code = m.group(1)

    # imagen principal (carrusel)
    image_url = None
    img = soup.select_one(".image-gallery-image")
    if img and img.has_attr("src"):
        image_url = img["src"]

    data = {
        "url": url,
        "brand": brand,
        "name": name,
        "price_offer": price_offer,
        "price_regular": price_regular,
        "availability": availability,
        "product_code": product_code,
        "image_url": image_url,
    }

    # cerrar pestaña y volver al home
    driver.close()
    driver.switch_to.window(driver.window_handles[0])

    return data


def main():
    driver = make_driver(headless=True)
    out_rows: List[Dict] = []

    try:
        driver.get(BASE_URL)
        wait_for_any_product(driver)

        print(">> Haciendo scroll e indexando productos del home...")
        product_links = infinite_scroll_collect_product_links(driver, max_idle_rounds=3, pause=1.2)
        print(f">> Encontrados {len(product_links)} productos en el grid del home")

        if not product_links:
            # fallback: también hay tiles en carruseles; probar links dentro de cards sin /p
            anchors = driver.find_elements(By.CSS_SELECTOR, "div.product a")
            extra = []
            for a in anchors:
                try:
                    href = (a.get_attribute("href") or "").split("?")[0]
                    if href and href.startswith("http") and href.rstrip("/").endswith("/p"):
                        extra.append(href)
                except Exception:
                    continue
            product_links = sorted(set(extra))
            print(f">> Fallback: {len(product_links)} con selector alternativo")

        # De-duplicar por si el home repite productos
        product_links = sorted(set(product_links))

        # Visitar cada producto
        for i, url in enumerate(product_links, 1):
            try:
                print(f"[{i}/{len(product_links)}] {url}")
                row = extract_product_detail(driver, url)
                out_rows.append(row)
            except Exception as e:
                print(f"  ! Error con {url}: {e}")
                continue

        # Exportar
        if out_rows:
            df = pd.DataFrame(out_rows)
            # Orden de columnas agradable
            cols = [
                "brand", "name",
                "price_offer", "price_regular",
                "availability", "product_code",
                "image_url", "url"
            ]
            df = df.reindex(columns=cols)
            df.to_excel("comodin_items.xlsx", index=False)
            print(">> Exportado comodin_items.xlsx")
        else:
            print(">> No se recolectaron filas.")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
