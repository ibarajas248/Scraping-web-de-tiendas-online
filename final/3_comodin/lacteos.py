#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Comodín (home -> detalle) con inserción en MySQL
- Selenium para scroll infinito en el home
- BeautifulSoup para parsear el detalle de cada producto
- Guarda en MySQL:
  - tiendas (codigo='comodin')
  - productos (EAN NULL)
  - producto_tienda (sku_tienda = product_code)
  - historico_precios (precios como VARCHAR)

Esquema esperado (sugerencia de UNIQUE):
  tiendas(codigo) UNIQUE
  producto_tienda(tienda_id, sku_tienda) UNIQUE
  -- opcional respaldo si no hay sku:
  -- producto_tienda(tienda_id, url_tienda) UNIQUE
  historico_precios(tienda_id, producto_tienda_id, capturado_en) UNIQUE  [solo si quieres idempotencia por timestamp]
"""

import re
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from mysql.connector import Error as MySQLError
# añade la carpeta raíz (2 niveles más arriba) al sys.path
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)


from base_datos import get_conn  # <- tu conexión MySQL

# ===================== Config =====================
BASE_URL = "https://www.comodinencasa.com.ar/lacteos"


TIENDA_CODIGO = "comodin"
TIENDA_NOMBRE = "Comodín En Casa"

HEADLESS = True
SCROLL_IDLE_ROUNDS = 3
SCROLL_PAUSE = 1.2

OUT_XLSX = "comodin_items.xlsx"   # opcional, deja la export
SAVE_EXCEL = True

# ===================== Selenium =====================
def make_driver(headless: bool = True) -> Chrome:
    opts = Options()
    if headless:
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
    Scrollea hasta el fondo cargando más productos y devuelve URLs únicas de detalle (terminan en /p).
    """
    seen = set()

    def current_cards() -> List[str]:
        anchors = driver.find_elements(By.CSS_SELECTOR, "div.product a.product-header")
        links = []
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                if href and href.startswith("http") and href.rstrip("/").endswith("/p"):
                    links.append(href.split("?")[0])
            except Exception:
                continue
        return links

    idle = 0
    last_count = 0

    while True:
        for lk in current_cards():
            seen.add(lk)

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)

        cards_count = len(current_cards())
        if cards_count <= last_count:
            idle += 1
        else:
            idle = 0
        last_count = cards_count

        if idle >= max_idle_rounds:
            break

    return sorted(seen)

_price_clean_re = re.compile(r"[^\d,\.]")

def parse_price(text: str) -> Optional[float]:
    """
    Convierte precio argentino a float. Ej: "$ 3.599,00" -> 3599.00
    """
    if not text:
        return None
    t = _price_clean_re.sub("", text)
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
    Extrae del detalle:
      - brand (small gris)
      - name (h2)
      - offer price (p.offer-price)
      - regular price (si aparece)
      - availability (badge item-available)
      - product code (span.product-code -> "Código: 4638")
      - image url principal (.image-gallery-image)
    """
    driver.execute_script("window.open(arguments[0], '_blank');", url)
    driver.switch_to.window(driver.window_handles[-1])

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".shop-detail-right"))
        )
        time.sleep(0.8)
    except Exception:
        pass

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    brand = soup_select_text(soup, ".shop-detail-right small")
    name = soup_select_text(soup, ".shop-detail-right .header h2") or soup_select_text(soup, "h2")

    offer_raw = soup_select_text(soup, ".shop-detail-right .offer-price")
    price_offer = parse_price(offer_raw) if offer_raw else None

    regular_raw = None
    reg_el = soup.select_one(".shop-detail-right .regular-price")
    if reg_el:
        regular_raw = reg_el.get_text(strip=True)
    price_regular = parse_price(regular_raw) if regular_raw else None

    availability = None
    avail_badge = soup.select_one(".item-available")
    if avail_badge:
        availability = avail_badge.get_text(strip=True)

    product_code = None
    pc = soup.select_one(".product-code")
    if pc:
        txt = pc.get_text(" ", strip=True)
        m = re.search(r"C[oó]digo[:\-\s]*([A-Za-z0-9\-_.]+)", txt, re.I)
        if m:
            product_code = m.group(1)

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
        "product_code": product_code,   # <- sku_tienda
        "image_url": image_url,
    }

    driver.close()
    driver.switch_to.window(driver.window_handles[0])
    return data

# ===================== MySQL helpers =====================
def clean_txt(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def parse_price_to_varchar(x: Any) -> Optional[str]:
    """Guarda precios como VARCHAR (o None) según tu preferencia."""
    if x is None:
        return None
    try:
        v = float(x)
        if np.isnan(v):
            return None
        return f"{round(v, 2)}"
    except Exception:
        s = str(x).strip()
        return s if s else None

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, row: Dict[str, Any]) -> int:
    """
    En Comodín no hay EAN → ean=NULL.
    Match suave: (nombre, marca) si ambos existen; como fallback, solo nombre.
    """
    ean = None
    nombre = clean_txt(row.get("name"))
    marca  = clean_txt(row.get("brand"))

    if nombre and marca:
        cur.execute("SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1", (nombre, marca))
        r = cur.fetchone()
        if r:
            return r[0]

    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        r = cur.fetchone()
        if r:
            # si existe pero sin marca y ahora viene marca, actualiza suave
            pid = r[0]
            if marca:
                cur.execute("UPDATE productos SET marca=COALESCE(NULLIF(%s,''), marca) WHERE id=%s", (marca, pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULLIF(%s,''), NULL, NULL, NULL)
    """, (nombre or "", marca or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, row: Dict[str, Any]) -> int:
    """
    Clave natural preferida: (tienda_id, sku_tienda=product_code).
    Respaldo si no hay sku: (tienda_id, url_tienda) UNIQUE (si lo creas).
    """
    sku = clean_txt(row.get("product_code"))
    url = clean_txt(row.get("url"))
    nombre_tienda = clean_txt(row.get("name"))

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, NULL, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, url, nombre_tienda))
        return cur.lastrowid

    # Fallback sin sku: usa URL como unique si tienes índice (tienda_id, url_tienda)
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULL, NULL, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, row: Dict[str, Any], capturado_en: datetime):
    precio_lista = parse_price_to_varchar(row.get("price_regular"))
    precio_oferta = parse_price_to_varchar(row.get("price_offer"))
    # opcional: guardar disponibilidad en comentarios
    promo_comentarios = clean_txt(row.get("availability"))

    cur.execute("""
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = VALUES(promo_tipo),
          promo_texto_regular = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios = VALUES(promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista, precio_oferta, None,
        None, None, None, promo_comentarios
    ))

# ===================== Main =====================
def main():
    driver = make_driver(headless=HEADLESS)
    out_rows: List[Dict] = []

    try:
        driver.get(BASE_URL)
        wait_for_any_product(driver)

        print(">> Scroll e indexación de productos del home...")
        product_links = infinite_scroll_collect_product_links(driver, max_idle_rounds=SCROLL_IDLE_ROUNDS, pause=SCROLL_PAUSE)
        product_links = sorted(set(product_links))
        print(f">> Encontrados {len(product_links)} productos en el grid del home")

        if not product_links:
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

        for i, url in enumerate(product_links, 1):
            try:
                print(f"[{i}/{len(product_links)}] {url}")
                row = extract_product_detail(driver, url)
                out_rows.append(row)
            except Exception as e:
                print(f"  ! Error con {url}: {e}")
                continue

        if SAVE_EXCEL and out_rows:
            df = pd.DataFrame(out_rows)
            cols = ["brand","name","price_offer","price_regular","availability","product_code","image_url","url"]
            df = df.reindex(columns=cols)
            df.to_excel(OUT_XLSX, index=False)
            print(f">> Exportado {OUT_XLSX}")

        # ===== MySQL =====
        if not out_rows:
            print(">> No se recolectaron filas. Fin.")
            return

        capturado_en = datetime.now()
        conn = None
        try:
            conn = get_conn()
            conn.autocommit = False
            cur = conn.cursor()

            tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

            insertados = 0
            for row in out_rows:
                producto_id = find_or_create_producto(cur, row)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, row)
                insert_historico(cur, tienda_id, pt_id, row, capturado_en)
                insertados += 1

            conn.commit()
            print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

        except MySQLError as e:
            if conn: conn.rollback()
            print(f"❌ Error MySQL: {e}")
        finally:
            try:
                if conn: conn.close()
            except Exception:
                pass

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
