#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Comodín (categoría -> detalle) con inserción en MySQL

Mejoras:
- Normalización estricta de URL (sin query, sin slash final, host minúscula).
- Deduplicación en memoria por SKU (product_code) y, si no existe, por URL.
- Scroll infinito robusto (compara altura del documento + rondas inactivas).
- Extracción más tolerante a cambios de DOM.
- Limpieza de precios consistente (AR).
- Inserciones idempotentes con claves naturales y ON DUPLICATE.
- Posibilidad de exportar XLSX para control.
- Extracción de EAN desde scripts VTEX, con match preferente por EAN.

REQUISITOS SQL (recomendado):
  ALTER TABLE tiendas           ADD UNIQUE KEY ux_tiendas_codigo (codigo);
  ALTER TABLE producto_tienda   ADD UNIQUE KEY ux_pt_tienda_sku (tienda_id, sku_tienda);
  ALTER TABLE producto_tienda   ADD UNIQUE KEY ux_pt_tienda_url (tienda_id, url_tienda);
  ALTER TABLE historico_precios ADD UNIQUE KEY ux_hist (tienda_id, producto_tienda_id, capturado_en);

Si usas idempotencia diaria, crea una columna capturado_en_dia DATE y cambia la UNIQUE.
"""

import os
import re
import sys
import time
import json
from datetime import datetime
from typing import List, Dict, Optional, Any
from urllib.parse import urlsplit, urlunsplit

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from mysql.connector import Error as MySQLError

# ====== Paths / Imports del proyecto ======
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # type: ignore

# ===================== Config =====================
BASE_URL = "https://www.comodinencasa.com.ar/lacteos/quesos-y-ricotas"

TIENDA_CODIGO = "comodin"
TIENDA_NOMBRE = "Comodin"

HEADLESS = True
SCROLL_IDLE_ROUNDS = 3
SCROLL_PAUSE = 1.2

OUT_XLSX = "comodin_items.xlsx"
SAVE_EXCEL = True

PAGELOAD_TIMEOUT = 60
IMPLICIT_WAIT = 5

# ===================== Utilidades =====================
def normalize_url(u: str) -> str:
    """Normaliza URL: sin query/fragment, host minúscula, sin slash final."""
    if not u:
        return u
    p = urlsplit(u)
    path = (p.path or "").rstrip("/")
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), path, "", ""))


_price_clean_re = re.compile(r"[^\d,\.]")


def parse_price(text: Optional[str]) -> Optional[float]:
    """Convierte precio AR a float. Ej: '$ 3.599,00' -> 3599.00"""
    if not text:
        return None
    t = _price_clean_re.sub("", text)
    # casos: "3.599,00" o "3599,00" o "3599.00"
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
    return el.get_text(strip=True) if el else None


def clean_txt(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def parse_price_to_varchar(x: Any) -> Optional[str]:
    """Guarda precios como VARCHAR (o None)."""
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


def extract_ean_from_vtex(soup: BeautifulSoup) -> Optional[str]:
    """
    Busca en los <script> un fragmento con "productEans".
    Intenta extraer el primer EAN sin depender 100% del JSON bien formado.
    """
    scripts = soup.find_all("script")
    for script in scripts:
        txt = script.string or script.get_text() or ""
        if "productEans" not in txt:
            continue

        # 1) Intento simple por regex directo al array:
        m = re.search(r'"productEans"\s*:\s*\[\s*"(\d+)"', txt)
        if m:
            return m.group(1)

        # 2) Intento más genérico: buscar el objeto de addData y parsear JSON
        m2 = re.search(r'addData\(\s*(\{.*?\})\s*\)', txt, re.S)
        if m2:
            try:
                data = json.loads(m2.group(1))
                eans = data.get("productEans")
                if isinstance(eans, list) and eans:
                    return str(eans[0])
            except Exception:
                pass

    return None


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
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    driver.implicitly_wait(IMPLICIT_WAIT)
    return driver


def wait_for_any_product(driver: Chrome, timeout: int = 25):
    # selector principal de cards con link al detalle
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.product a.product-header"))
    )


def infinite_scroll_collect_product_links(
    driver: Chrome,
    max_idle_rounds: int = 3,
    pause: float = 1.2
) -> List[str]:
    """
    Scrollea hasta el fondo cargando más productos.
    Devuelve URLs únicas de detalle que terminan en '/p'.
    """
    seen: set[str] = set()

    def collect_from_dom() -> List[str]:
        anchors = driver.find_elements(By.CSS_SELECTOR, "div.product a.product-header")
        links: List[str] = []
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                if not href.startswith("http"):
                    continue
                u = normalize_url(href.split("?")[0])
                if u.endswith("/p"):
                    links.append(u)
            except Exception:
                continue
        return links

    idle_rounds = 0
    last_height = 0
    last_count = 0

    while True:
        # añade lo visible
        for lk in collect_from_dom():
            seen.add(lk)

        # scroll al pie
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)

        # compara altura y cantidad de cards
        new_height = driver.execute_script("return document.body.scrollHeight") or 0
        cards_count = len(collect_from_dom())

        if (new_height == last_height) and (cards_count <= last_count):
            idle_rounds += 1
        else:
            idle_rounds = 0

        last_height = new_height
        last_count = cards_count

        if idle_rounds >= max_idle_rounds:
            break

    return sorted(seen)


# ===================== Extracción de detalle =====================
def extract_product_detail(driver: Chrome, url: str) -> Dict[str, Any]:
    """
    Extrae del detalle:
      - ean (si aparece en scripts VTEX)
      - brand (small gris)
      - name (h2 dentro de .header, con fallback)
      - offer price (.offer-price)
      - regular price (.regular-price)
      - availability (.item-available)
      - product code (.product-code -> 'Código: 4638')
      - image url principal (.image-gallery-image)
    """
    target = normalize_url(url)
    driver.execute_script("window.open(arguments[0], '_blank');", target)
    driver.switch_to.window(driver.window_handles[-1])

    try:
        WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".shop-detail-right"))
        )
        time.sleep(0.8)
    except Exception:
        pass

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # EAN desde scripts VTEX (si existe)
    ean = extract_ean_from_vtex(soup)

    # Marca & Nombre
    brand = soup_select_text(soup, ".shop-detail-right small")
    # ojo: aquí uso .shop-detail-right .header h2 (con espacio) para evitar el bug de selector
    name = soup_select_text(soup, ".shop-detail-right .header h2") or soup_select_text(soup, "h2")

    # ===== Precios =====
    # En la página:
    #  - Sin oferta:
    #       <p class="offer-price mb-1">$ 2.615,59</p>
    #    → solo precio lista (NO oferta)
    #  - Con oferta:
    #       <span class="regular-price">$ 2.035,09</span>  (lista)
    #       <p class="offer-price mb-1">$ 1.599,00</p>     (oferta)

    # 1) Tomamos el texto de offer-price y regular-price
    offer_raw = soup_select_text(soup, ".shop-detail-right .offer-price") or soup_select_text(soup, ".offer-price")
    price_offer = parse_price(offer_raw) if offer_raw else None

    regular_raw = None
    reg_el = soup.select_one(".shop-detail-right .regular-price") or soup.select_one(".regular-price")
    if reg_el:
        regular_raw = reg_el.get_text(strip=True)
    price_regular = parse_price(regular_raw) if regular_raw else None

    # 2) Normalización de casos:
    #    - Si NO hay regular-price pero SÍ hay offer-price,
    #      interpretamos offer-price como precio_lista (sin oferta).
    if price_regular is None and price_offer is not None:
        price_regular = price_offer
        price_offer = None

    # Disponibilidad
    availability = None
    avail_badge = soup.select_one(".item-available")
    if avail_badge:
        availability = avail_badge.get_text(strip=True)

    # SKU / Código producto
    product_code = None
    pc = soup.select_one(".product-code")
    if pc:
        txt = pc.get_text(" ", strip=True)
        m = re.search(r"C[oó]digo[:\-\s]*([A-Za-z0-9\-_.]+)", txt, re.I)
        if m:
            product_code = m.group(1)

    # Imagen principal
    image_url = None
    img = soup.select_one(".image-gallery-image, .image-gallery img")
    if img and img.has_attr("src"):
        image_url = img["src"]

    data = {
        "url": target,
        "ean": clean_txt(ean),
        "brand": clean_txt(brand),
        "name": clean_txt(name),
        "price_offer": price_offer,
        "price_regular": price_regular,
        "availability": clean_txt(availability),
        "product_code": clean_txt(product_code),  # sku_tienda
        "image_url": clean_txt(image_url),
    }

    driver.close()
    driver.switch_to.window(driver.window_handles[0])
    return data


# ===================== Deduplicación =====================
def dedup_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prioriza dedupe por SKU (product_code).
    Si no hay SKU, dedupe por URL normalizada.
    """
    by_sku: dict[str, Dict[str, Any]] = {}
    by_url: dict[str, Dict[str, Any]] = {}

    for r in rows:
        sku = (r.get("product_code") or "").strip() if r.get("product_code") else ""
        url = normalize_url(r.get("url") or "")
        if sku:
            # última gana (puedes cambiar a 'setdefault' si prefieres la primera)
            by_sku[sku] = r
        else:
            by_url[url] = r

    urls_from_sku = {normalize_url(v.get("url") or "") for v in by_sku.values()}
    final = list(by_sku.values()) + [r for u, r in by_url.items() if normalize_url(u) not in urls_from_sku]
    return final


# ===================== MySQL helpers =====================
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
    Match preferente por EAN.
    Si no hay EAN → match suave: (nombre, marca) si ambos existen; fallback: solo nombre.
    """
    ean = clean_txt(row.get("ean"))
    nombre = clean_txt(row.get("name"))
    marca = clean_txt(row.get("brand"))

    # 1) Intentar por EAN si existe
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        r = cur.fetchone()
        if r:
            pid = r[0]
            # Actualiza campos básicos si vienen no vacíos
            cur.execute("""
                UPDATE productos
                SET nombre = COALESCE(NULLIF(%s,''), nombre),
                    marca  = COALESCE(NULLIF(%s,''), marca)
                WHERE id = %s
            """, (nombre or "", marca or "", pid))
            return pid

    # 2) Match por (nombre, marca)
    if nombre and marca:
        cur.execute(
            "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
            (nombre, marca)
        )
        r = cur.fetchone()
        if r:
            pid = r[0]
            # Si tenemos EAN nuevo, lo actualizamos
            if ean:
                cur.execute(
                    "UPDATE productos SET ean = COALESCE(NULLIF(%s,''), ean) WHERE id=%s",
                    (ean, pid)
                )
            return pid

    # 3) Match solo por nombre
    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        r = cur.fetchone()
        if r:
            pid = r[0]
            cur.execute("""
                UPDATE productos
                SET marca = COALESCE(NULLIF(%s,''), marca),
                    ean   = COALESCE(NULLIF(%s,''), ean)
                WHERE id=%s
            """, (marca or "", ean or "", pid))
            return pid

    # 4) No existe → insert nuevo
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULL, NULL, NULL)
    """, (ean or "", nombre or "", marca or ""))
    return cur.lastrowid


def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, row: Dict[str, Any]) -> int:
    """
    Clave preferida: (tienda_id, sku_tienda=product_code) UNIQUE.
    Respaldo: (tienda_id, url_tienda) UNIQUE.
    """
    sku = clean_txt(row.get("product_code"))
    url = normalize_url(clean_txt(row.get("url")) or "")
    nombre_tienda = clean_txt(row.get("name"))

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda
                (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, NULL, %s, %s)
            ON DUPLICATE KEY UPDATE
                id = LAST_INSERT_ID(id),
                producto_id = VALUES(producto_id),
                url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
                nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, url, nombre_tienda))
        return cur.lastrowid

    # Fallback sin SKU
    cur.execute("""
        INSERT INTO producto_tienda
            (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
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
    out_rows: List[Dict[str, Any]] = []

    try:
        driver.get(BASE_URL)
        wait_for_any_product(driver)

        print(">> Scroll e indexación de productos de la categoría...")
        product_links = infinite_scroll_collect_product_links(
            driver,
            max_idle_rounds=SCROLL_IDLE_ROUNDS,
            pause=SCROLL_PAUSE
        )
        product_links = sorted({normalize_url(u) for u in product_links})
        print(f">> Encontrados {len(product_links)} productos en el grid")

        if not product_links:
            anchors = driver.find_elements(By.CSS_SELECTOR, "div.product a")
            extra: List[str] = []
            for a in anchors:
                try:
                    href = (a.get_attribute("href") or "")
                    if not href.startswith("http"):
                        continue
                    u = normalize_url(href.split("?")[0])
                    if u.endswith("/p"):
                        extra.append(u)
                except Exception:
                    continue
            product_links = sorted({normalize_url(u) for u in extra})
            print(f">> Fallback: {len(product_links)} con selector alternativo")

        for i, url in enumerate(product_links, 1):
            try:
                print(f"[{i}/{len(product_links)}] {url}")
                row = extract_product_detail(driver, url)

                # --- PRINT con EAN, SKU y precios ---
                print(
                    f"  → {row.get('name')} | SKU={row.get('product_code')} | "
                    f"EAN={row.get('ean')} | Oferta={row.get('price_offer')} | Lista={row.get('price_regular')}"
                )

                out_rows.append(row)
            except Exception as e:
                print(f"  ! Error con {url}: {e}")
                continue

        # Deduplicación en memoria
        out_rows = dedup_rows(out_rows)

        if SAVE_EXCEL and out_rows:
            df = pd.DataFrame(out_rows)
            cols = [
                "ean",
                "brand",
                "name",
                "price_offer",
                "price_regular",
                "availability",
                "product_code",
                "image_url",
                "url",
            ]
            df = df.reindex(columns=cols)
            #df.to_excel(OUT_XLSX, index=False)
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
            if conn:
                conn.rollback()
            print(f"❌ Error MySQL: {e}")
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
