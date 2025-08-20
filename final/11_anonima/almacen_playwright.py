#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Anónima (categoría -> detalle) → MySQL con Playwright (headless)

- Recolecta detalles (sin EAN).
- Inserta/actualiza:
  * tiendas (codigo='laanonima', nombre='La Anónima Online')
  * productos (ean=NULL; match por (nombre, marca) o por nombre)
  * producto_tienda (sku_tienda=sku, record_id_tienda=id_item, url_tienda=url, nombre_tienda=nombre)
  * historico_precios (precio_lista, precio_oferta=precio_plus si existe, tipo_oferta='PLUS')

Índices/UNIQUE sugeridos:
  tiendas(codigo) UNIQUE
  producto_tienda(tienda_id, sku_tienda) UNIQUE
  -- opcional: producto_tienda(tienda_id, record_id_tienda) UNIQUE
  -- opcional: producto_tienda(tienda_id, url_tienda) UNIQUE
  historico_precios(tienda_id, producto_tienda_id, capturado_en) UNIQUE
"""

import re
import os
import html
import time
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urljoin
from datetime import datetime

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from mysql.connector import Error as MySQLError

# --- importar conexión
import sys
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ================= Config scraping =================
BASE = "https://supermercado.laanonimaonline.com"
START = f"{BASE}/almacen/n1_1/pag/1/"

PAGE_LOAD_TIMEOUT = 25000  # ms
WAIT_SELECTOR_TIMEOUT = 15000  # ms
SCROLL_PAUSES = [300, 600, 900]
SLEEP_BETWEEN_PAGES = 1.2
SLEEP_BETWEEN_PRODUCTS = 0.8
MAX_PAGES: Optional[int] = None  # None = todas

TIENDA_CODIGO = "laanonima"
TIENDA_NOMBRE = "La Anónima Online"

# ================= Utilidades scraping =================
def parse_price_text(txt: str) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip().replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None

def text_or_none(node) -> Optional[str]:
    if not node:
        return None
    return re.sub(r"\s+", " ", node.get_text(strip=True)) or None

def wait_dom(page, css: str, timeout_ms: int = WAIT_SELECTOR_TIMEOUT):
    page.wait_for_selector(css, timeout=timeout_ms, state="attached")

def try_click_cookies(page):
    sels = [
        "button#onetrust-accept-btn-handler",
        "button[aria-label='Aceptar']",
        "button.cookie-accept",
        "div.cookie a.btn, div.cookie button",
    ]
    for sel in sels:
        try:
            el = page.locator(sel)
            if el.count() > 0 and el.first.is_visible():
                el.first.click(timeout=2000)
                time.sleep(0.2)
                return
        except Exception:
            pass

def smooth_scroll(page):
    # Playwright ejecuta JS debajo; hacemos scrolls para forzar lazy-load si existiera
    for y in SCROLL_PAUSES:
        page.evaluate(f"window.scrollTo(0,{y});")
        time.sleep(0.3)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(0.4)

def get_list_links_from_html(html_source: str) -> List[str]:
    soup = BeautifulSoup(html_source, "lxml")
    links = []
    for card in soup.select("div.producto.item a[href*='/almacen/'][href*='/art_']"):
        href = (card.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(BASE, href)
        if "/art_" in full and full not in links:
            links.append(full)
    return links

def get_next_page_url(current_url: str, html_source: str) -> Optional[str]:
    soup = BeautifulSoup(html_source, "lxml")
    m = re.search(r"/pag/(\d+)/", current_url)
    if not m:
        return None
    cur = int(m.group(1))
    guess = re.sub(r"/pag/\d+/", f"/pag/{cur + 1}/", current_url)
    a = soup.select_one(f"a[href*='/pag/{cur + 1}/']")
    return urljoin(BASE, a["href"]) if a and a.has_attr("href") else guess

def parse_detail(html_source: str, url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_source, "lxml")

    nombre = text_or_none(soup.select_one("h1.titulo_producto.principal"))

    cod_txt = text_or_none(soup.select_one("div.codigo"))  # "Cod. 3115185"
    sku = None
    if cod_txt:
        m = re.search(r"(\d+)", cod_txt)
        if m:
            sku = m.group(1)

    # ocultos a veces
    sku_hidden = soup.select_one("input[id^='sku_item_imetrics_'][value]")
    if sku_hidden and not sku:
        sku = sku_hidden.get("value")

    id_item = None
    id_item_hidden = soup.select_one("input#id_item[value], input[id^='id_item_'][value]")
    if id_item_hidden:
        id_item = id_item_hidden.get("value")

    marca_hidden = soup.select_one("input[id^='brand_item_imetrics_'][value]")
    marca = marca_hidden.get("value") if marca_hidden else None

    cat_hidden = soup.select_one("input[id^='categorias_item_imetrics_'][value]")
    categorias = None
    if cat_hidden:
        categorias = html.unescape(cat_hidden.get("value") or "").strip()

    descripcion = text_or_none(soup.select_one("div.descripcion div.texto"))

    # precios
    precio_lista = None
    caja_lista = soup.select_one("div.precio.anterior")
    if caja_lista:
        precio_lista = parse_price_text(caja_lista.get_text(" ", strip=True))
    else:
        alt = soup.select_one(".precio_complemento .precio.destacado, .precio.destacado")
        if alt:
            precio_lista = parse_price_text(alt.get_text(" ", strip=True))

    precio_plus = None
    plus_node = soup.select_one(".precio-plus .precio b, .precio-plus .precio")
    if plus_node:
        precio_plus = parse_price_text(plus_node.get_text(" ", strip=True))

    # imágenes
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

def grab_category(page, start_url: str) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    visited = set()
    page_url = start_url
    page_idx = 0

    while True:
        page_idx += 1
        if MAX_PAGES is not None and page_idx > MAX_PAGES:
            break

        # navegar
        page.goto(page_url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
        try_click_cookies(page)

        try:
            wait_dom(page, "div.producto.item")
        except PWTimeout:
            print(f"⚠️ Sin productos visibles en: {page_url}")
            break

        smooth_scroll(page)
        html_source = page.content()
        links = get_list_links_from_html(html_source)
        print(f"🔗 P{page_idx} {len(links)} productos - {page_url}")
        if not links:
            break

        for href in links:
            if href in visited:
                continue
            visited.add(href)
            try:
                page.goto(href, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
                wait_dom(page, "h1.titulo_producto.principal")
                time.sleep(0.25)
                row = parse_detail(page.content(), href)
                all_rows.append(row)
                print(f"  ✔ {row.get('nombre') or ''} [{row.get('sku') or ''}]")
                time.sleep(SLEEP_BETWEEN_PRODUCTS)
            except PWTimeout:
                print(f"  ⚠️ Timeout detalle: {href}")
            except Exception as e:
                print(f"  ⚠️ Error detalle: {href} -> {e}")

        html_source = page.content()
        next_url = get_next_page_url(page_url, html_source)
        if not next_url or next_url == page_url:
            break
        page_url = next_url
        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_rows

# ================= Helpers MySQL =================
def clean_txt(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def price_to_varchar(x: Any) -> Optional[str]:
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

def split_categoria(categorias: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not categorias:
        return None, None
    parts = [p.strip() for p in re.split(r">|›", categorias) if p.strip()]
    cat = parts[0] if len(parts) > 0 else None
    sub = parts[1] if len(parts) > 1 else None
    return cat, sub

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = None  # explícitamente None
    nombre = clean_txt(r.get("nombre"))
    marca = clean_txt(r.get("marca"))
    cat, sub = split_categoria(clean_txt(r.get("categorias")))

    if nombre and marca:
        cur.execute("SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1", (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (cat or "", sub or "", pid))
            return pid

    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  marca = COALESCE(NULLIF(%s,''), marca),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (marca or "", cat or "", sub or "", pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULLIF(%s,''), NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (nombre or "", marca or "", cat or "", sub or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    sku = clean_txt(r.get("sku"))
    record_id = clean_txt(r.get("id_item"))
    url = clean_txt(r.get("url"))
    nombre_tienda = clean_txt(r.get("nombre"))

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    if record_id:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: datetime):
    precio_lista = price_to_varchar(r.get("precio_lista"))
    precio_plus  = price_to_varchar(r.get("precio_plus"))
    tipo_oferta  = "PLUS" if precio_plus is not None else None

    cur.execute("""
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta   = VALUES(tipo_oferta),
          promo_tipo    = VALUES(promo_tipo),
          promo_texto_regular   = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios     = VALUES(promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista, precio_plus, tipo_oferta,
        None, None, None, None
    ))

# ================= Orquestación =================
def main():
    rows: List[Dict[str, Any]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-sync",
                "--metrics-recording-only",
                "--disable-default-apps",
                "--mute-audio",
                "--lang=es-AR",
                "--window-size=1280,1800",
            ],
        )
        context = browser.new_context(
            locale="es-AR",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
            viewport={"width": 1280, "height": 1800},
        )
        page = context.new_page()
        page.set_default_timeout(PAGE_LOAD_TIMEOUT)

        print("[INFO] Scrapeando La Anónima…")
        rows = grab_category(page, START)

        context.close()
        browser.close()

    if not rows:
        print("[INFO] No se extrajeron registros.")
        return

    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for r in rows:
            producto_id = find_or_create_producto(cur, r)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, r)
            insert_historico(cur, tienda_id, pt_id, r, capturado_en)
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

if __name__ == "__main__":
    main()
