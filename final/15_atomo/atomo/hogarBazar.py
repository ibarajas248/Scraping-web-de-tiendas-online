#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Átomo (Prestashop) – Listing + detalle → MySQL

- Reutiliza tus funciones: crawl_listing_and_products() que devuelven filas
  con claves:
    EAN, Código Interno, Nombre Producto, Categoría, Subcategoría,
    Marca, Fabricante, Precio de Lista, Precio de Oferta, Tipo de Oferta, URL
- Inserta/actualiza:
  * tiendas (codigo='atomo', nombre='Átomo Conviene')
  * productos (preferencia por EAN; match suave por (nombre, marca))
  * producto_tienda (sku_tienda='Código Interno', url_tienda=URL)
  * historico_precios (precios como VARCHAR)

UNIQUE/índices sugeridos:
  tiendas(codigo)
  productos(ean)                 -- permite NULL
  producto_tienda(tienda_id, sku_tienda)
  -- opcional: producto_tienda(tienda_id, url_tienda) para fallback
  -- opcional: historico_precios(tienda_id, producto_tienda_id, capturado_en)
"""

import re, time, json, random, datetime as dt
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
from datetime import datetime

import numpy as np
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

# ====== (copiamos tus helpers/params de scraping tal cual) ======
BASE = "https://atomoconviene.com/atomo-ecommerce/"
LISTING_PATH = "315-hogar-bazar?page={page}"

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

TIMEOUT = 25
RETRIES = 3
SLEEP_BETWEEN = (0.35, 0.65)
SLEEP_BETWEEN_PAGES = (0.7, 1.2)

TIENDA_CODIGO = "atomo"
TIENDA_NOMBRE = "Atomo"

def jitter(a, b): time.sleep(random.uniform(a, b))

def get_soup(url: str, session: requests.Session, referer: Optional[str] = None,
             treat_400_as_none: bool = False) -> Optional[BS]:
    headers = dict(HEADERS_BASE)
    if referer: headers["Referer"] = referer
    last_err = None
    for i in range(RETRIES):
        try:
            r = session.get(url, headers=headers, timeout=TIMEOUT)
            if r.status_code == 404: return None
            if r.status_code == 400 and treat_400_as_none: return None
            r.raise_for_status()
            return BS(r.text, "html.parser")
        except requests.HTTPError as e:
            last_err = e; jitter(0.6 + i*0.4, 0.9 + i*0.5)
        except Exception as e:
            last_err = e; jitter(0.6 + i*0.4, 0.9 + i*0.5)
    if treat_400_as_none and isinstance(last_err, requests.HTTPError) and last_err.response is not None and last_err.response.status_code == 400:
        return None
    raise last_err

def normalize_price(text: Optional[str]) -> Optional[float]:
    if not text: return None
    t = re.sub(r"[^\d,\.]", "", text).replace(".", "").replace(",", ".")
    try: return float(t)
    except Exception: return None

def extract_ean_from_url(url: str) -> Optional[str]:
    m = re.search(r"-([0-9]{8,14})\.html?$", url)
    return m.group(1) if m else None

def find_last_page(soup: BS) -> int:
    last = 1
    for sel in ["ul.pagination a", ".page-list a", "nav.pagination a"]:
        links = soup.select(sel)
        if links:
            for a in links:
                txt = a.get_text(strip=True)
                if txt.isdigit(): last = max(last, int(txt))
    return last

def find_product_cards(soup: BS) -> List[BS]:
    return soup.select("article.product-miniature.js-product-miniature")

def parse_listing_card(article: BS) -> Optional[str]:
    a = article.select_one("h2.product-title a, a.thumbnail.product-thumbnail")
    return a.get("href") if a else None

def parse_product_detail(url: str, soup: BS) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "URL": url, "EAN": None, "Código Interno": None, "Nombre Producto": None,
        "Categoría": None, "Subcategoría": None, "Marca": None, "Fabricante": "",
        "Precio de Lista": None, "Precio de Oferta": None, "Tipo de Oferta": "",
    }
    h1 = soup.select_one("h1.h1, h1[itemprop='name']")
    if h1: out["Nombre Producto"] = h1.get_text(strip=True)

    brand = soup.select_one("div.product-manufacturer span a, .product-manufacturer a")
    if brand: out["Marca"] = brand.get_text(strip=True)

    ref = soup.select_one("div.product-reference span")
    if ref: out["Código Interno"] = ref.get_text(strip=True)

    price_span = soup.select_one("div.product__product-price .current-price .price, span.current-price-display.price")
    visible_price = normalize_price(price_span.get_text()) if price_span else None

    data_product_json = None
    data_node = soup.select_one("div#product-details[data-product], .tab-pane#product-details[data-product]")
    if data_node:
        raw = data_node.get("data-product")
        if raw:
            try: data_product_json = json.loads(raw)
            except Exception: data_product_json = None

    if data_product_json:
        pa = data_product_json.get("price_amount")
        pwr = data_product_json.get("price_without_reduction")
        try: pa = float(pa) if pa is not None else None
        except: pa = normalize_price(str(pa)) if pa else None
        try: pwr = float(pwr) if pwr is not None else None
        except: pwr = normalize_price(str(pwr)) if pwr else None
        out["Precio de Oferta"] = pa if pa is not None else visible_price
        out["Precio de Lista"]  = pwr if pwr is not None else out["Precio de Oferta"]
        if out["Precio de Lista"] and out["Precio de Oferta"] and out["Precio de Oferta"] < out["Precio de Lista"]:
            out["Tipo de Oferta"] = "promo"
        out["Categoría"] = data_product_json.get("category_name"); out["Subcategoría"] = ""
    else:
        out["Precio de Oferta"] = visible_price; out["Precio de Lista"]  = visible_price

    ean = extract_ean_from_url(url)
    if ean: out["EAN"] = ean
    if not out["EAN"] and out.get("Nombre Producto"):
        m = re.search(r"(^|[^0-9])([0-9]{8,14})([^0-9]|$)", out["Nombre Producto"])
        if m: out["EAN"] = m.group(2)

    return out

def crawl_listing_and_products() -> List[Dict[str, Any]]:
    s = requests.Session(); s.headers.update(HEADERS_BASE)
    try: s.get(BASE, headers=HEADERS_BASE, timeout=15); jitter(0.3, 0.6)
    except Exception: pass

    all_rows: List[Dict[str, Any]] = []; seen_urls = set()

    first_url = urljoin(BASE, LISTING_PATH.format(page=1))
    soup1 = get_soup(first_url, s, referer=BASE, treat_400_as_none=True)
    if soup1 is None:
        print("No se pudo cargar la página 1 del listado."); return all_rows
    last_page = find_last_page(soup1)
    print(f"Última página detectada: {last_page}")

    pages_to_visit = [1] + list(range(2, last_page + 1))
    for page in pages_to_visit:
        list_url = urljoin(BASE, LISTING_PATH.format(page=page))
        print(f"Página {page}: {list_url}")
        soup = get_soup(list_url, s, referer=first_url, treat_400_as_none=True)
        if soup is None:
            print("  (400/404 o sin contenido) Fin del paginado."); break

        cards = find_product_cards(soup)
        if not cards:
            print("  (sin cards) Fin."); break

        product_urls: List[str] = []
        for art in cards:
            href = parse_listing_card(art)
            if not href: continue
            if not href.startswith("http"): href = urljoin(BASE, href)
            if href not in seen_urls: seen_urls.add(href); product_urls.append(href)

        print(f"  Productos en página: {len(product_urls)}")

        for i, purl in enumerate(product_urls, 1):
            try:
                psoup = get_soup(purl, s, referer=list_url, treat_400_as_none=False)
                if psoup is None:
                    print(f"   [{i}/{len(product_urls)}] 400/404: {purl}"); continue
                row = parse_product_detail(purl, psoup); all_rows.append(row)
                print(f"   [{i}/{len(product_urls)}] OK: {row.get('Nombre Producto','(sin nombre)')}")
                jitter(*SLEEP_BETWEEN)
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else "?"
                print(f"   [{i}/{len(product_urls)}] HTTP {status} {purl} — reintento suave...")
                jitter(1.0, 1.8)
                try:
                    psoup = get_soup(purl, s, referer=list_url, treat_400_as_none=True)
                    if psoup:
                        row = parse_product_detail(purl, psoup); all_rows.append(row)
                        print(f"   [{i}/{len(product_urls)}] OK tras reintento")
                    else:
                        print(f"   [{i}/{len(product_urls)}] SKIP (400/404 persistente)")
                except Exception as e2:
                    print(f"   [{i}/{len(product_urls)}] ERROR definitivo: {e2}")
            except Exception as e:
                print(f"   [{i}/{len(product_urls)}] ERROR {purl}: {e}")

        jitter(*SLEEP_BETWEEN_PAGES)

    return all_rows

# ====== MySQL helpers ======
def clean_txt(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s if s else None

def price_to_varchar(x: Any) -> Optional[str]:
    if x is None: return None
    try:
        v = float(x)
        if np.isnan(v): return None
        return f"{round(v,2)}"
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

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    """
    Preferencia por EAN (si lo pudimos inferir). Si no hay EAN:
    - intenta por (Nombre Producto, Marca), y como fallback por nombre.
    """
    ean = clean_txt(r.get("EAN"))
    nombre = clean_txt(r.get("Nombre Producto"))
    marca  = clean_txt(r.get("Marca"))
    cat    = clean_txt(r.get("Categoría"))
    sub    = clean_txt(r.get("Subcategoría"))
    fabricante = clean_txt(r.get("Fabricante"))

    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (nombre or "", marca or "", fabricante or "", cat or "", sub or "", pid))
            return pid

    if nombre and marca:
        cur.execute("SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1", (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (ean or "", fabricante or "", cat or "", sub or "", pid))
            return pid

    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (ean or "", marca or "", fabricante or "", cat or "", sub or "", pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (ean or "", nombre or "", marca or "", fabricante or "", cat or "", sub or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    """
    Clave natural preferida: (tienda_id, sku_tienda = 'Código Interno').
    Respaldo por URL si definiste UNIQUE (tienda_id, url_tienda).
    """
    sku = clean_txt(r.get("Código Interno"))
    url = clean_txt(r.get("URL"))
    nombre_tienda = clean_txt(r.get("Nombre Producto"))

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
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: datetime):
    """
    Guarda:
      - precio_lista (Precio de Lista)
      - precio_oferta (Precio de Oferta)
      - tipo_oferta (Tipo de Oferta, si viene 'promo')
    """
    precio_lista  = price_to_varchar(r.get("Precio de Lista"))
    precio_oferta = price_to_varchar(r.get("Precio de Oferta"))
    tipo_oferta   = clean_txt(r.get("Tipo de Oferta"))

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
        precio_lista, precio_oferta, tipo_oferta,
        None, None, None, None
    ))

# ====== Orquestación ======
def main():
    print("[INFO] Scrapeando Átomo…")
    rows = crawl_listing_and_products()
    if not rows:
        print("[INFO] No se obtuvieron productos."); return

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
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
