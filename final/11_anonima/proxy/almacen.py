#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Anónima (categoría -> detalle) → MySQL via ZenRows API (requests)

- Reutiliza la lógica de scraping (sin EAN) pero SIN Selenium.
- Descarga HTML con ZenRows:
    GET https://api.zenrows.com/v1/?url=<URL>&apikey=<APIKEY>
- Inserta/actualiza:
  * tiendas (codigo='laanonima', nombre='La Anónima Online')
  * productos (ean=NULL; match por (nombre, marca) o por nombre)
  * producto_tienda (sku_tienda=sku, record_id_tienda=id_item, url_tienda=url, nombre_tienda=nombre)
  * historico_precios (precio_lista, precio_oferta=precio_plus si existe, tipo_oferta='PLUS')

Índices/UNIQUE sugeridos:
  tiendas(codigo) UNIQUE
  producto_tienda(tienda_id, sku_tienda) UNIQUE
  -- opcional: producto_tienda(tienda_id, record_id_tienda) UNIQUE
  -- opcional: producto_tienda(tienda_id, url_tienda) UNIQUE (para fallback por URL)
  historico_precios(tienda_id, producto_tienda_id, capturado_en) UNIQUE  [idempotencia temporal]
"""

import re
import time
import html
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urljoin
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from mysql.connector import Error as MySQLError
import sys, os

# ===== Añade la carpeta raíz (2 niveles arriba) al sys.path =====
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ================= Config scraping =================
BASE = "https://supermercado.laanonimaonline.com"
START = f"{BASE}/almacen/n1_1/pag/1/"

SLEEP_BETWEEN_PAGES = 1.2
SLEEP_BETWEEN_PRODUCTS = 0.8
MAX_PAGES: Optional[int] = None  # None = todas

TIENDA_CODIGO = "laanonima"
TIENDA_NOMBRE = "La Anónima Online"

# ================= Config ZenRows =================
ZENROWS_ENDPOINT = "https://api.zenrows.com/v1/"
ZENROWS_APIKEY = os.getenv("ZENROWS_APIKEY", "6b618c5088975d2258c9bfa362b05a3ce3460c7d")
# Si tu plan lo soporta y lo necesitas, puedes habilitar render JS:
ZENROWS_JS_RENDER = os.getenv("ZENROWS_JS_RENDER", "false").lower() in {"1", "true", "yes"}

# Retries para robustez (429/5xx)
_session = requests.Session()
_retries = Retry(
    total=3,
    backoff_factor=0.8,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"])
)
_session.mount("https://", HTTPAdapter(max_retries=_retries))
_session.mount("http://", HTTPAdapter(max_retries=_retries))

def fetch(url: str, *, timeout: int = 30) -> str:
    """Descarga HTML de `url` a través de ZenRows. Retorna `response.text` o lanza excepción."""
    params = {
        "url": url,
        "apikey": ZENROWS_APIKEY,
    }
    # Si deseas render JS (requiere plan adecuado)
    if ZENROWS_JS_RENDER:
        params["js_render"] = "true"
        # opcional: esperar a selectores, etc. (params["wait"] = "2000")

    r = _session.get(ZENROWS_ENDPOINT, params=params, timeout=timeout)
    r.raise_for_status()
    return r.text

# ================= Utilidades de parseo =================
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

def get_list_links_from_page_html(html_source: str) -> List[str]:
    soup = BeautifulSoup(html_source, "lxml")
    links = []
    # Tarjetas del listado que apunten a /almacen/.../art_...
    for a in soup.select("div.producto.item a[href*='/almacen/'][href*='/art_']"):
        href = a.get("href") or ""
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
    # Intenta encontrar un link explícito a la siguiente
    a = soup.select_one(f"a[href*='/pag/{cur + 1}/']")
    if a and a.has_attr("href"):
        return urljoin(BASE, a["href"])
    # Fallback: asumir patrón /pag/N
    guess = re.sub(r"/pag/\d+/", f"/pag/{cur + 1}/", current_url)
    return guess if guess != current_url else None

def parse_detail(html_source: str, url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_source, "lxml")

    # -------- metadatos básicos --------
    nombre = text_or_none(soup.select_one("h1.titulo_producto.principal"))

    cod_txt = text_or_none(soup.select_one("div.codigo"))  # "Cod. 3115185"
    sku = None
    if cod_txt:
        m = re.search(r"(\d+)", cod_txt)
        if m:
            sku = m.group(1)

    # a veces viene oculto
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

    # -------- precios --------
    # precio_lista: <div class="precio anterior">...</div>
    precio_lista = None
    caja_lista = soup.select_one("div.precio.anterior")
    if caja_lista:
        precio_lista = parse_price_text(caja_lista.get_text(" ", strip=True))
    else:
        # Fallback si no aparece "anterior"
        alt = soup.select_one(".precio_complemento .precio.destacado, .precio.destacado")
        if alt:
            precio_lista = parse_price_text(alt.get_text(" ", strip=True))

    # precio_plus (oferta PLUS) si existe
    precio_plus = None
    plus_node = soup.select_one(".precio-plus .precio b, .precio-plus .precio")
    if plus_node:
        precio_plus = parse_price_text(plus_node.get_text(" ", strip=True))

    # -------- imágenes --------
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
        "categorias": categorias,  # string "Almacén > Galletitas > ..."
        "precio_lista": precio_lista,
        "precio_plus": precio_plus,
        "descripcion": descripcion,
        "imagenes": " | ".join(imagenes) if imagenes else None,
    }

def grab_category(start_url: str) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    visited = set()
    page_url = start_url
    page_idx = 0

    while True:
        page_idx += 1
        if MAX_PAGES is not None and page_idx > MAX_PAGES:
            break

        try:
            page_html = fetch(page_url)
        except requests.HTTPError as e:
            print(f"⚠️ HTTP {e.response.status_code} cargando {page_url}")
            break
        except requests.RequestException as e:
            print(f"⚠️ Error de red cargando {page_url}: {e}")
            break

        links = get_list_links_from_page_html(page_html)
        print(f"🔗 P{page_idx} {len(links)} productos - {page_url}")
        if not links:
            break

        for href in links:
            if href in visited:
                continue
            visited.add(href)
            try:
                detail_html = fetch(href)
                row = parse_detail(detail_html, href)
                all_rows.append(row)
                print(f"  ✔ {row.get('nombre') or ''} [{row.get('sku') or ''}]")
                time.sleep(SLEEP_BETWEEN_PRODUCTS)
            except requests.RequestException as e:
                print(f"  ⚠️ Error detalle: {href} -> {e}")
            except Exception as e:
                print(f"  ⚠️ Parse detalle: {href} -> {e}")

        next_url = get_next_page_url(page_url, page_html)
        if not next_url or next_url == page_url:
            break
        page_url = next_url
        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_rows

# ================= MySQL helpers =================
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
    """Parte la cadena 'A > B > C' en (A, B)."""
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
    """
    No hay EAN → ean=NULL.
    Match preferente por (nombre, marca). Fallback por nombre.
    Guarda categoría/subcategoría derivadas de 'categorias'.
    """
    ean = None
    nombre = clean_txt(r.get("nombre"))
    marca  = clean_txt(r.get("marca"))
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
    """
    Clave natural preferida: (tienda_id, sku_tienda=sku).
    Respaldo: (tienda_id, record_id_tienda=id_item) o (tienda_id, url_tienda).
    """
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
    """
    Mapeo precios:
      - precio_lista: el 'precio principal' del detalle
      - precio_plus: si está presente, se guarda como 'precio_oferta' con tipo_oferta='PLUS'
    """
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
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = VALUES(promo_tipo),
          promo_texto_regular = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios = VALUES(promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista, precio_plus, tipo_oferta,
        None, None, None, None
    ))

# ================= Orquestación =================
def main():
    print("[INFO] Scrapeando La Anónima vía ZenRows…")
    rows = grab_category(START)

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
