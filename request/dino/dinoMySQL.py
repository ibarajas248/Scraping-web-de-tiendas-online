#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DinoOnline (Endeca/ATG) – Listing HTML -> MySQL

- Reutiliza tu scraper (requests + BeautifulSoup) para listar productos.
- Inserta/actualiza:
  * tiendas (codigo='dinoonline')
  * productos (EAN = NULL; match suave por nombre)
  * producto_tienda (sku_tienda = prod_id, url_tienda = url)
  * historico_precios (precios como VARCHAR)

Índices/UNIQUE sugeridos:
  tiendas(codigo) UNIQUE
  producto_tienda(tienda_id, sku_tienda) UNIQUE
  -- opcional respaldo: producto_tienda(tienda_id, url_tienda) UNIQUE
  historico_precios(tienda_id, producto_tienda_id, capturado_en) UNIQUE  [solo si buscas idempotencia]
"""

import re
import time
from html import unescape
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime
from typing import Optional, List, Dict, Any

import numpy as np
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

# ================== Config ==================
BASE = "https://www.dinoonline.com.ar"
START_URL = (
    "https://www.dinoonline.com.ar/super/categoria"
    "?_dyncharset=utf-8&Dy=1&Nty=1&minAutoSuggestInputLength=3"
    "&autoSuggestServiceUrl=%2Fassembler%3FassemblerContentCollection%3D%2Fcontent%2FShared%2FAuto-Suggest+Panels%26format%3Djson"
    "&searchUrl=%2Fsuper&containerClass=search_rubricator"
    "&defaultImage=%2Fimages%2Fno_image_auto_suggest.png&rightNowEnabled=false&Ntt="
)
SLEEP_BETWEEN_PAGES = 0.6
TIMEOUT = 25
MAX_SEEN_PAGES = 1000
TIENDA_CODIGO = "dinoonline"
TIENDA_NOMBRE = "Dino Online"

# ================== Sesión ==================
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    retry = Retry(
        total=5, connect=5, read=5, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

# ================== Utils ==================
def clean_money(txt: str):
    if not txt:
        return None
    t = re.sub(r"[^\d,.\-]", "", txt)
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None

def text_or_none(el):
    return el.get_text(strip=True) if el else None

def absolute_url(href: str):
    if not href:
        return None
    href = unescape(href)
    return urljoin(BASE, href)

def find_next_url_by_icon(soup: BeautifulSoup):
    caret = soup.select_one("a i.fa.fa-angle-right, a i.fa-angle-right")
    if caret and caret.parent and caret.parent.name == "a":
        return absolute_url(caret.parent.get("href"))
    for a in soup.select("a[href]"):
        i = a.select_one("i.fa-angle-right, i.fa.fa-angle-right")
        if i:
            return absolute_url(a.get("href"))
    return None

def detect_nrpp_and_base(current_url: str, soup: BeautifulSoup, page_items_found: int):
    cand = find_next_url_by_icon(soup)
    if cand:
        p = urlparse(cand); q = parse_qs(p.query, keep_blank_values=True)
        nrpp = None
        if "Nrpp" in q and q["Nrpp"]:
            try: nrpp = int(q["Nrpp"][0])
            except Exception: pass
        stable_params = {k: v[0] for k, v in q.items()}
        stable_params.pop("No", None)
        return (p.scheme + "://" + p.netloc + p.path, nrpp, stable_params)

    p = urlparse(current_url); q = parse_qs(p.query, keep_blank_values=True)
    nrpp = None
    if "Nrpp" in q and q["Nrpp"]:
        try: nrpp = int(q["Nrpp"][0])
        except Exception: nrpp = None
    if not nrpp:
        nrpp = page_items_found if page_items_found else 36
    stable_params = {k: v[0] for k, v in q.items()}
    stable_params.pop("No", None)
    base_path = p.scheme + "://" + p.netloc + p.path
    return base_path, nrpp, stable_params

def next_url_by_no(base_path: str, nrpp: int, stable_params: dict, page_index: int):
    params = stable_params.copy()
    params["No"] = str(page_index * nrpp)
    if "Nrpp" not in params:
        params["Nrpp"] = str(nrpp)
    query = urlencode(params, doseq=False)
    return f"{base_path}?{query}"

# ================== Parseo listing ==================
def parse_items(soup: BeautifulSoup):
    rows = []
    boxes = soup.select("div.item.col-lg-3, div.item.col-md-3, div.item.col-sm-4, div.item.col-xs-6")
    for box in boxes:
        prod = box.select_one("div.product")
        if not prod:
            continue

        prod_id = prod.get("id")  # ej. prod3390039
        pesable = prod.get("pesable")
        cantbulto = prod.get("cantbulto")
        categoryrec = prod.get("categoryrec")

        a = box.select_one(".image a[href]")
        href = absolute_url(a["href"]) if a else None

        img = box.select_one(".image img")
        img_src = img.get("src") if img else None
        if img_src and img_src.startswith("//"):
            img_src = "https:" + img_src
        img_alt = img.get("alt") if img else None

        precio_unidad_span = box.select_one(".precio-unidad span")
        precio_unidad_txt = text_or_none(precio_unidad_span)
        precio_unidad = clean_money(precio_unidad_txt)

        pu_div = box.select_one(".precio-unidad")
        precio_sin_imp = None
        precio_antes = None
        if pu_div:
            pu_text = pu_div.get_text(" ", strip=True)
            m1 = re.search(r"Precio\s*s/Imp.*?:\s*\$?\s*([\d\.,]+)", pu_text, re.I)
            if m1: precio_sin_imp = clean_money(m1.group(1))
            m2 = re.search(r"\bantes\s*\$?\s*([\d\.,]+)", pu_text, re.I)
            if m2: precio_antes = clean_money(m2.group(1))

        descripcion_div = box.select_one(".description")
        nombre = text_or_none(descripcion_div) or img_alt

        precio_ref_txt = text_or_none(box.select_one(".precio-referencia"))
        precio_ref_val = None
        unidad_ref = None
        if precio_ref_txt:
            m3 = re.search(r"\$?\s*([\d\.,]+)\s*x\s*(.+)", precio_ref_txt)
            if m3:
                precio_ref_val = clean_money(m3.group(1))
                unidad_ref = m3.group(2).strip()

        # Log rápido
        print(f"🛒 {nombre} - ${precio_unidad if precio_unidad is not None else 'N/D'} - URL: {href}")

        rows.append({
            "prod_id": prod_id,
            "nombre": nombre,
            "precio_unidad": precio_unidad,
            "precio_sin_imp": precio_sin_imp,
            "precio_antes": precio_antes,
            "precio_ref_valor": precio_ref_val,
            "precio_ref_unidad": unidad_ref,
            "precio_unidad_raw": precio_unidad_txt,
            "url": href,
            "img": img_src,
            "img_alt": img_alt,
            "pesable": pesable,
            "cantbulto": cantbulto,
            "categoryrec": categoryrec
        })
    return rows, len(boxes)

# ================== Navegación ==================
def get_with_fix(session: requests.Session, url: str):
    r = session.get(url, timeout=TIMEOUT)
    if r.status_code == 404 and "&amp;" in url:
        fixed = url.replace("&amp;", "&")
        print(f"[WARN] 404 con &amp;, reintentando: {fixed}")
        r = session.get(fixed, timeout=TIMEOUT)
        return r, fixed
    return r, url

def scrape_all(start_url=START_URL, limit_pages=None):
    s = make_session()
    url = start_url
    all_rows = []
    seen_urls = set()
    page_num = 1

    base_path = None
    nrpp = None
    stable_params = None
    page_index_for_no = 1  # primera página es No=0

    while url and page_num <= MAX_SEEN_PAGES:
        if url in seen_urls:
            print("[STOP] URL repetida, deteniendo.")
            break
        seen_urls.add(url)

        print(f"\n🌐 Página {page_num}: {url}")
        r, url = get_with_fix(s, url)
        if r.status_code == 404:
            print(f"[ERROR] 404 definitivo: {url}")
            break
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        rows, items_on_page = parse_items(soup)
        all_rows.extend(rows)

        if base_path is None or nrpp is None or stable_params is None:
            base_path, nrpp, stable_params = detect_nrpp_and_base(url, soup, items_on_page)

        if limit_pages and page_num >= limit_pages:
            break

        next_by_icon = find_next_url_by_icon(soup)
        if next_by_icon:
            next_by_icon = absolute_url(next_by_icon)
            if next_by_icon and next_by_icon not in seen_urls:
                url = next_by_icon
                page_num += 1
                time.sleep(SLEEP_BETWEEN_PAGES)
                continue

        if items_on_page == 0:
            print("[STOP] Página sin productos; fin.")
            break

        url = next_url_by_no(base_path, nrpp, stable_params, page_index_for_no)
        page_index_for_no += 1
        page_num += 1
        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_rows

# ================== MySQL helpers ==================
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
    No hay EAN → ean=NULL. Usamos match suave por 'nombre'.
    Si más adelante cargas tabla de mapeo (prod_id → EAN), podrás actualizar.
    """
    ean = None
    nombre = clean_txt(r.get("nombre"))
    marca = None  # el listing no trae marca confiable

    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        row = cur.fetchone()
        if row:
            return row[0]

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULL, NULL, NULL, NULL)
    """, (nombre or "",))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    """
    Clave natural: (tienda_id, sku_tienda=prod_id) – viene como 'prod123456'.
    Respaldo: UNIQUE por (tienda_id, url_tienda) si definís ese índice.
    """
    sku = clean_txt(r.get("prod_id"))
    url = clean_txt(r.get("url"))
    nombre_tienda = clean_txt(r.get("nombre"))

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

    # Fallback sin sku: obliga a tener UNIQUE (tienda_id, url_tienda) si querés upsert real
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
    Map de precios:
      - Si hay 'precio_antes' (tachado), lo usamos como 'precio_lista' y 'precio_unidad' como oferta.
      - Si NO hay 'precio_antes', guardamos 'precio_unidad' como 'precio_lista' (sin oferta).
      - 'precio_sin_imp' y refs quedan en comentarios para trazabilidad.
    """
    precio_unidad = r.get("precio_unidad")
    precio_antes = r.get("precio_antes")

    if precio_antes is not None:
        precio_lista = price_to_varchar(precio_antes)
        precio_oferta = price_to_varchar(precio_unidad)
        tipo_oferta = "OFERTA"
    else:
        precio_lista = price_to_varchar(precio_unidad)
        precio_oferta = None
        tipo_oferta = None

    comentarios_bits = []
    if r.get("precio_sin_imp") is not None:
        comentarios_bits.append(f"precio_sin_imp={r.get('precio_sin_imp')}")
    if r.get("precio_ref_valor") is not None and r.get("precio_ref_unidad"):
        comentarios_bits.append(f"precio_ref={r.get('precio_ref_valor')} x {r.get('precio_ref_unidad')}")
    if r.get("pesable") is not None:
        comentarios_bits.append(f"pesable={r.get('pesable')}")
    if r.get("cantbulto") is not None:
        comentarios_bits.append(f"cantbulto={r.get('cantbulto')}")
    comentarios = "; ".join(comentarios_bits) if comentarios_bits else None

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
        None, None, None, comentarios
    ))

# ================== Main ==================
def main():
    print("[INFO] Scrapeando Dino Online...")
    rows = scrape_all(START_URL)  # usa limit_pages=2 para testear rápido
    if not rows:
        print("[INFO] No se obtuvieron productos.")
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
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
