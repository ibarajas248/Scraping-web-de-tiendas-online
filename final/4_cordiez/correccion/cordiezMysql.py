#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cordiez (VTEX) – Descarga multi-categoría y guarda en MySQL + Excel.

FIX (disponibilidad):
- VTEX a veces manda IsAvailable como string "false"/"true" o 0/1.
- Si Disponible == False => NO inserta a MySQL (pero sí queda en el Excel).

Requisitos:
  pip install requests pandas tenacity xlsxwriter mysql-connector-python
  y un módulo base_datos.py con get_conn() (conecta a tu MySQL)
"""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import numpy as np
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from mysql.connector import Error as MySQLError

import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

# ---------- Config editable ----------
BASE = "https://www.cordiez.com.ar"
ORDER = "OrderByScoreDESC"
STEP = 50
SLEEP = 0.20
TIMEOUT = 25
OUT_XLSX = f"cordiez_todas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

CATEGORY_URLS = [
    "https://www.cordiez.com.ar/sin-gluten-y-diet",
    "https://www.cordiez.com.ar/almacen",
    "https://www.cordiez.com.ar/bazar/automotor",
    "https://www.cordiez.com.ar/bazar/platos-copas-y-cubiertos",
    "https://www.cordiez.com.ar/bebidas",
    "https://www.cordiez.com.ar/bebes-y-ninos",
    "https://www.cordiez.com.ar/carnes",
    "https://www.cordiez.com.ar/congelados",
    "https://www.cordiez.com.ar/cuidado-personal",
    "https://www.cordiez.com.ar/cuidado-de-la-ropa",
    "https://www.cordiez.com.ar/desayuno-y-merienda",
    "https://www.cordiez.com.ar/electrodomesticos",
    "https://www.cordiez.com.ar/fiambres-y-quesos",
    "https://www.cordiez.com.ar/frutas-y-verduras",
    "https://www.cordiez.com.ar/kiosco",
    "https://www.cordiez.com.ar/bazar/libreria",
    "https://www.cordiez.com.ar/limpieza-y-hogar",
    "https://www.cordiez.com.ar/lacteos",
    "https://www.cordiez.com.ar/mascotas",
    "https://www.cordiez.com.ar/panaderia",
    "https://www.cordiez.com.ar/pastas",
    "https://www.cordiez.com.ar/reposteria",
    "https://www.cordiez.com.ar/varios",
]

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "identity",
    "Connection": "keep-alive",
}

TIENDA_CODIGO = "cordiez"
TIENDA_NOMBRE = "Cordiez "

# ---------- Helpers HTTP ----------
class HTTPError(Exception):
    pass

def as_bool(v):
    """Parse defensivo: VTEX puede mandar IsAvailable como bool, string o 0/1."""
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y", "si", "sí"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None

def path_from_category_url(url: str) -> str:
    p = urlparse(url)
    return p.path.strip('/')

def humanize_path(path: str) -> str:
    def pretty(seg: str) -> str:
        return seg.replace('-', ' ').strip().title()
    parts = [pretty(seg) for seg in path.split('/') if seg]
    return " / ".join(parts)

def build_url(category_path: str, start: int, end: int) -> str:
    return f"{BASE}/api/catalog_system/pub/products/search/{category_path}?&_from={start}&_to={end}&O={ORDER}"

def make_session() -> requests.Session:
    s = requests.Session()
    retry_cfg = Retry(
        total=4, connect=4, read=4, backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_cfg, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)

    _orig_get = s.get
    def _get(url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = TIMEOUT
        return _orig_get(url, **kwargs)
    s.get = _get  # type: ignore
    return s

def try_parse_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return json.loads(resp.text)

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
    retry=retry_if_exception_type((requests.RequestException, HTTPError)),
)
def fetch_range(session: requests.Session, url: str, referer: str) -> List[Dict[str, Any]]:
    headers = dict(HEADERS_BASE)
    headers["Referer"] = referer
    resp = session.get(url, headers=headers)

    if resp.status_code not in (200, 206):
        raise HTTPError(f"HTTP {resp.status_code} for {url}")

    try:
        data = try_parse_json(resp)
    except Exception as e:
        raise HTTPError(f"JSON parse error for {url}: {e}")

    if not isinstance(data, list):
        raise HTTPError(f"Unexpected payload (not a list) for {url}")
    return data

def pick_best_offer(sellers: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Si hay algún seller con IsAvailable=True, usamos ese.
    Si no, usamos el primero como fallback (para reportar precios aunque esté no disponible).
    """
    best = None
    for s in sellers:
        co = s.get("commertialOffer") or {}
        av = as_bool(co.get("IsAvailable", co.get("isAvailable")))
        if av is True:
            return co
        if best is None:
            best = co
    return best or {}

# ---------- Normalización de productos ----------
def extract_rows(product: Dict[str, Any], fuente_categoria: str) -> List[Dict[str, Any]]:
    """
    Una fila por SKU (items[*]).
    Incluye productId para usar como record_id_tienda.
    """
    rows: List[Dict[str, Any]] = []

    product_id = product.get("productId")
    prod_name = product.get("productName") or product.get("productTitle")
    brand = product.get("brand")
    link = product.get("link")  # PDP
    categories = product.get("categories") or []
    categoria = subcategoria = ""
    if categories:
        parts = [p for p in (categories[0] or "").split("/") if p]
        if len(parts) >= 1: categoria = parts[0]
        if len(parts) >= 2: subcategoria = parts[1]

    items = product.get("items") or []
    for it in items:
        ean = (it.get("ean") or "") or None
        sku = it.get("itemId") or None
        name_it = it.get("name") or prod_name

        sellers = it.get("sellers") or []
        price = list_price = None
        is_available = None
        oferta_tipo = None

        if sellers:
            offer = pick_best_offer(sellers)
            price = offer.get("Price")
            list_price = offer.get("ListPrice")
            is_available = as_bool(offer.get("IsAvailable", offer.get("isAvailable")))

            teasers = offer.get("PromotionTeasers") or []
            if teasers:
                nombres = []
                for t in teasers:
                    n = (t.get("name") or t.get("Name") or "").strip()
                    if n:
                        nombres.append(n)
                oferta_tipo = "; ".join(nombres) if nombres else None

        rows.append({
            "EAN": ean,
            "SKU": sku,                        # Código Interno (SKU VTEX)
            "Nombre": name_it,
            "Categoría": categoria,
            "Subcategoría": subcategoria,
            "Marca": brand,
            "PrecioLista": list_price,
            "PrecioOferta": price,
            "TipoOferta": oferta_tipo,
            "URL": link,
            "Disponible": is_available,         # ✅ True/False/None (parseado)
            "CategoriaFuente": fuente_categoria,
            "ProductIdVTEX": product_id,        # record_id_tienda (respaldo)
        })
    return rows

def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set(); out: List[Dict[str, Any]] = []
    for r in rows:
        key = (r.get("SKU"), r.get("EAN"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def fetch_category(session: requests.Session, category_url: str) -> List[Dict[str, Any]]:
    category_path = path_from_category_url(category_url)
    if not category_path:
        raise HTTPError(f"URL inválida: {category_url}")

    filas: List[Dict[str, Any]] = []
    fuente_categoria = humanize_path(category_path)

    print(f"\n=== Explorando categoría: {fuente_categoria} ===")
    time.sleep(1.5)

    start = 0
    while True:
        end = start + STEP - 1
        url = build_url(category_path, start, end)
        chunk = fetch_range(session, url, referer=category_url)
        n = len(chunk)
        print(f"[{fuente_categoria}] _from={start} _to={end} -> {n} productos")

        if n == 0:
            break

        for prod in chunk:
            try:
                filas.extend(extract_rows(prod, fuente_categoria))
            except Exception as e:
                print(f"  - Warning: item malformado en {fuente_categoria}: {e}")

        if n < STEP:
            break
        start += STEP
        time.sleep(SLEEP)

    return filas

# ---------- MySQL helpers ----------
def clean_txt(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None

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
    Preferencia por EAN. Si no, usa (Nombre, Marca) como match suave.
    """
    ean = clean_txt(r.get("EAN"))
    nombre = clean_txt(r.get("Nombre"))
    marca = clean_txt(r.get("Marca"))

    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (nombre or "", marca or "", r.get("Categoría") or "", r.get("Subcategoría") or "", pid))
            return pid

    if nombre and marca:
        cur.execute("SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1", (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (ean or "", r.get("Categoría") or "", r.get("Subcategoría") or "", pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (ean or "", nombre or "", marca or "", r.get("Categoría") or "", r.get("Subcategoría") or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    """
    Clave natural preferida: (tienda_id, sku_tienda=SKU VTEX).
    Respaldo: (tienda_id, record_id_tienda=productId VTEX).
    """
    sku = clean_txt(r.get("SKU"))
    record_id = clean_txt(r.get("ProductIdVTEX"))
    url = clean_txt(r.get("URL"))
    nombre_tienda = clean_txt(r.get("Nombre"))

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
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: datetime):
    precio_lista = price_to_varchar(r.get("PrecioLista"))
    precio_oferta = price_to_varchar(r.get("PrecioOferta"))
    tipo_oferta = clean_txt(r.get("TipoOferta"))
    disponible = r.get("Disponible")
    comentarios = f"Disponible={disponible}" if disponible is not None else None

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

# ---------- Main ----------
def main():
    session = make_session()
    all_rows: List[Dict[str, Any]] = []

    for cat_url in CATEGORY_URLS:
        try:
            filas = fetch_category(session, cat_url)
            if filas:
                print(f"✔ {len(filas)} filas obtenidas en '{cat_url}'")
                all_rows.extend(filas)
            else:
                print(f"⚠ Sin resultados en '{cat_url}'")
        except Exception as e:
            print(f"❌ Error en categoría '{cat_url}': {e}")

    if not all_rows:
        print("No se encontraron productos en ninguna categoría.")
        return

    # Deduplicar globalmente por (SKU, EAN)
    all_rows = dedupe_rows(all_rows)

    # ----- Export opcional a Excel -----
    df = pd.DataFrame(all_rows)
    cols = [
        "EAN","SKU","Nombre","Categoría","Subcategoría","Marca",
        "PrecioLista","PrecioOferta","TipoOferta","URL",
        "Disponible","CategoriaFuente","ProductIdVTEX"
    ]
    df = df.reindex(columns=cols)

    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="cordiez_todo")
        wb = writer.book
        ws = writer.sheets["cordiez_todo"]
        money_fmt = wb.add_format({"num_format": "#,##0.00"})
        ws.set_column("A:A", 16); ws.set_column("B:B", 14); ws.set_column("C:C", 50)
        ws.set_column("D:E", 22); ws.set_column("F:F", 18)
        ws.set_column("G:H", 16, money_fmt); ws.set_column("I:I", 30)
        ws.set_column("J:J", 70); ws.set_column("K:K", 12); ws.set_column("L:M", 18)

    print(f"✅ Exportado: {OUT_XLSX} | Filas: {len(df)} | Categorías: {len(CATEGORY_URLS)}")

    # ----- Inserción en MySQL -----
    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        skipped_no_disp = 0

        for r in all_rows:
            av = as_bool(r.get("Disponible"))

            # ✅ Regla pedida: si IsAvailable == False => NO inserta
            if av is False:
                skipped_no_disp += 1
                continue

            producto_id = find_or_create_producto(cur, r)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, r)
            insert_historico(cur, tienda_id, pt_id, r, capturado_en)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} filas (saltados no disponibles: {skipped_no_disp}) para {TIENDA_NOMBRE} ({capturado_en})")

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
