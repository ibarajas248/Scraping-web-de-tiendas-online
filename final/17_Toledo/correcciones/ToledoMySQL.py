#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
17_Toledo Digital (VTEX) — Scraper catálogo completo + inserción MySQL (robusto anti-1205, SIN EAN)
---------------------------------------------------------------------------------------------------
- Paridad con tu script original (mismas columnas y lógica), pero forzando ean=NULL.
- Transacciones por lotes (BATCH commits).
- capturado_en = NOW(6) dentro del INSERT (no se pasa desde Python).
- Isolation level READ COMMITTED y lock_wait_timeout más alto.
- Retries con backoff en DML (1205/1213).
- Candado lógico por tienda (GET_LOCK/RELEASE_LOCK) para evitar concurrencia por tienda.

Salida por fila (SKU):
EAN | Código Interno | Nombre Producto | Categoría | Subcategoría | Marca | Fabricante |
Precio de Lista | Precio de Oferta | Tipo de Oferta | URL
"""

import time
import json
import logging
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime

import requests
import re
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

# ------------------ Configuración ------------------
BASE_WEB   = "https://www.toledodigital.com.ar"
API_TREE   = f"{BASE_WEB}/api/catalog_system/pub/category/tree/{{depth}}"
API_SEARCH = f"{BASE_WEB}/api/catalog_system/pub/products/search"

DEPTH = 3               # niveles de árbol de categorías
STEP = 50               # VTEX pagina por rango _from/_to
SLEEP = 0.25            # pausa suave entre requests
TIMEOUT = 25
RETRIES = 3
SC_DEFAULT = 1          # sales channel; si la tienda no lo usa, no afecta
MAX_VACIAS = 2          # corta categoría tras N páginas vacías seguidas

OUT_CSV  = "toledo_catalogo.csv"
OUT_XLSX = "toledo_catalogo.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# Identidad tienda en DB
TIENDA_CODIGO = "toledo"
TIENDA_NOMBRE = "Toledo"

# DB batching y retries
BATCH = 300
SQL_RETRIES = 4         # reintentos para 1205/1213
BACKOFF_BASE = 0.5      # segundos

# ------------------ Helpers comunes (DB/parse) ------------------
_price_clean_re = re.compile(r"[^\d,.\-]")
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean(val):
    if val is None: return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

def parse_price(val) -> float:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan
    if isinstance(val, (int, float)): return float(val)
    s = str(val).strip()
    if not s: return np.nan
    s = _price_clean_re.sub("", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

# ------------------ Utilidades HTTP ------------------
def build_session(retries: int = RETRIES, backoff: float = 0.5) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries, backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",)
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

# ------------------ Descubrimiento de categorías ------------------
def get_category_tree(session: requests.Session, depth: int = DEPTH) -> List[dict]:
    r = session.get(API_TREE.format(depth=depth), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def flatten_categories(tree: List[dict], prefix_path: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    Devuelve lista de (path_str, map_str). En VTEX, map es 'c' repetido por nivel.
    Ej: ('almacen', 'c') ; ('almacen/bebidas', 'c,c')
    """
    out: List[Tuple[str, str]] = []
    for node in tree:
        slug = (node.get("url", "") or node.get("link", "")).strip("/").split("/")[-1]
        if not slug:
            slug = (node.get("name") or "").strip().replace(" ", "-").lower()
        current_path = slug if not prefix_path else f"{prefix_path}/{slug}"
        depth = current_path.count("/") + 1
        map_str = ",".join(["c"] * depth)
        out.append((current_path, map_str))
        children = node.get("children") or node.get("Children") or []
        if children:
            out.extend(flatten_categories(children, current_path))
    return out

# ------------------ Parseo de productos ------------------
def safe_get(d: dict, key: str, default=""):
    v = d.get(key, default) if isinstance(d, dict) else default
    return "" if v is None else v

def extract_ean(item: dict, product: dict):
    # 1) campo directo
    ean = item.get("ean") or item.get("EAN")
    if ean:
        return str(ean).strip()

    # 2) referenceId (varía según tienda)
    refs = item.get("referenceId") or item.get("referenceIds") or []
    for r in refs:
        k = (r.get("Key") or r.get("key") or "").upper()
        v = (r.get("Value") or r.get("value") or "")
        if k == "EAN" and v:
            return str(v).strip()

    return None


def extract_teaser(co: dict) -> str:
    teasers = co.get("Teasers") or []
    if not teasers:
        return ""
    name = safe_get(teasers[0], "Name", "") or safe_get(teasers[0], "name", "")
    return name

def make_url(product: dict) -> str:
    link_text = product.get("linkText") or product.get("LinkText") or ""
    if link_text:
        return urljoin(BASE_WEB, f"/{link_text}/p")
    return urljoin(BASE_WEB, product.get("link") or product.get("Link", "/"))

def pick_category_fields(product: dict) -> Tuple[str, str]:
    cats: List[str] = [c.strip("/") for c in (product.get("categories") or []) if isinstance(c, str)]
    if not cats:
        return "", ""
    deep = max(cats, key=lambda c: c.count("/"))
    parts = deep.split("/")
    categoria = parts[0] if parts else ""
    subcategoria = parts[1] if len(parts) > 1 else ""
    return categoria, subcategoria

def row_from_product(product: dict) -> List[dict]:
    """
    Produce una fila por SKU (item). Extrae precios del seller 1 (o el primero).
    """
    rows: List[dict] = []
    product_id = str(product.get("productId", "")).strip()
    product_name = product.get("productName") or product.get("ProductName") or ""
    brand = product.get("brand") or product.get("Brand") or ""
    manufacturer = product.get("manufacturer") or product.get("Manufacturer") or ""
    categoria, subcategoria = pick_category_fields(product)
    url = make_url(product)

    items: List[dict] = product.get("items") or product.get("Items") or []
    for it in items:
        item_id = str(it.get("itemId", "")).strip() or product_id
        ean = extract_ean(it, product)  # -> None (DB: NULL)

        sellers = it.get("sellers") or it.get("Sellers") or []
        seller = sellers[0] if sellers else {}
        co = seller.get("commertialOffer") or seller.get("CommertialOffer") or {}
        list_price = co.get("ListPrice") or co.get("listPrice") or 0.0
        price = co.get("Price") or co.get("price") or 0.0
        teaser = extract_teaser(co)

        tipo_oferta = "Precio Regular"
        if teaser:
            tipo_oferta = teaser
        elif price and list_price and price < list_price:
            tipo_oferta = "Oferta"

        row = {
            "EAN": ean,                                 # None -> luego NULL en DB
            "Código Interno": item_id,                  # SKU (fallback: productId)
            "Nombre Producto": product_name,
            "Categoría": categoria,
            "Subcategoría": subcategoria,
            "Marca": brand,
            "Fabricante": manufacturer,
            "Precio de Lista": list_price or "",
            "Precio de Oferta": price or "",
            "Tipo de Oferta": tipo_oferta,
            "URL": url,
            "productId": product_id,                    # record_id_tienda
        }
        rows.append(row)
    return rows

# ------------------ Scraping por categoría ------------------
def fetch_category(session: requests.Session, path: str, map_str: str, sc: Optional[int] = SC_DEFAULT) -> List[dict]:
    """
    Itera páginas de una categoría (path='almacen/bebidas', map='c,c') hasta agotar resultados.
    """
    all_rows: List[dict] = []
    offset = 0
    vacias = 0
    while True:
        params = {"_from": offset, "_to": offset + STEP - 1, "map": map_str}
        if sc is not None:
            params["sc"] = sc

        url = f"{API_SEARCH}/{path}"
        r = session.get(url, params=params, timeout=TIMEOUT)
        try:
            data = r.json()
        except Exception:
            logging.warning(f"Respuesta no-JSON en {path} offset={offset}: {r.status_code}")
            break

        if not isinstance(data, list):
            logging.warning(f"Respuesta inesperada en {path} offset={offset}: tipo {type(data)}")
            break

        n = len(data)
        if n == 0:
            vacias += 1
            if vacias >= MAX_VACIAS:
                break
        else:
            vacias = 0

        for p in data:
            filas = row_from_product(p)
            all_rows.extend(filas)

        if n < STEP:
            break

        offset += STEP
        time.sleep(SLEEP)

    return all_rows

# ------------------ Scrape principal ------------------
def scrape_toledo() -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ses = build_session()

    logging.info("Descubriendo categorías…")
    tree = get_category_tree(ses, DEPTH)
    cats = flatten_categories(tree)
    # Quitar duplicados por path
    seen = set()
    cats = [c for c in cats if not (c[0] in seen or seen.add(c[0]))]
    logging.info(f"Categorías detectadas: {len(cats)}")

    rows: List[dict] = []
    for i, (path, map_str) in enumerate(cats, 1):
        logging.info(f"[{i}/{len(cats)}] {path} (map={map_str})")
        try:
            rows.extend(fetch_category(ses, path, map_str, sc=SC_DEFAULT))
        except requests.RequestException as e:
            logging.warning(f"⚠️ Error en {path}: {e}")
        time.sleep(SLEEP)

    if not rows:
        logging.warning("No se recolectó ningún dato.")
        return pd.DataFrame(columns=[
            "EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Marca",
            "Fabricante","Precio de Lista","Precio de Oferta","Tipo de Oferta","URL","productId"
        ])

    df = pd.DataFrame(rows)

    # Normalizaciones ligeras

    df["Código Interno"] = df["Código Interno"].astype(str)
    df["Precio de Lista"]  = pd.to_numeric(df["Precio de Lista"], errors="coerce")
    df["Precio de Oferta"] = pd.to_numeric(df["Precio de Oferta"], errors="coerce")

    # Deduplicar por SKU (Código Interno)
    df.drop_duplicates(subset=["Código Interno"], inplace=True)

    # Guardar respaldo
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    try:
        df.to_excel(OUT_XLSX, index=False)
    except Exception as e:
        logging.warning(f"XLSX no generado ({e}); queda CSV.")

    logging.info(f"Listo. Filas: {len(df)} | CSV: {OUT_CSV} | XLSX: {OUT_XLSX}")
    return df

# ------------------ MySQL helpers ------------------
def exec_retry(cur, sql, params=None, retries=SQL_RETRIES, label=""):
    """
    Ejecuta cur.execute con reintentos ante 1205 (Lock wait timeout) o 1213 (Deadlock).
    """
    for i in range(retries):
        try:
            cur.execute(sql, params or ())
            return
        except MySQLError as e:
            err = getattr(e, "errno", None)
            if err in (1205, 1213):  # lock wait timeout / deadlock
                time.sleep(BACKOFF_BASE * (2 ** i))
                continue
            raise

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre),
        label="upsert_tienda"
    )
    exec_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,), label="select_tienda")
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, any]) -> int:
    """
    Versión SIN EAN:
      - Nunca busca por EAN.
      - Criterio: (nombre, marca) si ambos existen; si no, inserta nuevo.
      - Mantiene fabricante/categoría/subcategoría por COALESCE.
    """
    nombre = clean(p.get("nombre")) or ""
    marca  = clean(p.get("marca")) or ""

    if nombre and marca:
        exec_retry(cur,
            "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
            (nombre, marca),
            label="sel_producto_nm"
        )
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_retry(cur, """
                UPDATE productos SET
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("fabricante") or "",
                p.get("categoria") or "",
                p.get("subcategoria") or "",
                pid
            ), label="upd_producto_meta")
            return pid

    # ean va SIEMPRE NULL
    exec_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        nombre, marca,
        p.get("fabricante") or "",
        p.get("categoria") or "",
        p.get("subcategoria") or ""
    ), label="ins_producto")
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, any]) -> int:
    """
    Para VTEX (17_Toledo):
      - sku_tienda = Código Interno (itemId)
      - record_id_tienda = productId (fallback universal)
    Requiere UNIQUE(tienda_id, sku_tienda) y/o UNIQUE(tienda_id, record_id_tienda).
    """
    sku = clean(p.get("sku")) or ""
    rec = clean(p.get("record_id")) or ""
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre") or ""

    if sku:
        exec_retry(cur, """
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda), label="upsert_pt_sku")
        return cur.lastrowid

    if rec:
        exec_retry(cur, """
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, url, nombre_tienda), label="upsert_pt_rec")
        return cur.lastrowid

    exec_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda), label="ins_pt_simple")
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, any]):
    """
    Inserta/actualiza histórico con capturado_en=NOW(6) (microsegundos).
    Columna capturado_en debe ser DATETIME(6).
    """
    def to_txt_or_none(x):
        v = parse_price(x)
        if x is None: return None
        if isinstance(v, float) and np.isnan(v): return None
        return f"{round(float(v), 2)}"  # guardamos como VARCHAR

    exec_retry(cur, """
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, NOW(6), %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = COALESCE(VALUES(promo_tipo), promo_tipo),
          promo_texto_regular = COALESCE(VALUES(promo_texto_regular), promo_texto_regular),
          promo_texto_descuento = COALESCE(VALUES(promo_texto_descuento), promo_texto_descuento),
          promo_comentarios = COALESCE(VALUES(promo_comentarios), promo_comentarios)
    """, (
        tienda_id, producto_tienda_id,
        to_txt_or_none(p.get("precio_lista")), to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None, p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None, p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ), label="upsert_hist")

# ------------------ Main (scrape + inserción) ------------------
def main():
    t0 = time.time()
    df = scrape_toledo()
    if df.empty:
        print("⚠️ Sin datos; fin.")
        return

    # ===== Inserción en MySQL =====
    conn = None
    lock_obtained = False
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Sesión menos contenciosa
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cur.execute("SET SESSION innodb_lock_wait_timeout = 60")

        # Candado lógico por tienda (evita 2 procesos concurrentes sobre la misma tienda)
        cur.execute("SELECT GET_LOCK(CONCAT('scrap_', %s), 30)", (TIENDA_CODIGO,))
        got = cur.fetchone()[0]
        lock_obtained = bool(got)
        if not lock_obtained:
            print("⚠️ No se pudo adquirir candado lógico de tienda; abortando para evitar carreras.")
            return

        conn.autocommit = False
        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        conn.commit()

        insertados = 0
        batch_count = 0

        for _, r in df.iterrows():
            # Derivar tipo oferta / promo
            tipo_oferta = r.get("Tipo de Oferta") or ""
            promo_tipo = None
            if tipo_oferta and tipo_oferta not in ("Precio Regular", "Oferta"):
                promo_tipo = tipo_oferta  # texto del teaser si aplica

            p = {
                "sku": clean(r.get("Código Interno")),      # SKU (itemId)
                "record_id": clean(r.get("productId")),     # productId VTEX
                "nombre": clean(r.get("Nombre Producto")),
                "marca": clean(r.get("Marca")),
                "fabricante": clean(r.get("Fabricante")),
                "categoria": clean(r.get("Categoría")),
                "subcategoria": clean(r.get("Subcategoría")),
                "precio_lista": r.get("Precio de Lista"),
                "precio_oferta": r.get("Precio de Oferta"),
                "tipo_oferta": "Oferta" if tipo_oferta == "Oferta" else ("Precio regular" if tipo_oferta in ("", "Precio Regular") else tipo_oferta),
                "promo_tipo": promo_tipo,
                "precio_regular_promo": None,
                "precio_descuento": None,
                "comentarios_promo": None,
                "url": clean(r.get("URL")),
            }

            producto_id = find_or_create_producto(cur, p)     # ean siempre NULL
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p)

            insertados += 1
            batch_count += 1

            if batch_count >= BATCH:
                conn.commit()
                batch_count = 0

        if batch_count > 0:
            conn.commit()

        print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} (EAN NULL)")
    except MySQLError as e:
        if conn:
            conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if lock_obtained and conn:
                cur = conn.cursor()
                cur.execute("SELECT RELEASE_LOCK(CONCAT('scrap_', %s))", (TIENDA_CODIGO,))
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    print(f"⏱️ Tiempo total: {time.time() - t0:.2f} s")

if __name__ == "__main__":
    import re  # asegura 're' importado para helpers arriba
    main()
