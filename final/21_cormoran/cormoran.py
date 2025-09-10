#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cormoran (VTEX) -> Scraper + Upload MySQL

Dependencias:
  pip install requests pandas openpyxl mysql-connector-python

Requiere:
  - base_datos.get_conn() que devuelva una conexión mysql.connector.connect(...)
  - Tablas: tiendas, productos, producto_tienda, historico_precios
"""

from __future__ import annotations
from mysql.connector import Error as MySQLError
import sys, os
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn
import argparse
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError


# =============== Config de la tienda (ajustable) ===============
BASE_DEFAULT = "https://www.cormoran.com.ar"
TIENDA_CODIGO = "cormoran"
TIENDA_NOMBRE = "Cormoran"
STEP_DEFAULT = 50
PAUSE_DEFAULT = 0.3

# =============== Sesión HTTP con reintentos ===============
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    })
    return s

# =============== Categorías ===============
def fetch_category_tree(session: requests.Session, base: str) -> List[Dict[str, any]]:
    url = f"{base.rstrip('/')}/api/catalog_system/pub/category/tree/50"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Formato inesperado del árbol de categorías")
    return data

def flatten_categories(tree: List[Dict[str, any]]) -> List[Tuple[int, str, str]]:
    flat: List[Tuple[int, str, str]] = []

    def walk(node: Dict[str, any], path: List[str]):
        cid = int(node.get("id"))
        name = str(node.get("name"))
        url = str(node.get("url"))
        full_name = " / ".join(path + [name]) if path else name
        flat.append((cid, full_name, url))
        for child in node.get("children", []) or []:
            walk(child, path + [name])

    for n in tree:
        walk(n, [])

    # elimina duplicados por id
    seen = set()
    unique: List[Tuple[int, str, str]] = []
    for tup in flat:
        if tup[0] not in seen:
            seen.add(tup[0])
            unique.append(tup)
    return unique

# =============== Productos por API VTEX / Fallback HTML ===============
def try_fetch_products_api(
    session: requests.Session,
    base: str,
    category_id: int,
    step: int = 50,
    max_sc: int = 15,
) -> Optional[List[Dict[str, any]]]:
    """
    Busca productos por categoría usando VTEX API. Prueba sc=1..max_sc.
    """
    search_url = f"{base.rstrip('/')}/api/catalog_system/pub/products/search"
    for sc in range(1, max_sc + 1):
        products: List[Dict[str, any]] = []
        _from = 0
        found_valid = False
        while True:
            params = {
                "fq": f"C:{category_id}",
                "_from": _from,
                "_to": _from + step - 1,
                "sc": sc,
            }
            resp = session.get(search_url, params=params, timeout=30)
            text = resp.text.strip()
            if "Sales channel not found" in text or resp.status_code == 404:
                break
            if resp.status_code != 200:
                break
            try:
                data = resp.json()
            except Exception:
                break
            if not data:
                break
            page = [data] if isinstance(data, dict) else list(data)
            products.extend(page)
            found_valid = True
            if len(page) < step:
                break
            _from += step
            time.sleep(0.1)
        if found_valid and products:
            return products
    return None

def parse_jsonld_from_html(html: str) -> List[Dict[str, any]]:
    """
    Extrae productos de JSON-LD (itemListElement/Product) en el HTML.
    """
    products: List[Dict[str, any]] = []
    for match in re.finditer(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE,
    ):
        content = match.group(1)
        content_clean = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
        try:
            data = json.loads(content_clean)
        except Exception:
            continue
        if isinstance(data, dict) and 'itemListElement' in data:
            for elem in data['itemListElement']:
                item = elem.get('item')
                if isinstance(item, dict) and item.get('@type') == 'Product':
                    products.append(item)
        elif isinstance(data, dict) and data.get('@type') == 'Product':
            products.append(data)
    return products

def fetch_products_html(session: requests.Session, category_url: str) -> List[Dict[str, any]]:
    r = session.get(category_url, timeout=30)
    r.raise_for_status()
    return parse_jsonld_from_html(r.text)

# =============== Normalización de filas ===============
def product_rows_from_vtex_product(
    base: str, prod: Dict[str, any], category_name: str
) -> List[Dict[str, any]]:
    rows: List[Dict[str, any]] = []
    product_id = prod.get("productId")
    name = prod.get("productName") or prod.get("productTitle")
    brand = prod.get("brand")
    link_text = prod.get("linkText")
    url = f"{base.rstrip('/')}/{link_text}/p" if link_text else None

    for item in prod.get("items", []) or []:
        sku = item.get("itemId")
        ean = (
            item.get("ean")
            or next(
                (ref.get("Value") for ref in item.get("referenceId", []) or []
                 if ref.get("Key") in {"EAN", "ean", "GTIN", "RefId"}),
                None,
            )
            or None
        )
        sellers = item.get("sellers") or []
        seller_data = sellers[0] if sellers else {}
        offer = seller_data.get("commertialOffer", {}) if seller_data else {}
        price = offer.get("Price")
        list_price = offer.get("ListPrice")
        available_qty = offer.get("AvailableQuantity")
        image_url = None
        images = item.get("images") or []
        if images:
            image_url = images[0].get("imageUrl")
        unit_multiplier = item.get("unitMultiplier")
        measurement_unit = item.get("measurementUnit")
        seller_name = seller_data.get("sellerName") if seller_data else None

        rows.append({
            "ProductId": product_id,
            "SKU": sku,
            "EAN": ean,
            "Nombre": name,
            "Marca": brand,
            "Categoria": category_name,
            "PrecioLista": list_price,
            "PrecioOferta": price,
            "StockDisponible": available_qty,
            "URL": url,
            "Imagen": image_url,
            "UnitMultiplier": unit_multiplier,
            "MeasurementUnit": measurement_unit,
            "Seller": seller_name,
        })

    if not rows:
        rows.append({
            "ProductId": product_id, "SKU": None, "EAN": None,
            "Nombre": name, "Marca": brand, "Categoria": category_name,
            "PrecioLista": None, "PrecioOferta": None, "StockDisponible": None,
            "URL": url, "Imagen": None, "UnitMultiplier": None,
            "MeasurementUnit": None, "Seller": None,
        })
    return rows

def product_rows_from_jsonld(prod: Dict[str, any], category_name: str) -> List[Dict[str, any]]:
    name = prod.get("name")
    brand = prod.get("brand").get("name") if isinstance(prod.get("brand"), dict) else None
    sku = prod.get("sku")
    mpn = prod.get("mpn")
    url = prod.get("@id") or prod.get("url")
    image = prod.get("image")
    offers = prod.get("offers") or {}
    price = None
    list_price = None
    available = None
    if isinstance(offers, dict):
        price = offers.get("lowPrice") or offers.get("price")
        list_price = offers.get("highPrice") or offers.get("price")
        available = 1 if offers.get("offers") else None
    elif isinstance(offers, list) and offers:
        offer = offers[0]
        price = offer.get("price")
        list_price = offer.get("price")
        avail = offer.get("availability") or ""
        available = 1 if "InStock" in avail else 0

    return [{
        "ProductId": mpn, "SKU": sku, "EAN": None,
        "Nombre": name, "Marca": brand, "Categoria": category_name,
        "PrecioLista": list_price, "PrecioOferta": price, "StockDisponible": available,
        "URL": url, "Imagen": image, "UnitMultiplier": None,
        "MeasurementUnit": None, "Seller": None,
    }]

# =============== Crawl orquestador ===============
def crawl_cormoran(
    base: str = BASE_DEFAULT,
    step: int = STEP_DEFAULT,
    pause: float = PAUSE_DEFAULT,
    max_categories: Optional[int] = None,
) -> pd.DataFrame:
    session = make_session()
    tree = fetch_category_tree(session, base)
    categories = flatten_categories(tree)
    if max_categories:
        categories = categories[:max_categories]

    all_rows: List[Dict[str, any]] = []
    seen_keys: set = set()
    total_categories = len(categories)

    for idx, (cid, cname, curl) in enumerate(categories, start=1):
        print(f"=== Categoria {idx}/{total_categories}: {cname} ===")

        # API VTEX
        api_data = None
        try:
            api_data = try_fetch_products_api(session, base, cid, step=step)
        except Exception:
            api_data = None

        if api_data:
            for prod in api_data:
                rows = product_rows_from_vtex_product(base, prod, cname)
                for row in rows:
                    key = (row.get("ProductId"), row.get("SKU"))
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    all_rows.append(row)
                    nombre = row.get("Nombre") or ""
                    precio = row.get("PrecioOferta") or row.get("PrecioLista") or "-"
                    stock_text = "OK" if row.get("StockDisponible") else "SIN STOCK"
                    print(f" - {nombre[:60]} | ${precio} | {stock_text}")
            continue

        # Fallback HTML
        try:
            html_products = fetch_products_html(session, curl)
        except Exception as e:
            print(f" [warn] No se pudo procesar {curl}: {e}")
            continue

        for prod in html_products:
            rows = product_rows_from_jsonld(prod, cname)
            for row in rows:
                key = (row.get("ProductId"), row.get("SKU"))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                all_rows.append(row)
                nombre = row.get("Nombre") or ""
                precio = row.get("PrecioOferta") or row.get("PrecioLista") or "-"
                stock_text = "OK" if row.get("StockDisponible") else "SIN STOCK"
                print(f" - {nombre[:60]} | ${precio} | {stock_text}")

        time.sleep(pause)

    df = pd.DataFrame(all_rows)
    cols = [
        "ProductId","SKU","EAN","Nombre","Marca","Categoria",
        "PrecioLista","PrecioOferta","StockDisponible","URL",
        "Imagen","UnitMultiplier","MeasurementUnit","Seller",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].drop_duplicates().reset_index(drop=True)
    return df

# =============== MySQL: helpers ===============
 # <- tu conexión MySQL

def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    return s

def _price(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return str(round(float(v), 2))
    try:
        x = float(str(v).replace(",", "."))
        return str(round(x, 2))
    except Exception:
        return None

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: dict) -> int:
    # p: {"ean","nombre","marca","fabricante","categoria","subcategoria"}
    ean = _clean(p.get("ean"))
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        r = cur.fetchone()
        if r:
            pid = r[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (p.get("nombre") or "", p.get("marca") or "", p.get("fabricante") or "",
                  p.get("categoria") or "", p.get("subcategoria") or "", pid))
            return pid

    nombre = _clean(p.get("nombre")) or ""
    marca  = _clean(p.get("marca")) or ""
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca))
        r = cur.fetchone()
        if r:
            pid = r[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (p.get("ean") or "", p.get("fabricante") or "",
                  p.get("categoria") or "", p.get("subcategoria") or "", pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (p.get("ean") or "", nombre, marca, p.get("fabricante") or "",
          p.get("categoria") or "", p.get("subcategoria") or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: dict) -> int:
    # p: {"sku_tienda","record_id_tienda","url_tienda","nombre_tienda"}
    sku = _clean(p.get("sku_tienda"))
    rec = _clean(p.get("record_id_tienda"))
    url = p.get("url_tienda") or ""
    nombre_tienda = p.get("nombre_tienda") or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid

    if rec:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: dict, capturado_en: datetime):
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
        _price(p.get("precio_lista")), _price(p.get("precio_oferta")),
        p.get("tipo_oferta") or None, None, None, None, None
    ))

def upload_cormoran_df_to_mysql(df: pd.DataFrame) -> None:
    """
    Sube el DataFrame a MySQL usando el esquema tiendas/productos/producto_tienda/historico_precios.
    Columnas esperadas en df:
      ProductId, SKU, EAN, Nombre, Marca, Categoria,
      PrecioLista, PrecioOferta, URL, Imagen, UnitMultiplier, MeasurementUnit, Seller
    """
    if df is None or df.empty:
        print("⚠️ DataFrame vacío: no hay nada para subir.")
        return

    now = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        inserted = 0
        for _, r in df.iterrows():
            prod = {
                "ean": _clean(r.get("EAN")),
                "nombre": _clean(r.get("Nombre")),
                "marca": _clean(r.get("Marca")),
                "fabricante": None,
                "categoria": _clean(r.get("Categoria")),
                "subcategoria": None,
            }
            pid = find_or_create_producto(cur, prod)

            pt = {
                "sku_tienda": _clean(r.get("SKU")),
                "record_id_tienda": _clean(r.get("ProductId")),
                "url_tienda": _clean(r.get("URL")),
                "nombre_tienda": _clean(r.get("Nombre")),
            }
            pt_id = upsert_producto_tienda(cur, tienda_id, pid, pt)

            hist = {
                "precio_lista": r.get("PrecioLista"),
                "precio_oferta": r.get("PrecioOferta"),
                "tipo_oferta": None,
            }
            insert_historico(cur, tienda_id, pt_id, hist, now)
            inserted += 1

        conn.commit()
        print(f"💾 MySQL OK: {inserted} registros para {TIENDA_NOMBRE} ({now})")

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

# =============== Main CLI ===============
def main() -> None:
    parser = argparse.ArgumentParser(description="Dump completo de productos de Cormoran + MySQL")
    parser.add_argument("--base", default=BASE_DEFAULT, help="URL base de la tienda")
    parser.add_argument("--step", type=int, default=STEP_DEFAULT, help="Tamaño de página para la API VTEX (máx 50)")
    parser.add_argument("--outfile", default="productos_cormoran.xlsx", help="Archivo XLSX de salida")
    parser.add_argument("--csv", default=None, help="Archivo CSV adicional (opcional)")
    parser.add_argument("--pause", type=float, default=PAUSE_DEFAULT, help="Pausa entre requests HTML (seg)")
    parser.add_argument("--maxcats", type=int, default=None, help="Limitar número de categorías (debug)")
    args = parser.parse_args()

    df = crawl_cormoran(
        base=args.base,
        step=args.step,
        pause=args.pause,
        max_categories=args.maxcats,
    )
    print(f"\nTotal de filas (SKU) obtenidas: {len(df)}")

    # Exportar a disco
    #df.to_excel(args.outfile, index=False)
    if args.csv:
        df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    print(f"Datos exportados a {args.outfile}" + (f" y {args.csv}" if args.csv else ""))

    # Subir a MySQL
    upload_cormoran_df_to_mysql(df)

if __name__ == "__main__":
    main()
