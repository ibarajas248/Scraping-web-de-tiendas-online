#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cormoran (VTEX) — Dump completo + Upload MySQL

Mejoras clave vs versión previa:
- API VTEX: prueba sin sc y sc=1..MAX_SC; paginación _from/_to con corte por
  MAX_EMPTY_PAGES consecutivas vacías y MAX_PAGES_API por seguridad.
- Fallback HTML: paginación sobre page, PageNumber y p + rel="next".
- Normalización robusta de filas por SKU.
- Logs detallados para detectar categorías “problemáticas”.

Dependencias:
  pip install requests pandas openpyxl mysql-connector-python

Requiere:
  - base_datos.get_conn() -> mysql.connector.connect(...)
  - Tablas: tiendas, productos, producto_tienda, historico_precios
"""

from __future__ import annotations
import sys, os, argparse, json, re, time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # type: ignore

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError

# ===================== Config =====================
BASE_DEFAULT = "https://www.cormoran.com.ar"
TIENDA_CODIGO = "cormoran"
TIENDA_NOMBRE = "Cormoran"

STEP_DEFAULT = 50                 # VTEX soporta hasta 50
PAUSE_DEFAULT = 0.25              # pausa entre requests HTML
TREE_DEPTH = 50                   # profundidad del árbol VTEX
MAX_SC = 20                       # sc=1..MAX_SC (además se prueba sin sc)
MAX_PAGES_API = 1000000              # hard-stop por categoría en API
MAX_EMPTY_PAGES = 2               # corta tras N páginas vacías consecutivas en API
MAX_PAGES_HTML = 300              # hard-stop HTML (por seguridad)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

# ================== Sesión HTTP ===================
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS.copy())
    return s

# ================ Categorías VTEX =================
def fetch_category_tree(session: requests.Session, base: str) -> List[Dict[str, Any]]:
    url = f"{base.rstrip('/')}/api/catalog_system/pub/category/tree/{TREE_DEPTH}"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Formato inesperado del árbol de categorías")
    return data

def flatten_categories(tree: List[Dict[str, Any]]) -> List[Tuple[int, str, str]]:
    flat: List[Tuple[int, str, str]] = []

    def walk(node: Dict[str, Any], path: List[str]):
        cid = int(node.get("id"))
        name = str(node.get("name"))
        url = str(node.get("url"))
        full_name = " / ".join(path + [name]) if path else name
        flat.append((cid, full_name, url))
        for child in node.get("children", []) or []:
            walk(child, path + [name])

    for n in tree:
        walk(n, [])

    # únicos por id
    seen = set()
    uniq: List[Tuple[int, str, str]] = []
    for t in flat:
        if t[0] not in seen:
            seen.add(t[0])
            uniq.append(t)
    return uniq

# ======= API VTEX (robusta: sc y paginación) =======
def fetch_products_api_all(
    session: requests.Session,
    base: str,
    category_id: int,
    step: int = STEP_DEFAULT,
    max_sc: int = MAX_SC,
    max_pages_api: int = MAX_PAGES_API,
    max_empty_pages: int = MAX_EMPTY_PAGES,
) -> List[Dict[str, Any]]:
    """
    Extrae todos los productos de una categoría por API.
    Estrategia:
      1) probar sin 'sc' (algunas tiendas publican allí)
      2) probar sc=1..max_sc y quedarnos con la PRIMERA que dé contenido
         - Si una 'sc' posterior también tiene productos distintos, también se suma
           (esto evita “huecos” en catálogos multi-canal).
    """
    search_url = f"{base.rstrip('/')}/api/catalog_system/pub/products/search"
    all_products: List[Dict[str, Any]] = []
    all_keys: set = set()

    def page_iter(params_base: Dict[str, Any]) -> int:
        """Itera páginas _from/_to; devuelve cuántos ítems nuevos agregó."""
        empty_streak = 0
        pages_seen = 0
        new_items_count = 0
        _from = 0

        while True:
            if pages_seen >= max_pages_api:
                print(f"   [api] corte por MAX_PAGES_API={max_pages_api}")
                break

            params = params_base.copy()
            params.update({"_from": _from, "_to": _from + step - 1})

            resp = session.get(search_url, params=params, timeout=40)
            if resp.status_code == 404:
                break
            if resp.status_code != 200:
                # En VTEX a veces devuelve vacío con 200, a veces 500…
                break

            try:
                data = resp.json()
            except Exception:
                break

            items = [data] if isinstance(data, dict) else list(data or [])
            # filtrar duplicados globales (por ProductId + itemId)
            added_this_page = 0
            for prod in items:
                pid = str(prod.get("productId") or "")
                for it in (prod.get("items") or []):
                    sku = str(it.get("itemId") or "")
                    key = (pid, sku)
                    if key in all_keys:
                        continue
                    all_keys.add(key)
                    all_products.append(prod)
                    added_this_page += 1

                # si no tiene items (raro), igual considerar el productId
                if not (prod.get("items") or []):
                    key = (pid, "")
                    if key not in all_keys:
                        all_keys.add(key)
                        all_products.append(prod)
                        added_this_page += 1

            if added_this_page == 0:
                empty_streak += 1
            else:
                empty_streak = 0
                new_items_count += added_this_page

            pages_seen += 1
            if added_this_page < step or empty_streak >= max_empty_pages:
                break

            _from += step
            time.sleep(0.05)

        return new_items_count

    # (a) sin sc
    base_params = {"fq": f"C:{category_id}"}
    got = page_iter(base_params)
    if got:
        print(f"   [api] sin sc -> {got} ítems nuevos")

    # (b) sc=1..max_sc — si agregan cosas distintas, también quedan
    for sc in range(1, max_sc + 1):
        params_sc = {"fq": f"C:{category_id}", "sc": sc}
        got_sc = page_iter(params_sc)
        if got_sc:
            print(f"   [api] sc={sc} -> {got_sc} ítems nuevos")
        # si no trae nada nuevo varias veces seguidas, igual seguimos:
        # hay catálogos con huecos salteados de sc

    return all_products

# ============== Fallback HTML (paginado) ==============
SCRIPT_JSONLD_RE = re.compile(
    r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE
)

def parse_jsonld_from_html(html: str) -> List[Dict[str, Any]]:
    prods: List[Dict[str, Any]] = []
    for m in SCRIPT_JSONLD_RE.finditer(html):
        content = re.sub(r'<!--.*?-->', '', m.group(1), flags=re.DOTALL)
        try:
            data = json.loads(content)
        except Exception:
            continue
        if isinstance(data, dict) and data.get('@type') == 'ItemList' and 'itemListElement' in data:
            for elem in data['itemListElement']:
                item = elem.get('item')
                if isinstance(item, dict) and item.get('@type') == 'Product':
                    prods.append(item)
        elif isinstance(data, dict) and data.get('@type') == 'Product':
            prods.append(data)
        elif isinstance(data, list):
            for d in data:
                if isinstance(d, dict) and d.get('@type') == 'Product':
                    prods.append(d)
    return prods

def has_rel_next(html: str) -> bool:
    return 'rel="next"' in html.lower()

def build_page_url(base_url: str, param: str, page: int) -> str:
    sep = '&' if ('?' in base_url) else '?'
    return f"{base_url}{sep}{param}={page}"

def fetch_products_html_paged(
    session: requests.Session, category_url: str, pause: float, max_pages_html: int
) -> List[Dict[str, Any]]:
    """
    Pagina con 'page', 'PageNumber' y 'p' + link rel="next".
    Se detiene si no encuentra JSON-LD nuevo o no hay 'next'.
    """
    products: List[Dict[str, Any]] = []
    keys: set = set()

    # página 1 (tal cual la URL)
    r = session.get(category_url, timeout=40)
    if r.status_code != 200:
        return []
    html = r.text
    first = parse_jsonld_from_html(html)
    for p in first:
        k = (str(p.get("sku") or p.get("mpn") or p.get("name") or ""), str(p.get("url") or ""))
        if k not in keys:
            keys.add(k)
            products.append(p)

    # si hay rel=next, seguimos usando el patrón de link; si no, probamos parámetros
    # estrategia: intentamos en este orden: rel="next", ?page=, ?PageNumber=, ?p=
    # — paramos cuando no haya nuevos o excedamos max_pages_html
    page = 2
    while page <= max_pages_html:
        next_tried = False
        if has_rel_next(html):
            # heurística: si hay rel="next", probamos ?page=page
            url_try = build_page_url(category_url, "page", page)
            rr = session.get(url_try, timeout=40)
            next_tried = True
            if rr.status_code == 200:
                html = rr.text
                plist = parse_jsonld_from_html(html)
                added = 0
                for p in plist:
                    k = (str(p.get("sku") or p.get("mpn") or p.get("name") or ""), str(p.get("url") or ""))
                    if k not in keys:
                        keys.add(k)
                        products.append(p)
                        added += 1
                if added == 0:
                    break
                page += 1
                time.sleep(pause)
                continue

        # si no hay rel=next o falló, probamos parámetros clásicos
        added_any = False
        for param in ("page", "PageNumber", "p"):
            url_try = build_page_url(category_url, param, page)
            rr = session.get(url_try, timeout=40)
            if rr.status_code != 200:
                continue
            html = rr.text
            plist = parse_jsonld_from_html(html)
            added = 0
            for p in plist:
                k = (str(p.get("sku") or p.get("mpn") or p.get("name") or ""), str(p.get("url") or ""))
                if k not in keys:
                    keys.add(k)
                    products.append(p)
                    added += 1
            if added > 0:
                page += 1
                added_any = True
                time.sleep(pause)
                break  # siguiente ciclo con la siguiente página
        if not added_any:
            break

    return products

# ============ Normalización de filas ============
def rows_from_vtex_product(base: str, prod: Dict[str, Any], category_name: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    product_id = prod.get("productId")
    name = prod.get("productName") or prod.get("productTitle")
    brand = prod.get("brand")
    link_text = prod.get("linkText")
    url = f"{base.rstrip('/')}/{link_text}/p" if link_text else None

    items = prod.get("items") or []
    if not items:
        rows.append({
            "ProductId": product_id, "SKU": None, "EAN": None,
            "Nombre": name, "Marca": brand, "Categoria": category_name,
            "PrecioLista": None, "PrecioOferta": None, "StockDisponible": None,
            "URL": url, "Imagen": None, "UnitMultiplier": None,
            "MeasurementUnit": None, "Seller": None,
        })
        return rows

    for item in items:
        sku = item.get("itemId")
        # EAN puede venir en item.ean o en referenceId
        ean = (
            item.get("ean")
            or next(
                (ref.get("Value")
                 for ref in (item.get("referenceId") or [])
                 if ref.get("Key") in {"EAN", "ean", "GTIN", "RefId"}),
                None,
            )
            or None
        )
        images = item.get("images") or []
        image_url = images[0].get("imageUrl") if images else None

        unit_multiplier = item.get("unitMultiplier")
        measurement_unit = item.get("measurementUnit")

        sellers = item.get("sellers") or []
        if not sellers:
            rows.append({
                "ProductId": product_id, "SKU": sku, "EAN": ean,
                "Nombre": name, "Marca": brand, "Categoria": category_name,
                "PrecioLista": None, "PrecioOferta": None, "StockDisponible": None,
                "URL": url, "Imagen": image_url, "UnitMultiplier": unit_multiplier,
                "MeasurementUnit": measurement_unit, "Seller": None,
            })
            continue

        for seller in sellers:
            offer = seller.get("commertialOffer") or {}
            rows.append({
                "ProductId": product_id,
                "SKU": sku,
                "EAN": ean,
                "Nombre": name,
                "Marca": brand,
                "Categoria": category_name,
                "PrecioLista": offer.get("ListPrice"),
                "PrecioOferta": offer.get("Price"),
                "StockDisponible": offer.get("AvailableQuantity"),
                "URL": url,
                "Imagen": image_url,
                "UnitMultiplier": unit_multiplier,
                "MeasurementUnit": measurement_unit,
                "Seller": seller.get("sellerName"),
            })
    return rows

def rows_from_jsonld(prod: Dict[str, Any], category_name: str) -> List[Dict[str, Any]]:
    name = prod.get("name")
    brand = prod.get("brand").get("name") if isinstance(prod.get("brand"), dict) else (prod.get("brand") if isinstance(prod.get("brand"), str) else None)
    sku = prod.get("sku")
    mpn = prod.get("mpn")
    url = prod.get("@id") or prod.get("url")
    image = prod.get("image")

    price = None
    list_price = None
    available = None
    offers = prod.get("offers")
    if isinstance(offers, dict):
        price = offers.get("lowPrice") or offers.get("price")
        list_price = offers.get("highPrice") or offers.get("price")
        avail = offers.get("availability") or ""
        available = 1 if ("InStock" in str(avail)) else None
    elif isinstance(offers, list) and offers:
        of0 = offers[0]
        price = of0.get("price")
        list_price = of0.get("price")
        avail = of0.get("availability") or ""
        available = 1 if ("InStock" in str(avail)) else None

    return [{
        "ProductId": mpn or None,
        "SKU": sku,
        "EAN": None,
        "Nombre": name,
        "Marca": brand,
        "Categoria": category_name,
        "PrecioLista": list_price,
        "PrecioOferta": price,
        "StockDisponible": available,
        "URL": url,
        "Imagen": image,
        "UnitMultiplier": None,
        "MeasurementUnit": None,
        "Seller": None,
    }]

# ============== Orquestador Crawl ==============
def crawl_cormoran(
    base: str = BASE_DEFAULT,
    step: int = STEP_DEFAULT,
    pause: float = PAUSE_DEFAULT,
    max_categories: Optional[int] = None,
    max_pages_html: int = MAX_PAGES_HTML,
) -> pd.DataFrame:
    session = make_session()
    tree = fetch_category_tree(session, base)
    categories = flatten_categories(tree)
    if max_categories:
        categories = categories[:max_categories]

    all_rows: List[Dict[str, Any]] = []
    seen_row_keys: set = set()

    print(f"Total categorías: {len(categories)}")

    for idx, (cid, cname, curl) in enumerate(categories, start=1):
        print(f"\n=== {idx}/{len(categories)}: {cname} (id={cid}) ===")

        # 1) API VTEX (robusta)
        try:
            api_products = fetch_products_api_all(
                session, base, cid, step=step
            )
        except Exception as e:
            print(f" [warn] API fallo en C:{cid} -> {e}")
            api_products = []

        if api_products:
            # Expandir a filas
            for prod in api_products:
                for row in rows_from_vtex_product(base, prod, cname):
                    key = (row.get("ProductId"), row.get("SKU"), row.get("Seller"))
                    if key in seen_row_keys:
                        continue
                    seen_row_keys.add(key)
                    all_rows.append(row)
            print(f"   [api] filas acumuladas: {len(all_rows)}")
            # Algunos sitios publican TODO vía API, igual mantenemos fallback por seguridad.

        # 2) Fallback HTML solo si API no arrojó nada para esta categoría
        if not api_products:
            try:
                html_products = fetch_products_html_paged(
                    session, curl, pause=pause, max_pages_html=max_pages_html
                )
            except Exception as e:
                print(f" [warn] HTML fallo en {curl} -> {e}")
                html_products = []

            if html_products:
                for prod in html_products:
                    for row in rows_from_jsonld(prod, cname):
                        key = (row.get("ProductId"), row.get("SKU"), row.get("URL"))
                        if key in seen_row_keys:
                            continue
                        seen_row_keys.add(key)
                        all_rows.append(row)
                print(f"   [html] filas acumuladas: {len(all_rows)}")
            else:
                print("   [info] categoría sin resultados en API ni HTML")

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

# ================= MySQL helpers =================
def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"nan","none","null"}:
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
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))""",
        (p.get("ean") or "", nombre, marca, p.get("fabricante") or "",
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
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))""",
        (tienda_id, producto_id, url, nombre_tienda))
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

# ===================== CLI =======================
def main() -> None:
    parser = argparse.ArgumentParser(description="Cormoran (VTEX) — Full dump + MySQL")
    parser.add_argument("--base", default=BASE_DEFAULT, help="URL base de la tienda")
    parser.add_argument("--step", type=int, default=STEP_DEFAULT, help="Tamaño de página VTEX API (1..50)")
    parser.add_argument("--pause", type=float, default=PAUSE_DEFAULT, help="Pausa entre requests HTML (seg)")
    parser.add_argument("--maxcats", type=int, default=None, help="Limitar # de categorías (debug)")
    parser.add_argument("--maxpages-html", type=int, default=MAX_PAGES_HTML, help="Máx páginas HTML por categoría")
    parser.add_argument("--csv", default=None, help="Archivo CSV adicional (opcional)")
    parser.add_argument("--outfile", default="productos_cormoran.xlsx", help="Archivo XLSX de salida")
    args = parser.parse_args()

    step = max(1, min(int(args.step), 50))  # VTEX no admite >50
    df = crawl_cormoran(
        base=args.base,
        step=step,
        pause=args.pause,
        max_categories=args.maxcats,
        max_pages_html=args.maxpages_html,
    )
    print(f"\nTotal de filas (SKU) obtenidas: {len(df)}")

    # Exportar
    try:
        df.to_excel(args.outfile, index=False)
    except Exception as e:
        print(f"[warn] No se pudo escribir XLSX ({e}). Continuo…")
    if args.csv:
        df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    print(f"Datos exportados a {args.outfile}" + (f" y {args.csv}" if args.csv else ""))

    # Subir a MySQL
    upload_cormoran_df_to_mysql(df)

if __name__ == "__main__":
    main()
