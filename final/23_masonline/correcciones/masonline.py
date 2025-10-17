#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Masonline (VTEX) — Ingesta MySQL por productClusterIds con fallback recursivo por ft
(con truncado DINÁMICO según límites reales de columnas en MySQL)

- Intenta la ventana estándar (_from/_to, hasta ~2.500).
- Si topa la ventana, divide recursivamente por 'ft' (0–9, a–z, áéíóúüñ) hasta 3 chars.
- Si aún faltan, fallback por categorías (category-1).
- Dedup por productId/SKU.
- Inserta/actualiza en: tiendas, productos, producto_tienda, historico_precios.

Requisitos:
  pip install requests pandas mysql-connector-python urllib3 xlsxwriter

Config MySQL:
  from base_datos import get_conn  # Debe devolver mysql.connector.connect(...)
"""

import time
import argparse
import string
from typing import List, Dict, Any, Optional, Tuple, Set

import requests
from requests.adapters import HTTPAdapter
from requests import HTTPError
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np
import datetime as dt
import sys, os
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- helper de conexión mysql.connector

# ---------------- Identidad de tienda ----------------
TIENDA_CODIGO = "masonline"
TIENDA_NOMBRE = "Masonline (VTEX)"

# ---------------- Límites por defecto (se sobreescriben dinámicamente) ----------------
DEFAULT_LIMITS = {
    ("historico_precios", "tipo_oferta"): 255,
    ("historico_precios", "promo_comentarios"): 1000,
    ("historico_precios", "promo_texto_regular"): 255,
    ("historico_precios", "promo_texto_descuento"): 255,
    ("producto_tienda", "nombre_tienda"): 255,
}

# se rellena al conectar
DB_LIMITS: Dict[Tuple[str, str], Optional[int]] = {}

def _truncate_dyn(s: Optional[str], table: str, column: str) -> Optional[str]:
    """Trunca usando el límite real de la columna en MySQL (o el default si no se pudo leer).
       Si la columna es TEXT (sin max length), no trunca.
    """
    if s is None:
        return None
    s = str(s)
    limit = DB_LIMITS.get((table, column))
    if limit is None:
        return s
    if len(s) > limit:
        return s[:limit]
    return s

def _parse_price(val) -> Optional[str]:
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

# ---------------- Config VTEX ----------------
BASE = "https://www.masonline.com.ar"
SEARCH_API = f"{BASE}/api/catalog_system/pub/products/search"
FACETS_API = f"{BASE}/api/catalog_system/pub/facets/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

STEP = 50                  # VTEX: _to - _from <= 49
SLEEP_BETWEEN = 0.30
MAX_WINDOW_RESULTS = 2500  # 50 páginas * 50 ítems
ORDER_BY = "OrderByNameASC"

# Alfabeto extendido para español
ALPHA_TERMS_EXT = list(string.digits + string.ascii_lowercase) + list("áéíóúüñ")

# Canal de ventas opcional (None si no aplica en la tienda)
SALES_CHANNEL: Optional[int] = None

# ---------------- Sesión HTTP ----------------
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=40, pool_maxsize=40))
    s.headers.update(HEADERS)
    return s

def _append_sc(params: Dict[str, Any]) -> Dict[str, Any]:
    if SALES_CHANNEL is not None:
        params = dict(params)
        params["sc"] = SALES_CHANNEL
    return params

# ---------------- Facets / Estimación ----------------
def _deep_find_ints(obj) -> List[int]:
    """Extrae posibles contadores (Quantity/quantity/recordsFiltered) recursivamente."""
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("recordsFiltered", "Quantity", "quantity", "RecordsFiltered", "Total", "total"):
                try:
                    found.append(int(v))
                except Exception:
                    pass
            found.extend(_deep_find_ints(v))
    elif isinstance(obj, list):
        for it in obj:
            found.extend(_deep_find_ints(it))
    return found

def estimate_count(session: requests.Session, cluster_id: str, term: Optional[str] = None) -> Optional[int]:
    """
    Estima cuántos productos hay para (cluster_id, ft=term) usando facets.
    Si no se puede determinar, devuelve None.
    """
    try:
        if term:
            url = f"{FACETS_API}/{cluster_id}/{term}"
            params = {"map": "productClusterIds,ft"}
        else:
            url = f"{FACETS_API}/{cluster_id}"
            params = {"map": "productClusterIds"}
        params = _append_sc(params)
        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "recordsFiltered" in data:
            return int(data["recordsFiltered"])
        ints = _deep_find_ints(data)
        return max(ints) if ints else None
    except Exception:
        return None

# ---------------- Fetchers VTEX ----------------
def fetch_page(session: requests.Session, cluster_id: str, start: int, step: int) -> List[Dict[str, Any]]:
    params = {
        "fq": f"productClusterIds:{cluster_id}",
        "_from": start,
        "_to": start + step - 1,
        "O": ORDER_BY,
    }
    params = _append_sc(params)
    r = session.get(SEARCH_API, params=params, timeout=30)
    if r.status_code == 400:
        raise HTTPError("VTEX 50-page window reached", response=r)
    r.raise_for_status()
    try:
        data = r.json()
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        return data if isinstance(data, list) else []
    except Exception:
        return []

def fetch_page_alpha(session: requests.Session, cluster_id: str, term: str, start: int, step: int) -> List[Dict[str, Any]]:
    url = f"{SEARCH_API}/{cluster_id}/{term}"
    params = {
        "map": "productClusterIds,ft",
        "_from": start,
        "_to": start + step - 1,
        "O": ORDER_BY,
    }
    params = _append_sc(params)
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 400:
        raise HTTPError("Bad Request on alpha slice", response=r)
    r.raise_for_status()
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []

def fetch_page_by_cat(session: requests.Session, cluster_id: str, cat_slug: str, start: int, step: int) -> List[Dict[str, Any]]:
    url = f"{SEARCH_API}/{cluster_id}/{cat_slug}"
    params = _append_sc({
        "map": "productClusterIds,category-1",
        "_from": start,
        "_to": start + step - 1,
        "O": ORDER_BY,
    })
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 400:
        raise HTTPError("Bad Request on category slice", response=r)
    r.raise_for_status()
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []

# ---------------- Helpers de parseo ----------------
def split_categories(paths: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not paths:
        return None, None, None
    best = max(paths, key=lambda p: p.count("/"))
    parts = [p for p in best.strip("/").split("/") if p]
    categoria = parts[0] if len(parts) >= 1 else None
    subcategoria = " > ".join(parts[1:]) if len(parts) > 1 else None
    ruta_full = " / ".join(parts) if parts else None
    return categoria, subcategoria, ruta_full

def extract_offer_type(p: Dict[str, Any], item: Dict[str, Any]) -> str:
    names = []
    sellers = item.get("sellers") or []
    for s in sellers:
        co = (s or {}).get("commertialOffer") or {}
        for t in co.get("Teasers") or []:
            n = (t or {}).get("Name") or (t or {}).get("name")
            if n:
                names.append(str(n))
        for t in co.get("PromotionTeasers") or []:
            n = (t or {}).get("Name") or (t or {}).get("name")
            if n:
                names.append(str(n))
    clusters = p.get("productClusters") or {}
    for _, cname in clusters.items():
        if isinstance(cname, str) and cname:
            names.append(cname)
    names = list(dict.fromkeys([n.strip() for n in names if n and n.strip()]))
    return " | ".join(names)

def choose_seller(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sellers = item.get("sellers") or []
    for s in sellers:
        if s.get("sellerDefault"):
            return s
    for s in sellers:
        co = (s or {}).get("commertialOffer") or {}
        if co.get("IsAvailable"):
            return s
    return sellers[0] if sellers else None

def flatten(products: List[Dict[str, Any]], cluster_id: str, verbose: bool = False) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for p in products:
        categoria, subcategoria, _ruta = split_categories(p.get("categories") or [])
        brand = p.get("brand")
        manufacturer = p.get("Manufacturer") or p.get("manufacturer") or None
        url = p.get("link") or f"{BASE}/{p.get('linkText')}/p"
        cluster_name = None
        pcs = p.get("productClusters") or {}
        if cluster_id in pcs:
            cluster_name = pcs.get(cluster_id)

        for it in p.get("items") or []:
            ean = it.get("ean") or None
            ref_val = None
            for ref in it.get("referenceId") or []:
                if (ref or {}).get("Key") == "RefId":
                    ref_val = ref.get("Value")
                    break
            if not ref_val:
                ref_val = p.get("productReference") or it.get("itemId")

            seller = choose_seller(it) or {}
            co = (seller.get("commertialOffer") or {}) if seller else {}
            price = co.get("Price")
            list_price = co.get("ListPrice")
            tipo_oferta = extract_offer_type(p, it)

            row = {
                "EAN": ean,
                "CodigoInterno": ref_val,
                "NombreProducto": p.get("productName") or it.get("name"),
                "Categoria": categoria,
                "Subcategoria": subcategoria,
                "Marca": brand,
                "Fabricante": manufacturer,
                "PrecioLista": list_price,
                "PrecioOferta": price,
                "TipoOferta": tipo_oferta,
                "URL": url,
                "SKU": it.get("itemId"),
                "ProductId": p.get("productId"),
                "ClusterId": cluster_id,
                "ClusterNombre": cluster_name,
            }
            rows.append(row)

            if verbose:
                print(
                    f"➡ {row['EAN'] or '-'} | {row['NombreProducto']} | {row['Marca'] or '-'} | "
                    f"Lista: {row['PrecioLista'] if row['PrecioLista'] is not None else '-'} | "
                    f"Oferta: {row['PrecioOferta'] if row['PrecioOferta'] is not None else '-'} | "
                    f"{row['URL']}",
                    flush=True
                )
    return rows

# ---------------- Paginador genérico ----------------
def paginate_term(session, start_term_fetcher, step=STEP):
    """
    start_term_fetcher(start:int, step:int) -> List[dict]
    Pagina hasta que (len < step) o 400 (límite VTEX).
    """
    start = 0
    while True:
        try:
            chunk = start_term_fetcher(start, step)
        except HTTPError as e:
            # Golpeó la ventana VTEX
            break
        if not chunk:
            break
        yield chunk
        start += step
        time.sleep(SLEEP_BETWEEN)
        if len(chunk) < step:
            break

# ---------------- Scraping por categorías (fallback) ----------------
def fetch_categories_for_cluster(session, cluster_id) -> List[str]:
    """
    Devuelve slugs de categoría nivel 1 relevantes al cluster,
    p.ej. ['alimentos-y-bebidas','hogar',...]
    """
    try:
        url = f"{FACETS_API}/{cluster_id}"
        params = _append_sc({"map": "productClusterIds"})
        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        cats = []
        for k in ("Departments", "CategoriesTrees"):
            nodes = data.get(k) or []
            for node in nodes:
                if isinstance(node, dict):
                    link = (node.get("Link") or node.get("link") or "").strip("/")
                    if link:
                        slug = link.split("/")[-1]
                        if slug:
                            cats.append(slug)
        return list(dict.fromkeys(cats))
    except Exception:
        return []

def scrape_by_categories(session, cluster_id, seen_products) -> List[Dict[str, Any]]:
    rows = []
    cats = fetch_categories_for_cluster(session, cluster_id)
    for cat in cats:
        print(f"[categoría] {cat}", flush=True)
        def _fetch(start, step):
            return fetch_page_by_cat(session, cluster_id, cat, start, step)
        for chunk in paginate_term(session, _fetch, STEP):
            fresh = [p for p in chunk if p.get("productId") not in seen_products]
            for p in fresh:
                seen_products.add(p.get("productId"))
            rows.extend(flatten(fresh, cluster_id, verbose=True))
    return rows

# ---------------- Scraping recursivo por ft ----------------
def scrape_by_ft_recursive(session, cluster_id, prefix="", depth=0, seen_products=None, max_depth=3) -> List[Dict[str, Any]]:
    """
    Divide recursivamente por ft. Si la partición (prefix) puede exceder la ventana,
    se subdivide añadiendo más caracteres. depth limita la longitud (hasta max_depth).
    """
    if seen_products is None:
        seen_products = set()
    rows: List[Dict[str, Any]] = []

    # 0) Estimar conteo para decidir si subdividir
    est = estimate_count(session, cluster_id, prefix) if prefix else estimate_count(session, cluster_id, None)
    if est is not None and est > MAX_WINDOW_RESULTS and depth < max_depth:
        for ch in ALPHA_TERMS_EXT:
            child = f"{prefix}{ch}"
            print(f"→ subdividir '{prefix}' → '{child}' (est={est})", flush=True)
            rows.extend(scrape_by_ft_recursive(session, cluster_id, child, depth+1, seen_products, max_depth))
        return rows

    # 1) Paginar esta partición
    if prefix:
        print(f"[ft='{prefix}'] (depth={depth}, est={est})", flush=True)
        def _fetch(start, step):
            return fetch_page_alpha(session, cluster_id, prefix, start, step)
        for chunk in paginate_term(session, _fetch, STEP):
            fresh = [p for p in chunk if p.get("productId") not in seen_products]
            for p in fresh:
                seen_products.add(p.get("productId"))
            rows.extend(flatten(fresh, cluster_id, verbose=True))
    else:
        # prefix vacío → usa ventana estándar primero
        start = 0
        while True:
            try:
                if start >= MAX_WINDOW_RESULTS:
                    print(f"Ventana estándar alcanzada; cambiando a ft recursivo…", flush=True)
                    for ch in ALPHA_TERMS_EXT:
                        rows.extend(scrape_by_ft_recursive(session, cluster_id, prefix=ch, depth=1, seen_products=seen_products, max_depth=max_depth))
                    break
                chunk = fetch_page(session, cluster_id, start, STEP)
            except HTTPError as e:
                print("HTTP 400 en estándar → partición ft recursiva…", flush=True)
                for ch in ALPHA_TERMS_EXT:
                    rows.extend(scrape_by_ft_recursive(session, cluster_id, prefix=ch, depth=1, seen_products=seen_products, max_depth=max_depth))
                break

            if not chunk:
                break
            fresh = [p for p in chunk if p.get("productId") not in seen_products]
            for p in fresh:
                seen_products.add(p.get("productId"))
            rows.extend(flatten(fresh, cluster_id, verbose=True))
            start += STEP
            time.sleep(SLEEP_BETWEEN)
            if len(chunk) < STEP:
                break

    return rows

# ---------------- Scraper principal de cluster ----------------
def scrape_cluster(cluster_id: str, max_depth: int = 3) -> pd.DataFrame:
    session = make_session()
    seen_ids: Set[str] = set()

    # 1) Estándar + recursivo por ft
    all_rows = scrape_by_ft_recursive(session, cluster_id, prefix="", depth=0, seen_products=seen_ids, max_depth=max_depth)

    # 2) Comparar contra estimate_count y fallback por categorías si faltan
    est_total = estimate_count(session, cluster_id, None)
    unique_products = len({r["ProductId"] for r in all_rows})
    if est_total is not None and unique_products < est_total:
        print(f"Aún faltan productos según facets (tenemos {unique_products} de ~{est_total}) → scraping por categorías…", flush=True)
        extra = scrape_by_categories(session, cluster_id, seen_ids)
        all_rows.extend(extra)

    df = pd.DataFrame(all_rows)
    cols = [
        "EAN", "CodigoInterno", "NombreProducto", "Categoria", "Subcategoria",
        "Marca", "Fabricante", "PrecioLista", "PrecioOferta", "TipoOferta",
        "URL", "SKU", "ProductId", "ClusterId", "ClusterNombre"
    ]
    if not df.empty:
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df.reindex(columns=cols)
    return df

# ---------------- Helpers SQL dinámicos ----------------
def load_db_limits(cur) -> None:
    """Carga DB_LIMITS leyendo INFORMATION_SCHEMA. Si no encuentra, usa DEFAULT_LIMITS.
       Si CHARACTER_MAXIMUM_LENGTH es NULL (TEXT), dejamos None (sin truncado).
    """
    global DB_LIMITS
    DB_LIMITS = DEFAULT_LIMITS.copy()
    targets = [
        ("historico_precios", "tipo_oferta"),
        ("historico_precios", "promo_comentarios"),
        ("historico_precios", "promo_texto_regular"),
        ("historico_precios", "promo_texto_descuento"),
        ("producto_tienda", "nombre_tienda"),
    ]
    cur.execute("SELECT DATABASE()")
    dbname = cur.fetchone()[0]

    for table, column in targets:
        cur.execute("""
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
            LIMIT 1
        """, (dbname, table, column))
        row = cur.fetchone()
        if row is None:
            continue
        maxlen = row[0]
        if maxlen is None:
            DB_LIMITS[(table, column)] = None
        else:
            try:
                DB_LIMITS[(table, column)] = int(maxlen)
            except Exception:
                pass

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = (r.get("EAN") or None)
    nombre = (r.get("NombreProducto") or "").strip()
    marca = (r.get("Marca") or None)
    fabricante = (r.get("Fabricante") or None)
    categoria = (r.get("Categoria") or None)
    subcategoria = (r.get("Subcategoria") or None)

    # 1) Por EAN
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(%s, marca),
                  fabricante = COALESCE(%s, fabricante),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (nombre, marca, fabricante, categoria, subcategoria, pid))
            return pid

    # 2) Por (nombre, marca)
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca or ""))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(%s, ean),
                  fabricante = COALESCE(%s, fabricante),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (ean, fabricante, categoria, subcategoria, pid))
            return pid

    # 3) Insert nuevo
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    sku = (r.get("SKU") or None)
    url = (r.get("URL") or None)
    nombre_tienda = _truncate_dyn((r.get("NombreProducto") or None), "producto_tienda", "nombre_tienda")
    record_id = (r.get("ProductId") or None)

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

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: dt.datetime):
    precio_lista = _parse_price(r.get("PrecioLista"))
    precio_oferta = _parse_price(r.get("PrecioOferta"))

    tipo_oferta_raw = (r.get("TipoOferta") or None)
    tipo_oferta = _truncate_dyn(tipo_oferta_raw, "historico_precios", "tipo_oferta")

    promo_comentarios = None
    cid = r.get("ClusterId")
    cname = r.get("ClusterNombre")
    if cid or cname:
        promo_comentarios = f"cluster_id={cid or ''}; cluster_nombre={cname or ''}"
    promo_comentarios = _truncate_dyn(promo_comentarios, "historico_precios", "promo_comentarios")

    promo_tipo = _truncate_dyn(tipo_oferta, "historico_precios", "tipo_oferta")
    promo_texto_regular = _truncate_dyn(None, "historico_precios", "promo_texto_regular")
    promo_texto_descuento = _truncate_dyn(None, "historico_precios", "promo_texto_descuento")

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
        promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios
    ))

# ---------------- Orquestador MySQL ----------------
def run_to_mysql(cluster_ids: List[str], out_prefix: Optional[str] = None, max_depth: int = 3):
    frames = []
    for cid in cluster_ids:
        print(f"\n=== Cluster {cid} ===", flush=True)
        df = scrape_cluster(cid, max_depth=max_depth)
        print(f"Cluster {cid}: {len(df)} filas totales", flush=True)
        frames.append(df)

    if not frames:
        print("No se obtuvieron datos.", flush=True)
        return

    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    if df.empty:
        print("Sin filas para insertar.", flush=True)
        return

    # Dedupe por SKU (o ProductId)
    if "SKU" in df.columns:
        df.drop_duplicates(subset=["SKU"], inplace=True, keep="first")
    elif "ProductId" in df.columns:
        df.drop_duplicates(subset=["ProductId"], inplace=True, keep="first")

    print(f"💾 Preparando inserción MySQL ({len(df)} filas únicas)…", flush=True)
    capturado_en = dt.datetime.now()

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        load_db_limits(cur)
        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        inserted_hist = 0
        for _, r in df.iterrows():
            rec = r.to_dict()
            producto_id = find_or_create_producto(cur, rec)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, rec)
            insert_historico(cur, tienda_id, pt_id, rec, capturado_en)
            inserted_hist += 1

        conn.commit()
        print(f"✅ Guardado en MySQL: {inserted_hist} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    # Export opcional local (útil para auditoría)
    if out_prefix:
        csv_path = f"{out_prefix}.csv"
        xlsx_path = f"{out_prefix}.xlsx"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name="productos")
        print(f"\nArchivos guardados:\n- {csv_path}\n- {xlsx_path}\n", flush=True)

# ---------------- CLI ----------------
def main():
    global SALES_CHANNEL
    parser = argparse.ArgumentParser(description="Masonline (VTEX) — Ingesta a MySQL por cluster IDs (recursivo por ft)")
    parser.add_argument("--clusters", type=str, default="3454",
                        help="IDs de cluster separados por coma (ej: 3454,3627)")
    parser.add_argument("--out", type=str, default="masonline_cluster",
                        help="Prefijo de archivo de salida (CSV/XLSX opcional)")
    parser.add_argument("--sc", type=int, default=None,
                        help="Sales channel (sc) para VTEX (ej. 1). Si no aplica, omitir.")
    parser.add_argument("--max-depth", type=int, default=3,
                        help="Profundidad máxima de recursión por ft (1–4 recomendado).")
    args = parser.parse_args()

    SALES_CHANNEL = args.sc
    cluster_ids = [c.strip() for c in args.clusters.split(",") if c.strip()]
    run_to_mysql(cluster_ids, out_prefix=args.out, max_depth=args.max_depth)

if __name__ == "__main__":
    main()
