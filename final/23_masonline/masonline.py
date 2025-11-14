#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Masonline (VTEX) — Catálogo completo a MySQL (sin argumentos)
--------------------------------------------------------------

- Recorre TODO el catálogo usando /api/catalog_system/pub/products/search/{ft}?map=ft
  con paginación (_from/_to) y particionado recursivo (0–9, a–z, ñ).
- Dedup por ProductId y SKU.
- Inserta/actualiza en tablas:
    tiendas, productos, producto_tienda, historico_precios
- Exporta XLSX y CSV locales.

Requisitos:
  pip install requests pandas mysql-connector-python urllib3 xlsxwriter

Debe existir un archivo base_datos.py con:
  def get_conn():
      return mysql.connector.connect(host=..., user=..., password=..., database=...)
"""

import time
import string
import requests
from requests.adapters import HTTPAdapter
from requests import HTTPError
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np
import datetime as dt
import sys, os
from typing import List, Dict, Any, Optional, Tuple, Set

# Importa tu conexión MySQL
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn

# ----------------------------------------------------------------
# Configuración tienda y VTEX
# ----------------------------------------------------------------
TIENDA_CODIGO = "https://www.masonline.com.ar"
TIENDA_NOMBRE = "Masonline (VTEX)"
BASE = "https://www.masonline.com.ar"
SEARCH_API = f"{BASE}/api/catalog_system/pub/products/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
STEP = 50
SLEEP_BETWEEN = 0.30
MAX_WINDOW_RESULTS = 2500
ORDER_BY = "OrderByNameASC"
ALPHA_TERMS = list(string.digits + string.ascii_lowercase) + ["ñ"]

# ----------------------------------------------------------------
# Límite dinámico de columnas
# ----------------------------------------------------------------
DEFAULT_LIMITS = {
    ("historico_precios", "tipo_oferta"): 255,
    ("historico_precios", "promo_comentarios"): 1000,
    ("historico_precios", "promo_texto_regular"): 255,
    ("historico_precios", "promo_texto_descuento"): 255,
    ("producto_tienda", "nombre_tienda"): 255,
}
DB_LIMITS: Dict[Tuple[str, str], Optional[int]] = {}

def _truncate_dyn(s: Optional[str], table: str, column: str) -> Optional[str]:
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

# ----------------------------------------------------------------
# Sesión HTTP robusta
# ----------------------------------------------------------------
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

# ----------------------------------------------------------------
# Scraping VTEX
# ----------------------------------------------------------------
def fetch_page_ft(session: requests.Session, term: str, start: int, step: int) -> List[Dict[str, Any]]:
    url = f"{SEARCH_API}/{term}"
    params = {"map": "ft", "_from": start, "_to": start + step - 1, "O": ORDER_BY}
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 400:
        raise HTTPError("VTEX 50-page window reached", response=r)
    r.raise_for_status()
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []

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
            if n: names.append(str(n))
        for t in co.get("PromotionTeasers") or []:
            n = (t or {}).get("Name") or (t or {}).get("name")
            if n: names.append(str(n))
    clusters = p.get("productClusters") or {}
    for _, cname in clusters.items():
        if isinstance(cname, str): names.append(cname)
    names = list(dict.fromkeys([n.strip() for n in names if n.strip()]))
    return " | ".join(names)

def choose_seller(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sellers = item.get("sellers") or []
    for s in sellers:
        if s.get("sellerDefault"): return s
    for s in sellers:
        if (s or {}).get("commertialOffer", {}).get("IsAvailable"):
            return s
    return sellers[0] if sellers else None

def flatten(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for p in products:
        categoria, subcategoria, _ruta = split_categories(p.get("categories") or [])
        brand = p.get("brand")
        manufacturer = p.get("Manufacturer") or p.get("manufacturer")
        url = p.get("link") or f"{BASE}/{p.get('linkText')}/p"
        for it in p.get("items") or []:
            ean = it.get("ean")
            ref_val = None
            for ref in it.get("referenceId") or []:
                if (ref or {}).get("Key") == "RefId":
                    ref_val = ref.get("Value")
                    break
            if not ref_val:
                ref_val = p.get("productReference") or it.get("itemId")
            seller = choose_seller(it) or {}
            co = seller.get("commertialOffer") or {}
            price = co.get("Price")
            list_price = co.get("ListPrice")
            tipo_oferta = extract_offer_type(p, it)
            rows.append({
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
            })
    return rows

def _scrape_ft_term(session, term, seen_products):
    all_rows = []
    start = 0
    hit_cap = False
    while True:
        try:
            if start >= MAX_WINDOW_RESULTS:
                hit_cap = True
                print(f"'{term}': alcanzó {MAX_WINDOW_RESULTS}, subdividiendo...")
                break
            chunk = fetch_page_ft(session, term, start, STEP)
        except HTTPError:
            hit_cap = True
            print(f"'{term}': ventana llena → subdividir")
            break
        if not chunk:
            if start == 0:
                print(f"'{term}': sin resultados.")
            break
        fresh = [p for p in chunk if p.get("productId") not in seen_products]
        for p in fresh:
            seen_products.add(p.get("productId"))
        print(f"'{term}': +{len(fresh)} nuevos (total únicos: {len(seen_products)})")
        rows = flatten(fresh)
        all_rows.extend(rows)
        start += STEP
        time.sleep(SLEEP_BETWEEN)
        if len(chunk) < STEP:
            break
    return all_rows, hit_cap

def scrape_all_catalog() -> pd.DataFrame:
    session = make_session()
    seen: Set[str] = set()
    all_rows: List[Dict[str, Any]] = []
    stack = ALPHA_TERMS.copy()
    while stack:
        term = stack.pop(0)
        print(f"\n=== Explorando '{term}' ===")
        rows, hit_cap = _scrape_ft_term(session, term, seen)
        all_rows.extend(rows)
        if hit_cap:
            for ch in ALPHA_TERMS:
                stack.append(term + ch)
    df = pd.DataFrame(all_rows)
    cols = ["EAN","CodigoInterno","NombreProducto","Categoria","Subcategoria",
            "Marca","Fabricante","PrecioLista","PrecioOferta","TipoOferta",
            "URL","SKU","ProductId"]
    for c in cols:
        if c not in df.columns: df[c] = None
    return df[cols]

# ----------------------------------------------------------------
# Ingesta MySQL
# ----------------------------------------------------------------
def load_db_limits(cur):
    global DB_LIMITS
    DB_LIMITS = DEFAULT_LIMITS.copy()
    targets = [("historico_precios", "tipo_oferta"),
               ("historico_precios", "promo_comentarios"),
               ("historico_precios", "promo_texto_regular"),
               ("historico_precios", "promo_texto_descuento"),
               ("producto_tienda", "nombre_tienda")]
    cur.execute("SELECT DATABASE()")
    dbname = cur.fetchone()[0]
    for table, col in targets:
        cur.execute("""SELECT CHARACTER_MAXIMUM_LENGTH
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s LIMIT 1""",
                    (dbname, table, col))
        row = cur.fetchone()
        if not row: continue
        maxlen = row[0]
        DB_LIMITS[(table, col)] = None if maxlen is None else int(maxlen)

def upsert_tienda(cur):
    cur.execute("INSERT INTO tiendas (codigo, nombre) VALUES (%s,%s) "
                "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
                (TIENDA_CODIGO, TIENDA_NOMBRE))
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s", (TIENDA_CODIGO,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r):
    ean, nombre, marca, fabricante, cat, subcat = (
        r.get("EAN"), r.get("NombreProducto"), r.get("Marca"),
        r.get("Fabricante"), r.get("Categoria"), r.get("Subcategoria")
    )
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""UPDATE productos SET nombre=COALESCE(NULLIF(%s,''),nombre),
                           marca=COALESCE(%s,marca), fabricante=COALESCE(%s,fabricante),
                           categoria=COALESCE(%s,categoria), subcategoria=COALESCE(%s,subcategoria)
                           WHERE id=%s""",
                        (nombre, marca, fabricante, cat, subcat, pid))
            return pid
    cur.execute("""INSERT INTO productos (ean,nombre,marca,fabricante,categoria,subcategoria)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (ean, nombre, marca, fabricante, cat, subcat))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id, producto_id, r):
    sku, url = r.get("SKU"), r.get("URL")
    nombre_tienda = _truncate_dyn(r.get("NombreProducto"), "producto_tienda", "nombre_tienda")
    record_id = r.get("ProductId")
    cur.execute("""INSERT INTO producto_tienda (tienda_id,producto_id,sku_tienda,record_id_tienda,url_tienda,nombre_tienda)
                   VALUES (%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id),
                   producto_id=VALUES(producto_id),
                   url_tienda=COALESCE(VALUES(url_tienda),url_tienda),
                   nombre_tienda=COALESCE(VALUES(nombre_tienda),nombre_tienda)""",
                (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id, producto_tienda_id, r, capturado_en):
    precio_lista = _parse_price(r.get("PrecioLista"))
    precio_oferta = _parse_price(r.get("PrecioOferta"))
    tipo_oferta = _truncate_dyn(r.get("TipoOferta"), "historico_precios", "tipo_oferta")
    promo_com = _truncate_dyn("ft_scan", "historico_precios", "promo_comentarios")
    cur.execute("""INSERT INTO historico_precios
                   (tienda_id,producto_tienda_id,capturado_en,precio_lista,precio_oferta,tipo_oferta,promo_comentarios)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                   precio_lista=VALUES(precio_lista),precio_oferta=VALUES(precio_oferta),
                   tipo_oferta=VALUES(tipo_oferta),promo_comentarios=VALUES(promo_comentarios)""",
                (tienda_id, producto_tienda_id, capturado_en,
                 precio_lista, precio_oferta, tipo_oferta, promo_com))

def run_ingesta(df: pd.DataFrame):
    if df.empty:
        print("Sin datos para insertar.")
        return
    if "SKU" in df.columns and df["SKU"].notna().any():
        df.drop_duplicates(subset=["SKU"], inplace=True)
    elif "ProductId" in df.columns:
        df.drop_duplicates(subset=["ProductId"], inplace=True)
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    load_db_limits(cur)
    tienda_id = upsert_tienda(cur)
    capturado_en = dt.datetime.now()
    total = 0
    for _, r in df.iterrows():
        rec = r.to_dict()
        pid = find_or_create_producto(cur, rec)
        ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)
        insert_historico(cur, tienda_id, ptid, rec, capturado_en)
        total += 1
    conn.commit()
    conn.close()
    print(f"✅ {total} filas insertadas ({TIENDA_NOMBRE})")

# ----------------------------------------------------------------
# MAIN AUTOEJECUCIÓN
# ----------------------------------------------------------------
if __name__ == "__main__":
    print("🔍 Iniciando descarga completa de catálogo Masonline...")
    df = scrape_all_catalog()
    print(f"\nTotal productos únicos: {len(df)}")
    # Guardar XLSX / CSV
    df.to_csv("masonline_full.csv", index=False, encoding="utf-8")
    with pd.ExcelWriter("masonline_full.xlsx", engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="productos")
    print("📁 Archivos guardados: masonline_full.csv / masonline_full.xlsx")
    # Ingestar a MySQL
    run_ingesta(df)
    print("🚀 Proceso completo.")
