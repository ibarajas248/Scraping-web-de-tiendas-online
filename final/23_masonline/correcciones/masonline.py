#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Masonline (VTEX) — Catálogo completo a MySQL (categorías → marcas → FT)
------------------------------------------------------------------------

Orden de scraping:
  1) Categorías (fq=C:{categoryId} con IDs NUMÉRICOS desde /category/tree)
  2) Fallback por Marcas (fq=B:{brandId})
  3) Fallback FT recursivo (0–9, a–z, ñ)

- Dedup por ProductId y SKU.
- Inserta/actualiza en tablas: tiendas, productos, producto_tienda, historico_precios
- Exporta XLSX y CSV locales.

Requisitos:
  pip install requests pandas mysql-connector-python urllib3 xlsxwriter
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

# ---------- Conexión MySQL ----------
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn

# IMPORTANTE: manejaremos errores específicos de MySQL
from mysql.connector import Error as MySQLError
from mysql.connector.errors import IntegrityError, DatabaseError, ProgrammingError

# ---------- Config VTEX ----------
TIENDA_CODIGO = "https://www.masonline.com.ar"
TIENDA_NOMBRE = "Masonline (VTEX)"
BASE = "https://www.masonline.com.ar"
SEARCH_API = f"{BASE}/api/catalog_system/pub/products/search"
CATEGORY_TREE_API = f"{BASE}/api/catalog_system/pub/category/tree/100"
FACETS_API = f"{BASE}/api/catalog_system/pub/facets/search/*?map=ft"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
}
STEP = 50
SLEEP_BETWEEN = 0.30
MAX_WINDOW_RESULTS = 2500
ORDER_BY = "OrderByNameASC"
ALPHA_TERMS = list(string.digits + string.ascii_lowercase) + ["ñ"]

# ---------- Límite dinámico ----------
DEFAULT_LIMITS = {
    # historico_precios
    ("historico_precios", "tipo_oferta"): 255,
    ("historico_precios", "promo_comentarios"): 1000,
    ("historico_precios", "promo_texto_regular"): 255,
    ("historico_precios", "promo_texto_descuento"): 255,
    # producto_tienda
    ("producto_tienda", "nombre_tienda"): 255,
    # productos (agregados ahora)
    ("productos", "ean"): 50,
    ("productos", "nombre"): 255,
    ("productos", "marca"): 255,
    ("productos", "fabricante"): 255,
    ("productos", "categoria"): 255,
    ("productos", "subcategoria"): 255,
}
DB_LIMITS: Dict[Tuple[str, str], Optional[int]] = {}

def _collapse_spaces(s: str) -> str:
    return " ".join(s.split())

def _norm_str(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return _collapse_spaces(s)

def _truncate_dyn(s: Optional[str], table: str, column: str) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    limit = DB_LIMITS.get((table, column))
    if limit is None:
        return s
    return s[:limit] if len(s) > limit else s

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

# ---------- Sesión HTTP ----------
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

# ---------- Helpers VTEX ----------
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
        if s.get("sellerDefault"):
            return s
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

# ---------- Categorías (IDs numéricos desde /category/tree) ----------
def _walk_category_tree(nodes, acc: Set[Tuple[str, str]]):
    if not nodes:
        return
    for n in nodes:
        cid = n.get("id")
        name = n.get("name") or ""
        if cid is not None:
            acc.add((str(cid), name))
        _walk_category_tree(n.get("children") or [], acc)

def get_all_category_ids(session: requests.Session) -> List[Tuple[str, str]]:
    try:
        r = session.get(CATEGORY_TREE_API, timeout=30)
        if r.status_code != 200:
            print(f"⚠️ category/tree devolvió {r.status_code}.")
            return []
        data = r.json()
        acc: Set[Tuple[str, str]] = set()
        if isinstance(data, list):
            _walk_category_tree(data, acc)
        acc = set([(cid, name) for cid, name in acc if cid.isdigit()])
        cats = sorted(list(acc), key=lambda x: int(x[0]))
        return cats
    except Exception as e:
        print(f"⚠️ Error leyendo category/tree: {e}")
        return []

def fetch_category_page(session: requests.Session, category_id: str, start: int, step: int) -> List[Dict[str, Any]]:
    params = {"fq": f"C:{category_id}", "_from": start, "_to": start + step - 1, "O": ORDER_BY}
    r = session.get(SEARCH_API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []

# ---------- Marcas (fallback) ----------
def get_all_brand_ids(session: requests.Session) -> List[Tuple[str, str]]:
    try:
        r = session.get(FACETS_API, timeout=30)
        if r.status_code != 200:
            print(f"⚠️ facets/search devolvió {r.status_code}.")
            return []
        data = r.json()
        brands = []
        bag = data.get("Brands") or data.get("brands") or []
        for b in bag:
            bid = str(b.get("Id") or b.get("id") or b.get("Value") or "")
            name = b.get("Name") or b.get("name") or ""
            if bid.isdigit():
                brands.append((bid, name))
        return brands
    except Exception as e:
        print(f"⚠️ Error leyendo brands en Facets: {e}")
        return []

def fetch_brand_page(session: requests.Session, brand_id: str, start: int, step: int) -> List[Dict[str, Any]]:
    params = {"fq": f"B:{brand_id}", "_from": start, "_to": start + step - 1, "O": ORDER_BY}
    r = session.get(SEARCH_API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []

# ---------- Orquestador ----------
def scrape_all_catalog() -> pd.DataFrame:
    session = make_session()
    seen: Set[str] = set()
    all_rows: List[Dict[str, Any]] = []

    # 1) Categorías
    categories = get_all_category_ids(session)
    if categories:
        print(f"🗂️ Categorías con ID detectadas: {len(categories)}")
        for cid, cname in categories:
            start = 0
            print(f"\n=== Categoría {cid} — {cname} ===")
            while True:
                try:
                    chunk = fetch_category_page(session, cid, start, STEP)
                except Exception as e:
                    print(f"Cat {cid}: error {e} (continúo).")
                    break
                if not chunk:
                    if start == 0:
                        print(f"Cat {cid}: sin productos.")
                    break
                fresh = [p for p in chunk if p.get("productId") not in seen]
                for p in fresh:
                    seen.add(p.get("productId"))
                print(f"Cat {cid}: +{len(fresh)} nuevos (acum: {len(seen)})")
                rows = flatten(fresh)
                all_rows.extend(rows)
                start += STEP
                time.sleep(SLEEP_BETWEEN)

        if all_rows:
            df = pd.DataFrame(all_rows)
            cols = ["EAN","CodigoInterno","NombreProducto","Categoria","Subcategoria",
                    "Marca","Fabricante","PrecioLista","PrecioOferta","TipoOferta",
                    "URL","SKU","ProductId"]
            for c in cols:
                if c not in df.columns: df[c] = None
            return df[cols]

    # 2) Fallback por marcas
    print("ℹ️ Pasando a fallback por MARCAS…")
    brands = get_all_brand_ids(session)
    for bid, bname in brands:
        start = 0
        print(f"\n=== Marca {bid} — {bname} ===")
        while True:
            try:
                chunk = fetch_brand_page(session, bid, start, STEP)
            except Exception as e:
                print(f"Brand {bid}: error {e} (continúo).")
                break
            if not chunk:
                if start == 0:
                    print(f"Brand {bid}: sin productos.")
                break
            fresh = [p for p in chunk if p.get("productId") not in seen]
            for p in fresh:
                seen.add(p.get("productId"))
            print(f"Brand {bid}: +{len(fresh)} nuevos (acum: {len(seen)})")
            rows = flatten(fresh)
            all_rows.extend(rows)
            start += STEP
            time.sleep(SLEEP_BETWEEN)

    if all_rows:
        df = pd.DataFrame(all_rows)
        cols = ["EAN","CodigoInterno","NombreProducto","Categoria","Subcategoria",
                "Marca","Fabricante","PrecioLista","PrecioOferta","TipoOferta",
                "URL","SKU","ProductId"]
        for c in cols:
            if c not in df.columns: df[c] = None
        return df[cols]

    # 3) Fallback FT
    print("⚠️ Facets/Brands insuficiente. Uso FT recursivo como último recurso.")
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

# ---------- Ingesta MySQL ----------
def load_db_limits(cur):
    global DB_LIMITS
    DB_LIMITS = DEFAULT_LIMITS.copy()
    targets = [
        # historico_precios
        ("historico_precios", "tipo_oferta"),
        ("historico_precios", "promo_comentarios"),
        ("historico_precios", "promo_texto_regular"),
        ("historico_precios", "promo_texto_descuento"),
        # producto_tienda
        ("producto_tienda", "nombre_tienda"),
        # productos
        ("productos", "ean"),
        ("productos", "nombre"),
        ("productos", "marca"),
        ("productos", "fabricante"),
        ("productos", "categoria"),
        ("productos", "subcategoria"),
    ]
    cur.execute("SELECT DATABASE()")
    dbname = cur.fetchone()[0]
    for table, col in targets:
        cur.execute("""SELECT CHARACTER_MAXIMUM_LENGTH
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s LIMIT 1""",
                    (dbname, table, col))
        row = cur.fetchone()
        if not row:
            continue
        maxlen = row[0]
        DB_LIMITS[(table, col)] = None if maxlen is None else int(maxlen)

def upsert_tienda(cur):
    cur.execute("INSERT INTO tiendas (codigo, nombre) VALUES (%s,%s) "
                "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
                (TIENDA_CODIGO, TIENDA_NOMBRE))
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s", (TIENDA_CODIGO,))
    return cur.fetchone()[0]

def _sanitize_producto_fields(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza + trunca campos de 'productos' según límites reales de la DB.
    """
    ean = _truncate_dyn(_norm_str(r.get("EAN")), "productos", "ean")
    nombre = _truncate_dyn(_norm_str(r.get("NombreProducto")), "productos", "nombre")
    marca = _truncate_dyn(_norm_str(r.get("Marca")), "productos", "marca")
    fabricante = _truncate_dyn(_norm_str(r.get("Fabricante")), "productos", "fabricante")
    categoria = _truncate_dyn(_norm_str(r.get("Categoria")), "productos", "categoria")
    subcategoria = _truncate_dyn(_norm_str(r.get("Subcategoria")), "productos", "subcategoria")
    return {
        "ean": ean,
        "nombre": nombre,
        "marca": marca,
        "fabricante": fabricante,
        "categoria": categoria,
        "subcategoria": subcategoria
    }

def find_or_create_producto(cur, r) -> int:
    """
    Upsert inteligente:
      - Si hay EAN -> buscar por ean; si existe, UPDATE metadata; si no, INSERT.
      - Si no hay EAN -> buscar por (nombre, marca); si existe, UPDATE; si no, INSERT.
    Maneja colisiones de UNIQUE(ean) con UPDATE.
    """
    fields = _sanitize_producto_fields(r)
    ean = fields["ean"]
    nombre = fields["nombre"]
    marca = fields["marca"]
    fabricante = fields["fabricante"]
    categoria = fields["categoria"]
    subcategoria = fields["subcategoria"]

    # preferencia por EAN si existe
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""UPDATE productos SET 
                             nombre=COALESCE(%s, nombre),
                             marca=COALESCE(%s, marca),
                             fabricante=COALESCE(%s, fabricante),
                             categoria=COALESCE(%s, categoria),
                             subcategoria=COALESCE(%s, subcategoria)
                           WHERE id=%s""",
                        (nombre, marca, fabricante, categoria, subcategoria, pid))
            return pid
        # no existe por EAN → intentar insert directo
        try:
            cur.execute("""INSERT INTO productos (ean,nombre,marca,fabricante,categoria,subcategoria)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        (ean, nombre, marca, fabricante, categoria, subcategoria))
            return cur.lastrowid
        except IntegrityError:
            # choque de UNIQUE(ean) por carrera o normalización: reintentar con SELECT y UPDATE
            cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
            row2 = cur.fetchone()
            if row2:
                pid = row2[0]
                cur.execute("""UPDATE productos SET 
                                 nombre=COALESCE(%s, nombre),
                                 marca=COALESCE(%s, marca),
                                 fabricante=COALESCE(%s, fabricante),
                                 categoria=COALESCE(%s, categoria),
                                 subcategoria=COALESCE(%s, subcategoria)
                               WHERE id=%s""",
                            (nombre, marca, fabricante, categoria, subcategoria, pid))
                return pid
            # si no lo encuentra, último intento: insert sin ean
            cur.execute("""INSERT INTO productos (ean,nombre,marca,fabricante,categoria,subcategoria)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        (ean, nombre, marca, fabricante, categoria, subcategoria))
            return cur.lastrowid

    # sin EAN → heurística por (nombre, marca)
    if nombre and marca:
        cur.execute("""SELECT id FROM productos 
                       WHERE nombre=%s AND marca=%s LIMIT 1""", (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""UPDATE productos SET 
                             fabricante=COALESCE(%s, fabricante),
                             categoria=COALESCE(%s, categoria),
                             subcategoria=COALESCE(%s, subcategoria)
                           WHERE id=%s""",
                        (fabricante, categoria, subcategoria, pid))
            return pid

    # crear nuevo sin EAN o sin match
    cur.execute("""INSERT INTO productos (ean,nombre,marca,fabricante,categoria,subcategoria)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id, producto_id, r):
    sku = _norm_str(r.get("SKU"))
    url = _norm_str(r.get("URL"))
    nombre_tienda = _truncate_dyn(_norm_str(r.get("NombreProducto")), "producto_tienda", "nombre_tienda")
    record_id = _norm_str(r.get("ProductId"))

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
    tipo_oferta = _truncate_dyn(_norm_str(r.get("TipoOferta")), "historico_precios", "tipo_oferta")
    promo_com = _truncate_dyn("scan_auto", "historico_precios", "promo_comentarios")
    cur.execute("""INSERT INTO historico_precios
                   (tienda_id,producto_tienda_id,capturado_en,precio_lista,precio_oferta,tipo_oferta,promo_comentarios)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                     precio_lista=VALUES(precio_lista),
                     precio_oferta=VALUES(precio_oferta),
                     tipo_oferta=VALUES(tipo_oferta),
                     promo_comentarios=VALUES(promo_comentarios)""",
                (tienda_id, producto_tienda_id, capturado_en,
                 precio_lista, precio_oferta, tipo_oferta, promo_com))

def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Normalizar tipos básicos
    for col in ["EAN","CodigoInterno","NombreProducto","Categoria","Subcategoria",
                "Marca","Fabricante","TipoOferta","URL","SKU","ProductId"]:
        if col in df.columns:
            df[col] = df[col].astype(str, errors="ignore").where(df[col].notna(), None)
            # evitar strings "None"/"nan"
            df[col] = df[col].apply(lambda x: None if x in (None, "None", "nan", "NaN") else x)
    # Dedupe por SKU y ProductId si existen
    if "SKU" in df.columns and df["SKU"].notna().any():
        df.drop_duplicates(subset=["SKU"], inplace=True)
    elif "ProductId" in df.columns:
        df.drop_duplicates(subset=["ProductId"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def run_ingesta(df: pd.DataFrame):
    if df.empty:
        print("Sin datos para insertar.")
        return

    df = _clean_dataframe(df)

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    load_db_limits(cur)
    tienda_id = upsert_tienda(cur)
    capturado_en = dt.datetime.now()

    total = 0
    ok = 0
    fail = 0
    BATCH = 2000  # commits periódicos

    try:
        for idx, r in df.iterrows():
            rec = r.to_dict()
            try:
                pid = find_or_create_producto(cur, rec)
                ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)
                insert_historico(cur, tienda_id, ptid, rec, capturado_en)
                ok += 1
            except (IntegrityError, DatabaseError, ProgrammingError, MySQLError) as e:
                fail += 1
                # registro mínimo del error y continúo
                nombre_log = rec.get("NombreProducto")
                ean_log = rec.get("EAN")
                print(f"❌ Fila {idx} falló (EAN={ean_log}, nombre={nombre_log}): {e}")
            total += 1
            if total % BATCH == 0:
                conn.commit()
                print(f"💾 Commit intermedio — procesadas: {total} | OK: {ok} | FAIL: {fail}")

        conn.commit()
    finally:
        conn.close()
    print(f"✅ Ingesta finalizada — total: {total} | insert/updates OK: {ok} | fallidas: {fail}")

# ---------- MAIN ----------
if __name__ == "__main__":
    print("🔍 Iniciando descarga completa de catálogo Masonline...")
    df = scrape_all_catalog()
    print(f"\nTotal productos únicos: {len(df)}")
    df.to_csv("masonline_full.csv", index=False, encoding="utf-8")
    with pd.ExcelWriter("masonline_full.xlsx", engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="productos")
    print("📁 Archivos guardados: masonline_full.csv / masonline_full.xlsx")
    run_ingesta(df)
    print("🚀 Proceso completo.")
