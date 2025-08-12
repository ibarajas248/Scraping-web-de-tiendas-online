#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, re, time, json, unicodedata
from typing import List, Dict, Tuple, Any, Optional
from urllib.parse import urlparse
from datetime import datetime

import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mysql.connector import Error as MySQLError
from base_datos import get_conn  # <- tu conexión MySQL

# ===================== Config =====================
BASE = "https://www.hiperlibertad.com.ar"

# Árbol de categorías (sube a 8–10 si ves hojas faltantes)
TREE_DEPTH = 6
TREE_URL = f"{BASE}/api/catalog_system/pub/category/tree/{TREE_DEPTH}"

# Búsqueda VTEX
SEARCH = f"{BASE}/api/catalog_system/pub/products/search"

STEP = 50                 # VTEX: _from/_to (0-49, 50-99, …)
TIMEOUT = 25
SLEEP_OK = 0.25
MAX_EMPTY = 2             # corta si hay 2 páginas seguidas vacías
RETRIES = 3               # reintentos HTTP

# Canal de ventas (si devuelve vacío, probá 2 o 3)
SALES_CHANNELS = ["1"]

# Prefijos a incluir (vacío = TODAS las familias del árbol)
INCLUDE_PREFIXES: List[str] = []  # p.ej. ["tecnologia","almacen"]

# Umbral para fallback por ID si ruta trae pocos
FALLBACK_THRESHOLD = 5

# Identidad de la tienda (BD)
TIENDA_CODIGO = "hiperlibertad"
TIENDA_NOMBRE = "Hiper Libertad"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# ===================== Utils =====================
def s_requests() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=RETRIES,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    return s

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def clean(val):
    if val is None: return None
    s = str(val).strip()
    return s if s else None

_price_clean_re = re.compile(r"[^\d,.\-]")
def parse_price(val) -> float:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan
    if isinstance(val, (int, float)):
        return float(val)
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

def to_txt_or_none(x):
    v = parse_price(x)
    if x is None: return None
    if isinstance(v, float) and np.isnan(v): return None
    return f"{round(float(v), 2)}"

# ===================== Categorías =====================
def load_tree(session: requests.Session) -> List[Dict[str, Any]]:
    r = session.get(TREE_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def url_to_path(url: str) -> List[str]:
    p = urlparse(url)
    path = p.path.lstrip("/")  # ej: 'tecnologia/audio/auriculares'
    return [seg for seg in path.split("/") if seg]

def flatten_leaves(tree: List[Dict[str, Any]]) -> List[Tuple[List[str], int, str]]:
    """
    Devuelve hojas como (path_segments, category_id, category_name).
    """
    leaves: List[Tuple[List[str], int, str]] = []

    def dfs(node: Dict[str, Any]):
        path = url_to_path(node.get("url", ""))
        children = node.get("children") or []
        if not children:
            if path:
                try:
                    cid = int(node.get("id"))
                except Exception:
                    cid = int(str(node.get("id") or "0") or "0")
                leaves.append((path, cid, node.get("name", "")))
            return
        for ch in children:
            dfs(ch)

    for root in tree:
        dfs(root)
    return leaves

def build_map_for_path(path_segments: List[str]) -> str:
    return ",".join(["c"] * len(path_segments))

# ===================== Parsing producto =====================
def parse_price_fields(item: Dict[str, Any]):
    sellers = item.get("sellers") or []
    s0 = first(sellers, {}) or {}
    co = (s0.get("commertialOffer") or {}) if isinstance(s0, dict) else {}
    list_price = float(co.get("ListPrice") or 0.0)
    price = float(co.get("Price") or 0.0)
    stock = int(co.get("AvailableQuantity") or 0)
    return list_price, price, stock

def extract_ean(item: Dict[str, Any]) -> str:
    ean = (item.get("ean") or "").strip()
    if ean:
        return ean
    for ref in item.get("referenceId") or []:
        key = (ref.get("Key") or ref.get("key") or "").upper()
        if key in ("EAN", "EAN13", "COD_EAN", "COD.EAN"):
            val = (ref.get("Value") or ref.get("value") or "").strip()
            if val:
                return val
    return ""

def extract_codigo_interno(p: Dict[str, Any], item: Dict[str, Any]) -> str:
    cod = str(p.get("productReference") or "").strip()
    if not cod:
        cod = str(item.get("itemId") or "").strip()
    if not cod:
        ref = first(item.get("referenceId") or [])
        cod = (ref.get("Value") or ref.get("value") or "").strip() if isinstance(ref, dict) else ""
    return cod

def product_url(p: Dict[str, Any]) -> str:
    link = (p.get("linkText") or "").strip()
    return f"{BASE}/{link}/p" if link else ""

def parse_records(p: Dict[str, Any], fullpath: List[str]) -> List[Dict[str, Any]]:
    """
    Devuelve filas con las claves que espera tu lógica de BD:
      ean, sku, record_id (opcional), nombre, marca, fabricante,
      categoria, subcategoria, precio_lista, precio_oferta, tipo_oferta, url
    """
    rows = []
    items = p.get("items") or []
    for item in items:
        ean = extract_ean(item)
        list_price, price, stock = parse_price_fields(item)
        tipo_oferta = "Oferta" if price and list_price and price < list_price else ""
        rows.append({
            "ean": clean(ean),
            "sku": clean(extract_codigo_interno(p, item)),     # usamos itemId / productReference
            "record_id": None,                                 # VTEX no tiene record.id tipo ATG; no hace falta
            "nombre": clean(p.get("productName")),
            "marca": clean(p.get("brand")),
            "fabricante": clean(p.get("Manufacturer") or p.get("brand")),
            "categoria": fullpath[0] if len(fullpath) >= 1 else None,
            "subcategoria": fullpath[1] if len(fullpath) >= 2 else None,
            "precio_lista": list_price,
            "precio_oferta": price,
            "tipo_oferta": tipo_oferta,
            "url": product_url(p),
            # datos extra útiles (no se insertan, pero sirven para debug)
            "_stock": stock,
            "_ruta": "/".join(fullpath)
        })
    return rows

# ===================== Fetching =====================
def fetch_category_by_path(session: requests.Session, path_segments: List[str], sc: str) -> List[Dict[str, Any]]:
    map_str = build_map_for_path(path_segments)
    path = "/".join(path_segments)
    rows: List[Dict[str, Any]] = []
    offset = 0
    empty_in_a_row = 0

    while True:
        params = {
            "_from": offset,
            "_to": offset + STEP - 1,
            "map": map_str,
            "sc": sc
        }
        url = f"{SEARCH}/{path}"
        r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 429:
            time.sleep(0.8)
            continue
        r.raise_for_status()
        try:
            data = r.json()
        except Exception:
            data = []
        if not data:
            empty_in_a_row += 1
            if empty_in_a_row >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue
        empty_in_a_row = 0
        for p in data:
            pr_rows = parse_records(p, path_segments)
            rows.extend(pr_rows)
            if pr_rows:
                s = pr_rows[0]
                print(f"[PATH sc={sc}] {s['nombre']} | EAN:{s['ean']} | ${s['precio_oferta']} | {s['url']}")
        offset += STEP
        time.sleep(SLEEP_OK)
    return rows

def fetch_category_by_id(session: requests.Session, category_id: int, fullpath: List[str], sc: str) -> List[Dict[str, Any]]:
    """Fallback: usa fq=C:<id>"""
    rows: List[Dict[str, Any]] = []
    offset = 0
    empty_in_a_row = 0

    while True:
        params = {
            "_from": offset,
            "_to": offset + STEP - 1,
            "fq": f"C:{category_id}",
            "sc": sc
        }
        r = session.get(SEARCH, headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 429:
            time.sleep(0.8)
            continue
        r.raise_for_status()
        try:
            data = r.json()
        except Exception:
            data = []
        if not data:
            empty_in_a_row += 1
            if empty_in_a_row >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue
        empty_in_a_row = 0
        for p in data:
            pr_rows = parse_records(p, fullpath)
            rows.extend(pr_rows)
            if pr_rows:
                s = pr_rows[0]
                print(f"[CID  sc={sc}] {s['nombre']} | EAN:{s['ean']} | ${s['precio_oferta']} | {s['url']}")
        offset += STEP
        time.sleep(SLEEP_OK)
    return rows

def fetch_category(session: requests.Session, path_segments: List[str], category_id: int) -> List[Dict[str, Any]]:
    """
    Intenta por ruta+map y, si hay pocos resultados, hace fallback por ID.
    Además prueba múltiples sales channels si se configuraron.
    """
    best_rows: List[Dict[str, Any]] = []
    for sc in SALES_CHANNELS:
        # 1) Por ruta
        rows_path = fetch_category_by_path(session, path_segments, sc)
        if len(rows_path) > len(best_rows):
            best_rows = rows_path

        # 2) Fallback por ID
        if len(rows_path) < FALLBACK_THRESHOLD:
            rows_id = fetch_category_by_id(session, category_id, path_segments, sc)
            if len(rows_id) > len(best_rows):
                best_rows = rows_id

    return best_rows

# ===================== Helpers BD (upserts) =====================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
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
            """, (
                p.get("nombre") or "", p.get("marca") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    cur.execute("""
        SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1
    """, (p.get("nombre") or "", p.get("marca") or ""))
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
        """, (
            p.get("ean") or "", p.get("marca") or "", p.get("fabricante") or "",
            p.get("categoria") or "", p.get("subcategoria") or "", pid
        ))
        return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", p.get("nombre") or "", p.get("marca") or "",
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              record_id_tienda=COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, p.get("url") or "", p.get("nombre") or ""))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND sku_tienda=%s LIMIT 1",
                    (tienda_id, sku))
        return cur.fetchone()[0]

    if rec:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, p.get("url") or "", p.get("nombre") or ""))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND record_id_tienda=%s LIMIT 1",
                    (tienda_id, rec))
        return cur.fetchone()[0]

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, p.get("url") or "", p.get("nombre") or ""))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
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
        to_txt_or_none(p.get("precio_lista")), to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None,
        None, None, None, None     # VTEX genérico: si luego detectas descuentos, puedes mapearlos aquí
    ))

# ===================== Main =====================
def main():
    session = s_requests()
    print(f"Descargando árbol de categorías (depth={TREE_DEPTH})…")
    try:
        tree = load_tree(session)
    except Exception as e:
        print("No se pudo cargar el árbol:", e)
        sys.exit(1)

    leaves = flatten_leaves(tree)

    # Filtrado por prefijos (si se definieron); si está vacío, se procesan TODAS
    if INCLUDE_PREFIXES:
        prefset = {s.lower() for s in INCLUDE_PREFIXES}
        leaves = [(p, cid, name) for (p, cid, name) in leaves if p and p[0].lower() in prefset]
    else:
        leaves = [(p, cid, name) for (p, cid, name) in leaves if p]

    print(f"Se detectaron {len(leaves)} hojas en el árbol a procesar:")
    for p, cid, name in leaves[:10]:
        print(" -", "/".join(p), f"(id={cid}, name={name})")
    if len(leaves) > 10:
        print(f"   … y {len(leaves)-10} más")

    if not leaves:
        print("No se hallaron hojas. ¿Endpoint o depth correcto?")
        sys.exit(1)

    productos: List[Dict[str, Any]] = []
    seen = set()

    for idx, (path, cid, name) in enumerate(leaves, 1):
        print(f"\n[{idx}/{len(leaves)}] --- Categoría hoja: /{'/'.join(path)} (id={cid}, name={name}) ---")
        try:
            cat_rows = fetch_category(session, path, cid)
        except Exception as e:
            print("  ⚠️ Error categoría:", e)
            continue

        # dedupe por (sku || ean, url)
        nuevos = 0
        for p in cat_rows:
            key = (p.get("sku") or p.get("ean") or "", p.get("url") or "")
            if key in seen:
                continue
            seen.add(key)
            productos.append(p)
            nuevos += 1
        print(f"   → {len(cat_rows)} filas (nuevos únicos: {nuevos})")

    if not productos:
        print("No se obtuvieron productos. Considera cambiar SALES_CHANNELS o TREE_DEPTH.")
        return

    # ====== Inserción directa en MySQL ======
    capturado_en = datetime.now()

    conn = None
    t0 = time.time()
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for p in productos:
            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            insertados += 1
            if insertados % 500 == 0:
                conn.commit()
                print(f"   💾 commit parcial: {insertados}")

        conn.commit()
        print(f"\n✅ Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

    print(f"⏱️ Tiempo total: {time.time() - t0:.2f} s")

if __name__ == "__main__":
    main()
