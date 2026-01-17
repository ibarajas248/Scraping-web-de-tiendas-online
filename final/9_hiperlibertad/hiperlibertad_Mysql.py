#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper HiperLibertad (VTEX)
- Extrae categorías y productos completos
- Inserta en MySQL con commits pequeños y retries por fila para evitar bucles por lock (1205)
"""

import sys, re, time, json, os
from typing import List, Dict, Tuple, Any
from urllib.parse import urlparse
from datetime import datetime

import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- tu conexión MySQL

# ===================== Config =====================
BASE = "https://www.hiperlibertad.com.ar"
TREE_DEPTH = 6
TREE_URL = f"{BASE}/api/catalog_system/pub/category/tree/{TREE_DEPTH}"
SEARCH = f"{BASE}/api/catalog_system/pub/products/search"

STEP = 50
TIMEOUT = 25
SLEEP_OK = 0.25
MAX_EMPTY = 2
RETRIES = 3
SALES_CHANNELS = ["1"]
INCLUDE_PREFIXES: List[str] = []  # si quieres filtrar por prefijos de ruta
FALLBACK_THRESHOLD = 5

TIENDA_CODIGO = "hiperlibertad"
TIENDA_NOMBRE = "Libertad"

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# --- Retries por fila ---
ROW_MAX_RETRIES = 2        # 2 intentos adicionales (total hasta 3 ejecuciones)
ROW_BACKOFF_BASE = 0.6     # segundos de backoff (exponencial: 0.6, 1.2, ...)

BATCH_COMMIT_EVERY = 100   # commit cada N filas

# ===================== Utils =====================
def s_requests() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=RETRIES,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    return s

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None

_price_clean_re = re.compile(r"[^\d,.\-]")
def parse_price(val) -> float:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return np.nan
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
    if x is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    return f"{round(float(v), 2)}"

def is_retryable_mysql(err: MySQLError) -> bool:
    """
    1205 = Lock wait timeout exceeded
    1213 = Deadlock found
    """
    code = None
    try:
        code = getattr(err, "errno", None)
        if code is None and getattr(err, "args", None):
            code = err.args[0]
    except Exception:
        pass
    return code in (1205, 1213)

def sleep_backoff(attempt: int):
    # attempt 1 -> 0.6s, attempt 2 -> ~1.2s
    time.sleep(ROW_BACKOFF_BASE * (2 ** (attempt - 1)))

# ===================== Categorías =====================
def load_tree(session: requests.Session) -> List[Dict[str, Any]]:
    r = session.get(TREE_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def url_to_path(url: str) -> List[str]:
    p = urlparse(url)
    return [seg for seg in p.path.lstrip("/").split("/") if seg]

def flatten_leaves(tree: List[Dict[str, Any]]) -> List[Tuple[List[str], int, str]]:
    leaves: List[Tuple[List[str], int, str]] = []

    def dfs(node: Dict[str, Any]):
        path = url_to_path(node.get("url", ""))
        children = node.get("children") or []
        # filtro opcional por prefijos (si se usa)
        if INCLUDE_PREFIXES and path:
            joined = "/".join(path)
            if not any(joined.startswith(pref) for pref in INCLUDE_PREFIXES):
                # si no coincide prefijo, no sigo por aquí
                return
        if not children:
            if path:
                cid = int(node.get("id") or 0)
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
    return float(co.get("ListPrice") or 0.0), float(co.get("Price") or 0.0), int(co.get("AvailableQuantity") or 0)

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
    return str(p.get("productReference") or item.get("itemId") or "").strip()

def product_url(p: Dict[str, Any]) -> str:
    return f"{BASE}/{(p.get('linkText') or '').strip()}/p"

def parse_records(p: Dict[str, Any], fullpath: List[str]) -> List[Dict[str, Any]]:
    rows = []
    for item in p.get("items") or []:
        ean = extract_ean(item)
        list_price, price, stock = parse_price_fields(item)
        rows.append({
            "ean": clean(ean),
            "sku": clean(extract_codigo_interno(p, item)),
            "record_id": None,
            "nombre": clean(p.get("productName")),
            "marca": clean(p.get("brand")),
            "fabricante": clean(p.get("Manufacturer") or p.get("brand")),
            "categoria": fullpath[0] if len(fullpath) >= 1 else None,
            "subcategoria": fullpath[1] if len(fullpath) >= 2 else None,
            "precio_lista": list_price,
            "precio_oferta": price,
            "tipo_oferta": "Oferta" if price and list_price and price < list_price else "",
            "url": product_url(p),
            "_stock": stock,
            "_ruta": "/".join(fullpath)
        })
    return rows

# ===================== Fetching =====================
def fetch_category_by_path(session: requests.Session, path_segments: List[str], sc: str) -> List[Dict[str, Any]]:
    rows, offset, empty_in_a_row = [], 0, 0
    map_str, path = build_map_for_path(path_segments), "/".join(path_segments)
    while True:
        params = {"_from": offset, "_to": offset + STEP - 1, "map": map_str, "sc": sc}
        r = session.get(f"{SEARCH}/{path}", headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 429:
            time.sleep(0.8)
            continue
        if r.status_code == 206:
            # VTEX a veces devuelve 206 con contenido parcial; intenta parsear igualmente
            try:
                data = r.json()
            except Exception:
                data = []
        else:
            r.raise_for_status()
            data = r.json() if r.content else []

        if not data:
            empty_in_a_row += 1
            if empty_in_a_row >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue

        empty_in_a_row = 0
        for p in data:
            rows.extend(parse_records(p, path_segments))
        offset += STEP
        time.sleep(SLEEP_OK)
    return rows

def fetch_category(session: requests.Session, path_segments: List[str], category_id: int) -> List[Dict[str, Any]]:
    best = []
    for sc in SALES_CHANNELS:
        rows = fetch_category_by_path(session, path_segments, sc)
        if len(rows) > len(best):
            best = rows
        if len(rows) < FALLBACK_THRESHOLD:
            # Fallback por ID si hiciera falta (no siempre es útil en VTEX)
            pass
    return best

# ===================== Helpers BD =====================
def upsert_tienda(cur, codigo, nombre) -> int:
    cur.execute("""
        INSERT INTO tiendas (codigo, nombre)
        VALUES (%s,%s)
        ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)
    """, (codigo, nombre))
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p) -> int:
    ean = clean(p.get("ean"))
    nombre = p.get("nombre")
    marca = p.get("marca")
    fabricante = p.get("fabricante")
    categoria = p.get("categoria")
    subcategoria = p.get("subcategoria")

    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            return row[0]

    cur.execute("""
        INSERT IGNORE INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))

    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            return row[0]

    # Fallback por nombre (riesgo de colisiones si no es único)
    cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
    row = cur.fetchone()
    if row:
        return row[0]

    # Si INSERT IGNORE ignoró por unique distinto, intenta forzar un insert normal
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    cur.execute("SELECT LAST_INSERT_ID()")
    return cur.fetchone()[0]

def upsert_producto_tienda(cur, tienda_id, producto_id, p) -> int:
    sku = clean(p.get("sku"))
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          producto_id=VALUES(producto_id),
          url_tienda=VALUES(url_tienda),
          nombre_tienda=VALUES(nombre_tienda)
    """, (tienda_id, producto_id, sku, p.get("url"), p.get("nombre")))
    # NULL-safe equality para sku
    cur.execute("""
        SELECT id FROM producto_tienda
        WHERE tienda_id=%s AND sku_tienda <=> %s
        LIMIT 1
    """, (tienda_id, sku))
    return cur.fetchone()[0]

def insert_historico(cur, tienda_id, pt_id, p, capturado_en):
    cur.execute("""
        INSERT INTO historico_precios
            (tienda_id, producto_tienda_id, capturado_en, precio_lista, precio_oferta, tipo_oferta)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            precio_lista = VALUES(precio_lista),
            precio_oferta = VALUES(precio_oferta),
            tipo_oferta = VALUES(tipo_oferta)
    """, (
        tienda_id,
        pt_id,
        capturado_en,
        to_txt_or_none(p.get("precio_lista")),
        to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta"),
    ))

# ===================== Main =====================
def main():
    session = s_requests()
    print(f"Descargando categorías (depth={TREE_DEPTH})…")
    try:
        leaves = flatten_leaves(load_tree(session))
    except Exception as e:
        print("❌ No se pudo cargar el árbol:", e)
        return

    if not leaves:
        print("❌ Árbol vacío")
        return

    print(f"Encontradas {len(leaves)} hojas/categorías")
    productos, seen = [], set()

    for idx, (path, cid, name) in enumerate(leaves, 1):
        print(f"[{idx}/{len(leaves)}] {name}")
        try:
            rows = fetch_category(session, path, cid)
        except Exception as e:
            print(" ⚠️ Error fetch categoría:", e)
            continue

        for p in rows:
            key = (p.get("sku") or p.get("ean") or "", p.get("url") or "")
            if key in seen:
                continue
            seen.add(key)
            productos.append(p)

    if not productos:
        print("❌ No se obtuvieron productos")
        return

    capturado_en = datetime.now()
    print(f"Insertando {len(productos)} productos…")

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        # (opcional) menor timeout de lock para fallar rápido en esta sesión
        try:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 5")
        except Exception:
            pass

        # (opcional) aislamiento menos estricto
        try:
            cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception:
            pass

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        ok, skipped = 0, 0
        for i, p in enumerate(productos, 1):
            sp_name = f"sp_row_{i}"
            # crear savepoint por fila (si el motor/privilegios lo permiten)
            try:
                cur.execute(f"SAVEPOINT {sp_name}")
            except Exception:
                sp_name = None

            attempted = 0
            while True:
                attempted += 1
                try:
                    pid = find_or_create_producto(cur, p)
                    ptid = upsert_producto_tienda(cur, tienda_id, pid, p)
                    insert_historico(cur, tienda_id, ptid, p, capturado_en)
                    ok += 1
                    # liberar savepoint si se creó
                    if sp_name:
                        try:
                            cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                        except Exception:
                            pass
                    break  # fila OK

                except MySQLError as e:
                    # ¿vale la pena reintentar?
                    if is_retryable_mysql(e) and attempted <= ROW_MAX_RETRIES:
                        # rollback solo a la fila actual
                        if sp_name:
                            try:
                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                            except Exception:
                                pass
                        # reconectar por seguridad
                        try:
                            if hasattr(conn, "reconnect"):
                                conn.reconnect(attempts=1, delay=0)
                            elif hasattr(conn, "ping"):
                                conn.ping(reconnect=True, attempts=1, delay=0)
                        except Exception:
                            pass
                        print(f" ❌ Error fila (reintento {attempted}/{ROW_MAX_RETRIES}): {e}")
                        sleep_backoff(attempted)
                        continue  # vuelve a intentar

                    # No retryable o agotados intentos: saltar
                    if sp_name:
                        try:
                            cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                            cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                        except Exception:
                            pass
                    skipped += 1
                    print(f" ⏭️  Saltando fila {i} (sku={p.get('sku')}, ean={p.get('ean')}): {e}")
                    break  # salir del while, seguir con siguiente fila

            # commits pequeños
            if i % BATCH_COMMIT_EVERY == 0:
                try:
                    conn.commit()
                    print(f" 💾 commit {i} (ok={ok}, skipped={skipped})")
                except MySQLError as ce:
                    print(f" ⚠️ Commit falló: {ce}. Haciendo ROLLBACK y reconectando.")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        if hasattr(conn, "reconnect"):
                            conn.reconnect(attempts=1, delay=0)
                        elif hasattr(conn, "ping"):
                            conn.ping(reconnect=True, attempts=1, delay=0)
                    except Exception:
                        pass

        # Commit final
        try:
            conn.commit()
        except MySQLError as ce:
            print(f" ⚠️ Commit final falló: {ce}. Intento ROLLBACK.")
            try:
                conn.rollback()
            except Exception:
                pass

        print(f"✅ Guardado {ok} filas, ⏭️ saltadas {skipped}, tienda {TIENDA_NOMBRE}")

    except MySQLError as e:
        print("❌ Error MySQL:", e)
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
