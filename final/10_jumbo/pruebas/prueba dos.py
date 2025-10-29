#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Jumbo (VTEX) → MySQL con el patrón de inserción "Coto" mejorado
# - Usa base_datos.get_conn()
# - Inserta en: tiendas, productos, producto_tienda, historico_precios
# - Mapea VTEX → dict p (sku, record_id, ean, nombre, marca, categoria, subcategoria, url, precio_lista, precio_oferta, promo_tipo, etc.)
# - Parada por ENTER para guardar parcial.
# - Manejo de locks, truncado seguro de columnas de texto, commits incrementales
#   y tolerancia a 1264 (out-of-range) en columnas numéricas de precio.

import requests, time, re, json, sys, os, threading
import pandas as pd
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import quote
from datetime import datetime
from typing import Any, Dict, List, Optional
import numpy as np
from mysql.connector import Error as MySQLError, errors as myerr

# ========= Conexión =========
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- tu conexión MySQL

# ========= Config =========
BASE = "https://www.jumbo.com.ar"
STEP = 50                    # VTEX: 0-49, 50-99, ...
SLEEP_OK = 0.25              # pausa entre páginas
TIMEOUT = 25
MAX_EMPTY = 8                # corta tras N páginas vacías seguidas
TREE_DEPTH = 5               # profundidad para descubrir categorías
RETRIES = 3                  # reintentos por request

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

TIENDA_CODIGO = "jumbo_ar"
TIENDA_NOMBRE = "Jumbo Argentina"

# ========= Límites columnas (ajusta si tu schema difiere) =========
MAXLEN_NOMBRE       = 255
MAXLEN_URL          = 512
MAXLEN_TIPO_OFERTA  = 191
MAXLEN_PROMO_TXT    = 191
MAXLEN_PROMO_COMENT = 255
LOCK_ERRNOS = {1205, 1213}
OUT_OF_RANGE_ERRNO = 1264  # DataError: out of range value for column

# ========= Parada por ENTER =========
class StopController:
    def __init__(self):
        self._ev = threading.Event()
        self._t = threading.Thread(target=self._wait_enter, daemon=True)

    def _wait_enter(self):
        try:
            print("🛑 Presiona ENTER en cualquier momento para PARAR y guardar lo procesado…")
            _ = sys.stdin.readline()
            self._ev.set()
        except Exception:
            pass

    def start(self):
        self._t.start()

    def tripped(self) -> bool:
        return self._ev.is_set()

# ========= Helpers scraping =========
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
_price_clean_re = re.compile(r"[^\d,.\-]")
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean_text(v):
    if v is None:
        return ""
    if not isinstance(v, str):
        return v
    try:
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", v)

def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

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

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def req_json(url, session, params=None):
    for i in range(RETRIES):
        r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                time.sleep(0.6)
        elif r.status_code in (429, 408, 500, 502, 503, 504):
            time.sleep(0.6 + 0.4 * i)
        else:
            time.sleep(0.3)
    return None

# ========= Categorías =========
def get_category_tree(session, depth=TREE_DEPTH):
    url = f"{BASE}/api/catalog_system/pub/category/tree/{depth}"
    data = req_json(url, session)
    return data or []

def iter_paths(tree):
    """Devuelve todas las rutas 'slug/slug2/...' (incluye hojas y nodos intermedios)."""
    out = []
    def walk(node, path):
        slug = node.get("url", "").strip("/").split("/")[-1] or node.get("slug") or node.get("Name")
        if not slug:
            return
        new_path = path + [slug]
        out.append("/".join(new_path))
        for ch in (node.get("children") or []):
            walk(ch, new_path)
    for n in tree:
        walk(n, [])
    # normaliza y dedup
    uniq = []
    seen = set()
    for p in out:
        ps = p.strip("/").lower()
        if ps and ps not in seen:
            seen.add(ps)
            uniq.append(ps)
    return uniq

def map_for_path(path_str):
    depth = len([p for p in path_str.split("/") if p])
    return ",".join(["c"] * depth)

# ========= Utils DB =========
def _truncate(s, n):
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]

def _price_txt_or_none(x):
    """Devuelve string redondeado a 2 decimales o None si x no es convertible."""
    v = parse_price(x)
    if x is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    return f"{round(float(v), 2)}"

def exec_retry(cur, sql, params=(), max_retries=5, base_sleep=0.5):
    att = 0
    while True:
        try:
            cur.execute(sql, params)
            return
        except myerr.DatabaseError as e:
            code = getattr(e, "errno", None)
            if code in LOCK_ERRNOS and att < max_retries:
                wait = base_sleep * (2 ** att)
                print(f"[LOCK] errno={code} retry {att+1}/{max_retries} in {wait:.2f}s")
                time.sleep(wait)
                att += 1
                continue
            raise

# ========= MySQL helpers (patrón Coto endurecido) =========
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    exec_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
    nombre = _truncate((clean(p.get("nombre")) or ""), MAXLEN_NOMBRE)
    marca  = clean(p.get("marca")) or ""
    categoria   = clean(p.get("categoria"))
    subcategoria= clean(p.get("subcategoria"))
    fabricante  = clean(p.get("fabricante"))

    if ean:
        exec_retry(cur, "SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_retry(cur, """
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (nombre, marca or "", fabricante or "", categoria or "", subcategoria or "", pid))
            return pid

    if nombre and marca:
        exec_retry(cur, """SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                   (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_retry(cur, """
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (ean or "", fabricante or "", categoria or "", subcategoria or "", pid))
            return pid

    exec_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (ean or "", nombre, marca or "", fabricante or "", categoria or "", subcategoria or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = _truncate(clean(p.get("url")), MAXLEN_URL)
    nombre_tienda = _truncate((clean(p.get("nombre")) or ""), MAXLEN_NOMBRE)

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
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
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
        """, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    exec_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    """
    Inserta histórico. Si el esquema tiene DECIMAL ajustado (p.ej. DECIMAL(5,2))
    y el precio excede el rango → MySQL arroja 1264.
    Ante 1264, reintenta **una vez** con precios = NULL; si vuelve a fallar, omite.
    """
    # Precios como string o None (si no convertibles)
    precio_lista_txt  = _price_txt_or_none(p.get("precio_lista"))
    precio_oferta_txt = _price_txt_or_none(p.get("precio_oferta"))

    # tipo_oferta/promo recortados
    tipo_oferta = _truncate(clean(p.get("tipo_oferta")), MAXLEN_TIPO_OFERTA)
    promo_tipo  = _truncate(clean(p.get("promo_tipo")),  MAXLEN_PROMO_TXT)

    # textos de promo (si los usás)
    promo_texto_regular   = _truncate(clean(p.get("precio_regular_promo")), MAXLEN_PROMO_TXT)
    promo_texto_descuento = _truncate(clean(p.get("precio_descuento")),     MAXLEN_PROMO_TXT)

    # comentarios (útiles para debug)
    comentarios = []
    if p.get("categoria"):   comentarios.append(f"cat={p['categoria']}")
    if p.get("subcategoria"):comentarios.append(f"sub={p['subcategoria']}")
    promo_comentarios = _truncate(" | ".join(comentarios), MAXLEN_PROMO_COMENT) if comentarios else None

    sql = """
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
    """

    params = (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista_txt, precio_oferta_txt, tipo_oferta,
        promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios
    )

    try:
        exec_retry(cur, sql, params)
        return
    except myerr.DatabaseError as e:
        # Si es out-of-range en DECIMAL → reintento con precios NULL y sigo
        if getattr(e, "errno", None) == OUT_OF_RANGE_ERRNO:
            print(f"[WARN] 1264 out-of-range en precios (pid_tienda={producto_tienda_id}). Reintentando con NULL…")
            params_null = (
                tienda_id, producto_tienda_id, capturado_en,
                None, None, tipo_oferta,
                promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios
            )
            try:
                exec_retry(cur, sql, params_null)
                return
            except Exception as e2:
                print(f"[WARN] No se pudo insertar ni con precios NULL (pid_tienda={producto_tienda_id}). Omito. Detalle: {e2}")
                return
        # Otros errores: propagar (serán manejados por el caller con rollback/commit)
        raise

# ========= Parsing de producto (mapea a dict p) =========
def parse_rows_from_product_and_ps(p, base):
    """
    Devuelve:
      - rows: filas detalladas (por SKU/seller) — útil para debug/export
      - ps:   lista de dict 'p' (forma patrón Coto) para DB
    """
    rows = []
    ps: List[Dict[str, Any]] = []

    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = p.get("brand")
    brand_id = p.get("brandId")
    link_text = p.get("linkText")
    link = f"{base}/{link_text}/p" if link_text else ""
    categories = [c.strip("/") for c in (p.get("categories") or [])]
    category_path = " > ".join(categories[:1]) if categories else ""
    full_category_path = " > ".join(categories)

    # Derivar Categoria/Subcategoria
    parts = [x.strip() for x in full_category_path.split(">")] if full_category_path else []
    parts = [x for x in parts if x]
    categoria = parts[0] if parts else None
    subcategoria = parts[-1] if len(parts) >= 2 else None

    # Atributos/Specs
    specs = {}
    for grp in (p.get("specificationGroups") or []):
        for it in (grp.get("specifications") or []):
            k = it.get("name")
            v = it.get("value")
            if k and v:
                specs[k] = v

    cluster = p.get("clusterHighlights") or {}
    props = p.get("properties") or {}

    desc = clean_text(p.get("description") or p.get("descriptionShort") or p.get("metaTagDescription") or "")

    items = p.get("items") or []
    for it in items:
        sku_id = it.get("itemId")
        sku_name = clean_text(it.get("name"))
        ean = ""
        for ref in (it.get("referenceId") or []):
            if ref.get("Value"):
                ean = ref["Value"]; break

        measurement_unit = it.get("measurementUnit")
        unit_multiplier = it.get("unitMultiplier")
        images = ", ".join(img.get("imageUrl", "") for img in (it.get("images") or []))

        sellers = it.get("sellers") or []
        if not sellers:
            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                "sellerId": "",
                "sellerName": "",
                "availableQty": None,
                "price": None,
                "listPrice": None,
                "priceWithoutDiscount": None,
                "installments_json": "",
                "teasers_json": "",
                "tax": None,
                "rewardValue": None,
                "spotPrice": None,
                "name": name,
                "skuName": sku_name,
                "brand": brand,
                "brandId": brand_id,
                "ean": ean,
                "categoryTop": category_path,
                "categoryFull": full_category_path,
                "link": link,
                "linkText": link_text,
                "measurementUnit": measurement_unit,
                "unitMultiplier": unit_multiplier,
                "images": images,
                "description": desc,
                "specs_json": json.dumps(specs, ensure_ascii=False),
                "cluster_json": json.dumps(cluster, ensure_ascii=False),
                "properties_json": json.dumps(props, ensure_ascii=False),
            })
            ps.append({
                "sku": clean(sku_id),
                "record_id": clean(product_id),
                "ean": clean(ean),
                "nombre": _truncate(clean(name), MAXLEN_NOMBRE),
                "marca": clean(brand),
                "fabricante": None,
                "precio_lista": None,
                "precio_oferta": None,
                "tipo_oferta": None,
                "promo_tipo": None,
                "precio_regular_promo": None,
                "precio_descuento": None,
                "comentarios_promo": None,
                "categoria": clean(categoria),
                "subcategoria": clean(subcategoria),
                "url": _truncate(clean(link), MAXLEN_URL),
            })
            continue

        for s in sellers:
            s_id = s.get("sellerId")
            s_name = s.get("sellerName")
            offer = s.get("commertialOffer") or {}
            price = offer.get("Price")
            list_price = offer.get("ListPrice")
            pwd = offer.get("PriceWithoutDiscount")
            avail = offer.get("AvailableQuantity")
            tax = offer.get("Tax")
            reward = offer.get("RewardValue")
            installments = offer.get("Installments") or []
            teasers = offer.get("Teasers") or []
            spot = offer.get("spotPrice", None)

            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                "sellerId": s_id,
                "sellerName": s_name,
                "availableQty": avail,
                "price": price,
                "listPrice": list_price,
                "priceWithoutDiscount": pwd,
                "installments_json": json.dumps(installments, ensure_ascii=False),
                "teasers_json": json.dumps(teasers, ensure_ascii=False),
                "tax": tax,
                "rewardValue": reward,
                "spotPrice": spot,
                "name": name,
                "skuName": sku_name,
                "brand": brand,
                "brandId": brand_id,
                "ean": ean,
                "categoryTop": category_path,
                "categoryFull": full_category_path,
                "link": link,
                "linkText": link_text,
                "measurementUnit": measurement_unit,
                "unitMultiplier": unit_multiplier,
                "images": images,
                "description": desc,
                "specs_json": json.dumps(specs, ensure_ascii=False),
                "cluster_json": json.dumps(cluster, ensure_ascii=False),
                "properties_json": json.dumps(props, ensure_ascii=False),
            })

            # promociones
            promo_txts = []
            for t in teasers:
                nm = t.get("name") or t.get("title") or ""
                if nm:
                    promo_txts.append(str(nm))
            promo_tipo = "; ".join(promo_txts) if promo_txts else None

            ps.append({
                "sku": clean(sku_id),
                "record_id": clean(product_id),
                "ean": clean(ean),
                "nombre": _truncate(clean(name), MAXLEN_NOMBRE),
                "marca": clean(brand),
                "fabricante": None,
                "precio_lista": list_price,
                "precio_oferta": (spot if spot not in (None, "") else price),
                "tipo_oferta": None,
                "promo_tipo": promo_tipo,
                "precio_regular_promo": None,
                "precio_descuento": None,
                "comentarios_promo": None,
                "categoria": clean(categoria),
                "subcategoria": clean(subcategoria),
                "url": _truncate(clean(link), MAXLEN_URL),
            })

    return rows, ps

# ========= Scrape por categoría (con ENTER) =========
def fetch_category(session, cat_path, stopper: StopController):
    rows, ps_all = [], []
    offset, empty_streak = 0, 0

    map_str = map_for_path(cat_path)
    encoded_path = quote(cat_path, safe="/")

    while True:
        if stopper.tripped():
            break

        url = f"{BASE}/api/catalog_system/pub/products/search/{encoded_path}?map={map_str}&_from={offset}&_to={offset+STEP-1}"
        data = req_json(url, session)

        if data is None:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue

        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue

        empty_streak = 0

        for p in data:
            r, ps = parse_rows_from_product_and_ps(p, BASE)
            rows.extend(r)
            ps_all.extend(ps)

            if r:
                print(f"  -> {p.get('productName')} ({len(r)} filas, ej. precio: {r[0].get('price')}) [Cat: {cat_path}]")
            if stopper.tripped():
                break

        offset += STEP
        if stopper.tripped():
            break
        time.sleep(SLEEP_OK)

    return rows, ps_all

# ========= Inserción incremental =========
def insert_batch(conn, tienda_id: int, ps: List[Dict[str, Any]], capturado_en: datetime) -> int:
    """Inserta un lote (ya dentro de una transacción abierta)."""
    if not ps:
        return 0
    cur = conn.cursor()
    total = 0
    seen = set()
    for p in ps:
        key = (p.get("sku"), p.get("record_id"), p.get("url"), p.get("precio_oferta"))
        if key in seen:
            continue
        seen.add(key)
        producto_id = find_or_create_producto(cur, p)
        pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
        insert_historico(cur, tienda_id, pt_id, p, capturado_en)
        total += 1
    return total

# ========= Main =========
def main():
    stopper = StopController()
    stopper.start()
    session = requests.Session()

    print("Descubriendo categorías…")
    tree = get_category_tree(session, TREE_DEPTH)
    cat_paths = iter_paths(tree)
    print(f"Categorías detectadas: {len(cat_paths)}")

    conn = None
    capturado_en = datetime.now()
    total_insertados = 0

    try:
        conn = get_conn()
        # Afinar sesión para menos bloqueos
        try:
            with conn.cursor() as cset:
                cset.execute("SET SESSION innodb_lock_wait_timeout = 5")
                cset.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
        except Exception:
            pass

        conn.autocommit = False
        cur = conn.cursor()
        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        conn.commit()

        for i, path in enumerate(cat_paths, 1):
            if stopper.tripped():
                print("🛑 Parada solicitada. Guardando lo acumulado…")
                break

            print(f"[{i}/{len(cat_paths)}] {path}")
            rows, ps = fetch_category(session, path, stopper)

            if ps:
                inc = insert_batch(conn, tienda_id, ps, capturado_en)
                conn.commit()
                total_insertados += inc
                print(f"💾 Commit categoría '{path}' → +{inc} registros (acum: {total_insertados})")

            if stopper.tripped():
                break

            time.sleep(0.25)

    except KeyboardInterrupt:
        print("🛑 Interrumpido por usuario (Ctrl+C). Guardando lo acumulado…")
        try:
            if conn: conn.commit()
        except Exception:
            pass
    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
        raise
    finally:
        if conn:
            try: conn.close()
            except: pass
        print(f"🏁 Finalizado. Histórico insertado: {total_insertados} filas para {TIENDA_NOMBRE} ({capturado_en})")

if __name__ == "__main__":
    main()
