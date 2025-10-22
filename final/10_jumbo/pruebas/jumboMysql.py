#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Jumbo (VTEX) — Scraper + Ingesta MySQL

- Descubre categorías vía /api/catalog_system/pub/category/tree/<depth>
- Recorre todas las rutas (incluye nodos intermedios) usando VTEX products/search
- Aplana a filas por (SKU, seller)
- Inserta/actualiza en:
    tiendas, productos, producto_tienda, historico_precios
  * precio_lista  = ListPrice
  * precio_oferta = Price
  * tipo_oferta   = nombres de teasers (recortados)

Depende de base_datos.get_conn() -> mysql.connector.connect(...)

Instalar:
  pip install requests pandas beautifulsoup4 lxml mysql-connector-python
"""

import requests, time, re, json, sys, argparse
import pandas as pd
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import quote
from datetime import datetime as dt
import mysql.connector
from mysql.connector import errors as myerr

# ======= Config scraping =======
BASE = "https://www.jumbo.com.ar"
STEP = 50                    # VTEX: 0-49, 50-99, ...
SLEEP_OK = 0.25
TIMEOUT = 25
MAX_EMPTY = 2
TREE_DEPTH = 5
RETRIES = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# ======= Identidad tienda =======
TIENDA_CODIGO = "jumbo_vtex"
TIENDA_NOMBRE = "Jumbo (VTEX)"

# ======= Límites de columnas (ajusta a tu schema) =======
MAXLEN_NOMBRE       = 255
MAXLEN_URL          = 512
MAXLEN_TIPO_OFERTA  = 191
MAXLEN_COMENTARIOS  = 255
LOCK_ERRNOS = {1205, 1213}

import sys, os
# ---------- MySQL ----------
import mysql.connector
from mysql.connector import errors as myerr
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
# --

# ======= Conexión MySQL (tu helper) =======
from base_datos import get_conn  # debe devolver mysql.connector.connect(...)

# ========= Helpers de texto =========
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

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
    uniq, seen = [], set()
    for p in out:
        ps = p.strip("/").lower()
        if ps and ps not in seen:
            seen.add(ps)
            uniq.append(ps)
    return uniq

def map_for_path(path_str):
    depth = len([p for p in path_str.split("/") if p])
    return ",".join(["c"] * depth)

# ========= Parsing de producto =========
def parse_rows_from_product(p, base):
    """Una fila por SKU y seller."""
    rows = []
    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = p.get("brand")
    link_text = p.get("linkText")
    link = f"{base}/{link_text}/p" if link_text else ""
    categories = [c.strip("/") for c in (p.get("categories") or [])]
    categoria = categories[0] if categories else ""
    subcategoria = categories[-1] if categories else ""
    full_category_path = " > ".join(categories)

    # Specs / clusters / props
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
        # ean suele venir en it['ean'] o en referenceId
        ean = (it.get("ean") or "").strip()
        if not ean:
            for ref in (it.get("referenceId") or []):
                if ref.get("Value"):
                    ean = str(ref["Value"]).strip(); break

        images = ", ".join(img.get("imageUrl", "") for img in (it.get("images") or []))
        sellers = it.get("sellers") or []

        if not sellers:
            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                "sellerId": "",
                "sellerName": "",
                "price": None,
                "listPrice": None,
                "name": name,
                "skuName": sku_name,
                "brand": brand,
                "ean": ean,
                "categoria": categoria,
                "subcategoria": subcategoria,
                "categoryFull": full_category_path,
                "link": link,
                "images": images,
                "description": desc,
                "teasers_names": None,
                "specs_json": json.dumps(specs, ensure_ascii=False),
                "cluster_json": json.dumps(cluster, ensure_ascii=False),
                "properties_json": json.dumps(props, ensure_ascii=False),
            })
            continue

        for s in sellers:
            s_id = s.get("sellerId")
            s_name = s.get("sellerName")
            offer = s.get("commertialOffer") or {}
            price = offer.get("Price")
            list_price = offer.get("ListPrice")
            # Teasers -> nombres
            teasers = offer.get("Teasers") or []
            pteasers = offer.get("PromotionTeasers") or []
            teaser_names = []
            for t in teasers + pteasers:
                nm = t.get("Name") or t.get("name")
                if nm:
                    teaser_names.append(str(nm))
            # dedup conservando orden
            seen = set(); teaser_names2 = []
            for nm in teaser_names:
                if nm not in seen:
                    seen.add(nm); teaser_names2.append(nm)

            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                "sellerId": s_id,
                "sellerName": s_name,
                "price": price,
                "listPrice": list_price,
                "name": name,
                "skuName": sku_name,
                "brand": brand,
                "ean": ean,
                "categoria": categoria,
                "subcategoria": subcategoria,
                "categoryFull": full_category_path,
                "link": link,
                "images": images,
                "description": desc,
                "teasers_names": ", ".join(teaser_names2) if teaser_names2 else None,
                "specs_json": json.dumps(specs, ensure_ascii=False),
                "cluster_json": json.dumps(cluster, ensure_ascii=False),
                "properties_json": json.dumps(props, ensure_ascii=False),
            })
    return rows

# ========= Scrape por categoría =========
def fetch_category(session, cat_path):
    rows, offset, empty_streak = [], 0, 0
    map_str = map_for_path(cat_path)
    encoded_path = quote(cat_path, safe="/")

    while True:
        url = f"{BASE}/api/catalog_system/pub/products/search/{encoded_path}?map={map_str}&_from={offset}&_to={offset+STEP-1}"
        data = req_json(url, session)

        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue

        empty_streak = 0

        for p in data:
            parsed = parse_rows_from_product(p, BASE)
            rows.extend(parsed)

            # log
            try:
                sample = parsed[0]
                print(f"  -> {p.get('productName')} ({len(parsed)} filas) $ej: {sample.get('price')}  [Cat: {cat_path}]")
            except Exception:
                pass

        offset += STEP
        time.sleep(SLEEP_OK)

    return rows

# ========= SQL helpers =========
def _truncate(s, n):
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]

def _price_str(val):
    if val is None:
        return None
    try:
        f = float(val)
        if pd.isna(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

def exec_retry(cur, sql, params=(), max_retries=5, base_sleep=0.4):
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

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    exec_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: dict) -> int:
    ean = (r.get("ean") or None)
    nombre = _truncate(r.get("name") or r.get("skuName") or "", MAXLEN_NOMBRE)
    marca = (r.get("brand") or None)
    categoria = (r.get("categoria") or None)
    subcategoria = (r.get("subcategoria") or None)

    # 1) por EAN
    if ean:
        exec_retry(cur, "SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_retry(cur, """
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(%s, marca),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (nombre, marca, categoria, subcategoria, pid))
            return pid

    # 2) por (nombre, marca)
    if nombre and marca:
        exec_retry(cur, "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
                   (nombre, marca or ""))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_retry(cur, """
                UPDATE productos SET
                  ean = COALESCE(%s, ean),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (ean, categoria, subcategoria, pid))
            return pid

    # 3) nuevo
    exec_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, %s, %s, NULL, %s, %s)
    """, (ean, nombre, marca, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: dict) -> int:
    sku = (r.get("skuId") or None)
    record_id = (r.get("productId") or None)
    url = _truncate(r.get("link") or None, MAXLEN_URL)
    nombre_tienda = _truncate(r.get("name") or r.get("skuName") or None, MAXLEN_NOMBRE)

    if sku:
        exec_retry(cur, """
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
        exec_retry(cur, """
            INSERT INTO producto_tienda (tienda_id, producto_id, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    exec_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: dict, capturado_en: dt):
    precio_lista  = _price_str(r.get("listPrice"))
    precio_oferta = _price_str(r.get("price"))
    # tipo_oferta: nombres de teasers (recortado)
    tipo = _truncate(r.get("teasers_names") or "", MAXLEN_TIPO_OFERTA) or None

    # Comentarios útiles
    comentarios = []
    if r.get("sellerName"): comentarios.append(f"seller={r['sellerName']}")
    if r.get("categoryFull"): comentarios.append(f"cat={r['categoryFull']}")
    promo_comentarios = _truncate(" | ".join(comentarios), MAXLEN_COMENTARIOS) if comentarios else None

    exec_retry(cur, """
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
    """, (tienda_id, producto_tienda_id, capturado_en,
          precio_lista, precio_oferta, tipo,
          None, None, None, promo_comentarios))

def ingest_to_mysql(rows: list):
    if not rows:
        print("⚠ No hay filas para insertar.")
        return

    conn = None
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
        cur = conn.cursor(buffered=True)

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        capturado_en = dt.now()

        # Dedupe conservador por (skuId, sellerId) si existen, si no por (productId, skuId)
        df = pd.DataFrame(rows)
        if not df.empty:
            if {"skuId","sellerId"} <= set(df.columns):
                df = df.drop_duplicates(subset=["skuId","sellerId"], keep="first")
            elif {"productId","skuId"} <= set(df.columns):
                df = df.drop_duplicates(subset=["productId","skuId"], keep="first")
        rows = df.to_dict(orient="records")

        total = 0
        batch = 0
        for rec in rows:
            pid  = find_or_create_producto(cur, rec)
            ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)
            insert_historico(cur, tienda_id, ptid, rec, capturado_en)

            batch += 1
            total += 1
            if batch >= 50:
                conn.commit()
                batch = 0

        if batch:
            conn.commit()

        print(f"✅ MySQL: {total} filas guardadas/actualizadas en histórico.")
    except mysql.connector.Error as e:
        if conn: conn.rollback()
        print(f"❌ MySQL error {getattr(e,'errno',None)}: {e}")
        raise
    except Exception as e:
        if conn: conn.rollback()
        raise
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

# ========= Runner =========
def scrape_all(depth=TREE_DEPTH) -> list:
    session = requests.Session()
    print("Descubriendo categorías…")
    tree = get_category_tree(session, depth)
    cat_paths = iter_paths(tree)
    print(f"Categorías detectadas: {len(cat_paths)}")

    all_rows = []
    for i, path in enumerate(cat_paths, 1):
        try:
            print(f"[{i}/{len(cat_paths)}] {path}")
            rows = fetch_category(session, path)
            if rows:
                all_rows.extend(rows)
        except KeyboardInterrupt:
            print("Interrumpido por usuario."); break
        except Exception as e:
            print(f"  ! Error en {path}: {e}")
        time.sleep(0.25)
    return all_rows

def main():
    ap = argparse.ArgumentParser(description="Jumbo (VTEX) → MySQL")
    ap.add_argument("--depth", type=int, default=TREE_DEPTH, help="Profundidad de árbol de categorías")
    ap.add_argument("--dry", action="store_true", help="Solo scrapea (no inserta)")
    ap.add_argument("--xlsx", type=str, default=None, help="Guardar XLSX de respaldo (opcional)")
    args = ap.parse_args()

    rows = scrape_all(depth=args.depth)

    if not rows:
        print("No se obtuvieron filas.")
        sys.exit(1)

    if args.xlsx:
        df = pd.DataFrame(rows)
        # columnas principales para auditoría rápida
        cols = [
            "productId","skuId","sellerId","sellerName",
            "price","listPrice","name","skuName","brand","ean",
            "categoria","subcategoria","categoryFull","link"
        ]
        for c in cols:
            if c not in df.columns: df[c] = ""
        df[cols].to_excel(args.xlsx, index=False)
        print(f"📄 XLSX guardado: {args.xlsx}")

    if not args.dry:
        ingest_to_mysql(rows)

if __name__ == "__main__":
    main()
