#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests, time, re, json
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import quote
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ========= Config =========
BASE = "https://www.jumbo.com.ar"
STEP = 50                    # VTEX: 0-49, 50-99, ...
SLEEP_OK = 0.25
TIMEOUT = 25
MAX_EMPTY = 2                # corta tras N páginas vacías seguidas
TREE_DEPTH = 5               # profundidad para descubrir categorías
RETRIES = 3                  # reintentos por request

TIENDA_CODIGO = "jumbo_ar"
TIENDA_NOMBRE = "Jumbo Argentina"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# ========= Helpers de limpieza =========
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')  # (queda, pero no exportamos XLSX)

def clean_text(v):
    if v is None:
        return ""
    if not isinstance(v, str):
        return str(v)
    try:
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", v)

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def req_json(url, session, params=None):
    last = None
    for i in range(RETRIES):
        try:
            r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    last = e
                    time.sleep(0.6)
            elif r.status_code in (429, 408, 500, 502, 503, 504):
                time.sleep(0.6 + 0.4 * i)
            else:
                time.sleep(0.3)
        except Exception as e:
            last = e
            time.sleep(0.5)
    return None

def parse_price_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def to_txt_or_none(x) -> Optional[str]:
    v = parse_price_float(x)
    if v is None:
        return None
    # guardamos como texto (compatible con tu esquema VARCHAR en historico_precios)
    return f"{round(float(v), 2)}"

# ========= Árbol de categorías =========
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

# ========= Parse de producto (a una fila por SKU, por vendedor) =========
def parse_rows_from_product(p: Dict[str, Any], base: str) -> List[Dict[str, Any]]:
    rows = []
    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = (p.get("brand") or "") or ""
    # brand_id = p.get("brandId")     # <-- (no usamos en BD)  ❮❮❮ COMENTADO
    link_text = p.get("linkText")
    link = f"{base}/{link_text}/p" if link_text else ""
    categories = [c.strip("/") for c in (p.get("categories") or [])]
    category_top = categories[0] if categories else ""
    category_full = " > ".join(categories)

    # specificationGroups → podrías extraer fabricante si te interesa
    specs = {}
    for grp in (p.get("specificationGroups") or []):
        for it in (grp.get("specifications") or []):
            k = it.get("name")
            v = it.get("value")
            if k and v:
                specs[k] = v
    # fabricante = specs.get("Fabricante") or ""  # <-- (no usamos ahora)  ❮❮❮ COMENTADO

    # clusterHighlights / properties (no usamos por el momento)
    cluster = p.get("clusterHighlights") or {}           # ❮❮❮ COMENTADO EN INSERCIÓN
    props   = p.get("properties") or {}                  # ❮❮❮ COMENTADO EN INSERCIÓN

    # Descripciones (no guardamos por ahora)
    # desc = clean_text(p.get("description") or p.get("descriptionShort") or p.get("metaTagDescription") or "")  # ❮❮❮ COMENTADO

    items = p.get("items") or []
    for it in items:
        sku_id = it.get("itemId")
        sku_name = clean_text(it.get("name") or "")
        ean = ""
        for ref in (it.get("referenceId") or []):
            if ref.get("Value"):
                ean = str(ref["Value"]); break

        # measurement_unit = it.get("measurementUnit")   # ❮❮❮ COMENTADO
        # unit_multiplier = it.get("unitMultiplier")     # ❮❮❮ COMENTADO

        # images = ", ".join(img.get("imageUrl", "") for img in (it.get("images") or []))  # ❮❮❮ COMENTADO

        sellers = it.get("sellers") or []
        if not sellers:
            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                # "sellerId": "", "sellerName": "",  # ❮❮❮ COMENTADO
                "price": None, "listPrice": None, "spotPrice": None,
                "name": name, "skuName": sku_name, "brand": brand,
                "ean": ean,
                "categoryTop": category_top, "categoryFull": category_full,
                "link": link, "linkText": link_text,
                # "measurementUnit": measurement_unit, "unitMultiplier": unit_multiplier,  # ❮❮❮ COMENTADO
                # "images": images, "description": desc,                                  # ❮❮❮ COMENTADO
                # "specs_json": json.dumps(specs, ensure_ascii=False),                    # ❮❮❮ COMENTADO
                # "cluster_json": json.dumps(cluster, ensure_ascii=False),                # ❮❮❮ COMENTADO
                # "properties_json": json.dumps(props, ensure_ascii=False),               # ❮❮❮ COMENTADO
                "teasers": [],   # guardamos lista cruda para promos
            })
            continue

        for s in sellers:
            offer = s.get("commertialOffer") or {}
            price = offer.get("Price")
            list_price = offer.get("PriceWithoutDiscount") #ListPrice en la api daba un precio altom diferente al real
            spot = offer.get("spotPrice", None)
            teasers = offer.get("Teasers") or []
            # installments = offer.get("Installments") or []  # ❮❮❮ COMENTADO
            # tax = offer.get("Tax"); reward = offer.get("RewardValue")  # ❮❮❮ COMENTADO

            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                # "sellerId": s.get("sellerId"), "sellerName": s.get("sellerName"),  # ❮❮❮ COMENTADO
                "price": price, "listPrice": list_price, "spotPrice": spot,
                "name": name, "skuName": sku_name, "brand": brand,
                "ean": ean,
                "categoryTop": category_top, "categoryFull": category_full,
                "link": link, "linkText": link_text,
                # "measurementUnit": measurement_unit, "unitMultiplier": unit_multiplier,  # ❮❮❮ COMENTADO
                # "images": images, "description": desc,                                  # ❮❮❮ COMENTADO
                # "specs_json": json.dumps(specs, ensure_ascii=False),                    # ❮❮❮ COMENTADO
                # "cluster_json": json.dumps(cluster, ensure_ascii=False),                # ❮❮❮ COMENTADO
                # "properties_json": json.dumps(props, ensure_ascii=False),               # ❮❮❮ COMENTADO
                "teasers": teasers,  # guardamos lista cruda para promos
            })
    return rows

# ========= Scrape por categoría =========
def fetch_category(session, cat_path):
    rows, empty_streak = [], 0
    offset = 0
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
            # Log breve
            try:
                ej = parsed[0]
                print(f"  -> {p.get('productName')} (filas: {len(parsed)}, ej. precio: {ej.get('price')}) [Cat: {cat_path}]")
            except Exception:
                pass

        offset += STEP
        time.sleep(SLEEP_OK)

    return rows

# ========= MySQL helpers =========
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, row: Dict[str, Any]) -> int:
    ean = (row.get("ean") or "").strip() or None
    nombre = (row.get("name") or "").strip()
    marca  = (row.get("brand") or "").strip()
    categoria = (row.get("categoryTop") or "").strip()
    subcategoria = ""
    cf = (row.get("categoryFull") or "")
    if " > " in cf:
        parts = [x.strip() for x in cf.split(">") if x.strip()]
        if parts:
            categoria = parts[0]
            subcategoria = parts[-1] if len(parts) > 1 else ""

    # fabricante = ""  # ← si quisieras mapear desde specs, actívalo

    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        r = cur.fetchone()
        if r:
            pid = r[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (nombre, marca, categoria, subcategoria, pid))
            return pid

    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca))
        r = cur.fetchone()
        if r:
            pid = r[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (ean or "", categoria, subcategoria, pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (ean or "", nombre, marca, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, row: Dict[str, Any]) -> int:
    sku = (row.get("skuId") or "").strip() or None
    record_id = (row.get("productId") or "").strip() or None
    url = (row.get("link") or "").strip()
    nombre_tienda = (row.get("name") or "").strip()

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
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    if record_id:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, row: Dict[str, Any], capturado_en: datetime):
    # preferimos spotPrice si existe
    precio_oferta = row.get("spotPrice", None)
    if precio_oferta is None:
        precio_oferta = row.get("price", None)

    # Teasers → nombres como “3x2”, “-20%”, etc.
    teasers = row.get("teasers") or []
    promo_tipo = "; ".join([str(t.get("name") or t.get("teaserType") or "").strip() for t in teasers if t]) or None
    # Para no perder señal completa guardamos el JSON en comentarios
    comentarios = json.dumps(teasers, ensure_ascii=False) if teasers else None

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
        to_txt_or_none(row.get("listPrice")), to_txt_or_none(precio_oferta),
        None,                               # tipo_oferta (no expuesto directamente en VTEX)  ❮❮❮ COMENTADO
        promo_tipo,
        None,                               # promo_texto_regular (p.ej. resumen de cuotas)   ❮❮❮ COMENTADO
        None,                               # promo_texto_descuento                             ❮❮❮ COMENTADO
        comentarios
    ))

# ========= Main =========
def main():
    session = requests.Session()
    print("Descubriendo categorías…")
    tree = get_category_tree(session, TREE_DEPTH)
    cat_paths = iter_paths(tree)
    print(f"Categorías detectadas: {len(cat_paths)}")

    capturado_en = datetime.now()

    conn = None
    total_insertados = 0
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        for i, path in enumerate(cat_paths, 1):
            try:
                print(f"[{i}/{len(cat_paths)}] {path}")
                rows = fetch_category(session, path)
                if not rows:
                    continue

                # Dedup conservador por (productId, skuId, listPrice, price/spotPrice)
                seen = set()
                filtered: List[Dict[str, Any]] = []
                for r in rows:
                    key = (r.get("productId"), r.get("skuId"), r.get("listPrice"), r.get("price"), r.get("spotPrice"))
                    if key in seen:
                        continue
                    seen.add(key)
                    filtered.append(r)

                for r in filtered:
                    producto_id = find_or_create_producto(cur, r)
                    pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, r)
                    insert_historico(cur, tienda_id, pt_id, r, capturado_en)
                    total_insertados += 1

                conn.commit()

            except KeyboardInterrupt:
                print("Interrumpido por usuario.")
                break
            except Exception as e:
                print(f"  ! Error en {path}: {e}")
                conn.rollback()

            time.sleep(0.25)  # micro pausa entre categorías

        print(f"\n💾 Guardado en MySQL: {total_insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
