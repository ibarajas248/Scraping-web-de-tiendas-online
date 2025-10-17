#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Jumbo (VTEX) → MySQL con el patrón de inserción "Coto"
# - Usa base_datos.get_conn()
# - Inserta en: tiendas, productos, producto_tienda, historico_precios
# - Mapea VTEX → dict p con mismas claves que Coto
# - Añadido: PARADA POR ENTER para insertar parcial y salir.

import requests, time, re, json, sys, os, threading
import pandas as pd
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import quote
from datetime import datetime
from typing import Any, Dict, List, Optional
import numpy as np
from mysql.connector import Error as MySQLError

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

# ========= MySQL helpers (patrón Coto) =========
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

    # evitar pegar por (nombre, marca) si marca viene vacía
    nombre = clean(p.get("nombre")) or ""
    marca  = clean(p.get("marca")) or ""
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("ean") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", nombre, marca,
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    """Upsert que devuelve ID con LAST_INSERT_ID para evitar SELECT extra."""
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre") or ""

    # Preferimos clave única por SKU si existe
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

    # Si no hay SKU, usamos record_id
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

    # Último recurso (sin llaves naturales)
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    def to_txt_or_none(x):
        v = parse_price(x)
        if x is None: return None
        if isinstance(v, float) and np.isnan(v): return None
        return f"{round(float(v), 2)}"  # guardamos como VARCHAR

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
        p.get("tipo_oferta") or None, p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None, p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# ========= Parsing de producto (tu misma función, pero además mapeo a dict p) =========
def parse_rows_from_product_and_ps(p, base):
    """
    Devuelve:
      - rows: mismas filas que tu XLSX (por SKU/seller)
      - ps:   lista de dict 'p' (misma forma que Coto) para insertar en DB
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

    # Derivar Categoria/Subcategoria para DB
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
            # fila “sin seller” (para Excel)
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
            # dict p para DB (sin precios)
            ps.append({
                "sku": clean(sku_id),
                "record_id": clean(product_id),
                "ean": clean(ean),
                "nombre": clean(name),
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
                "url": clean(link),
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

            # promos
            promo_txts = []
            for t in teasers:
                nm = t.get("name") or t.get("title") or ""
                if nm: promo_txts.append(str(nm))
            promo_tipo = "; ".join(promo_txts) if promo_txts else None

            # dict p para DB (precio_oferta prioriza spotPrice si existe)
            ps.append({
                "sku": clean(sku_id),
                "record_id": clean(product_id),
                "ean": clean(ean),
                "nombre": clean(name),
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
                "url": clean(link),
            })

    return rows, ps

# ========= Scrape por categoría (con ENTER) =========
def fetch_category(session, cat_path, stopper: StopController):
    """Devuelve rows (para debug/xlsx si quisieras) y ps (para DB). Respeta parada por ENTER."""
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

            # --- 📢 Seguimiento en consola ---
            if r:
                print(f"  -> {p.get('productName')} "
                      f"({len(r)} filas, ej. precio: {r[0].get('price')}) "
                      f"[Cat: {cat_path}]")

            if stopper.tripped():
                break

        offset += STEP
        if stopper.tripped():
            break
        time.sleep(SLEEP_OK)

    return rows, ps_all

# ========= Inserción en DB =========
def insert_mysql(ps: List[Dict[str, Any]], capturado_en: datetime):
    if not ps:
        print("ℹ️ Nada para insertar.")
        return 0

    conn = None
    total = 0
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        seen = set()
        for p in ps:
            # dedup conservador para no spamear histórico si VTEX repite
            key = (p.get("sku"), p.get("record_id"), p.get("url"), p.get("precio_oferta"))
            if key in seen:
                continue
            seen.add(key)

            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            total += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {total} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

    return total

def insert_batch(conn, tienda_id: int, ps: List[Dict[str, Any]], capturado_en: datetime) -> int:
    """Igual a insert_mysql pero reutilizable por categoría con conexión abierta."""
    if not ps:
        return 0
    cur = conn.cursor()
    seen = set()
    total = 0
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

    # Cambiamos a inserción incremental por CATEGORÍA (para poder parar con ENTER y conservar lo procesado)
    conn = None
    capturado_en = datetime.now()
    total_insertados = 0

    try:
        conn = get_conn()
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

            # Si se activó ENTER durante la categoría, ya hicimos commit del parcial:
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
    finally:
        if conn:
            try: conn.close()
            except: pass
        print(f"🏁 Finalizado. Histórico insertado: {total_insertados} filas para {TIENDA_NOMBRE} ({capturado_en})")

if __name__ == "__main__":
    main()
