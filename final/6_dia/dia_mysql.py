#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time, json
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional

import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError

import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # ⚠️ Asegúrate de configurarlo a tu entorno

# =============== Config ===============
CATEGORIAS = [
    "almacen","bebidas","frescos","desayuno","limpieza","perfumeria",
    "congelados","bebes-y-ninos","hogar-y-deco","mascotas",
    "almacen/golosinas-y-alfajores","frescos/frutas-y-verduras","electro-hogar",
]

BASE_API = "https://diaonline.supermercadosdia.com.ar/api/catalog_system/pub/products/search"
BASE_WEB = "https://diaonline.supermercadosdia.com.ar"
STEP = 50
SLEEP_OK = 0.35
TIMEOUT = 25
MAX_EMPTY = 2
HEADERS = {"User-Agent": "Mozilla/5.0"}

TIENDA_CODIGO = "dia"
TIENDA_NOMBRE = "Dia"

# =============== Sesión HTTP con retries ===============
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4, backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update(HEADERS)
    return s

# =============== Utils scraping/parsing ===============
def split_cat(path_or_slug: str) -> Tuple[str, str]:
    if not path_or_slug: return "", ""
    parts = [p for p in path_or_slug.strip("/").split("/") if p]
    if not parts: return "", ""
    norm = lambda s: s.replace("-", " ").strip().title()
    return norm(parts[0]), norm(parts[1]) if len(parts) > 1 else ""

def first_category_parts(prod: dict, fallback_slug: str) -> Tuple[str, str]:
    cats = prod.get("categories") or []
    if isinstance(cats, list) and cats and isinstance(cats[0], str):
        return split_cat(cats[0])
    return split_cat(fallback_slug)

def tipo_de_oferta(offer: dict, list_price: float, price: float) -> str:
    try:
        dh = offer.get("DiscountHighLight") or []
        if isinstance(dh, list) and dh:
            name = (dh[0].get("Name") or "").strip()
            if name:
                return name
    except Exception:
        pass
    return "Descuento" if (price is not None and list_price is not None and price < list_price) else "Precio regular"

def safe_float(x) -> Optional[float]:
    try:
        if x is None: return None
        v = float(x)
        if np.isnan(v): return None
        return v
    except Exception:
        return None

# =============== Helpers BD (mismo patrón que Coto) ===============
def clean(val):
    if val is None: return None
    s = str(val).strip()
    return s if s else None

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
            """, (p.get("nombre") or "", p.get("marca") or "", p.get("fabricante") or "",
                  p.get("categoria") or "", p.get("subcategoria") or "", pid))
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
        """, (p.get("ean") or "", p.get("marca") or "", p.get("fabricante") or "",
              p.get("categoria") or "", p.get("subcategoria") or "", pid))
        return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (p.get("ean") or "", p.get("nombre") or "", p.get("marca") or "",
          p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))              # VTEX itemId
    pid = clean(p.get("product_id"))       # VTEX productId

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              record_id_tienda=COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, pid, p.get("url") or "", p.get("nombre") or ""))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND sku_tienda=%s LIMIT 1",
                    (tienda_id, sku))
        return cur.fetchone()[0]

    if pid:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, pid, p.get("url") or "", p.get("nombre") or ""))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND record_id_tienda=%s LIMIT 1",
                    (tienda_id, pid))
        return cur.fetchone()[0]

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, p.get("url") or "", p.get("nombre") or ""))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    def to_txt_or_none(x):
        v = safe_float(x)
        if v is None: return None
        return f"{round(float(v), 2)}"

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
        p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None,
        p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# =============== Scraping DIA → objetos normalizados ===============
def scrape_categoria(session: requests.Session, slug_categoria: str) -> List[Dict[str, Any]]:
    print(f"\n🔎 Explorando categoría: {slug_categoria}")
    out: List[Dict[str, Any]] = []
    offset = 0
    empty_streak = 0

    while True:
        url = f"{BASE_API}/{slug_categoria}?_from={offset}&_to={offset+STEP-1}"
        try:
            r = session.get(url, timeout=TIMEOUT)
        except Exception as e:
            print(f"⚠️ Red: {e}"); break

        if r.status_code not in (200, 206):
            print(f"⚠️ HTTP {r.status_code} — corto '{slug_categoria}'"); break

        try:
            data = r.json()
        except Exception as e:
            print(f"❌ JSON err en '{slug_categoria}': {e}"); break

        if not data:
            empty_streak += 1
            print(f"✔️ página vacía {empty_streak}/{MAX_EMPTY} en {offset}-{offset+STEP-1}")
            if empty_streak >= MAX_EMPTY: break
            offset += STEP; time.sleep(SLEEP_OK); continue

        empty_streak = 0
        nuevos = 0

        for prod in data:
            try:
                items = prod.get("items") or []
                sellers = items[0].get("sellers") if items else []
                offer = (sellers[0].get("commertialOffer") if sellers else {}) or {}

                list_price = safe_float(offer.get("ListPrice"))
                price = safe_float(offer.get("Price"))

                ean = (items[0].get("ean") if items else None) or None
                item_id = (items[0].get("itemId") if items else None) or None
                product_id = prod.get("productId")

                nombre = prod.get("productName") or ""
                marca = prod.get("brand") or ""
                fabricante = prod.get("manufacturer") or ""
                cat, sub = first_category_parts(prod, slug_categoria)
                slug = prod.get("linkText") or ""
                url_prod = f"{BASE_WEB}/{slug}/p" if slug else ""

                oferta_tipo = tipo_de_oferta(offer, list_price, price)

                # Opcionales VTEX de promos:
                dh_list = (offer.get("DiscountHighLight") or [])
                promo_tipo = "; ".join([d.get("Name") for d in dh_list if isinstance(d, dict) and d.get("Name")]) or None
                promo_texto_regular = None
                promo_texto_descuento = None
                promo_comentarios = None

                out.append({
                    # Base común:
                    "sku": item_id,
                    "record_id": None,         # DIA VTEX no usa record_id; dejamos productId abajo
                    "product_id": product_id,
                    "ean": ean,
                    "nombre": nombre,
                    "marca": marca,
                    "fabricante": fabricante,
                    "categoria": cat,
                    "subcategoria": sub,
                    "url": url_prod,
                    # Precios y promos:
                    "precio_lista": list_price,
                    "precio_oferta": price,
                    "tipo_oferta": oferta_tipo,
                    "promo_tipo": promo_tipo,
                    "precio_regular_promo": promo_texto_regular,
                    "precio_descuento": promo_texto_descuento,
                    "comentarios_promo": promo_comentarios,
                })
                nuevos += 1
            except Exception:
                continue

        print(f"➡️ {offset}-{offset+STEP-1}: +{nuevos} productos")
        offset += STEP
        time.sleep(SLEEP_OK)

    print(f"✅ Total en '{slug_categoria}': {len(out)}")
    return out

# =============== Pipeline completo a MySQL ===============
def main():
    t0 = time.time()
    s = build_session()

    # 1) Scrapeo de todas las categorías
    productos: List[Dict[str, Any]] = []
    seen = set()  # dedupe por sku > product_id > (nombre, url)
    for cat in CATEGORIAS:
        rows = scrape_categoria(s, cat)
        for p in rows:
            key = p.get("sku") or p.get("product_id") or (p.get("nombre"), p.get("url"))
            if key in seen: continue
            seen.add(key)
            productos.append(p)

    if not productos:
        print("⚠️ No se descargaron productos."); return

    # 2) Persistencia MySQL (transacción)
    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        inserted_hist = 0
        for p in productos:
            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            inserted_hist += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {inserted_hist} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

    print(f"⏱️ Tiempo total: {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
