#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests, time, re, json
from html import unescape
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ========= Config =========
BASE = "https://www.jumbo.com.ar"
STEP = 50                      # VTEX: 0-49, 50-99, ...
SLEEP_OK = 0.35
TIMEOUT = 25
MAX_EMPTY = 4                  # más tolerante con huecos en paginación
TREE_DEPTH = 10                # más profundidad para descubrir categorías
RETRIES = 4                    # reintentos por request

TIENDA_CODIGO = "jumbo_ar"
TIENDA_NOMBRE = "Jumbo Argentina"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Accept-Language": "es-AR,es;q=0.9"
}

# ========= Helpers de limpieza =========
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

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

def req_json(url: str, session: requests.Session, params: Optional[Dict] = None):
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
                time.sleep(0.6 + 0.5 * i)
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
    return f"{round(float(v), 2)}"

# ========= Warmup / Contexto VTEX =========
def warmup(session: requests.Session):
    try:
        session.get(BASE, headers={"User-Agent": HEADERS["User-Agent"], "Accept-Language": HEADERS["Accept-Language"]}, timeout=TIMEOUT)
        session.get(f"{BASE}/api/catalog_system/pub/category/tree/1", headers=HEADERS, timeout=TIMEOUT)
    except Exception:
        pass

# ========= Categorías (por ID, solo hojas) =========
def get_category_tree(session: requests.Session, depth: int = TREE_DEPTH) -> List[Dict[str, Any]]:
    url = f"{BASE}/api/catalog_system/pub/category/tree/{depth}"
    return req_json(url, session) or []

def iter_leaf_categories(tree: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    leaves = []
    def walk(n):
        ch = n.get("children") or []
        if not ch:
            leaves.append({"id": n.get("id"), "name": n.get("name")})
        else:
            for c in ch:
                walk(c)
    for root in tree:
        walk(root)
    return [x for x in leaves if x.get("id")]

# ========= Facets (para particionar por marca, precio, etc.) =========
def get_facets_for_category(session: requests.Session, cat_id: int, sc: int = 1) -> Dict[str, Any]:
    url = f"{BASE}/api/catalog_system/pub/facets/search"
    params = {"fq": f"C:{cat_id}", "sc": sc}
    return req_json(url, session, params=params) or {}

def extract_brands_from_facets(facets: Dict[str, Any]) -> List[Dict[str, Any]]:
    brands = []
    try:
        for b in (facets.get("Brands") or []):
            if b.get("Quantity", 0) > 0 and b.get("Id"):
                brands.append({"id": int(b["Id"]), "name": (b.get("Name") or "").strip(), "qty": int(b["Quantity"])})
    except Exception:
        pass
    return sorted(brands, key=lambda x: -x["qty"])

# ========= Parse de producto → filas por SKU (por vendedor si aplica) =========
def parse_rows_from_product(p: Dict[str, Any], base: str) -> List[Dict[str, Any]]:
    rows = []
    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = (p.get("brand") or "") or ""
    link_text = p.get("linkText")
    link = f"{base}/{link_text}/p" if link_text else ""
    categories = [c.strip("/") for c in (p.get("categories") or [])]
    category_top = categories[0] if categories else ""
    category_full = " > ".join(categories)

    items = p.get("items") or []
    for it in items:
        sku_id = it.get("itemId")
        sku_name = clean_text(it.get("name") or "")
        ean = ""
        for ref in (it.get("referenceId") or []):
            if ref.get("Value"):
                ean = str(ref["Value"]); break

        sellers = it.get("sellers") or []
        if not sellers:
            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                "price": None, "listPrice": None, "spotPrice": None,
                "name": name, "skuName": sku_name, "brand": brand,
                "ean": ean,
                "categoryTop": category_top, "categoryFull": category_full,
                "link": link, "linkText": link_text,
                "teasers": [],
            })
            continue

        for s in sellers:
            offer = s.get("commertialOffer") or {}
            price = offer.get("Price")
            list_price = offer.get("ListPrice")
            spot = offer.get("spotPrice", None)
            teasers = offer.get("Teasers") or []
            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                "price": price, "listPrice": list_price, "spotPrice": spot,
                "name": name, "skuName": sku_name, "brand": brand,
                "ean": ean,
                "categoryTop": category_top, "categoryFull": category_full,
                "link": link, "linkText": link_text,
                "teasers": teasers,
            })
    return rows

# ========= Fetch VTEX: categoría + marca + token ft =========
ALPHABET = list("0123456789abcdefghijklmnopqrstuvwxyz")

def _vtex_search(session: requests.Session, fq_list: List[str], sc: int = 1, ft: Optional[str] = None,
                 offset: int = 0) -> Optional[List[Dict[str, Any]]]:
    url = f"{BASE}/api/catalog_system/pub/products/search"
    # Repetir fq como lista para que requests lo encodee en múltiples parámetros
    params: List[Tuple[str, Any]] = [("sc", sc)]
    for fq in fq_list:
        params.append(("fq", fq))
    params.append(("_from", offset))
    params.append(("_to", offset + STEP - 1))
    if ft:
        params.append(("ft", ft))
    return req_json(url, session, params=params)

def fetch_cat_brand_token(session: requests.Session, cat_id: int, brand_id: Optional[int], sc: int = 1,
                          token: Optional[str] = None) -> List[Dict[str, Any]]:
    rows, empty_streak = [], 0
    offset = 0
    fq = [f"C:{cat_id}"]
    if brand_id:
        fq.append(f"B:{brand_id}")

    while True:
        data = _vtex_search(session, fq_list=fq, sc=sc, ft=token, offset=offset)
        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue

        empty_streak = 0
        for p in data:
            rows.extend(parse_rows_from_product(p, BASE))

        if len(data) < STEP:
            break

        offset += STEP
        time.sleep(SLEEP_OK)
    return rows

def fetch_category_partitioned(session: requests.Session, cat_id: int, sc: int = 1, brand_threshold: int = 1800) -> List[Dict[str, Any]]:
    """
    1) Intento por categoría "plana".
    2) Si huele a techo (>= brand_threshold), particiono por marca.
    3) Si una marca sigue grande, particiono por tokens ft=0..9,a..z.
    """
    # 1) intento crudo
    base_rows = fetch_cat_brand_token(session, cat_id, brand_id=None, sc=sc, token=None)
    if len(base_rows) < brand_threshold:
        # no parece capado; devolver
        # Dedup por (productId, skuId)
        seen, out = set(), []
        for r in base_rows:
            k = (r.get("productId"), r.get("skuId"))
            if k in seen: continue
            seen.add(k); out.append(r)
        return out

    # 2) facets → marcas
    facets = get_facets_for_category(session, cat_id, sc=sc)
    brands = extract_brands_from_facets(facets)
    if not brands:
        # sin facets, devolver lo obtenido
        seen, out = set(), []
        for r in base_rows:
            k = (r.get("productId"), r.get("skuId"))
            if k in seen: continue
            seen.add(k); out.append(r)
        return out

    all_rows: List[Dict[str, Any]] = []
    for b in brands:
        br_rows = fetch_cat_brand_token(session, cat_id, brand_id=b["id"], sc=sc, token=None)
        if len(br_rows) >= brand_threshold:
            # 3) sub-partición por token ft
            for tk in ALPHABET:
                tk_rows = fetch_cat_brand_token(session, cat_id, brand_id=b["id"], sc=sc, token=tk)
                all_rows.extend(tk_rows)
        else:
            all_rows.extend(br_rows)

    # Dedup final por (productId, skuId)
    seen, uniq = set(), []
    for r in all_rows:
        k = (r.get("productId"), r.get("skuId"))
        if k in seen: continue
        seen.add(k); uniq.append(r)
    return uniq

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
    precio_oferta = row.get("spotPrice", None)
    if precio_oferta is None:
        precio_oferta = row.get("price", None)

    teasers = row.get("teasers") or []
    promo_tipo = "; ".join([str(t.get("name") or t.get("teaserType") or "").strip() for t in teasers if t]) or None
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
        None,
        promo_tipo,
        None,
        None,
        comentarios
    ))

# ========= Main =========
def main():
    session = requests.Session()
    warmup(session)

    print("Descubriendo categorías…")
    tree = get_category_tree(session, TREE_DEPTH)
    leaves = iter_leaf_categories(tree)
    print(f"Hojas detectadas: {len(leaves)}")

    capturado_en = datetime.now()

    conn = None
    total_insertados = 0
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        for i, leaf in enumerate(leaves, 1):
            cat_id = int(leaf["id"])
            cat_name = leaf.get("name", str(cat_id))
            try:
                print(f"[{i}/{len(leaves)}] {cat_name} (ID {cat_id})")
                rows = fetch_category_partitioned(session, cat_id, sc=1, brand_threshold=1800)
                if not rows:
                    continue

                # Dedup final por SKU
                seen = set()
                filtered: List[Dict[str, Any]] = []
                for r in rows:
                    key = (r.get("productId"), r.get("skuId"))
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
                print(f"  ! Error en categoría {cat_id}: {e}")
                conn.rollback()

            time.sleep(SLEEP_OK)

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
