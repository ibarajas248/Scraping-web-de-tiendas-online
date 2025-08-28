#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modo Market (VTEX) — Rastreo robusto de TODO el catálogo + Persistencia MySQL

Estrategias:
  A) Categorías: /api/catalog_system/pub/category/tree/50  ->  fq=C:<catId>
  B) Términos:   /api/catalog_system/pub/products/search   ->  ft=<seed>

- Deduplica por SKU (itemId).
- Fila por SKU para preservar EAN.
- Incluye items no disponibles (si la tienda lo permite) con hideUnavailableItems=false.
- Exporta a Excel y persiste a MySQL con tu contrato estándar.

Uso:
python modomarket_crawler.py \
  --base https://www.modomarket.com \
  --step 50 \
  --outfile Listado_ModoMarket.xlsx \
  --sales-channel 1
"""

import argparse
import datetime as dt
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Persistencia MySQL ===
from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ==================== Config VTEX ====================

BASE_DEFAULT = "https://www.modomarket.com"
CAT_TREE = "/api/catalog_system/pub/category/tree/50"
SEARCH = "/api/catalog_system/pub/products/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

FT_SEEDS = [
    # letras y números
    *list("abcdefghijklmnopqrstuvwxyz0123456789"),
    # vocales acentuadas y ñ
    "á", "é", "í", "ó", "ú", "ñ",
    # bigramas comunes en español / retail
    "la", "de", "con", "sin", "en", "al", "para", "por",
    "ar", "er", "or", "le", "li", "lo",
    # marcas/patrones típicos
    "coca", "pepsi", "yerba", "arroz", "azucar", "harina",
]

NUMERIC_EAN = re.compile(r"^\d{8,14}$")

# ==================== Sesión con reintentos ====================

def make_session(timeout: int = 30) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=60, pool_maxsize=60)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    s.request = _with_timeout(s.request, timeout)  # type: ignore
    return s

def _with_timeout(func, timeout):
    def wrapper(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return func(method, url, **kwargs)
    return wrapper

# ==================== Categorías ====================

def fetch_category_tree(session: requests.Session, base: str) -> List[Dict[str, Any]]:
    url = base.rstrip("/") + CAT_TREE
    r = session.get(url)
    r.raise_for_status()
    return r.json() or []

def flatten_categories(tree: List[Dict[str, Any]], parent: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    parent = parent or []
    out: List[Dict[str, Any]] = []
    for node in tree:
        curr_path = parent + [node.get("name", "").strip()]
        out.append({
            "id": int(node.get("id")),
            "name": node.get("name"),
            "pathName": curr_path,
            "hasChildren": bool(node.get("hasChildren")),
        })
        children = node.get("children") or []
        if children:
            out.extend(flatten_categories(children, curr_path))
    return out

# ==================== Búsqueda ====================

def search_products(session: requests.Session, base: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = base.rstrip("/") + SEARCH
    r = session.get(url, params=params)
    if r.status_code in (404, 400):
        return []
    r.raise_for_status()
    try:
        return r.json() if r.content else []
    except Exception:
        return []

def iter_paginated(session: requests.Session, base: str, base_params: Dict[str, Any], step: int = 50
                   ) -> Iterable[List[Dict[str, Any]]]:
    start = 0
    while True:
        params = dict(base_params)
        params["_from"] = start
        params["_to"] = start + (step - 1)
        batch = search_products(session, base, params)
        if not batch:
            break
        yield batch
        start += step
        time.sleep(0.15)  # respirito

# ==================== Normalización filas ====================

def best_ean(item: Dict[str, Any]) -> str:
    ean = (item.get("ean") or "").strip()
    if NUMERIC_EAN.match(ean):
        return ean
    for ref in (item.get("referenceId") or []):
        val = (ref or {}).get("Value") or ""
        if NUMERIC_EAN.match(val.strip()):
            return val.strip()
    return ean  # lo que haya (o vacío)

def extract_teaser_names(offer: Dict[str, Any]) -> str:
    names: List[str] = []
    for key in ("PromotionTeasers", "Teasers"):
        for t in (offer or {}).get(key, []) or []:
            name = t.get("Name") or t.get("name")
            if name:
                names.append(str(name))
    seen = set()
    ordered = []
    for n in names:
        if n not in seen:
            seen.add(n)
            ordered.append(n)
    return ", ".join(ordered)

def product_rows(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for p in products:
        product_id = str(p.get("productId", ""))
        brand = p.get("brand", "") or ""
        manufacturer = p.get("Manufacturer", "") or ""
        link = p.get("link") or ""
        name_product = p.get("productName") or ""

        # Categoría / Subcategoría desde el primer path válido
        cats = []
        for c in (p.get("categories") or []):
            parts = [seg for seg in c.split("/") if seg]
            if parts:
                cats = parts
                break
        categoria = cats[0] if len(cats) >= 1 else ""
        subcategoria = cats[-1] if len(cats) >= 1 else ""

        codigo_producto = p.get("productReference") or p.get("productReferenceCode") or ""

        for item in (p.get("items") or []):
            item_id = str(item.get("itemId", ""))

            # Código interno: preferimos RefId si existe
            ref = ""
            for refobj in (item.get("referenceId") or []):
                if (refobj or {}).get("Key", "").lower() == "refid":
                    ref = (refobj or {}).get("Value") or ""
                    break
            codigo_interno = ref or codigo_producto

            ean = best_ean(item)

            sellers = item.get("sellers") or []
            seller = next((s for s in sellers if s.get("sellerDefault")), sellers[0] if sellers else None)

            price = list_price = offer_type = ""
            if seller:
                offer = (seller.get("commertialOffer") or {})
                price = offer.get("Price")
                list_price = offer.get("ListPrice")
                offer_type = extract_teaser_names(offer)
                try:
                    if not offer_type and price is not None and list_price is not None:
                        if float(price) < float(list_price):
                            offer_type = "Descuento"
                except Exception:
                    pass

            rows.append({
                "EAN": ean,
                "CodigoInterno": codigo_interno,
                "NombreProducto": name_product or item.get("nameComplete") or item.get("name") or "",
                "Categoria": categoria,
                "Subcategoria": subcategoria,
                "Marca": brand,
                "Fabricante": manufacturer,
                "PrecioLista": list_price,
                "PrecioOferta": price,
                "TipoOferta": offer_type,
                "URL": link,
                "SKU": item_id,
                "ProductId": product_id,
                "CategoryId": (p.get("categoryId") or "")
            })
    return rows

# ==================== Recolectores ====================

def collect_by_categories(session: requests.Session, base: str, step: int,
                          sales_channel: Optional[int]) -> List[Dict[str, Any]]:
    tree = fetch_category_tree(session, base)
    cats = flatten_categories(tree)
    print(f"📦 Categorías totales: {len(cats)}")

    all_rows: List[Dict[str, Any]] = []
    seen_skus: set = set()

    for i, c in enumerate(cats, 1):
        cat_id = c["id"]
        cat_path = " / ".join(c["pathName"])
        params = {
            "fq": f"C:{cat_id}",
            "O": "OrderByScoreDESC",
            "hideUnavailableItems": "false",
        }
        if sales_channel:
            params["sc"] = str(sales_channel)

        page = 0
        print(f"  [{i}/{len(cats)}] CatID {cat_id} :: {cat_path}")
        for batch in iter_paginated(session, base, params, step=step):
            page += 1
            rows = product_rows(batch)
            new_rows = []
            for r in rows:
                sku = r.get("SKU")
                if sku and sku not in seen_skus:
                    seen_skus.add(sku)
                    new_rows.append(r)
            print(f"     • Página {page} -> {len(new_rows)} filas nuevas (acum: {len(seen_skus)})")
            all_rows.extend(new_rows)
    return all_rows

def collect_by_terms(session: requests.Session, base: str, step: int,
                     sales_channel: Optional[int], seeds: List[str]) -> List[Dict[str, Any]]:
    print(f"🔎 Barrido por términos (semillas: {len(seeds)}) …")
    all_rows: List[Dict[str, Any]] = []
    seen_skus: set = set()

    for idx, seed in enumerate(seeds, 1):
        params = {
            "ft": seed,
            "O": "OrderByScoreDESC",
            "hideUnavailableItems": "false",
        }
        if sales_channel:
            params["sc"] = str(sales_channel)

        page = 0
        total_new_this_seed = 0
        for batch in iter_paginated(session, base, params, step=step):
            page += 1
            rows = product_rows(batch)
            new_rows = []
            for r in rows:
                sku = r.get("SKU")
                if sku and sku not in seen_skus:
                    seen_skus.add(sku)
                    new_rows.append(r)
            total_new_this_seed += len(new_rows)
            all_rows.extend(new_rows)
        print(f"  [{idx}/{len(seeds)}] '{seed}' -> {total_new_this_seed} filas nuevas")
    return all_rows

# ==================== VTEX → DataFrame ====================

def build_dataframe(base: str, step: int, sales_channel: Optional[int]) -> pd.DataFrame:
    session = make_session()
    # A) categorías
    rows_cat = collect_by_categories(session, base, step, sales_channel)
    # B) términos (para productos huérfanos)
    rows_ft = collect_by_terms(session, base, step, sales_channel, FT_SEEDS)
    # unir y deduplicar por SKU
    df_all = pd.DataFrame(rows_cat + rows_ft)
    if df_all.empty:
        raise RuntimeError("No se obtuvieron filas; la tienda puede requerir selección de sucursal o cambió la API.")
    df_all.drop_duplicates(subset=["SKU"], keep="first", inplace=True)
    # ordenar columnas
    cols = [
        "EAN", "CodigoInterno", "NombreProducto", "Categoria", "Subcategoria",
        "Marca", "Fabricante", "PrecioLista", "PrecioOferta", "TipoOferta",
        "URL", "SKU", "ProductId", "CategoryId"
    ]
    for c in cols:
        if c not in df_all.columns:
            df_all[c] = ""
    df_all = df_all[cols]
    return df_all

# ============================================================
# ====================  PERSISTENCIA DB  =====================
# ============================================================

_NULLLIKE = {"", "null", "none", "nan", "na"}
def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

TIENDA_CODIGO = "modomarket"
TIENDA_NOMBRE = "Modo Market"

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
        p.get("ean") or "", nombre, marca, p.get("fabricante") or "",
        p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre") or p.get("nombre_tienda") or ""

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

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def _to_txt_or_none_price(x):
    if x is None:
        return None
    try:
        # si es numérico puro
        return f"{round(float(x), 2)}"
    except Exception:
        # si viene como texto, intenta parseo laxo
        s = str(x).strip().replace("\xa0", " ")
        s = re.sub(r"[^\d,.\-]", "", s)
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s and "." not in s:
            s = s.replace(",", ".")
        try:
            return f"{round(float(s), 2)}"
        except Exception:
            return None

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en):
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
        _to_txt_or_none_price(p.get("precio_lista")),
        _to_txt_or_none_price(p.get("precio_oferta")),
        p.get("tipo_oferta") or None,
        p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None,
        p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# ---- mapeo específico VTEX -> contrato DB ----
def row_to_db_product(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "sku":        clean(row.get("SKU")),
        "record_id":  clean(row.get("ProductId")),  # opcional como apoyo
        "ean":        clean(row.get("EAN")),
        "nombre":     clean(row.get("NombreProducto")),
        "marca":      clean(row.get("Marca")),
        "fabricante": clean(row.get("Fabricante")),
        "categoria":  clean(row.get("Categoria")),
        "subcategoria": clean(row.get("Subcategoria")),

        "precio_lista":  clean(row.get("PrecioLista")),
        "precio_oferta": clean(row.get("PrecioOferta")),
        "tipo_oferta":   clean(row.get("TipoOferta")),
        "promo_tipo":    None,
        "precio_regular_promo": None,
        "precio_descuento":     None,
        "comentarios_promo":    None,

        "url":            clean(row.get("URL")),
        "nombre_tienda":  clean(row.get("NombreProducto")),
        "nombre":         clean(row.get("NombreProducto")),
    }

def persistir_df_en_mysql(df: pd.DataFrame, tienda_codigo=TIENDA_CODIGO, tienda_nombre=TIENDA_NOMBRE):
    productos = [row_to_db_product(r) for r in df.to_dict(orient="records")]
    if not productos:
        print("⚠️ No hay productos para guardar en DB.")
        return

    from datetime import datetime
    capturado_en = datetime.now()

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, tienda_codigo, tienda_nombre)

        insertados = 0
        for p in productos:
            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {tienda_nombre} ({capturado_en})")
    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

# ==================== Main ====================

def run(base: str, step: int, outfile: Optional[str], sales_channel: Optional[int]) -> Tuple[pd.DataFrame, str]:
    df_all = build_dataframe(base, step, sales_channel)
    if not outfile:
        today = dt.datetime.now().strftime("%Y%m%d")
        outfile = f"Listado_ModoMarket_{today}.xlsx"
    print(f"💾 Guardando {len(df_all)} filas únicas en {outfile} …")
    #df_all.to_excel(outfile, index=False)
    print("✅ Listo.")
    return df_all, outfile

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Modo Market (VTEX) — Rastreo robusto → Excel + MySQL")
    parser.add_argument("--base", default=BASE_DEFAULT, help="Base URL de la tienda VTEX")
    parser.add_argument("--step", type=int, default=50, help="Tamaño de página (_from/_to) — máx. recomendado 50")
    parser.add_argument("--outfile", default=None, help="Ruta del Excel de salida")
    parser.add_argument("--sales-channel", type=int, default=1, help="Canal de ventas (sc). Usual=1")
    args = parser.parse_args()

    df, outfile = run(args.base, args.step, args.outfile, args.sales_channel)

    # Persistir en MySQL con tu contrato estándar
    persistir_df_en_mysql(df, tienda_codigo=TIENDA_CODIGO, tienda_nombre=TIENDA_NOMBRE)
