#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Josimar (VTEX) — Ingesta a MySQL (tiendas/productos/producto_tienda/historico_precios)

- A) Categorías: /api/catalog_system/pub/category/tree/50  ->  fq=C:<catId>
- B) Búsqueda por texto (semillas): /api/catalog_system/pub/products/search -> ft=<seed>
- Deduplica por SKU (itemId).
- Fila por SKU para preservar EAN y precios.
- Incluye items no disponibles si la tienda lo permite (hideUnavailableItems=false).

Requisitos:
  pip install requests pandas mysql-connector-python urllib3

Config MySQL:
  from base_datos import get_conn   # Debe devolver mysql.connector.connect(...)

Tablas esperadas (como en tus otros scripts):
  - tiendas(id PK, codigo UNIQUE, nombre)
  - productos(id PK, ean UNIQUE NULL, nombre, marca, fabricante, categoria, subcategoria)
  - producto_tienda(id PK, tienda_id, producto_id, sku_tienda UNIQUE, record_id_tienda NULL, url_tienda, nombre_tienda)
  - historico_precios(UNIQUE KEY por (tienda_id, producto_tienda_id, capturado_en))
"""

import argparse
import datetime as dt
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np

import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión mysql.connector

# ---------------- Config tienda ----------------
TIENDA_CODIGO = "josimar"
TIENDA_NOMBRE = "Josimar (VTEX)"
BASE_DEFAULT = "https://www.josimar.com.ar"
CAT_TREE = "/api/catalog_system/pub/category/tree/50"
SEARCH = "/api/catalog_system/pub/products/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Semillas para ft=
FT_SEEDS = [
    *list("abcdefghijklmnopqrstuvwxyz0123456789"),
    "á", "é", "í", "ó", "ú", "ñ",
    "la", "de", "con", "sin", "en", "al", "para", "por",
    "ar", "er", "or", "le", "li", "lo",
    "leche", "yerba", "arroz", "azucar", "harina", "aceite", "fideos",
    "coca", "pepsi", "serenisima", "quilmes", "paty", "arcor",
]

NUMERIC_EAN = re.compile(r"^\d{8,14}$")


# ---------------- Sesión HTTP ----------------
def make_session(timeout: int = 30) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6, connect=6, read=6,
        backoff_factor=1.1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=60, pool_maxsize=60)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)

    _orig_req = s.request

    def _req(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return _orig_req(method, url, **kw)

    s.request = _req  # type: ignore
    return s


# ---------------- API VTEX ----------------
def fetch_category_tree(session: requests.Session, base: str) -> List[Dict[str, Any]]:
    url = base.rstrip("/") + CAT_TREE
    r = session.get(url)
    r.raise_for_status()
    return r.json() or []


def flatten_categories(tree: List[Dict[str, Any]], parent: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    parent = parent or []
    out: List[Dict[str, Any]] = []
    for node in tree:
        curr_path = parent + [str(node.get("name", "")).strip()]
        out.append({
            "id": int(node.get("id")),
            "name": node.get("name"),
            "pathName": curr_path,
            "hasChildren": bool(node.get("hasChildren")),
        })
        for child in (node.get("children") or []):
            out.extend(flatten_categories([child], curr_path))
    return out


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
        time.sleep(0.12)


# ---------------- Normalización ----------------
def best_ean(item: Dict[str, Any]) -> str:
    ean = (item.get("ean") or "").strip()
    if NUMERIC_EAN.match(ean):
        return ean
    for ref in (item.get("referenceId") or []):
        val = (ref or {}).get("Value") or ""
        if NUMERIC_EAN.match(val.strip()):
            return val.strip()
    return ean


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

        cats: List[str] = []
        for c in (p.get("categories") or []):
            parts = [seg for seg in c.split("/") if seg]
            if parts:
                cats = parts
                break
        categoria = cats[0] if len(cats) >= 1 else ""
        subcategoria = cats[-1] if len(cats) >= 1 else ""

        product_ref = p.get("productReference") or p.get("productReferenceCode") or ""

        for item in (p.get("items") or []):
            item_id = str(item.get("itemId", ""))

            ref = ""
            for refobj in (item.get("referenceId") or []):
                if (refobj or {}).get("Key", "").lower() == "refid":
                    ref = (refobj or {}).get("Value") or ""
                    break
            codigo_interno = ref or product_ref

            ean = best_ean(item)

            sellers = item.get("sellers") or []
            seller = next((s for s in sellers if s.get("sellerDefault")), sellers[0] if sellers else None)

            price = list_price = offer_type = None
            if seller:
                offer = (seller.get("commertialOffer") or {})
                price = offer.get("Price")
                list_price = offer.get("ListPrice")
                offer_type = extract_teaser_names(offer) or None
                try:
                    if not offer_type and price is not None and list_price is not None:
                        if float(price) < float(list_price):
                            offer_type = "Descuento"
                except Exception:
                    pass

            rows.append({
                "EAN": ean or None,
                "CodigoInterno": codigo_interno or None,
                "NombreProducto": (name_product or item.get("nameComplete") or item.get("name") or "").strip(),
                "Categoria": categoria or None,
                "Subcategoria": subcategoria or None,
                "Marca": brand or None,
                "Fabricante": manufacturer or None,
                "PrecioLista": list_price,
                "PrecioOferta": price,
                "TipoOferta": offer_type,
                "URL": link or None,
                "SKU": item_id or None,
                "ProductId": product_id or None,
                "CategoryId": (p.get("categoryId") or None)
            })
    return rows


# ---------------- Recolectores ----------------
def collect_by_categories(session: requests.Session, base: str, step: int,
                          sales_channel: Optional[int]) -> List[Dict[str, Any]]:
    tree = fetch_category_tree(session, base)
    if not tree:
        print("⚠️ Árbol de categorías vacío; se continuará solo con ft.")
        return []

    cats = flatten_categories(tree)
    print(f"📦 Categorías totales (sc={sales_channel}): {len(cats)}")

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
    print(f"🔎 Barrido por términos (semillas: {len(seeds)}) … (sc={sales_channel})")
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

        total_new_this_seed = 0
        for _page, batch in enumerate(iter_paginated(session, base, params, step=step), 1):
            rows = product_rows(batch)
            for r in rows:
                sku = r.get("SKU")
                if sku and sku not in seen_skus:
                    seen_skus.add(sku)
                    all_rows.append(r)
                    total_new_this_seed += 1
        if total_new_this_seed:
            print(f"  [{idx}/{len(seeds)}] '{seed}' -> {total_new_this_seed} filas nuevas (acum seed)")
        time.sleep(0.05)
    return all_rows


# ---------------- Utils SQL ----------------
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


def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]


def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = (r.get("EAN") or None)
    nombre = (r.get("NombreProducto") or "").strip()
    marca = (r.get("Marca") or None)
    fabricante = (r.get("Fabricante") or None)
    categoria = (r.get("Categoria") or None)
    subcategoria = (r.get("Subcategoria") or None)

    # 1) Por EAN
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(%s, marca),
                  fabricante = COALESCE(%s, fabricante),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (nombre, marca, fabricante, categoria, subcategoria, pid))
            return pid

    # 2) Por (nombre, marca)
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca or ""))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(%s, ean),
                  fabricante = COALESCE(%s, fabricante),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (ean, fabricante, categoria, subcategoria, pid))
            return pid

    # 3) Insert nuevo
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid


def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    sku = (r.get("SKU") or None)
    url = (r.get("URL") or None)
    nombre_tienda = (r.get("NombreProducto") or None)
    record_id = (r.get("ProductId") or None)

    if sku:
        cur.execute("""
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

    # Sin SKU: usar record_id_tienda si hay
    if record_id:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    # Último recurso
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid


def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: dt.datetime):
    precio_lista = _parse_price(r.get("PrecioLista"))
    precio_oferta = _parse_price(r.get("PrecioOferta"))
    tipo_oferta = r.get("TipoOferta") or None

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
        precio_lista, precio_oferta, tipo_oferta,
        tipo_oferta, None, None, None
    ))


# ---------------- Orquestador ----------------
def run_to_mysql(base: str, step: int, sales_channel: int):
    session = make_session()

    # Si sales_channel == 0 => modo auto: probar varios canales
    if sales_channel and sales_channel > 0:
        sc_list = [sales_channel]
        print(f"🛒 Usando solo canal de ventas sc={sales_channel}")
    else:
        sc_list = [1, 2, 3, 5]
        print(f"🛒 Modo auto-sc activado. Se probarán los canales: {sc_list}")

    all_rows: List[Dict[str, Any]] = []
    global_seen_skus: set = set()

    for sc in sc_list:
        print("\n" + "=" * 60)
        print(f"🔹 Canal de ventas sc={sc}")
        print("=" * 60)

        # A) categorías
        rows_cat = collect_by_categories(session, base, step, sc)

        # B) términos (para huérfanos/mal clasificados)
        rows_ft = collect_by_terms(session, base, step, sc, FT_SEEDS)

        # unir y dedupe global por SKU
        for r in rows_cat + rows_ft:
            sku = r.get("SKU")
            if sku and sku not in global_seen_skus:
                global_seen_skus.add(sku)
                all_rows.append(r)

        print(f"✅ Canal sc={sc}: acumuladas {len(global_seen_skus)} SKUs únicos hasta ahora.")

    if not all_rows:
        raise RuntimeError(
            "No se obtuvieron filas; la tienda puede requerir otra configuración de sc "
            "o ajustes adicionales en las semillas."
        )

    print(f"\n💾 Preparando inserción MySQL (total {len(all_rows)} filas únicas)…")
    capturado_en = dt.datetime.now()

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        inserted_hist = 0
        for r in all_rows:
            producto_id = find_or_create_producto(cur, r)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, r)
            insert_historico(cur, tienda_id, pt_id, r, capturado_en)
            inserted_hist += 1

        conn.commit()
        print(f"✅ Guardado en MySQL: {inserted_hist} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# ---------------- CLI ----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Josimar (VTEX) — Ingesta a MySQL")
    parser.add_argument("--base", default=BASE_DEFAULT, help="Base URL de la tienda VTEX")
    parser.add_argument("--step", type=int, default=50,
                        help="Tamaño de página (_from/_to) — máx. recomendado 50")
    parser.add_argument(
        "--sales-channel",
        type=int,
        default=0,
        help="Canal de ventas (sc). 0 = auto (probar 1,2,3,5). Si pones un valor > 0, usa solo ese."
    )
    args = parser.parse_args()

    run_to_mysql(args.base, args.step, args.sales_channel)
