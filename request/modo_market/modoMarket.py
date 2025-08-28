#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modo Market (VTEX) — Rastreo robusto de TODO el catálogo

Estrategias:
  A) Categorías: /api/catalog_system/pub/category/tree/50  ->  fq=C:<catId>
  B) Términos:   /api/catalog_system/pub/products/search   ->  ft=<seed>

- Deduplica por SKU (itemId).
- Fila por SKU para preservar EAN.
- Incluye items no disponibles (si la tienda lo permite) con hideUnavailableItems=false.
- Exporta a Excel.

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


# ---------------------- Sesión con reintentos ----------------------

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


# ---------------------- Categorías ----------------------

def fetch_category_tree(session: requests.Session, base: str) -> List[Dict[str, Any]]:
    url = base.rstrip("/") + CAT_TREE
    r = session.get(url)
    r.raise_for_status()
    return r.json() or []


def flatten_categories(tree: List[Dict[str, Any]],
                       parent: Optional[List[str]] = None) -> List[Dict[str, Any]]:
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


# ---------------------- Búsqueda ----------------------

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


# ---------------------- Normalización filas ----------------------

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
    # dedupe preservando orden
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

            # Código interno preferimos RefId si existe
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


# ---------------------- Recolectores ----------------------

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


# ---------------------- Main ----------------------

def run(base: str, step: int, outfile: Optional[str], sales_channel: Optional[int]) -> str:
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

    # salida
    if not outfile:
        today = dt.datetime.now().strftime("%Y%m%d")
        outfile = f"Listado_ModoMarket_{today}.xlsx"
    print(f"💾 Guardando {len(df_all)} filas únicas en {outfile} …")
    df_all.to_excel(outfile, index=False)
    print("✅ Listo.")
    return outfile


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Modo Market (VTEX) — Rastreo robusto → Excel")
    parser.add_argument("--base", default=BASE_DEFAULT, help="Base URL de la tienda VTEX")
    parser.add_argument("--step", type=int, default=50, help="Tamaño de página (_from/_to) — máx. recomendado 50")
    parser.add_argument("--outfile", default=None, help="Ruta del Excel de salida")
    parser.add_argument("--sales-channel", type=int, default=1, help="Canal de ventas (sc). Usual=1")
    args = parser.parse_args()

    run(args.base, args.step, args.outfile, args.sales_channel)
