#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Jumbo Argentina (VTEX) → XLSX
Barrido completo del catálogo usando la API pública VTEX.

- Primero intenta el endpoint global sin categorías:
    /api/catalog_system/pub/products/search/?_from=0&_to=49
- Si no trae nada, recorre el árbol de categorías:
    /api/catalog_system/pub/category/tree/{depth}
    y pagina cada ruta con map=c (ajustable si hiciera falta).
- Normaliza a filas por SKU y exporta jumbo_catalogo.xlsx

Requisitos:
    pip install requests pandas openpyxl
"""

import time
import json
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

import requests
import pandas as pd

# ================== Config ==================
BASE = "https://www.jumbo.com.ar"
OUT_XLSX = "jumbo_catalogo.xlsx"

STEP = 50                 # Máx VTEX por request
TIMEOUT = 25              # seg
RETRIES = 4
SLEEP_OK = 0.35           # pausa tras request OK
BACKOFF = 1.25            # multiplicador de backoff en errores
CAT_DEPTH = 10            # profundidad del árbol de categorías
MAX_EMPTY = 3             # corta tras N páginas vacías seguidas

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# =============== Helpers HTTP ===============
def http_get_json(url: str) -> Optional[Any]:
    delay = 0.8
    last_err = None
    for _ in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                try:
                    data = r.json()
                except json.JSONDecodeError:
                    data = None
                time.sleep(SLEEP_OK)
                return data
            # para 429/5xx hacemos backoff
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}"
            else:
                last_err = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(delay)
        delay *= BACKOFF
    print(f"[warn] GET falló: {url} ({last_err})")
    return None

# ============ Árbol de categorías ============
def fetch_category_paths() -> List[str]:
    url = f"{BASE}/api/catalog_system/pub/category/tree/{CAT_DEPTH}"
    cats = http_get_json(url)
    if not cats:
        return []
    paths: List[str] = []

    def walk(node: Dict[str, Any], prefix: str = ""):
        node_url = (node.get("url") or "").strip("/")
        path = f"{prefix}/{node_url}".strip("/")
        if path:
            paths.append(path)
        for child in node.get("children", []) or []:
            walk(child, path)

    for c in cats:
        walk(c, "")
    return sorted(list({p for p in paths if p}))

# ================ Paginación =================
def page_products(path: Optional[str], map_str: Optional[str]) -> Iterable[List[Dict[str, Any]]]:
    offset = 0
    empty_seq = 0
    while True:
        if path:
            enc = quote(path)
            url = (f"{BASE}/api/catalog_system/pub/products/search/"
                   f"{enc}?map={map_str}&_from={offset}&_to={offset+STEP-1}")
        else:
            url = (f"{BASE}/api/catalog_system/pub/products/search/"
                   f"?_from={offset}&_to={offset+STEP-1}")

        data = http_get_json(url)
        if data is None:
            break
        if not data:
            empty_seq += 1
            if empty_seq >= MAX_EMPTY:
                break
            offset += STEP
            continue
        empty_seq = 0
        yield data
        offset += STEP

# ======= Normalización a nivel SKU ==========
def rows_from_product(prod: Dict[str, Any]) -> List[Dict[str, Any]]:
    pid = prod.get("productId") or ""
    pname = prod.get("productName") or ""
    brand = prod.get("brand") or ""
    link_text = prod.get("linkText") or ""
    link = prod.get("link") or ""
    categories = " > ".join([(c or "").strip("/") for c in prod.get("categories", [])])
    category_id = prod.get("categoryId") or ""
    product_reference = prod.get("productReference") or ""
    cluster_highlights = []
    if isinstance(prod.get("clusterHighlights"), dict):
        cluster_highlights = list(prod["clusterHighlights"].keys())

    rows: List[Dict[str, Any]] = []
    for item in prod.get("items", []) or []:
        sku = item.get("itemId") or ""
        # EAN puede venir como lista o string; manejamos ambos
        ean_val = item.get("ean")
        if isinstance(ean_val, list):
            eans = [e for e in ean_val if e]
        elif isinstance(ean_val, str):
            eans = [ean_val] if ean_val else []
        else:
            eans = []

        ref_ids = []
        for ref in item.get("referenceId", []) or []:
            if isinstance(ref, dict) and ref.get("Value"):
                ref_ids.append(ref["Value"])

        price = None
        list_price = None
        avail = None
        seller_id = ""
        installments = ""

        for seller in item.get("sellers", []) or []:
            comm = (seller or {}).get("commertialOffer") or {}
            if comm:
                price = comm.get("Price")
                list_price = comm.get("ListPrice")
                avail = comm.get("AvailableQuantity")
                seller_id = seller.get("sellerId") or seller_id
                inst = comm.get("Installments")
                if isinstance(inst, list) and inst:
                    plan = inst[0]
                    installments = f"{plan.get('NumberOfInstallments')}x{plan.get('Value')}"
                break

        rows.append({
            "productId": pid,
            "skuId": sku,
            "ean": ", ".join(eans),
            "referenceId": ", ".join(ref_ids),
            "productName": pname,
            "productReference": product_reference,
            "brand": brand,
            "categories": categories,
            "categoryId": category_id,
            "price": price,
            "listPrice": list_price,
            "availableQty": avail,
            "installments": installments,
            "sellerId": seller_id,
            "linkText": link_text,
            "link": link,
            "clusterHighlights": ", ".join(cluster_highlights),
        })

    if not rows:  # fallback: fila a nivel producto
        rows.append({
            "productId": pid, "skuId": "", "ean": "", "referenceId": "",
            "productName": pname, "productReference": product_reference,
            "brand": brand, "categories": categories, "categoryId": category_id,
            "price": None, "listPrice": None, "availableQty": None,
            "installments": "", "sellerId": "", "linkText": link_text,
            "link": link, "clusterHighlights": ", ".join(cluster_highlights),
        })
    return rows

# =================== Main ===================
def main():
    all_rows: List[Dict[str, Any]] = []

    print("=== Jumbo AR • Intento global (sin categorías) ===")
    docs = 0
    for lot in page_products(path=None, map_str=None):
        docs += len(lot)
        for doc in lot:
            all_rows.extend(rows_from_product(doc))
        print(f"  +{len(lot)} docs (acum={docs}, filas={len(all_rows)})")

    if docs == 0:
        print("=== Recorriendo árbol de categorías (map=c) ===")
        paths = fetch_category_paths()
        if not paths:
            print("[error] No se pudo obtener el árbol de categorías.")
            return
        print(f"Rutas encontradas: {len(paths)}")
        for i, path in enumerate(paths, 1):
            print(f"[{i}/{len(paths)}] {path}")
            cat_docs = 0
            for lot in page_products(path=path, map_str="c"):
                cat_docs += len(lot)
                for doc in lot:
                    all_rows.extend(rows_from_product(doc))
                print(f"   +{len(lot)} docs (cat acum={cat_docs}, filas tot={len(all_rows)})")

    if not all_rows:
        print("[error] No se obtuvieron productos. ¿La tienda bloquea el endpoint?")
        return

    df = pd.DataFrame(all_rows)
    before = len(df)
    df.drop_duplicates(subset=["productId", "skuId"], inplace=True)
    after = len(df)
    print(f"Dedupe: {before} → {after} filas")

    # Orden de columnas
    cols = [
        "productId", "skuId", "ean", "referenceId",
        "productName", "productReference", "brand",
        "categories", "categoryId",
        "price", "listPrice", "availableQty", "installments",
        "sellerId",
        "linkText", "link",
        "clusterHighlights",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]

    print(f"Escribiendo XLSX → {OUT_XLSX}")
    df.to_excel(OUT_XLSX, index=False)
    print("Listo ✅")

if __name__ == "__main__":
    main()
