#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Masonline (VTEX) — Scraper por productClusterIds con fallback alfabético
- Intenta traer todo el cluster usando paginación estándar (_from/_to)
- Si la tienda corta en ~2.500 (50 páginas * 50), cambia a particiones por 'ft' (A–Z, 0–9)
- Deduplica por productId para evitar duplicados entre particiones
- Orden estable por nombre (O=OrderByNameASC)

Salida: CSV y XLSX con columnas:
EAN, CodigoInterno, NombreProducto, Categoria, Subcategoria, Marca, Fabricante,
PrecioLista, PrecioOferta, TipoOferta, URL, SKU, ProductId, ClusterId, ClusterNombre
"""

import time
import argparse
import string
from typing import List, Dict, Any, Optional, Tuple, Set

import requests
from requests.adapters import HTTPAdapter
from requests import HTTPError
from urllib3.util.retry import Retry
import pandas as pd

BASE = "https://www.masonline.com.ar"
SEARCH_API = f"{BASE}/api/catalog_system/pub/products/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Config VTEX
STEP = 50                  # VTEX: _to - _from <= 49
SLEEP_BETWEEN = 0.35       # para no molestar al servidor
MAX_WINDOW_RESULTS = 2500  # 50 páginas * 50 ítems
ORDER_BY = "OrderByNameASC"

# Particiones para 'ft' (puedes ampliar con acentos si lo necesitas)
ALPHA_TERMS = list(string.digits + string.ascii_lowercase)  # "0123456789abcdefghijklmnopqrstuvwxyz"


def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s


def fetch_page(session: requests.Session, cluster_id: str, start: int, step: int) -> List[Dict[str, Any]]:
    """
    Paginación estándar por ventana (hasta ~2.500 resultados). Si la tienda corta
    devuelve 400. En tal caso levantamos HTTPError para que el caller haga fallback.
    """
    params = [
        ("fq", f"productClusterIds:{cluster_id}"),
        ("_from", start),
        ("_to", start + step - 1),
        ("O", ORDER_BY),
    ]
    r = session.get(SEARCH_API, params=params, timeout=30)
    if r.status_code == 400:
        raise HTTPError("VTEX 50-page window reached", response=r)
    r.raise_for_status()
    try:
        data = r.json()
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        return data if isinstance(data, list) else []
    except Exception:
        return []


def fetch_page_alpha(session: requests.Session, cluster_id: str, term: str, start: int, step: int) -> List[Dict[str, Any]]:
    """
    Paginación por partición usando 'ft' (fulltext) junto al cluster:
    GET /api/catalog_system/pub/products/search/{cluster}/{term}?map=productClusterIds,ft&_from&_to&O
    """
    url = f"{SEARCH_API}/{cluster_id}/{term}"
    params = {
        "map": "productClusterIds,ft",
        "_from": start,
        "_to": start + step - 1,
        "O": ORDER_BY,
    }
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 400:
        raise HTTPError("Bad Request on alpha slice", response=r)
    r.raise_for_status()
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def split_categories(paths: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    paths p.ej: ["/Kiosco/Chocolates/Tabletas/", "/Kiosco/Chocolates/", "/Kiosco/"]
    Devolvemos: (categoria, subcategoria, ruta_full)
    """
    if not paths:
        return None, None, None
    best = max(paths, key=lambda p: p.count("/"))
    parts = [p for p in best.strip("/").split("/") if p]
    categoria = parts[0] if len(parts) >= 1 else None
    subcategoria = " > ".join(parts[1:]) if len(parts) > 1 else None
    ruta_full = " / ".join(parts) if parts else None
    return categoria, subcategoria, ruta_full


def extract_offer_type(p: Dict[str, Any], item: Dict[str, Any]) -> str:
    names = []
    sellers = item.get("sellers") or []
    for s in sellers:
        co = (s or {}).get("commertialOffer") or {}
        for t in co.get("Teasers") or []:
            n = (t or {}).get("Name") or (t or {}).get("name")
            if n:
                names.append(str(n))
        for t in co.get("PromotionTeasers") or []:
            n = (t or {}).get("Name") or (t or {}).get("name")
            if n:
                names.append(str(n))
    clusters = p.get("productClusters") or {}
    for _, cname in clusters.items():
        if isinstance(cname, str) and cname:
            names.append(cname)
    names = list(dict.fromkeys([n.strip() for n in names if n and n.strip()]))
    return " | ".join(names)


def choose_seller(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sellers = item.get("sellers") or []
    for s in sellers:
        if s.get("sellerDefault"):
            return s
    for s in sellers:
        co = (s or {}).get("commertialOffer") or {}
        if co.get("IsAvailable"):
            return s
    return sellers[0] if sellers else None


def flatten(products: List[Dict[str, Any]], cluster_id: str, verbose: bool = False) -> List[Dict[str, Any]]:
    """
    Aplana la estructura VTEX a filas por SKU, con impresión en vivo opcional.
    """
    rows: List[Dict[str, Any]] = []
    for p in products:
        categoria, subcategoria, _ruta = split_categories(p.get("categories") or [])
        brand = p.get("brand")
        manufacturer = p.get("Manufacturer") or p.get("manufacturer") or None
        url = p.get("link") or f"{BASE}/{p.get('linkText')}/p"
        cluster_name = None
        pcs = p.get("productClusters") or {}
        if cluster_id in pcs:
            cluster_name = pcs.get(cluster_id)

        for it in p.get("items") or []:
            ean = it.get("ean") or None
            ref_val = None
            for ref in it.get("referenceId") or []:
                if (ref or {}).get("Key") == "RefId":
                    ref_val = ref.get("Value")
                    break
            if not ref_val:
                ref_val = p.get("productReference") or it.get("itemId")

            seller = choose_seller(it) or {}
            co = (seller.get("commertialOffer") or {}) if seller else {}
            price = co.get("Price")
            list_price = co.get("ListPrice")
            tipo_oferta = extract_offer_type(p, it)

            row = {
                "EAN": ean,
                "CodigoInterno": ref_val,
                "NombreProducto": p.get("productName") or it.get("name"),
                "Categoria": categoria,
                "Subcategoria": subcategoria,
                "Marca": brand,
                "Fabricante": manufacturer,
                "PrecioLista": list_price,
                "PrecioOferta": price,
                "TipoOferta": tipo_oferta,
                "URL": url,
                "SKU": it.get("itemId"),
                "ProductId": p.get("productId"),
                "ClusterId": cluster_id,
                "ClusterNombre": cluster_name,
            }
            rows.append(row)

            if verbose:
                print(
                    f"➡ {row['EAN'] or '-'} | {row['NombreProducto']} | {row['Marca'] or '-'} | "
                    f"Lista: {row['PrecioLista'] if row['PrecioLista'] is not None else '-'} | "
                    f"Oferta: {row['PrecioOferta'] if row['PrecioOferta'] is not None else '-'} | "
                    f"{row['URL']}",
                    flush=True
                )
    return rows


def scrape_cluster_alpha(session: requests.Session, cluster_id: str, seen_products: Set[str]) -> List[Dict[str, Any]]:
    """
    Recorre A–Z y 0–9 con ft, deduplicando por productId.
    """
    all_rows: List[Dict[str, Any]] = []
    for term in ALPHA_TERMS:
        start = 0
        print(f"\n--- Partición '{term}' ---", flush=True)
        while True:
            try:
                chunk = fetch_page_alpha(session, cluster_id, term, start, STEP)
            except HTTPError as e:
                print(f"  {term}: stop por {e}.", flush=True)
                break

            if not chunk:
                if start == 0:
                    print(f"  {term}: sin resultados.", flush=True)
                break

            fresh = [p for p in chunk if p.get("productId") not in seen_products]
            for p in fresh:
                seen_products.add(p.get("productId"))

            print(f"  {term}: desde {start} -> {len(fresh)} productos nuevos (acum únicos: {len(seen_products)})", flush=True)
            rows = flatten(fresh, cluster_id, verbose=True)
            all_rows.extend(rows)

            start += STEP
            time.sleep(SLEEP_BETWEEN)
            if len(chunk) < STEP:
                break
    return all_rows


def scrape_cluster(cluster_id: str) -> pd.DataFrame:
    """
    1) Intenta traer por ventana estándar hasta que:
       - no haya más resultados, o
       - alcance ~2.500 ítems, o
       - la tienda devuelva 400 por límite.
    2) Si topa límite, continúa por particiones alfabéticas (ft).
    """
    session = make_session()
    start = 0
    seen_ids: Set[str] = set()
    all_rows: List[Dict[str, Any]] = []
    hit_window_cap = False

    # 1) Ventana estándar
    while True:
        try:
            if start >= MAX_WINDOW_RESULTS:
                hit_window_cap = True
                print(f"Ventana estándar alcanzó {MAX_WINDOW_RESULTS} ítems; cambiando a particiones…", flush=True)
                break
            chunk = fetch_page(session, cluster_id, start, STEP)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                hit_window_cap = True
                print(f"HTTP 400 en start={start}. Límite de ~2.500 resultados por ventana VTEX; cambiando a particiones…", flush=True)
                break
            else:
                raise

        if not chunk:
            break

        fresh = [p for p in chunk if p.get("productId") not in seen_ids]
        for p in fresh:
            seen_ids.add(p.get("productId"))

        print(f"Página desde {start}: {len(fresh)} productos nuevos (acum productos únicos: {len(seen_ids)})", flush=True)
        rows = flatten(fresh, cluster_id, verbose=True)
        all_rows.extend(rows)

        start += STEP
        time.sleep(SLEEP_BETWEEN)
        if len(chunk) < STEP:
            break

    # 2) Particiones alfabéticas si topamos el límite
    if hit_window_cap:
        extra_rows = scrape_cluster_alpha(session, cluster_id, seen_ids)
        all_rows.extend(extra_rows)

    df = pd.DataFrame(all_rows)
    cols = [
        "EAN", "CodigoInterno", "NombreProducto", "Categoria", "Subcategoria",
        "Marca", "Fabricante", "PrecioLista", "PrecioOferta", "TipoOferta",
        "URL", "SKU", "ProductId", "ClusterId", "ClusterNombre"
    ]
    if not df.empty:
        df = df.reindex(columns=cols)
    return df


def main():
    parser = argparse.ArgumentParser(description="Scraper Masonline VTEX por cluster IDs")
    parser.add_argument("--clusters", type=str, default="3454",
                        help="IDs de cluster separados por coma (ej: 3454,3627)")
    parser.add_argument("--out", type=str, default="masonline_cluster",
                        help="Prefijo de archivo de salida (sin extensión)")
    args = parser.parse_args()

    cluster_ids = [c.strip() for c in args.clusters.split(",") if c.strip()]
    frames = []
    for cid in cluster_ids:
        print(f"\n=== Cluster {cid} ===", flush=True)
        df = scrape_cluster(cid)
        print(f"Cluster {cid}: {len(df)} filas totales", flush=True)
        frames.append(df)

    if not frames:
        print("No se obtuvieron datos.", flush=True)
        return

    full = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    csv_path = f"{args.out}.csv"
    xlsx_path = f"{args.out}.xlsx"
    full.to_csv(csv_path, index=False, encoding="utf-8")
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as w:
        full.to_excel(w, index=False, sheet_name="productos")

    print(f"\n✅ Listo. Guardado:\n- {csv_path}\n- {xlsx_path}\nTotal filas: {len(full)}", flush=True)


if __name__ == "__main__":
    main()
