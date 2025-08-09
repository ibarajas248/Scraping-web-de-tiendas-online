# runner.py (solo colección fija de EANs; sin CSV)
import time, argparse, concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
import pandas as pd

# =========================
# Config global
# =========================
DEFAULT_TIMEOUT = 25
DEFAULT_RETRIES = 3
FAST_TIMEOUT = (6, 12)  # (connect, read) útil para DIA
SLEEP_BETWEEN_EANS = 0.1
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# Tiendas VTEX
JUMBO_BASE = "https://www.jumbo.com.ar"
DISCO_BASE = "https://www.disco.com.ar"
VEA_BASE   = "https://www.vea.com.ar"
DIA_BASE   = "https://diaonline.supermercadosdia.com.ar"

# Coto
COTO_BASE  = "https://www.cotodigital.com.ar"
COTO_URL   = f"{COTO_BASE}/sitios/cdigi/categoria"

# =========================
# EANs fijos (edita esta lista)
# =========================
EANS: List[str] = [
    "7622201705169","7622201705077","7792129000766","7792129003644","7792129000759","7792129000742",
    "7796373002330","7796373000206","7796373114903","7796373002156","7796373002736","7796373002453",
    "7798224212585","7796373113401","7796373002248","7796373002163","7796373002828","7796373002132",
    "7796373112701","7790580122300","7790580122287","7790411000012","7790411000814","7790411001378",
    "7790411001521","7790411000470","7790411000807","7790411000548","7791675000572"
]

# =========================
# Utils
# =========================


def _req_json(url: str, params=None, timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES) -> Any:
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout, allow_redirects=False)
            ct = (r.headers.get("content-type") or "").lower()
            if r.status_code in (200, 206) and ("json" in ct or (r.text and r.text.lstrip().startswith(("{","[")))):
                try:
                    return r.json()
                except Exception:
                    time.sleep(0.15 + 0.1*i); continue
            if r.status_code in (301,302,303,307,308) or "text/html" in ct:
                time.sleep(0.15 + 0.1*i); continue
            if r.status_code in (429,408,500,502,503,504):
                time.sleep(0.3 + 0.3*i); continue
            return None
        except requests.RequestException:
            time.sleep(0.3 + 0.3*i)
    return None

# =========================
# --- EAN Mode Providers ---
# =========================
def _vtex_lookup(base: str, ean: str, timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES) -> List[Dict[str, Any]]:
    """
    VTEX por EAN: {base}/api/catalog_system/pub/products/search?fq=alternateIds_Ean:<EAN>[&sc=1]
    Devuelve una fila por SKU y seller (con precios).
    """
    url = f"{base}/api/catalog_system/pub/products/search"
    params = {"fq": f"alternateIds_Ean:{ean}"}
    if base == DIA_BASE:
        params["sc"] = "1"  # DIA suele requerir sc=1
        timeout = FAST_TIMEOUT
        retries = 2

    data = _req_json(url, params=params, timeout=timeout, retries=retries)
    rows: List[Dict[str, Any]] = []
    if not data:
        return rows

    for p in data:
        items = p.get("items") or []
        for it in items:
            sku_ean = it.get("ean") or ""
            if sku_ean and sku_ean != ean:
                continue
            img = (it.get("images") or [{}])[0].get("imageUrl", "")
            for s in (it.get("sellers") or []):
                offer = s.get("commertialOffer") or {}
                rows.append({
                    "store": base,
                    "ean": ean,
                    "productId": p.get("productId"),
                    "skuId": it.get("itemId"),
                    "name": p.get("productName"),
                    "brand": p.get("brand"),
                    "seller": s.get("sellerName"),
                    "price": offer.get("Price"),
                    "listPrice": offer.get("ListPrice"),
                    "priceWithoutDiscount": offer.get("PriceWithoutDiscount"),
                    "availableQty": offer.get("AvailableQuantity"),
                    "isAvailable": offer.get("IsAvailable"),
                    "url": f"{base}/{p.get('linkText','')}/p" if p.get("linkText") else "",
                    "image": img,
                })
    return rows

def jumbo_by_ean(ean: str) -> List[Dict[str, Any]]: return _vtex_lookup(JUMBO_BASE, ean)
def disco_by_ean(ean: str) -> List[Dict[str, Any]]: return _vtex_lookup(DISCO_BASE, ean)
def vea_by_ean(ean: str)   -> List[Dict[str, Any]]: return _vtex_lookup(VEA_BASE, ean)
def dia_by_ean(ean: str)   -> List[Dict[str, Any]]: return _vtex_lookup(DIA_BASE, ean)

# ---- Coto por EAN (barrido defensivo con cortes tempranos) ----
def _coto_fetch_page(offset: int, nrpp: int = 50, timeout=DEFAULT_TIMEOUT):
    params = {"Dy": "1", "No": str(offset), "Nrpp": str(nrpp), "format": "json"}
    try:
        r = requests.get(COTO_URL, params=params, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def _coto_extract_records_tree(root) -> list:
    found = []
    def walk(node):
        if isinstance(node, dict):
            if "records" in node and isinstance(node["records"], list):
                found.extend(node["records"])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(root)
    return found

def _coto_get_attr(attrs: dict, key: str, default: str = "") -> str:
    if not isinstance(attrs, dict): return default
    v = attrs.get(key, [""])
    return v[0] if isinstance(v, list) and v else default

def _coto_parse_record(rec) -> Dict[str, Any]:
    attrs = rec.get("attributes", {})
    url_tail = ""
    if isinstance(rec, dict):
        da = rec.get("detailsAction", {}) or {}
        url_tail = da.get("recordState", "") or ""
    return {
        "sku": _coto_get_attr(attrs, "sku.repositoryId"),
        "ean": _coto_get_attr(attrs, "product.eanPrincipal"),
        "name": _coto_get_attr(attrs, "product.displayName"),
        "brand": _coto_get_attr(attrs, "product.brand") or _coto_get_attr(attrs, "product.MARCA"),
        "image": _coto_get_attr(attrs, "product.mediumImage.url") or _coto_get_attr(attrs, "product.largeImage.url"),
        "url": COTO_BASE + url_tail
    }

def coto_by_ean(
    ean: str,
    max_pages: int = 150,
    nrpp: int = 50,
    max_hits: int = 3,
    stall_pages: int = 10,
    sleep: float = 0.2
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    seen = set()
    pages_since_last_hit = 0

    while offset < max_pages * nrpp:
        data = _coto_fetch_page(offset, nrpp)
        if not data:
            break

        records = _coto_extract_records_tree(data)
        found_this_page = 0
        for rec in records:
            prod = _coto_parse_record(rec)
            if not prod.get("ean"):
                continue
            if prod["ean"] == ean:
                key = (prod["sku"], prod["ean"])
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    "store": COTO_BASE,
                    "ean": ean,
                    "productId": "",
                    "skuId": prod["sku"],
                    "name": prod["name"],
                    "brand": prod["brand"],
                    "seller": "Coto",
                    "price": None,
                    "listPrice": None,
                    "priceWithoutDiscount": None,
                    "availableQty": None,
                    "isAvailable": None,
                    "url": prod["url"],
                    "image": prod["image"],
                })
                found_this_page += 1
                if len(rows) >= max_hits:
                    return rows

        pages_since_last_hit = 0 if found_this_page else pages_since_last_hit + 1
        if pages_since_last_hit >= stall_pages:
            break
        if found_this_page == 0 and len(records) < nrpp:
            break

        offset += nrpp
        time.sleep(sleep)
    return rows

# =========================
# --- EAN Mode runner (paralelo por tienda)
# =========================
STORE_FUNCS_EAN = {
    "jumbo": jumbo_by_ean,
    "disco": disco_by_ean,
    "vea":   vea_by_ean,
    "dia":   dia_by_ean,
    "coto":  coto_by_ean,
}

def run_one_ean_parallel(ean: str, stores: List[str]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(stores)) as ex:
        futs = {ex.submit(STORE_FUNCS_EAN[name], ean): name for name in stores if name in STORE_FUNCS_EAN}
        for fut in concurrent.futures.as_completed(futs, timeout=90):
            name = futs[fut]
            try:
                rows = fut.result(timeout=45)
                if rows:
                    results.extend(rows)
                    print(f"   • {name}: {len(rows)} fila(s)")
                    for prod in rows:
                        print(f"      - {prod.get('name')} | ${prod.get('price')} | EAN: {prod.get('ean')}")
                else:
                    print(f"   • {name}: sin resultados")
            except concurrent.futures.TimeoutError:
                print(f"   • {name}: timeout")
            except Exception as e:
                print(f"   • {name}: error {e}")
    return results

def run_ean_mode(stores: List[str], out: str):
    print(f"🔎 EANs: {len(EANS)} | Tiendas: {', '.join(stores)}\n")
    all_rows: List[Dict[str, Any]] = []
    for i, ean in enumerate(EANS, 1):
        print(f"[{i}/{len(EANS)}] EAN {ean}")
        rows = run_one_ean_parallel(ean, stores)
        all_rows.extend(rows)
        print("")
        time.sleep(SLEEP_BETWEEN_EANS)

    df = pd.DataFrame(all_rows)
    if df.empty:
        print("Sin filas"); return

    for col in ["price","listPrice","priceWithoutDiscount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df.drop_duplicates(
        subset=["store","ean","skuId","seller","price","listPrice","priceWithoutDiscount"],
        inplace=True
    )
    df.to_excel(out, index=False)
    print(f"✅ OK -> {out} ({len(df)} filas)")

# =========================
# --- CLI con defaults (no obligatorio)
# =========================
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["ean"], default="ean", help="Modo (solo 'ean' en esta versión)")
    ap.add_argument("--stores", type=str,
                    default="jumbo,disco,vea,dia,coto",
                    help="Tiendas separadas por coma (default: todas)")
    ap.add_argument("--out", type=str, default=f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    stores = [s.strip().lower() for s in args.stores.split(",") if s.strip()]

    # Sólo modo EAN (sin CSV)
    run_ean_mode(stores, args.out)
