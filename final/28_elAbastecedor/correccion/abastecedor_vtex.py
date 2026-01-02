#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VTEX FULL CATALOGO – “full bacano” (categorías + split + multi-base + SC auto + fallback fulltext)
Objetivo:
- Traer MUCHOS más productos que el barrido por ft (fulltext) solamente.
- Estrategia principal: recorrer TODO el árbol de categorías y consultar por fq=C:<categoryId>.
- Cuando una categoría supera el límite (>=2500), hace split recursivo por marcas y/o rangos de precio usando facets.
- Soporta múltiples sellers/sku y exporta JSONL + CSV.

Notas importantes (realistas):
- En VTEX “público” NO siempre se puede ver “todo el catálogo interno”; depende de visibilidad, seller, segmento, etc.
- Si la tienda realmente publica 50k+ productos y los expone al público, este enfoque es el que más cerca te va a llevar.
"""

import time
import json
import csv
import os
import string
from typing import Dict, Any, List, Optional, Tuple, Set
import requests
from requests.exceptions import RequestException, HTTPError

# ===================== Config =====================

# A veces el dominio "vtexcommercestable.com.br" no coincide con el catálogo expuesto en el dominio público.
# Probamos ambos y elegimos el que responda mejor.
BASE_CANDIDATES = [
    "https://www.abastecedor.com.ar",
    "https://elabastecedorar.vtexcommercestable.com.br",
]

# Si quieres forzar un SC específico: AUTO_DETECT_SC=False y pon FORCED_SC_LIST=[1] por ejemplo.
AUTO_DETECT_SC = True
SC_CANDIDATES = list(range(1, 31))  # prueba sc=1..30

PAGE_SIZE_MAX = 50     # VTEX Search API: max 50 por página
SLEEP = 0.15
TIMEOUT = 30

MAX_RETRIES = 5
BACKOFF_BASE = 0.8
RETRY_STATUSES = {429, 500, 502, 503, 504}

# Fallback fulltext: úsalo como respaldo (no como estrategia principal)
FULLTEXT_TOKENS = (
    list(string.ascii_lowercase) +
    list("ñáéíóúü") +
    list("0123456789")
)

ENABLE_2CHAR_TOKENS = False
TOKENS_2CHAR = [a + b for a in "abcdef" for b in "abcdef"]  # ejemplo corto

# Árbol de categorías
CATEGORY_TREE_LEVEL = 4  # 3 o 4 suele estar bien (depende la tienda)

# Límite “duro” por búsqueda paginada; no deberías tocarlo.
HARD_STOP = 200000

# Profundidad máxima de split recursivo
MAX_DEPTH = 7

OUT_JSONL = "elabastecedor_catalogo_full.jsonl"
OUT_CSV   = "elabastecedor_catalogo_full.csv"
OUT_LOG   = "elabastecedor_catalogo_full.log"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VTEXCatalogFull/4.0)",
    "Accept": "application/json, text/plain, */*",
}

# ===================== Logging =====================

def log(msg: str) -> None:
    print(msg)
    with open(OUT_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# ===================== HTTP helpers =====================

def request_json(url: str, params: List[Tuple[str, str]]) -> Any:
    """
    Request JSON con retries SOLO para 429 y 5xx.
    - NO reintenta 404 ni otros 4xx (excepto 429).
    """
    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)

            # retryable
            if r.status_code in RETRY_STATUSES:
                raise HTTPError(f"HTTP {r.status_code}", response=r)

            # 4xx no retryable
            if 400 <= r.status_code < 500 and r.status_code != 429:
                log(f"  !! HTTP {r.status_code} (NO-RETRY) url={r.url}")
                r.raise_for_status()

            r.raise_for_status()
            return r.json()

        except HTTPError as e:
            last_err = e
            status = getattr(e.response, "status_code", None)
            if status is not None and 400 <= status < 500 and status != 429:
                raise

            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            log(f"  !! HTTPError status={status} attempt={attempt}/{MAX_RETRIES} wait={wait:.1f}s url={url}")
            time.sleep(wait)

        except RequestException as e:
            last_err = e
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            log(f"  !! RequestException attempt={attempt}/{MAX_RETRIES} wait={wait:.1f}s url={url} err={e}")
            time.sleep(wait)

    if last_err:
        raise last_err
    raise RuntimeError("request_json failed without exception")

def vtex_get(base: str,
             path: str,
             params: Optional[Dict[str, Any]] = None,
             fqs: Optional[List[str]] = None) -> Any:
    """
    GET VTEX con soporte de múltiples fq. (params va como dict; fqs como lista)
    """
    url = base.rstrip("/") + path
    q: List[Tuple[str, str]] = []
    if params:
        for k, v in params.items():
            q.append((k, str(v)))
    if fqs:
        for fq in fqs:
            q.append(("fq", fq))
    return request_json(url, q)

# ===================== Base / SC detection =====================

def quick_count(base: str, sc: int, ft: str) -> int:
    """
    Cuenta rápido con 1 página (0-49) para ver si ese SC tiene catálogo indexado.
    Si devuelve 404/4xx => 0.
    """
    try:
        batch = vtex_get(
            base,
            "/api/catalog_system/pub/products/search/",
            params={"sc": sc, "_from": 0, "_to": 49, "ft": ft},
            fqs=[]
        )
        if isinstance(batch, list):
            return len(batch)
        return 0
    except HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status is not None and 400 <= status < 500 and status != 429:
            return 0
        return 0
    except Exception:
        return 0

def detect_best_sc(base: str) -> List[int]:
    test_ft = "a"
    results: List[Tuple[int, int]] = []
    log(f"\n=== Detectando SC para base={base} con ft='a' ===")
    for sc in SC_CANDIDATES:
        n = quick_count(base, sc, test_ft)
        log(f"  sc={sc} -> {n} items en primera página")
        if n > 0:
            results.append((sc, n))
        time.sleep(0.05)

    results.sort(key=lambda x: x[1], reverse=True)
    if not results:
        log("!! Ningún sc devolvió resultados. Forzando sc=1.")
        return [1]

    top_sc, top_n = results[0]
    chosen = [sc for sc, n in results if n >= max(1, int(top_n * 0.7))]
    log(f"SC elegidos: {chosen} (top={top_sc} con {top_n})\n")
    return chosen

def pick_best_base_and_sc() -> Tuple[str, List[int]]:
    """
    Prueba bases candidatas y elige la que:
    - devuelve más items en quick_count con algún SC, y
    - luego selecciona SCs "buenos" con detect_best_sc.
    """
    log("=== Probando BASE candidates ===")
    best: Tuple[str, int, int] = ("", 0, 0)  # (base, sc, count)
    for base in BASE_CANDIDATES:
        # prueba unos SC comunes primero para decidir rápido
        for sc in [1, 2, 3, 4, 5, 10, 12]:
            n = quick_count(base, sc, "a")
            log(f"  base={base} sc={sc} -> {n}")
            if n > best[2]:
                best = (base, sc, n)
            time.sleep(0.03)

    if not best[0]:
        # fallback
        chosen_base = BASE_CANDIDATES[0]
        sc_list = [1]
        log(f"!! No hubo respuesta clara. Usando fallback base={chosen_base} sc={sc_list}")
        return chosen_base, sc_list

    chosen_base = best[0]
    sc_list = detect_best_sc(chosen_base) if AUTO_DETECT_SC else [best[1]]
    log(f"✅ BASE elegido: {chosen_base} | SCs: {sc_list}")
    return chosen_base, sc_list

# ===================== Normalización =====================

def normalize_product(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    product_id = p.get("productId")
    product_name = p.get("productName")
    brand = p.get("brand")
    brand_id = p.get("brandId")
    link = p.get("link") or p.get("linkText")
    categories = p.get("categories", [])
    category_tree = p.get("categoryTree", [])

    items = p.get("items") or []
    for it in items:
        sku_id = it.get("itemId")
        sku_name = it.get("name")
        ean = it.get("ean") or ""

        sellers = it.get("sellers") or []
        if not sellers:
            rows.append({
                "productId": product_id,
                "productName": product_name,
                "brand": brand,
                "brandId": brand_id,
                "link": link,
                "categories": " | ".join(categories) if isinstance(categories, list) else str(categories),
                "categoryTree": json.dumps(category_tree, ensure_ascii=False),
                "skuId": sku_id,
                "skuName": sku_name,
                "ean": ean,
                "sellerId": None,
                "price": None,
                "listPrice": None,
                "availableQty": None,
                "isAvailable": None,
            })
            continue

        for s in sellers:
            seller_id = s.get("sellerId")
            offer = (s.get("commertialOffer") or {})
            rows.append({
                "productId": product_id,
                "productName": product_name,
                "brand": brand,
                "brandId": brand_id,
                "link": link,
                "categories": " | ".join(categories) if isinstance(categories, list) else str(categories),
                "categoryTree": json.dumps(category_tree, ensure_ascii=False),
                "skuId": sku_id,
                "skuName": sku_name,
                "ean": ean,
                "sellerId": seller_id,
                "price": offer.get("Price"),
                "listPrice": offer.get("ListPrice"),
                "availableQty": offer.get("AvailableQuantity"),
                "isAvailable": offer.get("IsAvailable"),
            })
    return rows

# ===================== Facets (split) =====================

def get_facets(base: str, sc: int, fqs: List[str], ft: Optional[str]) -> Dict[str, Any]:
    params = {"sc": sc}
    if ft is not None:
        params["ft"] = ft
    return vtex_get(base, "/api/catalog_system/pub/facets/search/", params=params, fqs=fqs)

def extract_brand_buckets(facets: Dict[str, Any]) -> List[Tuple[str, int]]:
    buckets: List[Tuple[str, int]] = []
    for key in ("Brands", "brands"):
        if key in facets and isinstance(facets[key], list):
            for b in facets[key]:
                bid = b.get("Id") or b.get("id")
                qty = b.get("Quantity") or b.get("quantity") or 0
                if bid is not None:
                    buckets.append((str(bid), int(qty)))

    if not buckets:
        brand_obj = facets.get("Brand") or facets.get("brand")
        if isinstance(brand_obj, dict):
            children = brand_obj.get("Children") or brand_obj.get("children") or []
            for b in children:
                bid = b.get("Id") or b.get("id")
                qty = b.get("Quantity") or b.get("quantity") or 0
                if bid is not None:
                    buckets.append((str(bid), int(qty)))
    # ordena por qty desc
    buckets.sort(key=lambda x: x[1], reverse=True)
    return buckets

def extract_price_buckets(facets: Dict[str, Any]) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for key in ("PriceRanges", "priceRanges", "Prices", "prices"):
        if key in facets and isinstance(facets[key], list):
            for pr in facets[key]:
                name = pr.get("Name") or pr.get("name")
                qty = pr.get("Quantity") or pr.get("quantity") or 0
                if isinstance(name, str) and " TO " in name:
                    out.append((f"P:{name.strip()}", int(qty)))

    if not out:
        pr_obj = facets.get("PriceRanges") or facets.get("priceRanges")
        if isinstance(pr_obj, dict):
            children = pr_obj.get("Children") or pr_obj.get("children") or []
            for pr in children:
                name = pr.get("Name") or pr.get("name")
                qty = pr.get("Quantity") or pr.get("quantity") or 0
                if isinstance(name, str) and " TO " in name:
                    out.append((f"P:{name.strip()}", int(qty)))
    out.sort(key=lambda x: x[1], reverse=True)
    return out

# ===================== Robust pagination with range-splitting =====================

def fetch_range(base: str, sc: int, fqs: List[str], ft: Optional[str],
                _from: int, _to: int, indent: str) -> List[Dict[str, Any]]:
    try:
        params = {"sc": sc, "_from": _from, "_to": _to}
        if ft is not None:
            params["ft"] = ft

        batch = vtex_get(base, "/api/catalog_system/pub/products/search/", params=params, fqs=fqs)
        if not isinstance(batch, list):
            return []
        return batch

    except HTTPError as e:
        status = getattr(e.response, "status_code", None)

        # 4xx: no tiene sentido splitear, devolvemos vacío
        if status is not None and 400 <= status < 500 and status != 429:
            log(f"{indent}!! HTTP {status} (NO-RETRY) range={_from}-{_to} sc={sc} ft={ft} fqs={fqs} -> EMPTY")
            return []

        # retryable: split
        if _from == _to:
            log(f"{indent}!! SKIP range=({_from}) por HTTP {status} sc={sc} ft={ft} fqs={fqs}")
            return []

        mid = (_from + _to) // 2
        log(f"{indent}>> SPLIT HTTP {status}: {(_from,_to)} -> ({_from,mid}) + ({mid+1,_to}) | sc={sc} ft={ft}")
        left = fetch_range(base, sc, fqs, ft, _from, mid, indent + "  ")
        time.sleep(SLEEP)
        right = fetch_range(base, sc, fqs, ft, mid + 1, _to, indent + "  ")
        return left + right

    except RequestException as e:
        if _from == _to:
            log(f"{indent}!! SKIP range=({_from}) por red sc={sc} ft={ft} fqs={fqs} err={e}")
            return []
        mid = (_from + _to) // 2
        log(f"{indent}>> SPLIT red: {(_from,_to)} -> ({_from,mid}) + ({mid+1,_to}) | sc={sc} ft={ft}")
        left = fetch_range(base, sc, fqs, ft, _from, mid, indent + "  ")
        time.sleep(SLEEP)
        right = fetch_range(base, sc, fqs, ft, mid + 1, _to, indent + "  ")
        return left + right

def paginated_search(base: str, sc: int, fqs: List[str], ft: Optional[str],
                     hard_stop: int = HARD_STOP) -> Tuple[List[Dict[str, Any]], bool]:
    all_products: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    _from = 0
    truncated = False

    while _from < hard_stop:
        _to = _from + (PAGE_SIZE_MAX - 1)
        batch = fetch_range(base, sc, fqs, ft, _from, _to, indent="  ")

        if not batch:
            break

        for p in batch:
            pid = p.get("productId")
            if pid and pid in seen:
                continue
            if pid:
                seen.add(pid)
            all_products.append(p)

        # VTEX: muchas queries “se cortan” por ~2500 resultados (depende la implementación).
        if _from >= 2500:
            truncated = True
            break

        _from += PAGE_SIZE_MAX
        time.sleep(SLEEP)

    return all_products, truncated

# ===================== Recursive split to avoid 2500 =====================

def crawl_filter(base: str, sc: int, fqs: List[str], ft: Optional[str],
                 depth: int = 0, max_depth: int = MAX_DEPTH) -> List[Dict[str, Any]]:
    indent = "  " * depth
    prods, truncated = paginated_search(base, sc, fqs, ft)

    if not truncated:
        log(f"{indent}OK | {len(prods)} productos | sc={sc} ft={ft} fqs={fqs}")
        return prods

    log(f"{indent}TRUNCATED (>=2500) | sc={sc} ft={ft} fqs={fqs}")

    if depth >= max_depth:
        log(f"{indent}!! Max depth alcanzado -> parcial {len(prods)} | sc={sc} ft={ft} fqs={fqs}")
        return prods

    try:
        facets = get_facets(base, sc, fqs, ft)
    except Exception as e:
        log(f"{indent}!! facets error: {e} -> parcial {len(prods)} | sc={sc} ft={ft} fqs={fqs}")
        return prods

    # 1) split por marca
    brand_buckets = extract_brand_buckets(facets)
    if brand_buckets and len(brand_buckets) > 1:
        log(f"{indent}>> Split BRANDS: {len(brand_buckets)} buckets | sc={sc} ft={ft}")
        allp: List[Dict[str, Any]] = []
        seen2: Set[str] = set()
        for bid, qty in brand_buckets:
            child = crawl_filter(base, sc, fqs + [f"B:{bid}"], ft, depth=depth + 1, max_depth=max_depth)
            for p in child:
                pid = p.get("productId")
                if pid and pid in seen2:
                    continue
                if pid:
                    seen2.add(pid)
                allp.append(p)
            time.sleep(SLEEP)
        return allp

    # 2) split por rango de precio
    price_buckets = extract_price_buckets(facets)
    if price_buckets and len(price_buckets) > 1:
        log(f"{indent}>> Split PRICE: {len(price_buckets)} buckets | sc={sc} ft={ft}")
        allp = []
        seen2 = set()
        for pr_fq, qty in price_buckets:
            child = crawl_filter(base, sc, fqs + [pr_fq], ft, depth=depth + 1, max_depth=max_depth)
            for p in child:
                pid = p.get("productId")
                if pid and pid in seen2:
                    continue
                if pid:
                    seen2.add(pid)
                allp.append(p)
            time.sleep(SLEEP)
        return allp

    log(f"{indent}!! No buckets útiles (brands/price) -> parcial {len(prods)} | sc={sc} ft={ft} fqs={fqs}")
    return prods

# ===================== Category tree crawl =====================

def get_category_tree(base: str, level: int = CATEGORY_TREE_LEVEL) -> List[Dict[str, Any]]:
    return vtex_get(base, f"/api/catalog_system/pub/category/tree/{level}/", params={}, fqs=[])

def flatten_category_ids(tree: List[Dict[str, Any]]) -> List[int]:
    ids: List[int] = []

    def walk(node: Dict[str, Any]) -> None:
        cid = node.get("id")
        if isinstance(cid, int):
            ids.append(cid)
        for ch in (node.get("children") or []):
            if isinstance(ch, dict):
                walk(ch)

    for n in tree or []:
        if isinstance(n, dict):
            walk(n)

    # únicos manteniendo orden
    seen: Set[int] = set()
    out: List[int] = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

# ===================== Export =====================

def export_rows(rows: List[Dict[str, Any]]) -> None:
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    if rows:
        keys = list(rows[0].keys())
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for r in rows:
                w.writerow(r)

# ===================== Main =====================

def main():
    # reset log
    if os.path.exists(OUT_LOG):
        os.remove(OUT_LOG)

    log("=== VTEX FULL CATALOGO (categorías + split + multi-base + SC auto + fallback ft) ===")

    base, sc_list = pick_best_base_and_sc()

    all_products: List[Dict[str, Any]] = []
    seen_global: Set[str] = set()

    # ========== (A) Estrategia principal: categorías ==========
    for sc in sc_list:
        log(f"\n=== CRAWL POR CATEGORÍAS sc={sc} base={base} ===")

        try:
            tree = get_category_tree(base, level=CATEGORY_TREE_LEVEL)
            cat_ids = flatten_category_ids(tree)
            log(f"Category tree: {len(cat_ids)} categoryIds (level={CATEGORY_TREE_LEVEL})")
        except Exception as e:
            log(f"!! No pude leer category tree: {e}")
            cat_ids = []

        if not cat_ids:
            log("!! No hay categorías (o no expone category tree). Saltando a fallback fulltext.")
        else:
            for i, cid in enumerate(cat_ids, 1):
                log(f"\n[{i}/{len(cat_ids)}] CATEGORIA id={cid} sc={sc}")
                prods = crawl_filter(base, sc, fqs=[f"C:{cid}"], ft=None, depth=0, max_depth=MAX_DEPTH)

                added = 0
                for p in prods:
                    pid = p.get("productId")
                    if not pid:
                        continue
                    if pid in seen_global:
                        continue
                    seen_global.add(pid)
                    all_products.append(p)
                    added += 1

                log(f"   -> cat {cid} trajo {len(prods)} y agregó {added} nuevos | total únicos={len(all_products)}")
                time.sleep(SLEEP)

    # ========== (B) Fallback: fulltext tokens ==========
    # Si por categorías se trajeron muy pocos, hacemos un barrido extra por ft.
    # (Si ya estás sacando miles/decenas de miles, esto puede sumar “los raros”.)
    if len(all_products) < 2000:
        log(f"\n=== FALLBACK FULLTEXT (porque total únicos={len(all_products)} es bajo) ===")
        tokens = list(FULLTEXT_TOKENS)
        if ENABLE_2CHAR_TOKENS:
            tokens += TOKENS_2CHAR

        for sc in sc_list:
            log(f"\n=== FULLTEXT para sc={sc} base={base} ===")
            for i, tok in enumerate(tokens, 1):
                log(f"\n[{i}/{len(tokens)}] TOKEN ft='{tok}' sc={sc}")
                prods = crawl_filter(base, sc, fqs=[], ft=tok, depth=0, max_depth=MAX_DEPTH)

                added = 0
                for p in prods:
                    pid = p.get("productId")
                    if not pid:
                        continue
                    if pid in seen_global:
                        continue
                    seen_global.add(pid)
                    all_products.append(p)
                    added += 1

                log(f"   -> token '{tok}' trajo {len(prods)} y agregó {added} nuevos | total únicos={len(all_products)}")
                time.sleep(SLEEP)

    log(f"\nTOTAL productos únicos: {len(all_products)}")

    # Normaliza a filas SKU x seller
    rows: List[Dict[str, Any]] = []
    for p in all_products:
        rows.extend(normalize_product(p))

    log(f"TOTAL filas SKU x seller: {len(rows)}")

    export_rows(rows)
    log(f"\n✅ Exportado:\n- {OUT_JSONL}\n- {OUT_CSV}\n- {OUT_LOG}\n")
    log(f"BASE usado: {base} | SCs: {sc_list}")

if __name__ == "__main__":
    main()
