import os, time, json, re, requests, concurrent.futures
import pandas as pd
from typing import List, Dict, Any, Tuple

# =========================
# Config
# =========================
TIMEOUT = 25                      # timeout general (segundos) para requests
RETRIES = 3                       # reintentos generales
FAST_TIMEOUT = (6, 12)            # (connect, read) más corto para DIA
SLEEP = 0.2                       # pausa corta entre EANs/tiendas cuando aplica
OUT_XLSX = "precios_por_ean.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

DIA_BASE = "https://diaonline.supermercadosdia.com.ar"

# Fallback: recorrer categorías en DIA si la búsqueda por EAN devuelve vacío
ENABLE_DIA_CATEGORY_FALLBACK = True

# Tus categorías para DIA (se usarán SOLO si el EAN no aparece por API directa)
DIA_CATEGORIES = [
    "almacen",
    "bebidas",
    "frescos",
    "desayuno",
    "limpieza",
    "perfumeria",
    "congelados",
    "bebes-y-ninos",
    "hogar-y-deco",
    "mascotas",
    "almacen/golosinas-y-alfajores",
    "frescos/frutas-y-verduras",
    "electro-hogar"
]

# Lista de EANs: usa eans.csv si existe (columna 'ean'); si no, usa esta lista
EANS: List[str] = [
    "7622201705169","7622201705077","7792129000766","7792129003644","7792129000759","7792129000742",
    "7796373002330","7796373000206","7796373114903","7796373002156","7796373002736","7796373002453",
    "7798224212585","7796373113401","7796373002248","7796373002163","7796373002828","7796373002132",
    "7796373112701","7790580122300","7790580122287","7790411000012","7790411000814","7790411001378",
    "7790411001521","7790411000470","7790411000807","7790411000548","7791675000572"
]

# =========================
# Helpers
# =========================
def req_json(
    url: str,
    params: Dict[str, Any] = None,
    headers: Dict[str, str] = None,
    timeout=TIMEOUT,
    retries=RETRIES
) -> Any:
    """
    GET robusto:
     - Reintenta en 429/408/5xx
     - Detecta content-type; si es HTML o redirect, reintenta rápido sin dormir de más
    """
    h = headers or HEADERS
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=h, timeout=timeout, allow_redirects=False)
            ct = (r.headers.get("content-type") or "").lower()
            # JSON válido
            if r.status_code in (200, 206) and ("json" in ct or (r.text and r.text.lstrip().startswith(("{","[")))):
                try:
                    return r.json()
                except Exception:
                    time.sleep(0.15 + 0.1*i)
                    continue
            # Redirecciones/Captcha/HTML
            if r.status_code in (301, 302, 303, 307, 308) or "text/html" in ct:
                time.sleep(0.15 + 0.1*i)
                continue
            # Errores temporales
            if r.status_code in (429, 408, 500, 502, 503, 504):
                time.sleep(0.3 + 0.3*i)
                continue
            # Otros casos: corta
            return None
        except requests.RequestException:
            time.sleep(0.3 + 0.3*i)
    return None

def read_eans_from_csv(path="eans.csv") -> List[str]:
    if not os.path.exists(path):
        return EANS
    df = pd.read_csv(path, dtype=str)
    eans = [str(x).strip() for x in df["ean"].dropna().tolist() if str(x).strip()]
    # dedup manteniendo orden
    return list(dict.fromkeys(eans))

# =========================
# VTEX (Jumbo/Disco/Vea/DIA) - por EAN
# =========================
def vtex_lookup(base: str, ean: str) -> List[Dict[str, Any]]:
    """
    Busca por EAN en una tienda VTEX:
      GET {base}/api/catalog_system/pub/products/search?fq=alternateIds_Ean:<EAN>[&sc=...]
    Devuelve una fila por SKU y seller con precios desde commertialOffer.
    """
    url = f"{base}/api/catalog_system/pub/products/search"
    params = {"fq": f"alternateIds_Ean:{ean}"}

    # Tuning específico para DIA
    timeout = TIMEOUT
    retries = RETRIES
    if base == DIA_BASE:
        params["sc"] = "1"       # canal de ventas por defecto
        timeout = FAST_TIMEOUT   # timeouts más cortos
        retries = 2              # menos reintentos

    data = req_json(url, params=params, timeout=timeout, retries=retries)
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

def jumbo_by_ean(ean: str): return vtex_lookup("https://www.jumbo.com.ar", ean)
def disco_by_ean(ean: str): return vtex_lookup("https://www.disco.com.ar", ean)
def vea_by_ean(ean: str):   return vtex_lookup("https://www.vea.com.ar", ean)
def dia_by_ean(ean: str):   return vtex_lookup(DIA_BASE, ean)

# =========================
# DIA Fallback (por categorías) - SOLO si por EAN no hay resultado
# =========================
STEP = 50
SLEEP_OK = 0.35
MAX_EMPTY = 2

def map_for_path(path_str: str) -> str:
    depth = len([p for p in path_str.split("/") if p.strip()])
    return ",".join(["c"] * depth) if depth else "c"

def parse_rows_from_product_dia(p: dict) -> List[dict]:
    rows = []
    product_id = p.get("productId")
    name = p.get("productName")
    brand = p.get("brand")
    link_text = p.get("linkText")
    url = f"{DIA_BASE}/{link_text}/p" if link_text else ""
    items = p.get("items") or []
    for it in items:
        sku_id = it.get("itemId")
        ean = it.get("ean")
        sellers = it.get("sellers") or []
        if not sellers:
            rows.append({
                "store": DIA_BASE,
                "ean": ean,
                "productId": product_id,
                "skuId": sku_id,
                "name": name,
                "brand": brand,
                "seller": None,
                "price": None,
                "listPrice": None,
                "priceWithoutDiscount": None,
                "availableQty": None,
                "isAvailable": None,
                "url": url,
                "image": (it.get("images") or [{}])[0].get("imageUrl", ""),
            })
            continue
        for s in sellers:
            offer = s.get("commertialOffer") or {}
            rows.append({
                "store": DIA_BASE,
                "ean": ean,
                "productId": product_id,
                "skuId": sku_id,
                "name": name,
                "brand": brand,
                "seller": s.get("sellerName"),
                "price": offer.get("Price"),
                "listPrice": offer.get("ListPrice"),
                "priceWithoutDiscount": offer.get("PriceWithoutDiscount"),
                "availableQty": offer.get("AvailableQuantity"),
                "isAvailable": offer.get("IsAvailable"),
                "url": url,
                "image": (it.get("images") or [{}])[0].get("imageUrl", ""),
            })
    return rows

def dia_lookup_by_categories_for_ean(ean: str, categories: List[str]) -> List[Dict[str, Any]]:
    print("   • DIA[Fallback]: buscando por categorías…")
    found_rows: List[Dict[str, Any]] = []
    for cat in categories:
        map_str = map_for_path(cat)
        offset = 0
        empty_streak = 0
        while True:
            params = {"_from": offset, "_to": offset + STEP - 1, "map": map_str, "sc": "1"}
            url = f"{DIA_BASE}/api/catalog_system/pub/products/search/{cat}"
            data = req_json(url, params=params, timeout=FAST_TIMEOUT, retries=2)
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

            hits_this_page = 0
            for p in data:
                rows = parse_rows_from_product_dia(p)
                # Filtro por EAN exacto en los SKUs de esta página
                rows = [r for r in rows if (r.get("ean") or "") == ean]
                if rows:
                    for r in rows:
                        print(f"      - {r.get('name')} | Precio: {r.get('price')} | EAN: {r.get('ean')}  [Cat: {cat}]")
                    found_rows.extend(rows)
                    hits_this_page += len(rows)

            # si encontramos algo en esta categoría, podemos cortar la categoría
            if hits_this_page > 0:
                break

            offset += STEP
            time.sleep(SLEEP_OK)

        if found_rows:
            break  # no seguir otras categorías si ya apareció
    if not found_rows:
        print("   • DIA[Fallback]: sin resultados")
    return found_rows

# =========================
# Coto (Endeca/Oracle)
# =========================
COTO_URL = "https://www.cotodigital.com.ar/sitios/cdigi/categoria"

def coto_fetch_page(offset: int, nrpp: int = 50):
    params = {"Dy": "1", "No": str(offset), "Nrpp": str(nrpp), "format": "json"}
    try:
        r = requests.get(COTO_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def coto_extract_records_tree(root) -> List[Dict[str, Any]]:
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

def coto_get_attr(attrs: dict, key: str, default: str = "") -> str:
    if not isinstance(attrs, dict):
        return default
    v = attrs.get(key, [""])
    if isinstance(v, list) and v:
        return v[0]
    return default

def coto_parse_record(rec) -> Dict[str, Any]:
    attrs = rec.get("attributes", {})
    return {
        "sku": coto_get_attr(attrs, "sku.repositoryId"),
        "ean": coto_get_attr(attrs, "product.eanPrincipal"),
        "name": coto_get_attr(attrs, "product.displayName"),
        "brand": coto_get_attr(attrs, "product.brand") or coto_get_attr(attrs, "product.MARCA"),
        "price_ref": coto_get_attr(attrs, "sku.referencePrice"),
        "image": coto_get_attr(attrs, "product.mediumImage.url") or coto_get_attr(attrs, "product.largeImage.url"),
        "url": "https://www.cotodigital.com.ar" + (rec.get("detailsAction", {}).get("recordState", "") or ""),
    }

def coto_by_ean(
    ean: str,
    max_pages: int = 200,     # límite duro
    nrpp: int = 50,
    max_hits: int = 3,        # corta tras N coincidencias
    stall_pages: int = 10     # corta si pasan N páginas sin hits
) -> List[Dict[str, Any]]:
    rows = []
    offset = 0
    seen = set()
    pages_since_last_hit = 0

    while offset < max_pages * nrpp:
        data = coto_fetch_page(offset, nrpp)
        if not data:
            print(f"      (Coto) sin datos en offset={offset}, corto")
            break

        records = coto_extract_records_tree(data)
        found_this_page = 0

        for rec in records:
            prod = coto_parse_record(rec)
            if not prod.get("ean"):
                continue
            if prod["ean"] == ean:
                key = (prod["sku"], prod["ean"])
                if key in seen:
                    continue
                seen.add(key)
                fila = {
                    "store": "https://www.cotodigital.com.ar",
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
                }
                rows.append(fila)
                print(f"      - {fila['name']} | EAN: {fila['ean']} | SKU: {fila['skuId']}")
                found_this_page += 1
                if len(rows) >= max_hits:
                    return rows

        if found_this_page == 0:
            pages_since_last_hit += 1
        else:
            pages_since_last_hit = 0

        if pages_since_last_hit >= stall_pages:
            print(f"      (Coto) {stall_pages} páginas sin nuevos matches, corto")
            break

        if found_this_page == 0 and len(records) < nrpp:
            print(f"      (Coto) página incompleta y sin hits, corto")
            break

        offset += nrpp
        if (offset // nrpp) % 10 == 0:
            print(f"      (Coto) avance: página {offset // nrpp}")
        time.sleep(SLEEP)
    return rows

# =========================
# Orquestación
# =========================
def run_one_ean(ean: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Ejecuta consultas en paralelo para un EAN en todas las tiendas VTEX.
    Si DIA no devuelve nada por EAN y ENABLE_DIA_CATEGORY_FALLBACK=True,
    intenta por categorías hasta encontrar el EAN.
    """
    stores = [
        ("Jumbo", lambda e: jumbo_by_ean(e)),
        ("Disco", lambda e: disco_by_ean(e)),
        ("Vea",   lambda e: vea_by_ean(e)),
        ("DIA",   lambda e: dia_by_ean(e)),
        ("Coto",  lambda e: coto_by_ean(e)),
    ]
    results: List[Dict[str, Any]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(stores)) as ex:
        futs = {ex.submit(fn, ean): name for name, fn in stores}
        done_names = set()
        for fut in concurrent.futures.as_completed(futs, timeout=90):
            name = futs[fut]
            try:
                rows = fut.result(timeout=45)
                if rows:
                    results.extend(rows)
                    print(f"   • {name}: {len(rows)} fila(s)")
                    for prod in rows:
                        print(f"      - {prod.get('name')} | Precio: {prod.get('price')} | EAN: {prod.get('ean')}")
                else:
                    print(f"   • {name}: sin resultados")
                done_names.add(name)
            except concurrent.futures.TimeoutError:
                print(f"   • {name}: timeout, continúo")
            except Exception as e:
                print(f"   • {name}: error {e}")
                done_names.add(name)

    # Fallback DIA por categorías si no hubo filas de DIA y está habilitado
    if ENABLE_DIA_CATEGORY_FALLBACK and not any(r["store"] == DIA_BASE for r in results):
        try:
            rows_fallback = dia_lookup_by_categories_for_ean(ean, DIA_CATEGORIES)
            if rows_fallback:
                results.extend(rows_fallback)
        except Exception as e:
            print(f"   • DIA[Fallback]: error {e}")

    return ean, results

def main():
    eans = read_eans_from_csv()
    print(f"🔎 Buscar por EAN en 5 tiendas (con fallback DIA por categorías) | total EANs: {len(eans)}\n")

    all_rows: List[Dict[str, Any]] = []

    for i, ean in enumerate(eans, 1):
        print(f"[{i}/{len(eans)}] EAN {ean}")
        _, rows = run_one_ean(ean)
        all_rows.extend(rows)
        print("")
        time.sleep(0.1)  # respiro leve entre EANs

    if not all_rows:
        print("⚠️ No se encontraron coincidencias.")
        return

    df = pd.DataFrame(all_rows)

    order = [
        "store","ean","productId","skuId","name","brand","seller",
        "price","listPrice","priceWithoutDiscount","availableQty","isAvailable",
        "url","image"
    ]
    for c in order:
        if c not in df.columns:
            df[c] = ""

    df = df[order]
    # dedup conservador por tienda+ean+sku+seller+price
    df.drop_duplicates(
        subset=["store","ean","skuId","seller","price","listPrice","priceWithoutDiscount"],
        inplace=True
    )

    # Redondeo precios si están
    for col in ["price","listPrice","priceWithoutDiscount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df.to_excel(OUT_XLSX, index=False)
    print(f"✅ Guardado: {OUT_XLSX} ({len(df)} filas)")

if __name__ == "__main__":
    main()
