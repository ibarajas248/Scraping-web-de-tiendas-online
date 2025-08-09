import requests, time, re, json, sys
import pandas as pd
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import quote

# ========= Config =========
BASE = "https://www.jumbo.com.ar"
STEP = 50                    # VTEX: 0-49, 50-99, ...
SLEEP_OK = 0.25              # pausa entre páginas
TIMEOUT = 25
MAX_EMPTY = 2                # corta tras N páginas vacías seguidas
TREE_DEPTH = 5               # profundidad para descubrir categorías
RETRIES = 3                  # reintentos por request
OUT_XLSX = "jumbo_all_products.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# ========= Helpers =========
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

def clean_text(v):
    if v is None:
        return ""
    if not isinstance(v, str):
        return v
    try:
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", v)

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def req_json(url, session, params=None):
    for i in range(RETRIES):
        r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 200:
            # A veces devuelven HTML por bloqueos temporales: valida JSON
            try:
                return r.json()
            except Exception:
                time.sleep(0.6)
        elif r.status_code in (429, 408, 500, 502, 503, 504):
            time.sleep(0.6 + 0.4 * i)
        else:
            time.sleep(0.3)
    return None

# ========= Categorías =========
def get_category_tree(session, depth=TREE_DEPTH):
    url = f"{BASE}/api/catalog_system/pub/category/tree/{depth}"
    data = req_json(url, session)
    return data or []

def iter_paths(tree):
    """Devuelve todas las rutas 'slug/slug2/...' (incluye hojas y nodos intermedios)."""
    out = []
    def walk(node, path):
        slug = node.get("url", "").strip("/").split("/")[-1] or node.get("slug") or node.get("Name")
        if not slug:
            return
        new_path = path + [slug]
        out.append("/".join(new_path))
        for ch in (node.get("children") or []):
            walk(ch, new_path)
    for n in tree:
        walk(n, [])
    # normaliza y dedup
    uniq = []
    seen = set()
    for p in out:
        ps = p.strip("/").lower()
        if ps and ps not in seen:
            seen.add(ps)
            uniq.append(ps)
    return uniq

def map_for_path(path_str):
    depth = len([p for p in path_str.split("/") if p])
    return ",".join(["c"] * depth)

# ========= Parsing de producto =========
def parse_rows_from_product(p, base):
    """Devuelve lista de filas: una por SKU y vendedor."""
    rows = []
    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = p.get("brand")
    brand_id = p.get("brandId")
    link_text = p.get("linkText")
    link = f"{base}/{link_text}/p" if link_text else ""
    categories = [c.strip("/") for c in (p.get("categories") or [])]
    category_path = " > ".join(categories[:1]) if categories else ""
    full_category_path = " > ".join(categories)

    # Atributos/Specs (cuando vienen)
    specs = {}
    for grp in (p.get("specificationGroups") or []):
        for it in (grp.get("specifications") or []):
            k = it.get("name")
            v = it.get("value")
            if k and v:
                specs[k] = v

    # clusterHighlights / properties
    cluster = p.get("clusterHighlights") or {}
    props = p.get("properties") or {}

    # Descripción corta (cuando aparece como "description", "metaTagDescription" o similar)
    desc = clean_text(p.get("description") or p.get("descriptionShort") or p.get("metaTagDescription") or "")

    items = p.get("items") or []
    for it in items:
        sku_id = it.get("itemId")
        sku_name = clean_text(it.get("name"))
        ean = ""
        for ref in (it.get("referenceId") or []):
            if ref.get("Value"):
                ean = ref["Value"]; break

        measurement_unit = it.get("measurementUnit")
        unit_multiplier = it.get("unitMultiplier")

        # imágenes (puede haber varias)
        images = ", ".join(img.get("imageUrl", "") for img in (it.get("images") or []))

        # Sellers (una fila por seller con sus precios/ofertas)
        sellers = it.get("sellers") or []
        if not sellers:
            # crea fila “sin seller” para no perder el SKU
            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                "sellerId": "",
                "sellerName": "",
                "availableQty": None,
                "price": None,
                "listPrice": None,
                "priceWithoutDiscount": None,
                "installments_json": "",
                "teasers_json": "",
                "tax": None,
                "rewardValue": None,
                "spotPrice": None,  # algunas cuentas lo exponen
                "name": name,
                "skuName": sku_name,
                "brand": brand,
                "brandId": brand_id,
                "ean": ean,
                "categoryTop": category_path,
                "categoryFull": full_category_path,
                "link": link,
                "linkText": link_text,
                "measurementUnit": measurement_unit,
                "unitMultiplier": unit_multiplier,
                "images": images,
                "description": desc,
                "specs_json": json.dumps(specs, ensure_ascii=False),
                "cluster_json": json.dumps(cluster, ensure_ascii=False),
                "properties_json": json.dumps(props, ensure_ascii=False),
            })
            continue

        for s in sellers:
            s_id = s.get("sellerId")
            s_name = s.get("sellerName")
            offer = s.get("commertialOffer") or {}
            price = offer.get("Price")
            list_price = offer.get("ListPrice")
            pwd = offer.get("PriceWithoutDiscount")
            avail = offer.get("AvailableQuantity")
            tax = offer.get("Tax")
            reward = offer.get("RewardValue")

            # algunos accounts devuelven "Installments": lista con número y valor
            installments = offer.get("Installments") or []
            teasers = offer.get("Teasers") or []
            # algunas tiendas traen "spotPrice" dentro de Teasers/Attachments; si existe directo:
            spot = offer.get("spotPrice", None)

            rows.append({
                "productId": product_id,
                "skuId": sku_id,
                "sellerId": s_id,
                "sellerName": s_name,
                "availableQty": avail,
                "price": price,
                "listPrice": list_price,
                "priceWithoutDiscount": pwd,
                "installments_json": json.dumps(installments, ensure_ascii=False),
                "teasers_json": json.dumps(teasers, ensure_ascii=False),
                "tax": tax,
                "rewardValue": reward,
                "spotPrice": spot,
                "name": name,
                "skuName": sku_name,
                "brand": brand,
                "brandId": brand_id,
                "ean": ean,
                "categoryTop": category_path,
                "categoryFull": full_category_path,
                "link": link,
                "linkText": link_text,
                "measurementUnit": measurement_unit,
                "unitMultiplier": unit_multiplier,
                "images": images,
                "description": desc,
                "specs_json": json.dumps(specs, ensure_ascii=False),
                "cluster_json": json.dumps(cluster, ensure_ascii=False),
                "properties_json": json.dumps(props, ensure_ascii=False),
            })
    return rows

# ========= Scrape por categoría =========
def fetch_category(session, cat_path):
    rows, seen_pids = [], set()
    offset, empty_streak = 0, 0

    map_str = map_for_path(cat_path)
    encoded_path = quote(cat_path, safe="/")

    while True:
        url = f"{BASE}/api/catalog_system/pub/products/search/{encoded_path}?map={map_str}&_from={offset}&_to={offset+STEP-1}"
        data = req_json(url, session)

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

        for p in data:
            parsed = parse_rows_from_product(p, BASE)
            rows.extend(parsed)

            # --- 📢 Seguimiento en consola ---
            print(f"  -> {p.get('productName')} "
                  f"({len(parsed)} filas, ej. precio: {parsed[0].get('price')}) "
                  f"[Cat: {cat_path}]")

        offset += STEP
        time.sleep(SLEEP_OK)

    return rows


def main():
    session = requests.Session()

    print("Descubriendo categorías…")
    tree = get_category_tree(session, TREE_DEPTH)
    cat_paths = iter_paths(tree)

    # Heurística: prioriza hojas (las que no son prefijo de otras) + mantiene también nodos padres por si hay SKUs solo ahí.
    # Para no perder nada, las recorremos todas; luego deduplicamos filas exactas.
    print(f"Categorías detectadas: {len(cat_paths)}")

    all_rows = []
    for i, path in enumerate(cat_paths, 1):
        try:
            print(f"[{i}/{len(cat_paths)}] {path}")
            rows = fetch_category(session, path)
            if rows:
                all_rows.extend(rows)
        except KeyboardInterrupt:
            print("Interrumpido por usuario."); break
        except Exception as e:
            print(f"  ! Error en {path}: {e}")
        # pausa leve entre categorías
        time.sleep(0.25)

    if not all_rows:
        print("No se obtuvieron filas. ¿Bloqueo temporal? Prueba subir SLEEP_OK o ejecutar en otra IP.")
        return

    df = pd.DataFrame(all_rows)

    # Orden de columnas
    cols = [
        "productId","skuId","sellerId","sellerName",
        "price","listPrice","priceWithoutDiscount","spotPrice","availableQty",
        "installments_json","teasers_json","tax","rewardValue",
        "name","skuName","brand","brandId","ean",
        "categoryTop","categoryFull","link","linkText",
        "measurementUnit","unitMultiplier","images","description",
        "specs_json","cluster_json","properties_json",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df.reindex(columns=cols)

    # Dedup conservador (mismo productId, skuId, sellerId y precio)
    df.drop_duplicates(subset=["productId","skuId","sellerId","price","listPrice","priceWithoutDiscount"], inplace=True)

    # Exporta a Excel
    df.to_excel(OUT_XLSX, index=False)
    print(f"\n✅ Listo: {len(df)} filas -> {OUT_XLSX}")

if __name__ == "__main__":
    main()
