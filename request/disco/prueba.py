import requests, time, re
import pandas as pd
from html import unescape
from bs4 import BeautifulSoup

BASE = "https://www.disco.com.ar"
SEARCH = f"{BASE}/api/catalog_system/pub/products/search"
FACETS = f"{BASE}/api/catalog_system/pub/facets/search/*?map=c"
STEP = 50
SLEEP = 0.4
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# --- Limpieza para Excel (control chars ilegales) ---
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
def sanitize(v):
    if isinstance(v, str):
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
        return ILLEGAL_XLSX.sub("", v)
    return v

def get_categories():
    """Lee categorías top-level desde /facets (VTEX). Retorna lista de rutas ej: ['almacen','bebidas',...]"""
    r = requests.get(FACETS, headers=HEADERS, timeout=25)
    r.raise_for_status()
    js = r.json()
    trees = js.get("CategoriesTrees") or []
    cats = []
    for t in trees:
        for ch in (t.get("Children") or []):
            # Link suele venir como '/almacen?map=c'
            link = (ch.get("Link") or "").split("?")[0].strip("/")
            if link:
                cats.append(link)
    return sorted(set(cats))

def fetch_category(cat_route):
    """Descarga TODOS los productos de una categoría 'ruta' usando map=c y paginación."""
    rows, offset = [], 0
    while True:
        url = f"{SEARCH}/{cat_route}?map=c&_from={offset}&_to={offset+STEP-1}"
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.status_code not in (200, 206):
            break
        data = r.json()
        if not data:
            break
        for p in data:
            items = p.get("items") or []
            itm = items[0] if items else {}
            sellers = itm.get("sellers") or []
            seller0 = sellers[0] if sellers else {}
            offer = seller0.get("commertialOffer") or {}
            rows.append({
                "categoria_ruta": cat_route,
                "productId": p.get("productId"),
                "name": sanitize(p.get("productName")),
                "brand": sanitize(p.get("brand")),
                "linkText": p.get("linkText"),
                "sku": itm.get("itemId"),
                "ean": itm.get("ean"),
                "price": offer.get("Price"),
                "listPrice": offer.get("ListPrice"),
                "available": offer.get("AvailableQuantity"),
                "seller": seller0.get("sellerName"),
            })
        offset += STEP
        time.sleep(SLEEP)
    return rows

def main():
    print("🔎 Leyendo categorías ...")
    categorias = get_categories()
    # Si prefieres fijarlas a mano, descomenta y edita:
    # categorias = ["almacen", "bebidas", "frescos", "limpieza", "perfumeria", "hogar-y-textil"]

    all_rows = []
    for c in categorias:
        print(f"➡️  {c}")
        try:
            all_rows.extend(fetch_category(c))
        except Exception as e:
            print(f"  ⚠️ error en {c}: {e}")

    if not all_rows:
        print("No se obtuvieron productos.")
        return

    df = pd.DataFrame(all_rows)
    # Dedup por (productId, sku) y por seguridad quitamos blancos
    df = df.drop_duplicates(subset=["productId", "sku"]).reset_index(drop=True)

    # Ordenar columnas
    cols = ["categoria_ruta","productId","sku","ean","name","brand","price","listPrice","available","seller","linkText"]
    df = df[[c for c in cols if c in df.columns]]

    out_path = "disco_productos.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="productos")

    print(f"✅ Guardado {len(df)} productos en {out_path}")

if __name__ == "__main__":
    main()
