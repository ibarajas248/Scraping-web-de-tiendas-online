import requests
import pandas as pd
import time
import re
from html import unescape
from bs4 import BeautifulSoup

BASE = "https://www.carrefour.com.ar/api/catalog_system/pub/products/search"
CAT = "Electro-y-tecnologia"
MAP = "c"           # un segmento -> 'c'; si fueran 2 segmentos: 'c,c'
STEP = 50
SLEEP = 0.4
MAX_OFFSET = 10000   # salvavidas
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}
OUT_PATH = "carrefour_electro.xlsx"

# --- Excel fix: quitar caracteres de control ilegales ---
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
def sanitize_for_excel(v):
    if isinstance(v, str):
        return ILLEGAL_XLSX.sub('', v)
    return v

def clean_html(text: str) -> str:
    if not text:
        return ""
    try:
        return BeautifulSoup(unescape(text), "html.parser").get_text(" ", strip=True)
    except Exception:
        return text

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def parse_product(p: dict) -> dict:
    items = p.get("items") or []
    item0 = items[0] if items else {}
    sellers = item0.get("sellers") or []
    seller0 = sellers[0] if sellers else {}
    offer = seller0.get("commertialOffer") or {}

    img0 = first(item0.get("images") or [])
    image_url = img0.get("imageUrl") if isinstance(img0, dict) else None

    ean = item0.get("ean") or first(p.get("EAN"))

    refid = None
    for rid in item0.get("referenceId") or []:
        if isinstance(rid, dict) and rid.get("Key") == "RefId":
            refid = rid.get("Value") or refid

    categories = p.get("categories") or []
    leaf_category = None
    if categories:
        leaf = categories[0]
        parts = [s for s in leaf.split("/") if s.strip()]
        leaf_category = parts[-1] if parts else None

    return {
        "productId": p.get("productId"),
        "productName": p.get("productName"),
        "brand": p.get("brand"),
        "brandId": p.get("brandId"),
        "releaseDate": p.get("releaseDate"),
        "categoryId": p.get("categoryId"),
        "leafCategory": leaf_category,
        "link": p.get("link"),
        "ean": ean,
        "refId": refid,
        "price": offer.get("Price"),
        "listPrice": offer.get("ListPrice"),
        "priceValidUntil": offer.get("PriceValidUntil"),
        "availableQuantity": offer.get("AvailableQuantity"),
        "isAvailable": offer.get("IsAvailable"),
        "imageUrl": image_url,
        "description": clean_html(p.get("description")),
        "color": first(p.get("Color")),
        "modelo": first(p.get("Modelo")),
        "tipoProducto": first(p.get("Tipo de producto")),
        "origen": first(p.get("Origen")),
        "garantia": first(p.get("Garantía")),
    }

def save_partial(rows, path=OUT_PATH):
    df = pd.DataFrame(rows).drop_duplicates(subset=["productId"])
    if not df.empty:
        df = df.applymap(sanitize_for_excel)
    df.to_excel(path, index=False)
    print(f"💾 Guardado parcial: {len(df)} filas -> {path}")

def fetch_category(cat_path: str, map_str: str = "c", step: int = 50):
    out = []
    offset = 0
    seen = set()

    while True:
        if offset >= MAX_OFFSET:
            print("⚠️ MAX_OFFSET alcanzado")
            break

        url = f"{BASE}/{cat_path}?_from={offset}&_to={offset+step-1}&map={map_str}"
        r = requests.get(url, headers=HEADERS, timeout=30)

        if r.status_code not in (200, 206):
            time.sleep(1.0)
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code not in (200, 206):
                print("HTTP", r.status_code, "en", url)
                # guarda lo que tengas y salí
                save_partial(out)
                return out

        try:
            data = r.json()
        except Exception:
            print("Respuesta no JSON en", url)
            save_partial(out)
            return out

        if not data:
            break

        for p in data:
            pid = p.get("productId")
            if pid in seen:
                continue
            seen.add(pid)
            out.append(parse_product(p))

        offset += step
        time.sleep(SLEEP)

        # Si llegás al límite VTEX (~2500), guardá y salí
        if offset >= 2500:
            print("🚧 Límite VTEX (~2500) alcanzado; guardando y cerrando…")
            save_partial(out)
            return out

    return out

if __name__ == "__main__":
    rows = fetch_category(CAT, MAP, STEP)
    # guardado final (por si no se activó guardado parcial)
    save_partial(rows, OUT_PATH)
