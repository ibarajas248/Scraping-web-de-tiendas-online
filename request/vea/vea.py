import requests
import pandas as pd
import time
import re
from html import unescape
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional

# =========================
# Config
# =========================
CATEGORIAS: List[str] = [
    "electro",
    "tiempo-libre",
    "bebidas",
    "carnes",
    "almacen",
    "frutas-y-verduras",
    "lacteos",
    "perfumeria",
    "bebes-y-ninos",
    "limpieza",
    "quesos-y-fiambres",
    "congelados",
    "panaderia-y-pasteleria",
    "comidas-preparadas",
    "mascotas",
    "hogar-y-textil",
]

BASE_URL = "https://www.vea.com.ar/api/catalog_system/pub/products/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}
STEP = 50
SLEEP = 0.5
MAX_PAGES = 500             # salvavidas por categoría
MAX_RETRIES = 4             # reintentos por página
RETRY_BACKOFF = 1.5         # multiplicador del backoff
SAVE_CSV_EACH = 1000        # guarda CSV cada N filas nuevas

inicio = time.time()

# =========================
# Helpers de limpieza
# =========================
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]')

def clean_html(html_text: Optional[str]) -> str:
    if not html_text:
        return ""
    text = unescape(html_text)
    try:
        return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    except Exception:
        return text

def sanitize_excel(value: Any) -> Any:
    if isinstance(value, str):
        return ILLEGAL_XLSX.sub('', value)
    return value

# =========================
# Parsing
# =========================
def parse_product(p: Dict[str, Any]) -> Dict[str, Any]:
    items = p.get("items") or []
    first_item = items[0] if items else {}

    ean = first_item.get("ean")
    sellers = first_item.get("sellers") or []
    first_seller = sellers[0] if sellers else {}
    offer = first_seller.get("commertialOffer") or {}

    images = first_item.get("images") or []
    image_url = images[0].get("imageUrl") if images else None

    categories = " > ".join([c for c in (p.get("categories") or []) if c])
    categories_ids = " > ".join([c for c in (p.get("categoriesIds") or []) if c])

    return {
        "productId": p.get("productId"),
        "productName": p.get("productName"),
        "brand": p.get("brand"),
        "productReference": p.get("productReference"),
        "ean": ean,
        "price": offer.get("Price"),
        "priceWithoutDiscount": offer.get("PriceWithoutDiscount"),
        "listPrice": offer.get("ListPrice"),
        "priceValidUntil": offer.get("PriceValidUntil"),
        "isAvailable": offer.get("IsAvailable"),
        "availableQty": offer.get("AvailableQuantity"),
        "categoryId": p.get("categoryId"),
        "categories": categories,
        "categoriesIds": categories_ids,
        "releaseDate": p.get("releaseDate"),
        "linkText": p.get("linkText"),
        "link": p.get("link"),
        "imageUrl": image_url,
        "description": clean_html(p.get("description")),
    }

# =========================
# Requests con reintentos
# =========================
def fetch_page(categoria: str, offset: int, step: int = STEP) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/{categoria}"
    params = {"_from": offset, "_to": offset + step - 1}

    backoff = 1.0
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code in (200, 206):
                try:
                    return r.json() or []
                except ValueError:
                    print(f"⚠️ Respuesta no JSON en {categoria} offset={offset}")
                    return []
            # Errores “esperables”: corto devolviendo []
            if r.status_code in (400, 404):
                print(f"⚠️ HTTP {r.status_code} en {categoria} offset={offset}")
                return []
            # Errores temporales: reintento
            if r.status_code in (429, 500, 502, 503, 504):
                print(f"⏳ HTTP {r.status_code} en {categoria} offset={offset} (reintento {intento}/{MAX_RETRIES})")
                time.sleep(backoff)
                backoff *= RETRY_BACKOFF
                continue

            # Otro código: corto
            print(f"⚠️ HTTP {r.status_code} en {categoria} offset={offset}")
            return []

        except requests.RequestException as e:
            if intento == MAX_RETRIES:
                print(f"❌ Error de red en {categoria} offset={offset}: {e}")
                return []
            print(f"⏳ Error de red en {categoria} offset={offset}: {e} (reintento {intento}/{MAX_RETRIES})")
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF

    return []

# =========================
# Scrape por categoría
# =========================
def scrape_categoria(categoria: str) -> List[Dict[str, Any]]:
    productos_rows: List[Dict[str, Any]] = []
    seen_ids: set = set()
    offset = 0
    page = 1
    pages_without_new = 0

    while page <= MAX_PAGES:
        print(f"🔎 {categoria}: {offset}–{offset+STEP-1} (página {page})")
        data = fetch_page(categoria, offset, STEP)
        if not data:
            break

        prev_count = len(seen_ids)
        for p in data:
            pid = p.get("productId")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                productos_rows.append(parse_product(p))

        nuevos = len(seen_ids) - prev_count
        if nuevos == 0:
            pages_without_new += 1
        else:
            pages_without_new = 0

        # Cortes sanos: sin nuevos 2 veces o página incompleta
        if pages_without_new >= 2 or len(data) < STEP:
            break

        offset += STEP
        page += 1
        time.sleep(SLEEP)

    return productos_rows

# =========================
# Main
# =========================
if __name__ == "__main__":
    all_rows: List[Dict[str, Any]] = []
    total_since_save = 0

    for cat in CATEGORIAS:
        cat_rows = scrape_categoria(cat)
        if cat_rows:
            all_rows.extend(cat_rows)
            total_since_save += len(cat_rows)

            # Guardado incremental a CSV por si algo peta
            if total_since_save >= SAVE_CSV_EACH:
                df_tmp = pd.DataFrame(all_rows)
                df_tmp = df_tmp.drop_duplicates(subset=["productId"])
                df_tmp = df_tmp.applymap(sanitize_excel)
                df_tmp.to_csv("vea_productos_categorias_incremental.csv", index=False, encoding="utf-8")
                print(f"💾 Guardado incremental: {len(df_tmp)} filas")
                total_since_save = 0

    if all_rows:
        df = pd.DataFrame(all_rows)

        # Dedupe global
        if "productId" in df.columns:
            df = df.drop_duplicates(subset=["productId"])
        else:
            df = df.drop_duplicates()

        # Saneamos strings para Excel
        df = df.applymap(sanitize_excel)

        # Guarda CSV (respaldo)
        df.to_csv("vea_productos_categorias.csv", index=False, encoding="utf-8")

        # Excel (xlsxwriter reduce dolores de cabeza)
        try:
            df.to_excel("vea_productos_categorias.xlsx", index=False, engine="xlsxwriter")
        except Exception:
            # Fallback al engine por defecto si no está xlsxwriter
            df.to_excel("vea_productos_categorias.xlsx", index=False)

        print(f"✅ Guardado {len(df)} productos en vea_productos_categorias.xlsx")
        fin = time.time()
        print(f"⏱️ Tiempo total de ejecución: {fin - inicio:.2f} s")
    else:
        print("No se extrajeron productos.")
