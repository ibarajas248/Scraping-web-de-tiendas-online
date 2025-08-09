import time
import re
import requests
import pandas as pd
from html import unescape
from bs4 import BeautifulSoup
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------- Config -----------------
BASE = "https://www.disco.com.ar"
SEARCH = f"{BASE}/api/catalog_system/pub/products/search"
FACETS = f"{BASE}/api/catalog_system/pub/facets/search/*?map=c"

STEP = 50
SLEEP = 0.1              # duerme solo tras páginas OK; si hay 429 sube a 0.3–0.5
TIMEOUT = 25
MAX_EMPTY_PAGES = 2       # corta si 2 páginas seguidas llegan vacías
RETRIES = 3               # reintentos por solicitud
MAX_WORKERS = 3           # hilos por categoría (ajusta 2–4)
MAX_DEPTH = None          # p.ej. 2 para limitar profundidad

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

OUT_XLSX = "disco_productos.xlsx"
OUT_CSV  = "disco_productos.csv"

# --- Excel: quitar caracteres de control ilegales ---
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

# ----------------- Utils -----------------
def clean_text(v):
    """Limpia HTML y controla caracteres ilegales para Excel (versión completa)."""
    if v is None:
        return ""
    if not isinstance(v, str):
        return v
    try:
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", v)

def clean_text_fast(v):
    """Versión rápida: evita BeautifulSoup salvo que detecte tags."""
    if v is None:
        return ""
    if not isinstance(v, str):
        return v
    if "<" in v and ">" in v:
        return clean_text(v)
    return ILLEGAL_XLSX.sub("", v)

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

# ----------------- HTTP Session (pool + retry) -----------------
def make_session():
    s = requests.Session()
    retry = Retry(
        total=RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s

SESSION = make_session()

# ----------------- Descubrimiento de categorías -----------------
def _link_to_segments(link: str):
    """
    Convierte '/Frutas-y-Verduras/Verduras?map=c,c' -> ['frutas-y-verduras','verduras']
    """
    if not link:
        return []
    link = unquote(link)
    path = link.split("?", 1)[0].strip("/")  # quitar query ?map=...
    if not path:
        return []
    segs = [s.strip().lower() for s in path.split("/") if s.strip()]
    return segs

def _walk_categories(node, results):
    """
    Recorre CategoriesTrees y guarda TODAS las rutas (nivel 1..n).
    results agrega tuplas: (segments_list)
    """
    link = (node.get("Link") or node.get("link") or "").strip()
    segs = _link_to_segments(link)
    if segs:
        results.add(tuple(segs))  # ruta completa hasta este nodo
    children = node.get("Children") or node.get("children") or []
    for ch in children:
        _walk_categories(ch, results)

def get_category_paths(max_depth=None):
    """
    Retorna lista de rutas (tuplas) de categorías/subcategorías.
    Filtra por max_depth si se indica.
    """
    r = SESSION.get(FACETS, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()

    results = set()
    trees = data.get("CategoriesTrees") or []
    for n1 in trees:
        _walk_categories(n1, results)

    # Fallback: Departments si no hay trees
    if not results:
        for dep in data.get("Departments", []):
            link = dep.get("Link") or dep.get("link") or ""
            segs = _link_to_segments(link)
            if segs:
                results.add(tuple(segs))

    paths = sorted(results, key=lambda t: (len(t), t))
    if max_depth:
        paths = [p for p in paths if len(p) <= max_depth]
    return paths

# ----------------- Descarga por path -----------------
def fetch_page_by_path(path_segments, offset):
    """
    path_segments: ['frutas-y-verduras','verduras'] -> path='frutas-y-verduras/verduras' ; map='c,c'
    """
    path = "/".join(path_segments)
    map_str = ",".join(["c"] * len(path_segments))
    url = f"{SEARCH}/{path}?map={map_str}&_from={offset}&_to={offset + STEP - 1}"

    for attempt in range(RETRIES):
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
        except Exception:
            time.sleep(0.3 + attempt * 0.5)
            continue

        status = r.status_code
        if status in (200, 206):
            try:
                return r.json()
            except Exception:
                time.sleep(0.3 + attempt * 0.5)
                continue
        if status == 429:
            # backoff agresivo solo si hay rate-limit
            time.sleep(1.0 + attempt * 0.8)
            continue
        if status in (500, 503):
            time.sleep(0.6 + attempt * 0.5)
            continue
        # otros códigos -> detener rápido
        return []
    return []

# ----------------- Parseo de producto -----------------
def parse_row(p):
    productId   = p.get("productId")
    productName = clean_text_fast(p.get("productName"))
    brand       = clean_text_fast(p.get("brand"))
    link        = p.get("link") or (BASE + "/" + (p.get("linkText") or "") + "/p")

    # Categorías “bonitas”
    categories = " > ".join([clean_text_fast(c.strip("/")) for c in (p.get("categories") or [])])

    items = p.get("items") or []
    it0 = items[0] if items else {}

    # EAN
    ean = it0.get("ean") or ""

    # RefId interno
    refid = ""
    for rid in it0.get("referenceId") or []:
        if (rid.get("Key") or "").lower() == "refid":
            refid = rid.get("Value") or ""
            break

    # Imagen
    img = ""
    if it0.get("images"):
        img = it0["images"][0].get("imageUrl") or ""

    # Precio y disponibilidad
    sellers = it0.get("sellers") or []
    s0 = sellers[0] if sellers else {}
    sellerName = s0.get("sellerName") or ""
    offer = s0.get("commertialOffer") or {}
    price = offer.get("Price")
    list_price = offer.get("ListPrice")
    price_no_disc = offer.get("PriceWithoutDiscount")
    is_avail = offer.get("IsAvailable")
    stock = offer.get("AvailableQuantity")

    return {
        "EAN": ean,                           # EAN real (si lo publican)
        "CodigoInterno": refid,               # RefId (para mapear si falta EAN)
        "producto": productName,
        "marca": brand,
        "Categoria": categories,
        "Precio": price,
        "PrecioLista": list_price,
        "PrecioSinDto": price_no_disc,
        "Disponible": is_avail,
        "Stock": stock,
        "URL": link,
        "Imagen": img,
        "Seller": sellerName,
        "productId": productId,
        "releaseDate": p.get("releaseDate"),
    }

# ----------------- Scraping por categoría -----------------
def scrape_category(segs):
    etiqueta = "/".join(segs)
    rows = []
    offset = 0
    empty_streak = 0
    while True:
        data = fetch_page_by_path(segs, offset)
        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES:
                break
            offset += STEP
            continue

        empty_streak = 0
        print(f"\n🗂️ {etiqueta} (offset={offset}) -> {len(data)} productos encontrados")
        for p in data:
            try:
                row = parse_row(p)
                rows.append(row)
                # --- impresión de avance en tiempo real ---
                print(f"  📦 [EAN: {row.get('EAN','')}] {row.get('producto','')} - ${row.get('Precio','')}")
            except Exception as e:
                print(f"  ⚠️ Error parseando producto: {e}")

        # última página real → corta sin dormir
        if len(data) < STEP:
            break

        offset += STEP
        if SLEEP:
            time.sleep(SLEEP)
    return etiqueta, rows


# ----------------- Orquestación principal -----------------
def scrape_all(max_workers=MAX_WORKERS, max_depth=MAX_DEPTH):
    paths = get_category_paths(max_depth=max_depth)
    print(f"🔎 {len(paths)} rutas a scrapear (workers={max_workers})")
    if paths:
        ej = "/".join(paths[0]); mp = ",".join(["c"]*len(paths[0]))
        print(f"Ejemplo de ruta: {ej}  | map={mp}")

    all_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(scrape_category, segs): segs for segs in paths}
        for fut in as_completed(futures):
            etiqueta, rows = fut.result()
            print(f"🗂️ {etiqueta}: {len(rows)} filas")
            all_rows.extend(rows)

    df = pd.DataFrame(all_rows)

    # 🔹 Eliminar duplicados ANTES de devolver el DataFrame
    keys = [c for c in ["productId", "EAN", "CodigoInterno"] if c in df.columns]
    if keys:
        before = len(df)
        df.drop_duplicates(subset=keys, inplace=True, keep="first")
        after = len(df)
        print(f"🧹 Eliminadas {before - after} filas duplicadas, quedan {after} únicas.")

    return df


def postprocess_and_save(df: pd.DataFrame, out_xlsx=OUT_XLSX, out_csv=OUT_CSV):
    if df.empty:
        print("⚠️ No se obtuvieron productos.")
        return df

    # De-duplicar por claves fuertes
    keys = [c for c in ["productId","EAN","CodigoInterno"] if c in df.columns]
    if keys:
        df.drop_duplicates(subset=keys, inplace=True, keep="first")

    # Orden sugerido de columnas
    prefer = ["EAN","CodigoInterno","producto","marca","Categoria",
              "Precio","PrecioLista","PrecioSinDto","Disponible","Stock",
              "URL","Imagen","Seller","productId","releaseDate"]
    cols = [c for c in prefer if c in df.columns] + [c for c in df.columns if c not in prefer]
    df = df[cols]


    df.to_excel(out_xlsx, index=False)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\n💾 Guardado: {out_xlsx} ({len(df)} filas) y {out_csv}")
    return df

# ----------------- Entry point -----------------
if __name__ == "__main__":
    t0 = time.time()
    df = scrape_all(max_workers=MAX_WORKERS, max_depth=MAX_DEPTH)
    df = postprocess_and_save(df)
    print(f"⏱️ Tiempo total: {time.time() - t0:.1f}s")
