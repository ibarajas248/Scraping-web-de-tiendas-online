#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time, re, requests, pandas as pd
from bs4 import BeautifulSoup
from html import unescape
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config ----------
BASE = "https://www.disco.com.ar"
SEARCH = f"{BASE}/api/catalog_system/pub/products/search"
FACETS = f"{BASE}/api/catalog_system/pub/facets/search/*?map=c"

STEP = 50
SLEEP_BASE = 0.1
TIMEOUT = 25
MAX_EMPTY_PAGES = 2
RETRIES = 3
MAX_WORKERS = 3
MAX_DEPTH = None

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

OUT_XLSX = "disco_formato.xlsx"
OUT_CSV  = None  # p.ej. "disco_formato.csv"

COLS_FINAL = [
    "EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Marca",
    "Fabricante","Precio de Lista","Precio de Oferta","Tipo de Oferta","URL"
]

ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

# ---------- Utils ----------
def clean_text_fast(v):
    if v is None: return ""
    if not isinstance(v, str): return v
    if "<" in v and ">" in v:
        try:
            v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
        except Exception:
            pass
    return ILLEGAL_XLSX.sub("", v)

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def split_cat(path: str):
    if not path: return "", ""
    parts = [p for p in path.strip("/").split("/") if p]
    fix = lambda s: s.replace("-", " ").strip().title()
    cat = fix(parts[0]) if parts else ""
    sub = fix(parts[1]) if len(parts) > 1 else ""
    return cat, sub

def tipo_de_oferta(offer: dict, list_price: float, price: float) -> str:
    try:
        dh = offer.get("DiscountHighLight") or []
        if dh and isinstance(dh, list):
            name = (dh[0].get("Name") or "").strip()
            if name: return name
    except Exception:
        pass
    return "Descuento" if (price or 0) < (list_price or 0) else "Precio regular"

# ---------- HTTP session ----------
def make_session():
    s = requests.Session()
    retry = Retry(total=RETRIES, backoff_factor=0.5,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET"], raise_on_status=False)
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
    s.mount("http://", adapter); s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s

SESSION = make_session()

# ---------- Categorías ----------
def _link_to_segments(link: str):
    if not link: return []
    link = unquote(link)
    path = link.split("?", 1)[0].strip("/")
    if not path: return []
    return [s.strip().lower() for s in path.split("/") if s.strip()]

def _walk_categories(node, results):
    link = (node.get("Link") or node.get("link") or "").strip()
    segs = _link_to_segments(link)
    if segs: results.add(tuple(segs))
    for ch in (node.get("Children") or node.get("children") or []):
        _walk_categories(ch, results)

def get_category_paths(max_depth=None):
    r = SESSION.get(FACETS, timeout=TIMEOUT); r.raise_for_status()
    data = r.json()
    results = set()
    for n1 in (data.get("CategoriesTrees") or []):
        _walk_categories(n1, results)
    if not results:
        for dep in data.get("Departments", []):
            segs = _link_to_segments(dep.get("Link") or dep.get("link") or "")
            if segs: results.add(tuple(segs))
    paths = sorted(results, key=lambda t: (len(t), t))
    if max_depth: paths = [p for p in paths if len(p) <= max_depth]
    return paths

# ---------- Fetch ----------
def fetch_page_by_path(path_segments, offset, sleep_holder):
    path = "/".join(path_segments)
    map_str = ",".join(["c"] * len(path_segments))
    url = f"{SEARCH}/{path}?map={map_str}&_from={offset}&_to={offset + STEP - 1}"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
    except Exception:
        time.sleep(sleep_holder[0]); return []

    if r.status_code in (200,206):
        try:
            return r.json()
        except Exception:
            time.sleep(sleep_holder[0]); return []
    if r.status_code == 429:
        # backoff adaptativo
        sleep_holder[0] = min(1.0, sleep_holder[0] + 0.2)
        time.sleep(sleep_holder[0]); return []
    if r.status_code in (500,503):
        time.sleep(sleep_holder[0]); return []
    return []

# ---------- Clave priorizada y parse por-SKU ----------
def build_key(ean: str, item_id: str, url: str) -> str:
    ean = (ean or "").strip()
    if ean: return f"E:{ean}"
    iid = (item_id or "").strip()
    if iid: return f"I:{iid}"
    return f"U:{(url or '').strip()}"

def rows_from_product(p: dict):
    """Devuelve una lista de filas (una por SKU) ya mapeadas al formato final."""
    rows = []
    categories = p.get("categories") or []
    cat, sub = ("","")
    if categories and isinstance(categories, list) and isinstance(categories[0], str):
        cat, sub = split_cat(categories[0])

    slug = p.get("linkText")
    base_url = f"{BASE}/{slug}/p" if slug else (p.get("link") or "")

    product_name = clean_text_fast(p.get("productName"))
    brand = clean_text_fast(p.get("brand"))
    manufacturer = p.get("manufacturer") or ""

    for it in (p.get("items") or []):
        sellers = it.get("sellers") or []
        s0 = sellers[0] if sellers else {}
        offer = s0.get("commertialOffer") or {}
        list_price = float(offer.get("ListPrice") or 0)
        price      = float(offer.get("Price") or 0)

        row = {
            "EAN": it.get("ean") or first(p.get("EAN")),
            "Código Interno": it.get("itemId") or p.get("productId"),
            "Nombre Producto": product_name,
            "Categoría": cat,
            "Subcategoría": sub,
            "Marca": brand,
            "Fabricante": manufacturer,
            "Precio de Lista": round(list_price, 2),
            "Precio de Oferta": round(price, 2),
            "Tipo de Oferta": tipo_de_oferta(offer, list_price, price),
            "URL": base_url,
        }
        rows.append(row)
    return rows

# ---------- Scraping por categoría (con dedupe en vivo) ----------
def scrape_category(segs, seen_keys: set):
    etiqueta = "/".join(segs)
    out = []
    offset = 0
    empty_streak = 0
    sleep_holder = [SLEEP_BASE]  # mutable para backoff adaptativo

    while True:
        data = fetch_page_by_path(segs, offset, sleep_holder)
        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES: break
            offset += STEP; continue

        empty_streak = 0
        added = 0
        for p in data:
            try:
                for row in rows_from_product(p):
                    key = build_key(row["EAN"], row["Código Interno"], row["URL"])
                    if key in seen_keys:  # dedupe cross-categoría
                        continue
                    seen_keys.add(key)
                    out.append(row)
                    added += 1
            except Exception:
                continue

        if len(data) < STEP: break
        offset += STEP
        time.sleep(sleep_holder[0])
    print(f"🗂️ {etiqueta}: +{len(out)} filas únicas")
    return etiqueta, out

# ---------- Orquestación ----------
def scrape_all(max_workers=MAX_WORKERS, max_depth=MAX_DEPTH):
    paths = get_category_paths(max_depth=max_depth)
    print(f"🔎 {len(paths)} rutas a scrapear (workers={max_workers})")
    seen_keys = set()
    all_rows = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(scrape_category, segs, seen_keys): segs for segs in paths}
        for fut in as_completed(futures):
            etiqueta, rows = fut.result()
            all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    # Garantizar columnas/orden
    for c in COLS_FINAL:
        if c not in df.columns: df[c] = pd.NA
    df["EAN"] = df["EAN"].astype("string")
    for c in ["Precio de Lista","Precio de Oferta"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
    df = df[COLS_FINAL]
    return df

def postprocess_and_save(df: pd.DataFrame):
    if df.empty:
        print("⚠️ No se obtuvieron productos.");
        return df

    # 🔹 Dedupe final ANTES de exportar
    df["_k"] = df["EAN"].fillna("").str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "Código Interno"].fillna("").astype(str).str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "URL"].fillna("").astype(str).str.strip()

    before = len(df)
    df = df.drop_duplicates(subset=["_k"]).drop(columns=["_k"])
    print(f"🧹 Dedupe final: -{before-len(df)} duplicados → {len(df)} únicos")

    # (opcional) Resetear índice para Excel más limpio
    df = df.reset_index(drop=True)

    # Guardar en Excel
    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="productos")
        wb=w.book; ws=w.sheets["productos"]
        money=wb.add_format({"num_format":"0.00"})
        text=wb.add_format({"num_format":"@"})
        col={n:i for i,n in enumerate(COLS_FINAL)}
        ws.set_column(col["EAN"], col["EAN"], 18, text)
        ws.set_column(col["Nombre Producto"], col["Nombre Producto"], 52)
        for c in ["Categoría","Subcategoría","Marca","Fabricante"]:
            ws.set_column(col[c], col[c], 20)
        ws.set_column(col["Precio de Lista"], col["Precio de Lista"], 14, money)
        ws.set_column(col["Precio de Oferta"], col["Precio de Oferta"], 14, money)
        ws.set_column(col["URL"], col["URL"], 46)

    if OUT_CSV:
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"💾 XLSX: {OUT_XLSX} ({len(df)} filas)")
    return df


# ---------- Entry ----------
if __name__ == "__main__":
    t0 = time.time()
    df = scrape_all(max_workers=MAX_WORKERS, max_depth=MAX_DEPTH)
    df = postprocess_and_save(df)
    print(f"⏱️ Tiempo total: {time.time() - t0:.1f}s")
