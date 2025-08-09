"""
Carrefour AR (VTEX) — Scraper de todas las categorías (rápido y robusto)

• Descubre todo el árbol de categorías (category/tree).
• Paraleliza el scraping por categoría con límite de hilos.
• Reutiliza conexiones (Session) y aplica retries/backoff.
• Corta por categoría cuando no hay más páginas (página vacía o < STEP).
• Deduplica por productId y guarda en CSV (Excel opcional al final).
"""

import re
import time
import logging
from html import unescape
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------ Config ------------------
BASE = "https://www.carrefour.com.ar/api/catalog_system/pub/products/search"
TREE = "https://www.carrefour.com.ar/api/catalog_system/pub/category/tree/{depth}"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

STEP = 50              # ítems por página VTEX
SLEEP_PAGE = 0.25      # pausa entre páginas dentro de una misma categoría
MAX_OFFSET_HARD = 10000  # salvavidas por si algo queda en loop
MAX_WORKERS = 6        # hilos (categorías en paralelo). 5–8 suele andar bien
DEPTH = 10             # profundidad del árbol de categorías
CLEAN_HTML = True      # limpiar descripciones con HTML
SAVE_CSV = "carrefour_all_products.csv"
SAVE_XLSX = None       # Ej: "carrefour.xlsx" si quieres Excel

# Si usas Excel, se aplica formato a columnas de precio automáticamente
PRICE_COLS = ["price", "listPrice"]

# ------------------ Logging ------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ------------------ Utilidades ------------------
ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

def sanitize_for_excel(value):
    if isinstance(value, str):
        return ILLEGAL_XLSX.sub("", value)
    return value

def clean_html_text(text: Optional[str]) -> str:
    if not text:
        return ""
    if not CLEAN_HTML:
        return unescape(text)
    # Evita parsear si parece que no hay tags
    t = unescape(text)
    if "<" in t and ">" in t:
        try:
            return BeautifulSoup(t, "html.parser").get_text(" ", strip=True)
        except Exception:
            return t
    return t

def first(lst: Optional[Iterable], default=None):
    return lst[0] if isinstance(lst, list) and lst else default

# ------------------ Sesión HTTP con retries ------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

# ------------------ Parseo de producto ------------------
def parse_product(p: Dict) -> Dict:
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
        "description": clean_html_text(p.get("description")),
        "color": first(p.get("Color")),
        "modelo": first(p.get("Modelo")),
        "tipoProducto": first(p.get("Tipo de producto")),
        "origen": first(p.get("Origen")),
        "garantia": first(p.get("Garantía")),
    }

# ------------------ Árbol de categorías ------------------
def get_all_category_paths(depth: int = DEPTH) -> List[Tuple[str, str]]:
    url = TREE.format(depth=depth)
    logging.info("📂 Descargando árbol de categorías: %s", url)
    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logging.error("No se pudo obtener el árbol de categorías: %s", exc)
        return []

    out: List[Tuple[str, str]] = []

    def traverse(node: Dict):
        full_url = node.get("url") or ""
        if full_url:
            parsed = urlparse(full_url)
            path = parsed.path.lstrip("/")
            segs = [s for s in path.split("/") if s]
            if segs:
                map_str = ",".join(["c"] * len(segs))
                out.append((path, map_str))
        for ch in node.get("children", []) or []:
            traverse(ch)

    for root in data:
        traverse(root)

    # dedup manteniendo orden
    unique = list(dict.fromkeys(out))
    logging.info("✅ %d categorías descubiertas", len(unique))
    return unique

# ------------------ Descarga por categoría ------------------
def fetch_category(cat_path: str, map_str: str, step: int = STEP) -> List[Dict]:
    rows: List[Dict] = []
    seen = set()
    offset = 0
    empty_streak = 0

    while True:
        if offset >= MAX_OFFSET_HARD:
            logging.warning("Tope de offset alcanzado (%s) en %s", MAX_OFFSET_HARD, cat_path)
            break

        url = f"{BASE}/{cat_path}?_from={offset}&_to={offset + step - 1}&map={map_str}"
        try:
            r = SESSION.get(url, timeout=30)
        except Exception as exc:
            logging.error("Fallo de request en %s: %s", url, exc)
            break

        if r.status_code not in (200, 206):
            logging.warning("HTTP %s en %s; reintentando…", r.status_code, url)
            time.sleep(0.8)
            r = SESSION.get(url, timeout=30)
            if r.status_code not in (200, 206):
                logging.error("HTTP %s persistente en %s; corto categoría", r.status_code, url)
                break

        try:
            data = r.json()
        except Exception:
            logging.error("Respuesta no-JSON en %s", url)
            break

        if not data:
            empty_streak += 1
            if empty_streak >= 2:
                # dos vacías seguidas → cortar
                break
            # una vacía puede ser corte natural, intentamos una más
            offset += step
            time.sleep(SLEEP_PAGE)
            continue

        empty_streak = 0
        added = 0
        for p in data:
            pid = p.get("productId")
            if pid and pid not in seen:
                seen.add(pid)
                rows.append(parse_product(p))
                added += 1

        offset += step
        time.sleep(SLEEP_PAGE)

        # Si la página devolvió menos de 'step', probablemente no hay más
        if added < step:
            # confirmación suave: intentamos una página más
            continue

    return rows

# ------------------ Guardado ------------------
def save_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)
    logging.info("💾 CSV guardado: %s (%d filas)", path, len(df))

def save_xlsx(df: pd.DataFrame, path: str):
    # limpieza strings
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].map(sanitize_for_excel)

    # formateo precios
    for col in PRICE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        wb = writer.book
        ws = writer.sheets["Data"]
        money_fmt = wb.add_format({"num_format": "#,##0.00"})
        # ancho y formato de columnas de precio
        cols = list(df.columns)
        for c in PRICE_COLS:
            if c in cols:
                idx = cols.index(c)
                ws.set_column(idx, idx, 12, money_fmt)
        # ajuste simple de anchos (ligero)
        for i, name in enumerate(cols):
            if df[name].dtype == object:
                ws.set_column(i, i, min(60, max(12, int(df[name].astype(str).str.len().quantile(0.9)))))

    logging.info("💾 XLSX guardado: %s (%d filas)", path, len(df))

# ------------------ Orquestación ------------------
def fetch_all_categories(depth: int = DEPTH) -> pd.DataFrame:
    cats = get_all_category_paths(depth)
    all_rows: List[Dict] = []

    if not cats:
        logging.warning("No se encontraron categorías. Nada para hacer.")
        return pd.DataFrame()

    logging.info("🚀 Iniciando scraping paralelo de %d categorías (max_workers=%d)", len(cats), MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_category, cat, mp): (cat, mp) for (cat, mp) in cats}
        for fut in as_completed(futures):
            cat, mp = futures[fut]
            try:
                rows = fut.result()
                logging.info("📦 %s → %d items", cat, len(rows))
                all_rows.extend(rows)
            except Exception as e:
                logging.error("Error en categoría %s: %s", cat, e)

    if not all_rows:
        logging.warning("No se obtuvieron filas.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["productId"])
    logging.info("✅ Total productos únicos: %d", len(df))
    return df

# ------------------ Main ------------------
if __name__ == "__main__":
    t0 = time.time()
    df = fetch_all_categories(depth=DEPTH)

    if not df.empty:
        if SAVE_CSV:
            save_csv(df, SAVE_CSV)
        if SAVE_XLSX:
            save_xlsx(df, SAVE_XLSX)

    logging.info("⏱️ Tiempo total: %.1f s", time.time() - t0)
