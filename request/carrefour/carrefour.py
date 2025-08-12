#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Carrefour AR (VTEX) — Scraper de todas las categorías con salida en:
EAN, Código Interno, Nombre Producto, Categoría, Subcategoría, Marca,
Fabricante, Precio de Lista, Precio de Oferta, Tipo de Oferta, URL
"""

import re
import time
import logging
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------ Config ------------------
BASE_API = "https://www.carrefour.com.ar/api/catalog_system/pub/products/search"
BASE_WEB = "https://www.carrefour.com.ar"
TREE = "https://www.carrefour.com.ar/api/catalog_system/pub/category/tree/{depth}"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

STEP = 50               # ítems por página VTEX
SLEEP_PAGE = 0.25       # pausa entre páginas dentro de una misma categoría
MAX_OFFSET_HARD = 10000 # salvavidas por si algo queda en loop
MAX_WORKERS = 6         # hilos (categorías en paralelo). 5–8 suele andar bien
DEPTH = 10              # profundidad del árbol de categorías
CLEAN_HTML = False      # (dejado, pero NO lo usamos en la salida)
SAVE_CSV = None         # si quieres CSV, pon un path. Ej: "carrefour.csv"
SAVE_XLSX = "carrefour.xlsx"

# Columnas finales y columnas de dinero para formatear
COLS_FINAL = [
    "EAN", "Código Interno", "Nombre Producto", "Categoría", "Subcategoría",
    "Marca", "Fabricante", "Precio de Lista", "Precio de Oferta", "Tipo de Oferta", "URL"
]
PRICE_COLS = ["Precio de Lista", "Precio de Oferta"]

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

def first(lst: Optional[Iterable], default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def split_cat(path: str) -> Tuple[str, str]:
    """Convierte '/categoria/subcategoria/...' → ('Categoria','Subcategoria')."""
    if not path:
        return "", ""
    parts = [p for p in path.strip("/").split("/") if p]
    if not parts:
        return "", ""
    fix = lambda s: s.replace("-", " ").strip().title()
    cat = fix(parts[0])
    sub = fix(parts[1]) if len(parts) > 1 else ""
    return cat, sub

def tipo_de_oferta(offer: dict, list_price: float, price: float) -> str:
    try:
        dh = offer.get("DiscountHighLight") or []
        if dh and isinstance(dh, list):
            name = (dh[0].get("Name") or "").strip()
            if name:
                return name
    except Exception:
        pass
    return "Descuento" if (price or 0) < (list_price or 0) else "Precio regular"

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

# ------------------ Parseo de producto (solo columnas pedidas) ------------------
def parse_product_min(p: Dict) -> Dict:
    items = p.get("items") or []
    item0 = items[0] if items else {}
    sellers = item0.get("sellers") or []
    seller0 = sellers[0] if sellers else {}
    offer = seller0.get("commertialOffer") or {}

    # Identificadores
    ean = item0.get("ean") or first(p.get("EAN"))
    codigo_interno = item0.get("itemId") or p.get("productId")

    # Categoría / Subcategoría: primer path
    categories = p.get("categories") or []
    cat, sub = ("", "")
    if categories and isinstance(categories, list) and isinstance(categories[0], str):
        cat, sub = split_cat(categories[0])

    # URL (preferir linkText; si no, usar p['link'] absoluto)
    link_text = p.get("linkText")
    if link_text:
        url_prod = f"{BASE_WEB}/{link_text}/p"
    else:
        url_prod = p.get("link") or ""

    list_price = offer.get("ListPrice") or 0
    price = offer.get("Price") or 0

    return {
        # === columnas finales ===
        "EAN": ean,
        "Código Interno": codigo_interno,
        "Nombre Producto": p.get("productName"),
        "Categoría": cat,
        "Subcategoría": sub,
        "Marca": p.get("brand"),
        "Fabricante": p.get("manufacturer") or "",
        "Precio de Lista": list_price,
        "Precio de Oferta": price,
        "Tipo de Oferta": tipo_de_oferta(offer, list_price, price),
        "URL": url_prod,

        # --- Campos extra (NO requeridos) → dejados aquí solo COMENTADOS ---
        # "productId": p.get("productId"),
        # "brandId": p.get("brandId"),
        # "availableQuantity": offer.get("AvailableQuantity"),
        # "isAvailable": offer.get("IsAvailable"),
        # "priceValidUntil": offer.get("PriceValidUntil"),
        # "imageUrl": first(item0.get("images") or [], {}).get("imageUrl") if item0.get("images") else None,
        # "description": BeautifulSoup(p.get("description") or "", "html.parser").get_text(" ", strip=True) if CLEAN_HTML else p.get("description"),
        # "refId": next((rid.get("Value") for rid in (item0.get("referenceId") or []) if rid.get("Key") == "RefId"), None),
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

        url = f"{BASE_API}/{cat_path}?_from={offset}&_to={offset + step - 1}&map={map_str}"
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
                break
            offset += step
            time.sleep(SLEEP_PAGE)
            continue

        empty_streak = 0
        added = 0
        for p in data:
            pid = p.get("productId")
            if pid and pid not in seen:
                seen.add(pid)
                rows.append(parse_product_min(p))
                added += 1

        offset += step
        time.sleep(SLEEP_PAGE)

        # Si la página devolvió menos de 'step', probablemente no hay más
        if added < step:
            continue

    return rows

# ------------------ Guardado ------------------
def save_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)
    logging.info("💾 CSV guardado: %s (%d filas)", path, len(df))

def save_xlsx(df: pd.DataFrame, path: str):
    # ... (tu limpieza previa)

    # 👉 Desactiva la conversión automática de URLs a hipervínculos
    with pd.ExcelWriter(
        path,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}}
    ) as writer:
        df.to_excel(writer, index=False, sheet_name="productos")
        wb = writer.book
        ws = writer.sheets["productos"]

        money = wb.add_format({"num_format": "0.00"})
        text  = wb.add_format({"num_format": "@"})

        col_idx = {name: i for i, name in enumerate(df.columns)}
        if "EAN" in col_idx: ws.set_column(col_idx["EAN"], col_idx["EAN"], 18, text)
        if "Nombre Producto" in col_idx: ws.set_column(col_idx["Nombre Producto"], col_idx["Nombre Producto"], 52)
        for c in ["Categoría","Subcategoría","Marca","Fabricante"]:
            if c in col_idx: ws.set_column(col_idx[c], col_idx[c], 20)
        for c in ["Precio de Lista","Precio de Oferta"]:
            if c in col_idx: ws.set_column(col_idx[c], col_idx[c], 14, money)
        if "URL" in col_idx: ws.set_column(col_idx["URL"], col_idx["URL"], 46)


# ------------------ Orquestación ------------------
def fetch_all_categories(depth: int = DEPTH) -> pd.DataFrame:
    cats = get_all_category_paths(depth)
    all_rows: List[Dict] = []

    if not cats:
        logging.warning("No se encontraron categorías. Nada para hacer.")
        return pd.DataFrame(columns=COLS_FINAL)

    logging.info("🚀 Scraping paralelo de %d categorías (max_workers=%d)", len(cats), MAX_WORKERS)
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
        return pd.DataFrame(columns=COLS_FINAL)

    df = pd.DataFrame(all_rows)
    # asegurar columnas y orden final
    for c in COLS_FINAL:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[COLS_FINAL].drop_duplicates(keep="last")
    logging.info("✅ Total productos: %d", len(df))
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
