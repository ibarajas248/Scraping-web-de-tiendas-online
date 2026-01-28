#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Carrefour AR (VTEX) — Scraper de todas las categorías con salida en:
EAN, Código Interno, Nombre Producto, Categoría, Subcategoría, Marca,
Fabricante, Precio de Lista, Precio de Oferta, Tipo de Oferta, URL

→ Solo emite productos DISPONIBLES (IsAvailable verdadero, qty > 0 o tapón 99999 y precio > 0)

para consultarr un producto especifico en la api.... GET https://www.carrefour.com.ar/api/catalog_system/pub/products/search?q=7793913000139
"""

import re
import time
import logging
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup  # (no imprescindible, puedes quitarlo si no lo usas)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------ Config ------------------
BASE_API = "https://www.carrefour.com.ar/api/catalog_system/pub/products/search"
BASE_WEB = "https://www.carrefour.com.ar"
TREE = "https://www.carrefour.com.ar/api/catalog_system/pub/category/tree/{depth}"

#https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq=alternateIds_Ean:7793913000139

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

# ✅ NUEVO: archivo de “sin stock”
SAVE_SIN_STOCK_CSV = "productos_sin_stock.csv"

# --- Filtro de disponibilidad ---
REQUIRE_IS_AVAILABLE = True        # exige commertialOffer.IsAvailable == True
REQUIRE_QTY_POSITIVE = True        # además, exige AvailableQuantity > 0 (acepta 99999 como tapón)
REQUIRE_PRICE_POSITIVE = True      # y que el precio sea > 0

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
            # en muchas tiendas VTEX modernas viene "Name"; en otras, la key rara "<Name>k__BackingField"
            name = (dh[0].get("Name") or dh[0].get("\u003CName\u003Ek__BackingField") or "").strip()
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

# ------------------ Helpers booleanos (VTEX) ------------------
def parse_vtex_bool(v):
    """Interpreta True/False en VTEX aunque venga como string 'true'/'false' o 0/1.
    Retorna True/False o None si no se puede interpretar."""
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return None
    if isinstance(v, str):
        t = v.strip().lower()
        if t in ("true", "1", "t", "yes", "y", "si", "sí", "s"):
            return True
        if t in ("false", "0", "f", "no", "n"):
            return False
    return None

# ------------------ Filtro de disponibilidad ------------------
def pick_available_offer(items: List[dict]):
    """
    Devuelve (item, seller, commertialOffer) del primer seller disponible
    según las reglas configuradas arriba. Si no hay disponible, retorna (None, None, None).
    """
    for it in (items or []):
        for s in (it.get("sellers") or []):
            co = s.get("commertialOffer") or {}
            is_ok = True

            if REQUIRE_IS_AVAILABLE:
                av = parse_vtex_bool(co.get("IsAvailable"))
                # si VTEX manda "false" (string) bool("false")==True → por eso parseamos
                if av is False:
                    is_ok = False

            if REQUIRE_QTY_POSITIVE:
                qty = co.get("AvailableQuantity")
                try:
                    qty = int(qty)
                except Exception:
                    qty = 0
                # algunos VTEX usan 99999 como “tapón” → lo aceptamos
                is_ok = is_ok and (qty > 0 or qty == 99999)

            if REQUIRE_PRICE_POSITIVE:
                price = co.get("FullSellingPrice", co.get("Price", 0))
                try:
                    price = float(price or 0)
                except Exception:
                    price = 0.0
                is_ok = is_ok and (price > 0)

            if is_ok:
                return it, s, co
    return None, None, None

# ✅ NUEVO: tomar la primera offer aunque NO esté disponible (para “sin stock”)
def pick_first_offer(items: List[dict]):
    """
    Devuelve (item, seller, commertialOffer) del primer seller que tenga commertialOffer.
    Si no existe, retorna (None, None, None).
    """
    for it in (items or []):
        for s in (it.get("sellers") or []):
            co = s.get("commertialOffer") or {}
            # aunque venga vacío, lo devolvemos si existe la key
            return it, s, co
    return None, None, None

# ------------------ Parseo de producto (solo columnas pedidas) ------------------
def parse_product_min(p: Dict) -> Optional[Dict]:
    """
    Parser mínimo y robusto para VTEX (p = producto del endpoint /pub/products/search).
    Retorna el dict con las columnas finales requeridas o None si no hay disponibilidad.
    """
    # ---- helpers locales ----
    def first_val(v, default=None):
        if v is None:
            return default
        if isinstance(v, (list, tuple)):
            return v[0] if v else default
        return v

    def to_str(x):
        return "" if x is None else str(x).strip()

    def to_float(x, default=0.0):
        try:
            if isinstance(x, (int, float)):
                return float(x)
            s = str(x).strip()
            s = s.replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
            return float(s)
        except Exception:
            return float(default)

    def split_cat_path(path: str) -> (str, str):
        if not isinstance(path, str):
            return "", ""
        parts = [seg for seg in path.split("/") if seg]
        if not parts:
            return "", ""
        cat = parts[0]
        sub = parts[1] if len(parts) > 1 else ""
        return cat, sub

    def build_url(p: Dict, base_web: str) -> str:
        link_text = p.get("linkText")
        link = p.get("link") or ""
        if link_text:
            return f"{base_web.rstrip('/')}/{link_text}/p"
        if isinstance(link, str) and (link.startswith("http://") or link.startswith("https://")):
            return link
        if base_web and isinstance(link, str):
            return f"{base_web.rstrip('/')}/{link.lstrip('/')}"
        return to_str(link)

    # ---- elegir solo ofertas DISPONIBLES ----
    items = p.get("items") or []
    item0, seller0, offer = pick_available_offer(items)
    if not offer:
        # No hay disponibilidad → NO devolvemos nada
        return None

    # ---- Identificadores ----
    ean = (item0.get("ean") if item0 else None) or first_val(p.get("EAN"))
    if not ean and item0:
        for rid in (item0.get("referenceId") or []):
            k = (rid.get("Key") or "").upper()
            if k in ("EAN", "EAN13", "GTIN", "REFID"):
                ean = rid.get("Value")
                if ean:
                    break
    ean = to_str(ean)

    codigo_interno = to_str((item0 or {}).get("itemId") or p.get("productId"))

    # ---- Categoría / Subcategoría ----
    categories = p.get("categories") or []
    cat, sub = ("", "")
    if categories and isinstance(categories, list) and isinstance(categories[0], str):
        cat, sub = split_cat_path(categories[0])

    # ---- URL ----
    base_web = globals().get("BASE_WEB", "")
    url_prod = build_url(p, base_web)

    # ---- Precios ----
    list_price = offer.get("ListPrice")
    pwd = offer.get("PriceWithoutDiscount")
    price = offer.get("FullSellingPrice", offer.get("Price"))

    list_price = to_float(list_price) if list_price else (to_float(pwd) if pwd else to_float(price))
    price = to_float(price)

    # ---- Tipo de oferta ----
    tipo_oferta_str = ""
    try:
        tipo_oferta_str = tipo_de_oferta(offer, list_price, price)  # usa helper de arriba
    except NameError:
        if list_price > 0 and 0 <= price < list_price:
            off = (1 - (price / list_price)) * 100.0
            tipo_oferta_str = f"-{round(off)}%"

    brand = p.get("brand")
    manufacturer = p.get("manufacturer") or ""

    return {
        "EAN": ean,
        "Código Interno": codigo_interno,
        "Nombre Producto": to_str(p.get("productName")),
        "Categoría": to_str(cat),
        "Subcategoría": to_str(sub),
        "Marca": to_str(brand),
        "Fabricante": to_str(manufacturer),
        "Precio de Lista": list_price,
        "Precio de Oferta": price,
        "Tipo de Oferta": tipo_oferta_str,
        "URL": url_prod,
    }

# ✅ NUEVO: parse mínimo para SIN STOCK (solo si commertialOffer.IsAvailable == False)
def parse_product_min_sin_stock(p: Dict) -> Optional[Dict]:
    """
    Retorna el dict con columnas finales si encuentra una offer con IsAvailable == False.
    Si no puede determinarlo, retorna None.
    """
    def first_val(v, default=None):
        if v is None:
            return default
        if isinstance(v, (list, tuple)):
            return v[0] if v else default
        return v

    def to_str(x):
        return "" if x is None else str(x).strip()

    def to_float(x, default=0.0):
        try:
            if isinstance(x, (int, float)):
                return float(x)
            s = str(x).strip()
            s = s.replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
            return float(s)
        except Exception:
            return float(default)

    def split_cat_path(path: str) -> (str, str):
        if not isinstance(path, str):
            return "", ""
        parts = [seg for seg in path.split("/") if seg]
        if not parts:
            return "", ""
        cat = parts[0]
        sub = parts[1] if len(parts) > 1 else ""
        return cat, sub

    def build_url(p: Dict, base_web: str) -> str:
        link_text = p.get("linkText")
        link = p.get("link") or ""
        if link_text:
            return f"{base_web.rstrip('/')}/{link_text}/p"
        if isinstance(link, str) and (link.startswith("http://") or link.startswith("https://")):
            return link
        if base_web and isinstance(link, str):
            return f"{base_web.rstrip('/')}/{link.lstrip('/')}"
        return to_str(link)

    items = p.get("items") or []
    item0, seller0, offer = pick_first_offer(items)
    if offer is None:
        return None

    # condición pedida: IsAvailable false
    # (ojo: VTEX puede mandar None; pediste específicamente false)
    av = parse_vtex_bool(offer.get("IsAvailable"))
    if av is not False:
        return None

    # ---- Identificadores ----
    ean = (item0.get("ean") if item0 else None) or first_val(p.get("EAN"))
    if not ean and item0:
        for rid in (item0.get("referenceId") or []):
            k = (rid.get("Key") or "").upper()
            if k in ("EAN", "EAN13", "GTIN", "REFID"):
                ean = rid.get("Value")
                if ean:
                    break
    ean = to_str(ean)

    codigo_interno = to_str((item0 or {}).get("itemId") or p.get("productId"))

    # ---- Categoría / Subcategoría ----
    categories = p.get("categories") or []
    cat, sub = ("", "")
    if categories and isinstance(categories, list) and isinstance(categories[0], str):
        cat, sub = split_cat_path(categories[0])

    # ---- URL ----
    base_web = globals().get("BASE_WEB", "")
    url_prod = build_url(p, base_web)

    # ---- Precios (aunque no haya stock, vienen igual) ----
    list_price = offer.get("ListPrice")
    pwd = offer.get("PriceWithoutDiscount")
    price = offer.get("FullSellingPrice", offer.get("Price"))

    list_price = to_float(list_price) if list_price else (to_float(pwd) if pwd else to_float(price))
    price = to_float(price)

    # ---- Tipo de oferta ----
    try:
        tipo_oferta_str = tipo_de_oferta(offer, list_price, price)
    except Exception:
        tipo_oferta_str = "Descuento" if (price or 0) < (list_price or 0) else "Precio regular"

    brand = p.get("brand")
    manufacturer = p.get("manufacturer") or ""

    return {
        "EAN": ean,
        "Código Interno": codigo_interno,
        "Nombre Producto": to_str(p.get("productName")),
        "Categoría": to_str(cat),
        "Subcategoría": to_str(sub),
        "Marca": to_str(brand),
        "Fabricante": to_str(manufacturer),
        "Precio de Lista": list_price,
        "Precio de Oferta": price,
        "Tipo de Oferta": tipo_oferta_str,
        "URL": url_prod,
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
def fetch_category(cat_path: str, map_str: str, step: int = STEP) -> Tuple[List[Dict], List[Dict]]:
    rows: List[Dict] = []
    rows_sin_stock: List[Dict] = []  # ✅ NUEVO
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
        added_this_page = 0
        for p in data:
            pid = p.get("productId")
            if pid and pid in seen:
                continue

            prod = parse_product_min(p)
            if not prod:
                # ✅ NUEVO: si NO está disponible y IsAvailable == False → guardarlo en sin stock
                oos = parse_product_min_sin_stock(p)
                if oos:
                    rows_sin_stock.append(oos)
                    if pid:
                        seen.add(pid)
                continue

            seen.add(pid)
            rows.append(prod)
            added_this_page += 1

            # 👇 Mostrar en consola con ambos precios + URL
            nombre = prod.get("Nombre Producto", "??")
            ean = prod.get("EAN", "")
            precio_lista = prod.get("Precio de Lista", "N/A")
            precio_oferta = prod.get("Precio de Oferta", "N/A")
            urlp = prod.get("URL", "N/A")

            print(f"→ {ean} | {nombre} | Lista: ${precio_lista} | Oferta: ${precio_oferta} | URL: {urlp}")

        offset += step
        time.sleep(SLEEP_PAGE)

        # Si la página devolvió menos de 'step', probablemente no hay más
        if added_this_page < step:
            continue

    return rows, rows_sin_stock

# ------------------ Guardado ------------------
def save_csv(df: pd.DataFrame, path: str):
    #df.to_csv(path, index=False)
    logging.info("💾 CSV guardado: %s (%d filas)", path, len(df))

# ✅ NUEVO: guardado real para sin stock
def save_sin_stock_csv(df: pd.DataFrame, path: str):
    # aseguramos que EAN quede como texto
    if "EAN" in df.columns:
        df["EAN"] = df["EAN"].astype(str)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logging.info("📄 CSV SIN STOCK guardado: %s (%d filas)", path, len(df))

def save_xlsx(df: pd.DataFrame, path: str):
    # Limpieza de celdas "sucias" para Excel
    df = df.applymap(sanitize_for_excel)

    # 👉 Desactiva la conversión automática de URLs a hipervínculos
    with pd.ExcelWriter(
        path,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}}
    ) as writer:
        df.to_excel(writer, index=False, sheet_name="productos")  # <-- (si lo tenías comentado, déjalo como lo uses)
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

    logging.info("📗 XLSX guardado: %s (%d filas)", path, len(df))

# ------------------ Orquestación ------------------
def fetch_all_categories(depth: int = DEPTH) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cats = get_all_category_paths(depth)
    all_rows: List[Dict] = []
    all_rows_sin_stock: List[Dict] = []  # ✅ NUEVO

    if not cats:
        logging.warning("No se encontraron categorías. Nada para hacer.")
        return pd.DataFrame(columns=COLS_FINAL), pd.DataFrame(columns=COLS_FINAL)

    logging.info("🚀 Scraping paralelo de %d categorías (max_workers=%d)", len(cats), MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_category, cat, mp): (cat, mp) for (cat, mp) in cats}
        for fut in as_completed(futures):
            cat, mp = futures[fut]
            try:
                rows, rows_oos = fut.result()
                logging.info("📦 %s → %d items (disp) | %d items (sin stock)", cat, len(rows), len(rows_oos))
                all_rows.extend(rows)
                all_rows_sin_stock.extend(rows_oos)
            except Exception as e:
                logging.error("Error en categoría %s: %s", cat, e)

    # DF disponibles
    if all_rows:
        df = pd.DataFrame(all_rows)
        for c in COLS_FINAL:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[COLS_FINAL].drop_duplicates(keep="last")
        logging.info("✅ Total productos DISPONIBLES: %d", len(df))
    else:
        logging.warning("No se obtuvieron filas disponibles.")
        df = pd.DataFrame(columns=COLS_FINAL)

    # DF sin stock (IsAvailable == false)
    if all_rows_sin_stock:
        df_oos = pd.DataFrame(all_rows_sin_stock)
        for c in COLS_FINAL:
            if c not in df_oos.columns:
                df_oos[c] = pd.NA
        df_oos = df_oos[COLS_FINAL].drop_duplicates(keep="last")
        logging.info("🚫 Total productos SIN STOCK (IsAvailable=false): %d", len(df_oos))
    else:
        df_oos = pd.DataFrame(columns=COLS_FINAL)

    return df, df_oos

# ------------------ Main ------------------
if __name__ == "__main__":
    t0 = time.time()
    df, df_sin_stock = fetch_all_categories(depth=DEPTH)

    if not df.empty:
        if SAVE_CSV:
            save_csv(df, SAVE_CSV)
        if SAVE_XLSX:
            save_xlsx(df, SAVE_XLSX)

    # ✅ NUEVO: guardar productos sin stock (IsAvailable=false) en CSV aparte
    if df_sin_stock is not None and not df_sin_stock.empty:
        save_sin_stock_csv(df_sin_stock, SAVE_SIN_STOCK_CSV)
    else:
        logging.info("🚫 No se detectaron productos sin stock (IsAvailable=false) para guardar.")

    logging.info("⏱️ Tiempo total: %.1f s", time.time() - t0)
