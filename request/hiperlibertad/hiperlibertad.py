#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time, re, sys
from typing import List, Dict, Tuple, Any, Optional
from urllib.parse import urlparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
from html import unescape
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from warnings import filterwarnings
from bs4 import MarkupResemblesLocatorWarning

# ===================== Config =====================
BASE = "https://www.hiperlibertad.com.ar"

# Profundidad del árbol (subí a 5-10 si ves que faltan hojas profundas)
TREE_DEPTH = 5
TREE_URL = f"{BASE}/api/catalog_system/pub/category/tree/{TREE_DEPTH}"

SEARCH = f"{BASE}/api/catalog_system/pub/products/search"

STEP = 50                  # VTEX: _from/_to (0-49, 50-99, ...)
TIMEOUT = 25
SLEEP_OK = 0.25
MAX_EMPTY = 2              # corta si hay 2 páginas seguidas vacías
RETRIES = 3                # reintentos HTTP
OUT_XLSX = "hiperlibertad_all_products.xlsx"

# Canal de ventas (si devuelve vacío, probá 2 o 3)
SALES_CHANNELS = ["1"]     # puedes poner ["1","2"] para probar ambos

# Prefijos a incluir (vacío = TODAS las familias del árbol)
INCLUDE_PREFIXES: List[str] = []  # p.ej. ["tecnologia","almacen"]

# Umbral para considerar “poco” resultado por ruta y forzar fallback por ID:
FALLBACK_THRESHOLD = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# === Excel: quitar caracteres de control ilegales ===
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

# Silenciar warning de BeautifulSoup para strings que "parecen URL"
filterwarnings("ignore", category=MarkupResemblesLocatorWarning)


# ===================== Utils =====================
def clean_text(v):
    if v is None:
        return ""
    if not isinstance(v, str):
        return v
    # Si parece una URL, no lo pases por BS
    if v.startswith("http://") or v.startswith("https://"):
        return v
    try:
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", v)


def s_requests() -> requests.Session:
    s = requests.Session()
    r = Retry(total=RETRIES, backoff_factor=0.5,
              status_forcelist=(429, 500, 502, 503, 504),
              allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    return s


def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default


# ===================== Categorías =====================
def load_tree(session: requests.Session) -> List[Dict[str, Any]]:
    r = session.get(TREE_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def url_to_path(url: str) -> List[str]:
    p = urlparse(url)
    path = p.path.lstrip("/")  # ej: 'tecnologia/audio/auriculares'
    return [seg for seg in path.split("/") if seg]


def flatten_leaves(tree: List[Dict[str, Any]]) -> List[Tuple[List[str], int, str]]:
    """
    Devuelve hojas como (path_segments, category_id, category_name).
    """
    leaves: List[Tuple[List[str], int, str]] = []

    def dfs(node: Dict[str, Any]):
        path = url_to_path(node.get("url", ""))
        children = node.get("children") or []
        if not children:
            if path:
                leaves.append((path, int(node.get("id")), node.get("name", "")))
            return
        for ch in children:
            dfs(ch)

    for root in tree:
        dfs(root)
    return leaves


def build_map_for_path(path_segments: List[str]) -> str:
    return ",".join(["c"] * len(path_segments))


# ===================== Parsing producto =====================
def parse_price_fields(item: Dict[str, Any]):
    sellers = item.get("sellers") or []
    s0 = first(sellers, {}) or {}
    co = (s0.get("commertialOffer") or {}) if isinstance(s0, dict) else {}
    list_price = float(co.get("ListPrice") or 0.0)
    price = float(co.get("Price") or 0.0)
    stock = int(co.get("AvailableQuantity") or 0)
    return list_price, price, stock


def extract_ean(item: Dict[str, Any]) -> str:
    ean = (item.get("ean") or "").strip()
    if ean:
        return ean
    for ref in item.get("referenceId") or []:
        key = (ref.get("Key") or ref.get("key") or "").upper()
        if key in ("EAN", "EAN13", "COD_EAN", "COD.EAN"):
            val = (ref.get("Value") or ref.get("value") or "").strip()
            if val:
                return val
    return ""


def extract_codigo_interno(p: Dict[str, Any], item: Dict[str, Any]) -> str:
    cod = str(p.get("productReference") or "").strip()
    if not cod:
        cod = str(item.get("itemId") or "").strip()
    if not cod:
        ref = first(item.get("referenceId") or [])
        cod = (ref.get("Value") or ref.get("value") or "").strip() if isinstance(ref, dict) else ""
    return cod


def product_url(p: Dict[str, Any]) -> str:
    link = (p.get("linkText") or "").strip()
    return f"{BASE}/{link}/p" if link else ""


def parse_records(p: Dict[str, Any], fullpath: List[str]) -> List[Dict[str, Any]]:
    rows = []
    items = p.get("items") or []
    for item in items:
        ean = extract_ean(item)
        list_price, price, stock = parse_price_fields(item)
        tipo_oferta = "Oferta" if price and list_price and price < list_price else ""
        rows.append({
            "EAN": ean,
            "Código Interno": extract_codigo_interno(p, item),
            "Nombre Producto": clean_text(p.get("productName")),
            "Categoría": fullpath[0] if len(fullpath) >= 1 else "",
            "Subcategoría": fullpath[1] if len(fullpath) >= 2 else "",
            "Ruta Categoría": "/".join(fullpath),
            "Marca": clean_text(p.get("brand")),
            "Fabricante": clean_text(p.get("Manufacturer") or p.get("brand")),
            "Precio de Lista": list_price,
            "Precio de Oferta": price,
            "Stock": stock,
            "tipo de Oferta": tipo_oferta,
            "URL": product_url(p)
        })
    return rows


# ===================== Fetching =====================
def fetch_category_by_path(session: requests.Session, path_segments: List[str], sc: str) -> List[Dict[str, Any]]:
    map_str = build_map_for_path(path_segments)
    path = "/".join(path_segments)
    rows: List[Dict[str, Any]] = []
    offset = 0
    empty_in_a_row = 0

    while True:
        params = {
            "_from": offset,
            "_to": offset + STEP - 1,
            "map": map_str,
            "sc": sc
        }
        url = f"{SEARCH}/{path}"
        r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 429:
            time.sleep(0.8)
            continue
        r.raise_for_status()
        try:
            data = r.json()
        except Exception:
            data = []
        if not data:
            empty_in_a_row += 1
            if empty_in_a_row >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue
        empty_in_a_row = 0
        for p in data:
            pr_rows = parse_records(p, path_segments)
            rows.extend(pr_rows)
            if pr_rows:
                s = pr_rows[0]
                print(f"[PATH sc={sc}] {s['Nombre Producto']} | EAN:{s['EAN']} | ${s['Precio de Oferta']} | {s['URL']}")
        offset += STEP
        time.sleep(SLEEP_OK)
    return rows


def fetch_category_by_id(session: requests.Session, category_id: int, fullpath: List[str], sc: str) -> List[Dict[str, Any]]:
    """Fallback: usa fq=C:<id>"""
    rows: List[Dict[str, Any]] = []
    offset = 0
    empty_in_a_row = 0

    while True:
        params = {
            "_from": offset,
            "_to": offset + STEP - 1,
            "fq": f"C:{category_id}",
            "sc": sc
        }
        r = session.get(SEARCH, headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 429:
            time.sleep(0.8)
            continue
        r.raise_for_status()
        try:
            data = r.json()
        except Exception:
            data = []
        if not data:
            empty_in_a_row += 1
            if empty_in_a_row >= MAX_EMPTY:
                break
            offset += STEP
            time.sleep(SLEEP_OK)
            continue
        empty_in_a_row = 0
        for p in data:
            pr_rows = parse_records(p, fullpath)
            rows.extend(pr_rows)
            if pr_rows:
                s = pr_rows[0]
                print(f"[CID  sc={sc}] {s['Nombre Producto']} | EAN:{s['EAN']} | ${s['Precio de Oferta']} | {s['URL']}")
        offset += STEP
        time.sleep(SLEEP_OK)
    return rows


def fetch_category(session: requests.Session, path_segments: List[str], category_id: int) -> List[Dict[str, Any]]:
    """
    Intenta por ruta+map y, si hay pocos resultados, hace fallback por ID.
    Además prueba múltiples sales channels si se configuraron.
    """
    best_rows: List[Dict[str, Any]] = []
    for sc in SALES_CHANNELS:
        # 1) Por ruta
        rows_path = fetch_category_by_path(session, path_segments, sc)
        if len(rows_path) > len(best_rows):
            best_rows = rows_path

        # 2) Fallback por ID si la ruta arrojó pocos
        if len(rows_path) < FALLBACK_THRESHOLD:
            rows_id = fetch_category_by_id(session, category_id, path_segments, sc)
            if len(rows_id) > len(best_rows):
                best_rows = rows_id

    return best_rows


# ===================== Main =====================
def main():
    session = s_requests()
    print(f"Descargando árbol de categorías (depth={TREE_DEPTH})…")
    tree = load_tree(session)

    leaves = flatten_leaves(tree)

    # Filtrado por prefijos (si se definieron); si está vacío, se procesan TODAS
    if INCLUDE_PREFIXES:
        prefset = {s.lower() for s in INCLUDE_PREFIXES}
        leaves = [(p, cid, name) for (p, cid, name) in leaves if p and p[0].lower() in prefset]
    else:
        leaves = [(p, cid, name) for (p, cid, name) in leaves if p]

    print(f"Se detectaron {len(leaves)} hojas en el árbol a procesar:")
    for p, cid, name in leaves:
        print(" -", "/".join(p), f"(id={cid}, name={name})")

    if not leaves:
        print("No se hallaron hojas. ¿Endpoint o depth correcto?")
        sys.exit(1)

    all_rows: List[Dict[str, Any]] = []

    for idx, (path, cid, name) in enumerate(leaves, 1):
        print(f"\n[{idx}/{len(leaves)}] --- Categoría hoja: /{'/'.join(path)} (id={cid}, name={name}) ---")
        cat_rows = fetch_category(session, path, cid)
        print(f"   → {len(cat_rows)} filas")
        all_rows.extend(cat_rows)

    if not all_rows:
        print("No se obtuvieron productos. Considera cambiar SALES_CHANNELS o TREE_DEPTH.")
        return

    df = pd.DataFrame(all_rows)

    # Orden y existencia de columnas
    cols = ["EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Ruta Categoría",
            "Marca","Fabricante","Precio de Lista","Precio de Oferta","tipo de Oferta","Stock","URL"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]

    # Limpieza de texto
    text_cols = ["Nombre Producto","Categoría","Subcategoría","Ruta Categoría","Marca","Fabricante","tipo de Oferta","URL"]
    for c in text_cols:
        df[c] = df[c].map(clean_text)

    # (Opcional) quitar duplicados exactos de SKU/EAN+URL por seguridad
    if "EAN" in df.columns and "URL" in df.columns:
        before = len(df)
        df.drop_duplicates(subset=["EAN","Código Interno","URL"], inplace=True)
        after = len(df)
        if after < before:
            print(f"Deduplicadas {before - after} filas.")

    # Guardar
    df.to_excel(OUT_XLSX, index=False)
    print(f"\n✅ Listo. Guardado en: {OUT_XLSX}")
    print(f"Total de filas: {len(df)}")


if __name__ == "__main__":
    main()
