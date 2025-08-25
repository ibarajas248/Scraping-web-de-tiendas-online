#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
17_Toledo Digital (VTEX) — Scraper catálogo completo a CSV/XLSX

Salida por fila (SKU):
EAN | Código Interno | Nombre Producto | Categoría | Subcategoría | Marca | Fabricante |
Precio de Lista | Precio de Oferta | Tipo de Oferta | URL

Notas:
- Sitio VTEX: se usa /api/catalog_system/pub/products/search con paginado _from/_to.
- “Código Interno”: tomamos el itemId (SKU). Si falta, cae al productId.
- “Fabricante”: VTEX suele devolver manufacturer a nivel producto; si no, queda vacío.
- “Tipo de Oferta”: nombre del teaser si existe; si no, “Precio Regular” u “Oferta” si Price < ListPrice.
- Si una tienda requiere “sales channel” (sc), modifique SC_DEFAULT.
- Si más adelante necesitás mapear EAN manualmente, dejé un hook (dict EAN_MAP) para completar.
"""

import time
import json
import logging
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------ Configuración ------------------
BASE_WEB = "https://www.toledodigital.com.ar"
API_TREE = f"{BASE_WEB}/api/catalog_system/pub/category/tree/{{depth}}"
API_SEARCH = f"{BASE_WEB}/api/catalog_system/pub/products/search"

DEPTH = 3              # niveles de árbol de categorías
STEP = 50              # VTEX pagina por rango _from/_to
SLEEP = 0.25           # pausa suave entre requests
TIMEOUT = 25
RETRIES = 3
SC_DEFAULT = 1         # sales channel típico; si la tienda no lo usa, no afecta
MAX_VACIAS = 2         # corta categoría tras N páginas vacías seguidas

OUT_CSV = "toledo_catalogo.csv"
OUT_XLSX = "toledo_catalogo.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# Si alguna vez necesitás forzar EAN por SKU/producto:
EAN_MAP: Dict[str, str] = {
    # "itemId_o_productId": "ean_corregido",
}

# ------------------ Utilidades HTTP ------------------
def build_session(retries: int = RETRIES, backoff: float = 0.5) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries, backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",)
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

# ------------------ Descubrimiento de categorías ------------------
def get_category_tree(session: requests.Session, depth: int = DEPTH) -> List[dict]:
    r = session.get(API_TREE.format(depth=depth), timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def flatten_categories(tree: List[dict], prefix_path: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    Devuelve lista de (path_str, map_str). En VTEX, map es 'c' repetido por nivel.
    Ej: ('almacen', 'c') ; ('almacen/bebidas', 'c,c')
    """
    out: List[Tuple[str, str]] = []
    for node in tree:
        slug = (node.get("url", "") or node.get("link", "")).strip("/").split("/")[-1]
        if not slug:
            # alternativa: node.get("Title"), pero normalmente 'url' trae el slug
            slug = node.get("name", "").strip().replace(" ", "-").lower()
        current_path = slug if not prefix_path else f"{prefix_path}/{slug}"
        depth = current_path.count("/") + 1
        map_str = ",".join(["c"] * depth)
        out.append((current_path, map_str))
        children = node.get("children") or node.get("Children") or []
        if children:
            out.extend(flatten_categories(children, current_path))
    return out

# ------------------ Parseo de productos ------------------
def safe_get(d: dict, key: str, default=""):
    v = d.get(key, default) if isinstance(d, dict) else default
    return "" if v is None else v

def first_or_empty(seq, key=None):
    if not seq:
        return ""
    return safe_get(seq[0], key, "") if key else seq[0]

def extract_ean(item: dict, product: dict) -> str:
    """
    Estrategia usual VTEX:
    - item['ean']
    - item['referenceId'][0]['Value'] (o 'value' o 'Id'=='EAN')
    - fallback por mapeo manual EAN_MAP con itemId/productId
    """
    ean = safe_get(item, "ean", "")
    if not ean:
        refs = item.get("referenceId") or item.get("ReferenceId") or []
        # busca explícitamente key EAN
        ean = ""
        for ref in refs:
            key = (ref.get("Key") or ref.get("key") or "").upper()
            val = ref.get("Value") or ref.get("value") or ""
            if key in ("EAN", "GTIN", "BARRAS") and val:
                ean = val
                break
        if not ean and refs:
            ean = first_or_empty(refs, "Value") or first_or_empty(refs, "value")
    if not ean:
        ean = EAN_MAP.get(item.get("itemId") or product.get("productId"), "")
    return str(ean).strip()

def extract_teaser(co: dict) -> str:
    """
    commertialOffer.Teasers: lista con promos. Devuelve el primer nombre si hay.
    """
    teasers = co.get("Teasers") or []
    if not teasers:
        return ""
    # Estructuras típicas: {"Name": "3x2", "Conditions": {...}, ...}
    name = safe_get(teasers[0], "Name", "") or safe_get(teasers[0], "name", "")
    return name

def make_url(product: dict) -> str:
    # VTEX suele armar /{linkText}/p
    link_text = product.get("linkText") or product.get("LinkText") or ""
    if link_text:
        return urljoin(BASE_WEB, f"/{link_text}/p")
    # fallback: product['link'] completo si viene
    return urljoin(BASE_WEB, product.get("link") or product.get("Link", "/"))

def pick_category_fields(product: dict) -> Tuple[str, str]:
    """
    Devolvemos Categoría y Subcategoría desde 'categories' (rutas completas).
    Ej categories = ["/Almacen/Bebidas/...", "/..."] -> tomamos la más profunda.
    """
    cats: List[str] = [c.strip("/") for c in (product.get("categories") or []) if isinstance(c, str)]
    if not cats:
        return "", ""
    deep = max(cats, key=lambda c: c.count("/"))
    parts = deep.split("/")
    # Ajusta a tu criterio (a veces el primer nivel es 'Inicio' o similar)
    categoria = parts[0] if parts else ""
    subcategoria = parts[1] if len(parts) > 1 else ""
    return categoria, subcategoria

def row_from_product(product: dict) -> List[dict]:
    """
    Produce una fila por SKU (item). Extrae precios del seller 1 (o el primero).
    """
    rows: List[dict] = []
    product_id = str(product.get("productId", "")).strip()
    product_name = product.get("productName") or product.get("ProductName") or ""
    brand = product.get("brand") or product.get("Brand") or ""
    manufacturer = product.get("manufacturer") or product.get("Manufacturer") or ""
    categoria, subcategoria = pick_category_fields(product)
    url = make_url(product)

    items: List[dict] = product.get("items") or product.get("Items") or []
    for it in items:
        item_id = str(it.get("itemId", "")).strip() or product_id
        ean = extract_ean(it, product)

        sellers = it.get("sellers") or it.get("Sellers") or []
        seller = sellers[0] if sellers else {}
        co = seller.get("commertialOffer") or seller.get("CommertialOffer") or {}
        list_price = co.get("ListPrice") or co.get("listPrice") or 0.0
        price = co.get("Price") or co.get("price") or 0.0
        teaser = extract_teaser(co)

        tipo_oferta = "Precio Regular"
        if teaser:
            tipo_oferta = teaser
        elif price and list_price and price < list_price:
            tipo_oferta = "Oferta"

        row = {
            "EAN": ean,
            "Código Interno": item_id,                 # SKU (fallback: productId)
            "Nombre Producto": product_name,
            "Categoría": categoria,
            "Subcategoría": subcategoria,
            "Marca": brand,
            "Fabricante": manufacturer,
            "Precio de Lista": list_price or "",
            "Precio de Oferta": price or "",
            "Tipo de Oferta": tipo_oferta,
            "URL": url,
        }

        # --- Atributos extra (dejados como comentario para futuras ampliaciones) ---
        # row["productId"] = product_id
        # row["brandId"] = product.get("brandId")
        # row["linkText"] = product.get("linkText")
        # row["categoriesIds"] = ",".join(product.get("categoriesIds", []))
        # row["specs"] = json.dumps(product.get("specificationGroups", []), ensure_ascii=False)
        # row["availableQty"] = co.get("AvailableQuantity")
        # row["installments"] = json.dumps(co.get("Installments", []), ensure_ascii=False)
        # row["sellers"] = json.dumps(sellers, ensure_ascii=False)
        # row["images"] = json.dumps(it.get("images", []), ensure_ascii=False)

        rows.append(row)
    return rows

# ------------------ Scraping por categoría ------------------
def fetch_category(session: requests.Session, path: str, map_str: str, sc: Optional[int] = SC_DEFAULT) -> List[dict]:
    """
    Itera páginas de una categoría (path='almacen/bebidas', map='c,c') hasta agotar resultados.
    """
    all_rows: List[dict] = []
    offset = 0
    vacias = 0
    while True:
        params = {
            "_from": offset,
            "_to": offset + STEP - 1,
            "map": map_str,
        }
        if sc is not None:
            params["sc"] = sc

        url = f"{API_SEARCH}/{path}"
        r = session.get(url, params=params, timeout=TIMEOUT)
        try:
            data = r.json()
        except Exception:
            logging.warning(f"Respuesta no-JSON en {path} offset={offset}: {r.status_code}")
            break

        if not isinstance(data, list):
            logging.warning(f"Respuesta inesperada en {path} offset={offset}: tipo {type(data)}")
            break

        n = len(data)
        if n == 0:
            vacias += 1
            if vacias >= MAX_VACIAS:
                break
        else:
            vacias = 0

        for p in data:
            filas = row_from_product(p)
            for fila in filas:
                print(f"[{path}] EAN: {fila['EAN']} | SKU: {fila['Código Interno']} | {fila['Nombre Producto']}")
            all_rows.extend(filas)

        if n < STEP:
            break

        offset += STEP
        time.sleep(SLEEP)

    return all_rows


# ------------------ Principal ------------------
def run() -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ses = build_session()

    logging.info("Descubriendo categorías…")
    tree = get_category_tree(ses, DEPTH)
    cats = flatten_categories(tree)
    # Opcional: filtrar duplicados por path
    seen = set()
    cats = [c for c in cats if not (c[0] in seen or seen.add(c[0]))]
    logging.info(f"Categorías detectadas: {len(cats)}")

    rows: List[dict] = []
    for i, (path, map_str) in enumerate(cats, 1):
        logging.info(f"[{i}/{len(cats)}] {path} (map={map_str})")
        try:
            rows.extend(fetch_category(ses, path, map_str, sc=SC_DEFAULT))
        except requests.RequestException as e:
            logging.warning(f"⚠️ Error en {path}: {e}")
        time.sleep(SLEEP)

    if not rows:
        logging.warning("No se recolectó ningún dato.")
        return pd.DataFrame(columns=[
            "EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Marca",
            "Fabricante","Precio de Lista","Precio de Oferta","Tipo de Oferta","URL"
        ])

    df = pd.DataFrame(rows)

    # Normalizaciones ligeras
    df["EAN"] = df["EAN"].fillna("").astype(str).str.strip()
    df["Código Interno"] = df["Código Interno"].astype(str)
    df["Precio de Lista"] = pd.to_numeric(df["Precio de Lista"], errors="coerce")
    df["Precio de Oferta"] = pd.to_numeric(df["Precio de Oferta"], errors="coerce")

    # Deduplicar por SKU si fuese necesario
    df.drop_duplicates(subset=["Código Interno"], inplace=True)

    # Guardar
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    try:
        df.to_excel(OUT_XLSX, index=False)
    except Exception as e:
        logging.warning(f"XLSX no generado ({e}); queda CSV.")

    logging.info(f"Listo. Filas: {len(df)} | CSV: {OUT_CSV} | XLSX: {OUT_XLSX}")
    return df

if __name__ == "__main__":
    run()
