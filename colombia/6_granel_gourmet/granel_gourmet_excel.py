#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Shopify (Granel Gourmet) → TODOS los productos → Excel
→ 1 fila por VARIANTE (gramaje), con columna grams.

Estrategia:
A) Intenta endpoints JSON públicos (products.json o collections/all/products.json).
B) Si no están habilitados, recorre /collections/all?page=N, extrae handles y
   consulta cada producto (products/<handle>.js o .json). Si falla, parsea JSON embebido.

Salida Excel (1 fila por variante):
available, body, compare_at_price_max, compare_at_price_min, handle, id, image,
price, price_max, price_min, tags, title, type, url, vendor, featured_image_url,
grams, variant_id, variant_title, variant_sku
"""

import json
import re
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://www.granelgourmet.co"

LOG_LEVEL = logging.INFO
SLEEP_BETWEEN_REQ = 0.15
MAX_PAGES_COLLECTION = 300

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)


# ---------------- HTTP session robusta ----------------
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        "Accept": "application/json,text/html,*/*",
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        "Referer": BASE + "/",
    })
    return s


def safe_get(session: requests.Session, url: str, timeout: int = 30) -> requests.Response:
    try:
        return session.get(url, timeout=timeout)
    except requests.RequestException as e:
        logging.warning(f"[GET ERROR] {url} -> {e}")
        raise


# ---------------- Normalización de precios ----------------
def _to_float_price(v: Any) -> Optional[float]:
    """
    Shopify puede devolver:
    - string "4650.00"
    - int 465000 (centavos) o 4650 (depende del endpoint)
    - None
    """
    if v is None:
        return None

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        s = s.replace(",", "")
        try:
            return float(s)
        except ValueError:
            # si viniera "4.650" (miles), intenta quitar puntos
            s2 = s.replace(".", "")
            try:
                return float(s2)
            except ValueError:
                return None

    if isinstance(v, (int, float)):
        # si parece centavos
        if v >= 100000:
            return round(float(v) / 100.0, 2)
        return float(v)

    return None


# ---------------- Gramaje (variante) ----------------
def extract_grams(text: str, option_name: Optional[str] = None) -> Optional[float]:
    """
    Extrae gramos desde textos tipo:
      "50 g", "100g", "150 gr", "0.5 kg", "1 kilo"
    Si no hay unidad pero option_name contiene "gram", asume gramos.
    """
    if not text:
        return None

    s = str(text).strip().lower()
    s = s.replace(",", ".")  # por si viene "0,5 kg"

    m = re.search(r"(\d+(?:\.\d+)?)\s*(kg|kilo|kilogramo|kilogramos|g|gr|gramo|gramos)\b", s)
    if m:
        val = float(m.group(1))
        unit = m.group(2)
        if unit in ("kg", "kilo", "kilogramo", "kilogramos"):
            return round(val * 1000.0, 3)
        return round(val, 3)

    # si no hay unidad pero el nombre de la opción sugiere gramaje
    if option_name and re.search(r"gram|peso|gramaje", option_name.lower()):
        m2 = re.search(r"(\d+(?:\.\d+)?)", s)
        if m2:
            return round(float(m2.group(1)), 3)

    return None


# ---------------- Filas por variante ----------------
def product_rows_from_any_json(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Devuelve LISTA de filas: 1 por variante.
    Soporta:
    - products.json: {id,title,handle,body_html,product_type,tags,variants,images,vendor,...}
    - product .js:  {id,title,handle,description,product_type,tags,variants,images,featured_image,...}
    - product .json: a veces {product:{...}}
    """
    if "product" in p and isinstance(p["product"], dict):
        p = p["product"]

    handle = p.get("handle") or ""
    url_rel = "/products/" + handle if handle else ""
    _full_url = urljoin(BASE, url_rel) if url_rel else None

    variants = p.get("variants") or []
    images = p.get("images") or []
    if isinstance(images, list) and images and isinstance(images[0], dict):
        images_urls = [im.get("src") for im in images if isinstance(im, dict)]
        image0 = images_urls[0] if images_urls else None
    else:
        image0 = images[0] if images else None

    featured = p.get("featured_image")
    featured_url = None
    if isinstance(featured, dict):
        featured_url = featured.get("url") or featured.get("src")
    elif isinstance(featured, str):
        featured_url = featured

    body_html = p.get("body_html")
    if body_html is None:
        body_html = p.get("description")

    tags = p.get("tags")
    if isinstance(tags, str):
        tags_list = [t.strip() for t in tags.split(",") if t.strip()]
    elif isinstance(tags, list):
        tags_list = tags
    else:
        tags_list = []

    product_base = {
        "body": body_html or "",
        "handle": handle,
        "id": p.get("id"),
        "image": image0,
        "tags": tags_list,
        "title": p.get("title") or "",
        "type": p.get("product_type") or p.get("type") or "",
        "url": url_rel,
        "vendor": p.get("vendor") or "",
        "featured_image_url": featured_url or image0,
    }

    # Si NO hay variants, devuelve 1 fila “producto”
    if not variants:
        row = dict(product_base)
        row.update({
            "available": bool(p.get("available", False)),
            "price": "0.00",
            "price_min": "0.00",
            "price_max": "0.00",
            "compare_at_price_min": "0.00",
            "compare_at_price_max": "0.00",
            "grams": None,
            "variant_id": None,
            "variant_title": None,
            "variant_sku": None,
        })
        return [row]

    # Map option index -> option name (si está disponible)
    # products.json suele traer options como lista de dicts: [{"name":"GRAMaje",...}]
    # product.js puede traer options como lista de strings (nombres de opción)
    option_names: List[Optional[str]] = [None, None, None]
    opts = p.get("options")
    if isinstance(opts, list) and opts:
        if isinstance(opts[0], dict):
            for i, od in enumerate(opts[:3]):
                option_names[i] = od.get("name")
        elif isinstance(opts[0], str):
            for i, name in enumerate(opts[:3]):
                option_names[i] = name

    rows: List[Dict[str, Any]] = []
    for v in variants:
        v_available = bool(v.get("available", False))

        v_price = _to_float_price(v.get("price"))
        v_compare = _to_float_price(v.get("compare_at_price"))

        # detecta gramaje desde option1/2/3 o title
        grams = None
        for idx, opt_key in enumerate(["option1", "option2", "option3"]):
            grams = extract_grams(v.get(opt_key), option_name=option_names[idx])
            if grams is not None:
                break
        if grams is None:
            grams = extract_grams(v.get("title") or "")

        row = dict(product_base)
        row.update({
            "available": v_available,
            # en formato "suggest": usamos min/max iguales a la variante
            "price": f"{(v_price or 0.0):.2f}" if v_price is not None else "0.00",
            "price_min": f"{(v_price or 0.0):.2f}" if v_price is not None else "0.00",
            "price_max": f"{(v_price or 0.0):.2f}" if v_price is not None else "0.00",
            "compare_at_price_min": f"{(v_compare or 0.0):.2f}" if v_compare else "0.00",
            "compare_at_price_max": f"{(v_compare or 0.0):.2f}" if v_compare else "0.00",
            "grams": grams,
            "variant_id": v.get("id"),
            "variant_title": v.get("title") or v.get("name"),
            "variant_sku": v.get("sku"),
        })
        rows.append(row)

    return rows


# ---------------- Métodos A: JSON masivo ----------------
def fetch_all_via_products_json(session: requests.Session) -> Optional[List[Dict[str, Any]]]:
    """
    /products.json?limit=250&since_id=
    Protección anti-loop: usa max(id) y corta si no avanza.
    """
    logging.info("Intentando método A1: /products.json (since_id) ...")
    all_products: List[Dict[str, Any]] = []
    since_id = 0
    seen_ids = set()
    stagnant = 0
    page_count = 0

    while True:
        url = f"{BASE}/products.json?limit=250&since_id={since_id}"
        r = safe_get(session, url)
        logging.info(f"[A1] GET {url} -> {r.status_code}")

        if r.status_code != 200:
            return None

        try:
            data = r.json()
        except Exception:
            return None

        products = data.get("products")
        if not isinstance(products, list):
            return None
        if not products:
            logging.info(f"[A1] Fin paginación. Total únicos={len(all_products)} Pages={page_count}")
            break

        page_count += 1

        new_count = 0
        ids = []
        for p in products:
            pid = p.get("id")
            if isinstance(pid, int):
                ids.append(pid)
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    all_products.append(p)
                    new_count += 1

        if not ids:
            logging.warning("[A1] Lote sin IDs enteros. Corto para evitar loop.")
            break

        new_since = max(ids)
        logging.info(f"[A1] Page={page_count} -> +{new_count} nuevos (Total={len(all_products)}), since_id {since_id}->{new_since}")

        if new_since <= since_id:
            stagnant += 1
            logging.warning(f"[A1] since_id NO avanza (stagnant={stagnant}). Corto si se repite.")
            if stagnant >= 2:
                break
        else:
            stagnant = 0
            since_id = new_since

        time.sleep(SLEEP_BETWEEN_REQ)

    return all_products


def fetch_all_via_collection_products_json(session: requests.Session) -> Optional[List[Dict[str, Any]]]:
    """
    /collections/all/products.json?page=N
    """
    logging.info("Intentando método A2: /collections/all/products.json?page=N ...")
    all_products: List[Dict[str, Any]] = []
    page = 1

    while True:
        url = f"{BASE}/collections/all/products.json?limit=250&page={page}"
        r = safe_get(session, url)
        logging.info(f"[A2] GET {url} -> {r.status_code}")

        if r.status_code != 200:
            return None

        try:
            data = r.json()
        except Exception:
            return None

        products = data.get("products")
        if not isinstance(products, list):
            return None
        if not products:
            logging.info(f"[A2] Fin paginación. Pages={page-1}, Total={len(all_products)}")
            break

        all_products.extend(products)
        logging.info(f"[A2] Page={page} -> +{len(products)} (Total={len(all_products)})")
        page += 1

        time.sleep(SLEEP_BETWEEN_REQ)

    return all_products


# ---------------- Método B: HTML → handles → JSON por producto ----------------
def extract_handles_from_collection_page(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    handles = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        m = re.search(r"/products/([^/?#]+)", href)
        if m:
            handles.add(m.group(1).strip())
    return sorted(handles)


def fetch_handles_from_all_collection_pages(session: requests.Session, max_pages: int = MAX_PAGES_COLLECTION) -> List[str]:
    logging.info("Método B: recorriendo /collections/all?page=N para extraer handles...")
    all_handles = set()
    empty_streak = 0

    for page in range(1, max_pages + 1):
        url = f"{BASE}/collections/all?page={page}"
        r = safe_get(session, url)
        logging.info(f"[B] GET {url} -> {r.status_code}")

        if r.status_code != 200:
            break

        handles = extract_handles_from_collection_page(r.text)
        logging.info(f"[B] page={page} -> handles encontrados: {len(handles)}")

        if not handles:
            empty_streak += 1
            if empty_streak >= 2:
                break
        else:
            empty_streak = 0
            all_handles.update(handles)

        time.sleep(SLEEP_BETWEEN_REQ)

    handles_sorted = sorted(all_handles)
    logging.info(f"[B] Total handles únicos: {len(handles_sorted)}")
    return handles_sorted


def try_fetch_product_json_by_handle(session: requests.Session, handle: str) -> Optional[Dict[str, Any]]:
    # 1) .js
    url_js = f"{BASE}/products/{handle}.js"
    r = safe_get(session, url_js)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            pass

    # 2) .json
    url_json = f"{BASE}/products/{handle}.json"
    r = safe_get(session, url_json)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            pass

    # 3) HTML y JSON embebido
    url_html = f"{BASE}/products/{handle}"
    r = safe_get(session, url_html)
    if r.status_code != 200:
        return None

    html = r.text

    m = re.search(r"ShopifyAnalytics\.meta\s*=\s*(\{.*?\});", html, flags=re.DOTALL)
    if m:
        blob = m.group(1)
        try:
            return json.loads(blob)
        except Exception:
            pass

    soup = BeautifulSoup(html, "lxml")
    for sc in soup.find_all("script"):
        if sc.get("type", "") == "application/json":
            txt = (sc.string or "").strip()
            if not txt:
                continue
            try:
                j = json.loads(txt)
                if isinstance(j, dict) and ("product" in j or "variants" in j or "handle" in j):
                    return j
            except Exception:
                continue

    return None


def fetch_all_by_handles(session: requests.Session) -> List[Dict[str, Any]]:
    handles = fetch_handles_from_all_collection_pages(session)
    total = len(handles)
    products_json: List[Dict[str, Any]] = []

    for i, handle in enumerate(handles, 1):
        j = try_fetch_product_json_by_handle(session, handle)
        if j:
            products_json.append(j)
            pj = j.get("product") if isinstance(j, dict) and "product" in j else j
            title = (pj.get("title") if isinstance(pj, dict) else None) or handle
            logging.info(f"[B][{i}/{total}] OK  {handle} | {str(title)[:80]}")
        else:
            logging.warning(f"[B][{i}/{total}] FAIL {handle} (sin JSON usable)")

        if i % 25 == 0:
            time.sleep(0.5)
        else:
            time.sleep(0.15)

    return products_json


# ---------------- MAIN ----------------
def main():
    session = build_session()

    products = fetch_all_via_products_json(session)
    method_used = "A1:/products.json"
    if products is None:
        products = fetch_all_via_collection_products_json(session)
        method_used = "A2:/collections/all/products.json"

    rows: List[Dict[str, Any]] = []

    if products is not None:
        logging.info(f"Usando método {method_used}. Expandiendo a filas por variante...")
        for idx, p in enumerate(products, 1):
            rws = product_rows_from_any_json(p)
            rows.extend(rws)
            if idx <= 3:
                logging.info(f"[PREVIEW] producto {idx}: {rws[0].get('title')} -> filas={len(rws)}")
    else:
        logging.info("Métodos A no disponibles. Pasando a método B (HTML -> handles -> producto)...")
        products_json = fetch_all_by_handles(session)
        logging.info("Expandiendo a filas por variante...")
        for pj in products_json:
            rows.extend(product_rows_from_any_json(pj))

    if not rows:
        raise SystemExit("No pude obtener productos (bloqueo/endpoint no disponible).")

    df = pd.DataFrame(rows)

    # normaliza tags (lista → string)
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(lambda x: "|".join(x) if isinstance(x, list) else (x or ""))

    cols = [
        "available", "grams",
        "body", "compare_at_price_max", "compare_at_price_min",
        "handle", "id", "variant_id", "variant_title", "variant_sku",
        "image", "price", "price_max", "price_min",
        "tags", "title", "type", "url", "vendor", "featured_image_url"
    ]
    df = df[[c for c in cols if c in df.columns]]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"granelgourmet_variantes_{ts}.xlsx"
    df.to_excel(out, index=False)

    logging.info(f"[OK] Filas (variantes): {len(df)}")
    logging.info(f"[OK] Archivo: {out}")

    print("\n=== PREVIEW (primeras 15 filas) ===")
    print(df.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
