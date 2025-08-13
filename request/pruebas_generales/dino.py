#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dino Online (Super Mami) – Scraper de TODO el catálogo del sitio "super"
-----------------------------------------------------------------------------
- Recorre el listado paginado de /super/categoria con Nrpp/No (Endeca/ATG)
- Descubre categorías automáticamente desde el home /super/home y /super/
- Extrae links de productos y visita cada ficha para levantar datos ricos
  (JSON-LD: gtin13/gtin, brand, offers.price, breadcrumbs)
- Genera un Excel: Listado_Dino_YYYYMMDD.xlsx con las columnas requeridas:
  [EAN, Código Interno, Nombre Producto, Categoría, Subcategoría, Marca,
   Fabricante, Precio de Lista, Precio de Oferta, Tipo de Oferta, URL]

Notas:
- El sitio suele exponer parámetros típicos de Endeca: Nrpp (items/página),
  No (offset), Ns (orden). Este script pagina hasta agotar resultados.
- Si una ficha no publica EAN/GTIN, se deja vacío y luego puedes mapearlo
  con tu tabla de conversión.
- Respeta TOS y robots.txt del sitio. Usa una velocidad razonable.

Requisitos:
  pip install requests beautifulsoup4 lxml pandas openpyxl tqdm

Uso:
  python dino_scraper_todos.py
"""

import re
import time
import json
import random
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============== Config ==============
BASE = "https://www.dinoonline.com.ar"
SITE = "super"                     # "super" (alimentos) / "electro" (electro-hogar)
LISTING_PATH = f"/{SITE}/categoria"
HOME_PATHS = [f"/{SITE}/home", f"/{SITE}/"]

# Paginación y límites
NRPP = 36                          # items por página (observado en Endeca)
MAX_PAGES = 20000                  # salvavidas

# Timeouts y reintentos
CONNECT_TIMEOUT = 12
READ_TIMEOUT = 90
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)
MAX_RETRIES = 3                    # reintentos propios de get_soup

# Pausas para no saturar el sitio
SLEEP_LIST = (0.2, 0.6)            # espera entre páginas (min, max)
SLEEP_ITEM = (0.15, 0.45)          # espera entre fichas

# Concurrencia al visitar fichas
CONCURRENT_WORKERS = 12            # sube/baja según tu ancho de banda/CPU

# Filtros típicos (observados en la navegación)
NR_FILTER = (
    "AND("
    "product.disponible:Disponible,"
    "product.language:español,"
    "product.priceListPair:salePrices_listPrices,"
    "OR(product.siteId:superSite)"
    ")"
)

# Orden por nombre asc y precio activo asc
NS_ORDER = "product.displayName|0||sku.activePrice|0"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "Connection": "close",   # 🔸 evita sockets colgados por keep-alive
}

# Heurística para detectar URLs de producto en los listados
PATTERN_PRODUCT_HREF = re.compile(
    r'/((?:super|electro))/[^\s"\' ]*(?:producto|prod|p)/[\w\-]+', re.I
)
SELECTOR_CARD_ANCHORS = "a[href]"

# Selectores comunes en ficha
SELECTOR_JSONLD = 'script[type="application/ld+json"]'
SELECTOR_BREAD = 'nav.breadcrumb, ol.breadcrumb, ul.breadcrumb'

OUT_XLSX = f"Listado_Dino_{datetime.now().strftime('%Y%m%d')}.xlsx"

# Evitar la raíz /super/categoria (muy pesada) para reducir timeouts
INCLUDE_ROOT = False

# ============== Helpers HTTP ==============

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET", "HEAD"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def get_soup(s: requests.Session, url: str, params: Optional[Dict[str, Any]] = None) -> BeautifulSoup:
    for i in range(MAX_RETRIES):
        try:
            r = s.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except requests.RequestException:
            if i == MAX_RETRIES - 1:
                raise
            time.sleep(0.8 * (i + 1))
    return BeautifulSoup("", "lxml")

# ============== Descubrimiento de categorías ==============

def discover_category_urls(s: requests.Session) -> List[str]:
    seen: set = set()
    urls: List[str] = []
    for hp in HOME_PATHS:
        soup = get_soup(s, urljoin(BASE, hp))
        for a in soup.select('a[href*="/categoria"]'):
            href = a.get('href', '')
            if not href:
                continue
            url = urljoin(BASE, href)
            # sólo del site actual
            if f"/{SITE}/categoria" not in url:
                continue
            if url not in seen:
                seen.add(url)
                urls.append(url)
    # opcional: raíz del catálogo
    root_all = urljoin(BASE, LISTING_PATH)
    if INCLUDE_ROOT and root_all not in seen:
        urls.insert(0, root_all)
    return urls

# ============== Paginación del listado ==============

def _parse_qs(url: str) -> Dict[str, str]:
    u = urlparse(url)
    return dict(parse_qsl(u.query, keep_blank_values=True))


def rebuild_url(base_url: str, params: Dict[str, Any]) -> str:
    u = urlparse(base_url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q.update({k: str(v) for k, v in params.items()})
    new_q = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))


def extract_product_links_from_listing(soup: BeautifulSoup) -> List[str]:
    links: List[str] = []
    for a in soup.select(SELECTOR_CARD_ANCHORS):
        href = a.get('href', '')
        if not href:
            continue
        if PATTERN_PRODUCT_HREF.search(href):
            links.append(href)
    # normaliza y de-duplica
    out, seen = [], set()
    for href in links:
        url = urljoin(BASE, href)
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def iterate_listing(s: requests.Session, base_cat_url: str) -> List[str]:
    qs = _parse_qs(base_cat_url)
    # aseguremos filtros/paginación predecibles
    params: Dict[str, Any] = {
        "Dy": qs.get("Dy", "1"),
        "Nr": qs.get("Nr", NR_FILTER),
        "Nrpp": qs.get("Nrpp", str(NRPP)),
        "Ns": qs.get("Ns", NS_ORDER),
    }
    # algunos listados llevan Ntt/Nty; quitarlos para no sesgar resultados
    for k in ["Ntt", "Nty"]:
        params.pop(k, None)

    offset = 0
    all_links: List[str] = []
    empty_pages = 0

    for page in range(MAX_PAGES):
        params_page = dict(params)
        params_page["No"] = str(offset)  # offset
        url = rebuild_url(base_cat_url, params_page)
        soup = get_soup(s, url)
        links = extract_product_links_from_listing(soup)
        if not links:
            empty_pages += 1
            if empty_pages >= 2:
                break
        else:
            empty_pages = 0
            # agrega sólo nuevos
            new_links = [u for u in links if u not in all_links]
            all_links.extend(new_links)
        print(f"📄 Página offset={offset} → {len(links)} links (acum: {len(all_links)})")
        # siguiente página
        offset += int(params["Nrpp"]) if isinstance(params["Nrpp"], int) else int(str(params["Nrpp"]))
        time.sleep(random.uniform(*SLEEP_LIST))
    return all_links

# ============== Parseo de ficha ==============

def _first_str(x):
    if isinstance(x, list) and x:
        return str(x[0])
    return str(x) if x is not None else ""


def parse_product_page(s: requests.Session, url: str) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "EAN": "",
        "Código Interno": "",
        "Nombre Producto": "",
        "Categoría": "",
        "Subcategoría": "",
        "Marca": "",
        "Fabricante": "",
        "Precio de Lista": "",
        "Precio de Oferta": "",
        "Tipo de Oferta": "",
        "URL": url,
    }
    try:
        soup = get_soup(s, url)
        # 1) JSON-LD
        for tag in soup.select(SELECTOR_JSONLD):
            txt = tag.string or tag.get_text(strip=True)
            if not txt:
                continue
            try:
                data = json.loads(txt)
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for it in items:
                if not isinstance(it, dict):
                    continue
                # Producto
                if it.get("@type") in ("Product", ["Product"]):
                    d["Nombre Producto"] = d["Nombre Producto"] or it.get("name", "")
                    brand = it.get("brand")
                    if isinstance(brand, dict):
                        d["Marca"] = d["Marca"] or brand.get("name", "")
                    else:
                        d["Marca"] = d["Marca"] or _first_str(brand)
                    # EAN/GTIN/SKU
                    for k in ("gtin13", "gtin", "gtin12", "gtin8"):
                        if it.get(k):
                            d["EAN"] = str(it[k]).strip()
                            break
                    d["Código Interno"] = d["Código Interno"] or _first_str(it.get("sku"))
                    # Precio
                    off = it.get("offers") or {}
                    if isinstance(off, dict):
                        d["Precio de Oferta"] = d["Precio de Oferta"] or _first_str(off.get("price"))
                        d["Precio de Lista"] = d["Precio de Lista"] or _first_str(off.get("highPrice") or off.get("listPrice"))
                # Migas
                if it.get("@type") == "BreadcrumbList":
                    li = it.get("itemListElement", [])
                    crumbs = [x.get("name", "") for x in li if isinstance(x, dict)]
                    if crumbs:
                        d["Categoría"] = d["Categoría"] or (crumbs[1] if len(crumbs) > 1 else "")
                        d["Subcategoría"] = d["Subcategoría"] or (crumbs[2] if len(crumbs) > 2 else "")
        # 2) Fallbacks visibles
        if not d["Nombre Producto"]:
            h1 = soup.select_one("h1, h1.product-name, h1[itemprop='name']")
            if h1:
                d["Nombre Producto"] = h1.get_text(strip=True)
        if not d["Marca"]:
            b = soup.find(string=re.compile(r"Marca", re.I))
            if b and getattr(b, 'parent', None):
                val = b.parent.find_next("td") or b.parent.find_next("span")
                if val:
                    d["Marca"] = val.get_text(strip=True)
        # 3) Precio visible (fallback)
        if not d["Precio de Oferta"]:
            price = soup.select_one(".price, .product-price, [itemprop='price']")
            if price:
                d["Precio de Oferta"] = re.sub(r"[^0-9.,]", "", price.get_text())
        # Tipo de oferta (heurística)
        promo = soup.find(string=re.compile(r"Llevando|%|2x1|3x2|ahorro", re.I))
        if promo:
            d["Tipo de Oferta"] = promo.strip()[:120]
    except Exception as e:
        d["Tipo de Oferta"] = f"ERROR: {e}"
    return d

# ============== Pipeline principal ==============

def main():
    s = build_session()

    print("🔎 Descubriendo categorías...")
    cats = discover_category_urls(s)
    print(f"→ {len(cats)} categorías detectadas (incluida raíz del catálogo)")

    all_product_links: List[str] = []
    for i, cat_url in enumerate(cats, 1):
        print(f"\n==> Listando categoría {i}/{len(cats)}: {cat_url}")
        try:
            links = iterate_listing(s, cat_url)
        except Exception as e:
            print(f"⚠️  Falló el listado {cat_url}: {e}. Sigo con la próxima.")
            continue
        print(f"   ↳ {len(links)} links en esta categoría")
        for u in links:
            if u not in all_product_links:
                all_product_links.append(u)

    print(f"\nTOTAL links únicos recopilados: {len(all_product_links)}")

    # = Visitar fichas =
    registros: List[Dict[str, Any]] = []

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as ex:
        futs = {ex.submit(parse_product_page, s, url): url for url in all_product_links}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Fichas"):
            try:
                d = fut.result()
            except Exception as e:
                d = {"EAN":"","Código Interno":"","Nombre Producto":"","Categoría":"","Subcategoría":"","Marca":"","Fabricante":"","Precio de Lista":"","Precio de Oferta":"","Tipo de Oferta":f"ERROR: {e}","URL":futs[fut]}
            registros.append(d)
            # imprime lo que va encontrando
            print(f"✅ {d['Nombre Producto'][:80]} | EAN={d['EAN']} | $Oferta={d['Precio de Oferta']} | {d['URL']}")
            time.sleep(random.uniform(*SLEEP_ITEM))

    # Normalización y export
    df = pd.DataFrame(registros)
    COLS = [
        "EAN", "Código Interno", "Nombre Producto", "Categoría", "Subcategoría",
        "Marca", "Fabricante", "Precio de Lista", "Precio de Oferta",
        "Tipo de Oferta", "URL"
    ]
    for c in COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[COLS]

    df.to_excel(OUT_XLSX, index=False)
    print(f"\n📦 Exportado: {OUT_XLSX} ({len(df)} filas)")


if __name__ == "__main__":
    main()
