#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re, time, json, random, datetime as dt
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup as BS
import pandas as pd

BASE = "https://atomoconviene.com/atomo-ecommerce/"
LISTING_PATH = "3-almacen?page={page}"

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

TIMEOUT = 25
RETRIES = 3
SLEEP_BETWEEN = (0.35, 0.65)     # rango aleatorio
SLEEP_BETWEEN_PAGES = (0.7, 1.2) # rango aleatorio

def jitter(a, b):
    time.sleep(random.uniform(a, b))

def get_soup(url: str, session: requests.Session, referer: Optional[str] = None,
             treat_400_as_none: bool = False) -> Optional[BS]:
    headers = dict(HEADERS_BASE)
    if referer:
        headers["Referer"] = referer

    last_err = None
    for i in range(RETRIES):
        try:
            r = session.get(url, headers=headers, timeout=TIMEOUT)
            if r.status_code == 404:
                return None
            if r.status_code == 400 and treat_400_as_none:
                # para páginas fuera de rango o protecciones suaves
                return None
            r.raise_for_status()
            return BS(r.text, "html.parser")
        except requests.HTTPError as e:
            last_err = e
            # backoff leve y reintento
            jitter(0.6 + i*0.4, 0.9 + i*0.5)
        except Exception as e:
            last_err = e
            jitter(0.6 + i*0.4, 0.9 + i*0.5)
    # si agotó reintentos
    if treat_400_as_none and isinstance(last_err, requests.HTTPError) and last_err.response is not None and last_err.response.status_code == 400:
        return None
    raise last_err

def normalize_price(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    t = re.sub(r"[^\d,\.]", "", text)
    t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None

def extract_ean_from_url(url: str) -> Optional[str]:
    m = re.search(r"-([0-9]{8,14})\.html?$", url)
    return m.group(1) if m else None

def find_last_page(soup: BS) -> int:
    """
    Detecta el último número de página mirando la paginación.
    Si no encuentra, asume 1.
    """
    last = 1
    # Prestashop suele tener .pagination o .page-list
    for sel in ["ul.pagination a", ".page-list a", "nav.pagination a"]:
        links = soup.select(sel)
        if links:
            for a in links:
                txt = a.get_text(strip=True)
                if txt.isdigit():
                    last = max(last, int(txt))
    return last

def find_product_cards(soup: BS) -> List[BS]:
    return soup.select("article.product-miniature.js-product-miniature")

def parse_listing_card(article: BS) -> Optional[str]:
    a = article.select_one("h2.product-title a, a.thumbnail.product-thumbnail")
    return a.get("href") if a else None

def parse_product_detail(url: str, soup: BS) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "URL": url,
        "EAN": None,
        "Código Interno": None,
        "Nombre Producto": None,
        "Categoría": None,
        "Subcategoría": None,
        "Marca": None,
        "Fabricante": "",
        "Precio de Lista": None,
        "Precio de Oferta": None,
        "Tipo de Oferta": "",
    }

    h1 = soup.select_one("h1.h1, h1[itemprop='name']")
    if h1: out["Nombre Producto"] = h1.get_text(strip=True)

    brand = soup.select_one("div.product-manufacturer span a, .product-manufacturer a")
    if brand: out["Marca"] = brand.get_text(strip=True)

    ref = soup.select_one("div.product-reference span")
    if ref: out["Código Interno"] = ref.get_text(strip=True)

    price_span = soup.select_one("div.product__product-price .current-price .price, span.current-price-display.price")
    visible_price = normalize_price(price_span.get_text()) if price_span else None

    data_product_json = None
    data_node = soup.select_one("div#product-details[data-product], .tab-pane#product-details[data-product]")
    if data_node:
        raw = data_node.get("data-product")
        if raw:
            try:
                data_product_json = json.loads(raw)
            except Exception:
                data_product_json = None

    if data_product_json:
        pa = data_product_json.get("price_amount")
        pwr = data_product_json.get("price_without_reduction")
        try: pa = float(pa) if pa is not None else None
        except: pa = normalize_price(str(pa)) if pa else None
        try: pwr = float(pwr) if pwr is not None else None
        except: pwr = normalize_price(str(pwr)) if pwr else None

        out["Precio de Oferta"] = pa if pa is not None else visible_price
        out["Precio de Lista"]  = pwr if pwr is not None else out["Precio de Oferta"]

        if out["Precio de Lista"] and out["Precio de Oferta"] and out["Precio de Oferta"] < out["Precio de Lista"]:
            out["Tipo de Oferta"] = "promo"

        out["Categoría"] = data_product_json.get("category_name")
        out["Subcategoría"] = ""
    else:
        out["Precio de Oferta"] = visible_price
        out["Precio de Lista"]  = visible_price

    ean = extract_ean_from_url(url)
    if ean: out["EAN"] = ean
    if not out["EAN"] and out.get("Nombre Producto"):
        m = re.search(r"(^|[^0-9])([0-9]{8,14})([^0-9]|$)", out["Nombre Producto"])
        if m: out["EAN"] = m.group(2)

    return out

def crawl_listing_and_products() -> List[Dict[str, Any]]:
    s = requests.Session()
    s.headers.update(HEADERS_BASE)

    # 1) Warmup: visitar la home para setear cookies base
    try:
        s.get(BASE, headers=HEADERS_BASE, timeout=15)
        jitter(0.3, 0.6)
    except Exception:
        pass

    all_rows: List[Dict[str, Any]] = []
    seen_urls = set()

    # 2) Descubrir último número de página
    first_url = urljoin(BASE, LISTING_PATH.format(page=1))
    soup1 = get_soup(first_url, s, referer=BASE, treat_400_as_none=True)
    if soup1 is None:
        print("No se pudo cargar la página 1 del listado.")
        return all_rows
    last_page = find_last_page(soup1)
    print(f"Última página detectada: {last_page}")

    # Procesar página 1
    pages_to_visit = [1] + list(range(2, last_page + 1))

    for page in pages_to_visit:
        list_url = urljoin(BASE, LISTING_PATH.format(page=page))
        print(f"Página {page}: {list_url}")
        soup = get_soup(list_url, s, referer=first_url, treat_400_as_none=True)
        if soup is None:
            print("  (400/404 o sin contenido) Fin del paginado.")
            break

        cards = find_product_cards(soup)
        if not cards:
            print("  (sin cards) Fin.")
            break

        product_urls: List[str] = []
        for art in cards:
            href = parse_listing_card(art)
            if not href:
                continue
            if not href.startswith("http"):
                href = urljoin(BASE, href)
            if href not in seen_urls:
                seen_urls.add(href)
                product_urls.append(href)

        print(f"  Productos en página: {len(product_urls)}")

        # Visitar cada producto con referer del listado
        for i, purl in enumerate(product_urls, 1):
            try:
                psoup = get_soup(purl, s, referer=list_url, treat_400_as_none=False)
                # algunos detalles pueden devolver 400 por heurística -> reintento con más pausa+referer
                if psoup is None:
                    # En teoría no llega aquí porque treat_400_as_none=False,
                    # pero dejamos el control por si en el futuro decidimos cambiarlo.
                    print(f"   [{i}/{len(product_urls)}] 400/404: {purl}")
                    continue

                row = parse_product_detail(purl, psoup)
                all_rows.append(row)
                print(f"   [{i}/{len(product_urls)}] OK: {row.get('Nombre Producto','(sin nombre)')}")
                jitter(*SLEEP_BETWEEN)
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else "?"
                print(f"   [{i}/{len(product_urls)}] HTTP {status} {purl} — reintentando suave...")
                jitter(1.0, 1.8)
                try:
                    psoup = get_soup(purl, s, referer=list_url, treat_400_as_none=True)
                    if psoup:
                        row = parse_product_detail(purl, psoup)
                        all_rows.append(row)
                        print(f"   [{i}/{len(product_urls)}] OK tras reintento")
                    else:
                        print(f"   [{i}/{len(product_urls)}] SKIP (400/404 persistente)")
                except Exception as e2:
                    print(f"   [{i}/{len(product_urls)}] ERROR definitivo: {e2}")
            except Exception as e:
                print(f"   [{i}/{len(product_urls)}] ERROR {purl}: {e}")

        jitter(*SLEEP_BETWEEN_PAGES)

    return all_rows

def export_excel(rows: List[Dict[str, Any]], filename: Optional[str] = None) -> str:
    cols = [
        "EAN","Código Interno","Nombre Producto","Categoría","Subcategoría",
        "Marca","Fabricante","Precio de Lista","Precio de Oferta","Tipo de Oferta","URL"
    ]
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    if not filename:
        today = dt.datetime.now().strftime("%Y%m%d")
        filename = f"Listado_Atomo_Almacen_{today}.xlsx"
    df.to_excel(filename, index=False)
    return filename

def main():
    rows = crawl_listing_and_products()
    print(f"Total filas: {len(rows)}")
    out = export_excel(rows)
    print(f"✔ Excel generado: {out}")

if __name__ == "__main__":
    main()
