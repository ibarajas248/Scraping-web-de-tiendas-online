#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kilbel Online (AR) — Scraper PLP -> PDP con rotación de proxy (DataImpulse).

Lógica de precios:
- Si hay oferta (div.precio.anterior.codigo): base=anterior, oferta=actual
- Si NO hay oferta: base=actual (span/div.precio.aux1), oferta=None
- "Precio por ..." se guarda aparte como precio_por_unidad (NO afecta precio_base)

Rotación de proxy:
- Usa DataImpulse como proxy principal.
- Si una página no carga / devuelve HTML inválido, rota “identidad” del proxy
  agregando un sufijo a username (muchos proxies residenciales soportan esto).
  Si tu plan no soporta username dinámico, igual reintenta con el mismo.
"""

import os
import re
import time
import random
import argparse
import threading
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE = "https://www.kilbelonline.com"
START_PATTERN = "/lacteos/n1_994/pag/{page}/"

# -------------------------
# Proxy DataImpulse (HTTP)
# -------------------------
PROXY_HOST = "gw.dataimpulse.com"
PROXY_PORT = 823
PROXY_USER = "2cf8063dbace06f69df4"
PROXY_PASS = "61425d26fb3c7287"

# Cuántas identidades distintas probar al “rotar” en caso de HTML inválido
PROXY_ID_POOL_SIZE = 30

# Thread-local: sesiones por thread y por proxy_url
_tls = threading.local()


# -------------------------
# Utils texto / precio
# -------------------------
def clean_text(s: str):
    if not s:
        return None
    s = s.replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s or None


def parse_price_ar(text: str):
    """
    Convierte:
      "$ 2.220,00" -> 2220.00
      "Precio por 1 Lt: $ 1.690,00" -> 1690.00
      "$ 790,00" -> 790.00
    """
    if not text:
        return None
    t = text.strip()
    t = re.sub(r"[^\d\.,]", "", t)
    if not t:
        return None
    t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None


# -------------------------
# Sesiones + proxy url builder
# -------------------------
def build_proxy_url(identity: str | None = None) -> str:
    """
    Construye la URL del proxy.

    Nota:
    - Algunos servicios permiten “rotar” cambiando el username (ej: user-session-xxx).
    - Si DataImpulse NO soporta esto en tu plan, igual funcionará como reintento.

    identity: string corta que cambia el username (ej "s1234")
    """
    user = PROXY_USER
    if identity:
        # intento de “sticky/rotación” por username; si no aplica, no rompe nada
        user = f"{PROXY_USER}-session-{identity}"

    return f"http://{user}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"


def make_session(proxy_url: str):
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    })

    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    s.proxies = {"http": proxy_url, "https": proxy_url}

    return s


def get_thread_session(proxy_url: str):
    """
    1 sesión por thread y por proxy_url (no mezclar conexiones entre identidades)
    """
    if not hasattr(_tls, "sessions"):
        _tls.sessions = {}

    if proxy_url not in _tls.sessions:
        _tls.sessions[proxy_url] = make_session(proxy_url)

    return _tls.sessions[proxy_url]


# -------------------------
# Validadores HTML “bueno”
# -------------------------
def validator_listing(html: str) -> bool:
    if not html:
        return False
    return ("/art_" in html) and ("producto" in html)


def validator_pdp(html: str) -> bool:
    if not html:
        return False
    if "titulo_producto" not in html:
        return False
    if ("precio aux1" in html) or ("COD." in html) or ("COD" in html):
        return True
    return False


# -------------------------
# GET con rotación de identidad proxy
# -------------------------
def get_html_with_proxy_rotation(url: str, timeout=25, validator=None, max_identities=None):
    """
    Intenta cargar url con DataImpulse.
    Si falla (HTTP>=400 / excepción) o validator(html)==False, rota identidad.
    """
    max_identities = max_identities if max_identities is not None else PROXY_ID_POOL_SIZE

    # genera identidades distintas
    # (incluye un primer intento sin identity para usar el username base)
    identities = [None] + [str(random.randint(100000, 999999)) for _ in range(max_identities - 1)]

    for ident in identities:
        proxy_url = build_proxy_url(ident)
        s = get_thread_session(proxy_url)

        try:
            r = s.get(url, timeout=timeout)
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}")

            html = r.text or ""
            if validator is not None and not validator(html):
                raise RuntimeError("HTML inválido (validator)")

            return html

        except Exception:
            time.sleep(0.35 + random.random() * 0.8)

    return None


# -------------------------
# Scrape listado
# -------------------------
def listing_page_url(page: int):
    return urljoin(BASE, START_PATTERN.format(page=page))


def extract_product_links_from_listing(html: str):
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")

    links = []
    for a in soup.select("div.producto.item a[href*='/art_']"):
        href = a.get("href")
        if href:
            links.append(urljoin(BASE, href))

    # dedupe preservando orden
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# -------------------------
# Scrape PDP
# -------------------------
def extract_image_url_from_pdp(soup: BeautifulSoup):
    og = soup.select_one("meta[property='og:image']")
    if og and og.get("content"):
        return og["content"].strip()

    img = soup.select_one("img#img_01, img[src*='cdn1.kilbelonline.com/web/images/productos']")
    if img and img.get("src"):
        return img["src"].strip()

    lens = soup.select_one(".zoomLens")
    if lens:
        style = lens.get("style", "") or ""
        m = re.search(r'background-image:\s*url\(["\']?([^"\')]+)', style)
        if m:
            return m.group(1).strip()

    return None


def extract_sku_from_pdp(soup: BeautifulSoup):
    el = soup.select_one("div.der.precio.semibold.aux3")
    if not el:
        return None
    txt = clean_text(el.get_text(" ", strip=True)) or ""
    m = re.search(r"(\d+)", txt)
    return m.group(1) if m else txt


def extract_precio_por_unidad(soup: BeautifulSoup):
    for el in soup.select("div.codigo.aux1"):
        txt = clean_text(el.get_text(" ", strip=True)) or ""
        if "precio por" in txt.lower() and "$" in txt:
            val = parse_price_ar(txt)
            if val is not None:
                return val
    return None


def extract_prices_from_pdp(soup: BeautifulSoup):
    """
    - Oferta: base=anterior, oferta=actual
    - No oferta: base=actual, oferta=None
    """
    anterior_el = soup.select_one("div.precio.anterior.codigo")
    actual_el = soup.select_one("span.precio.aux1, div.precio.aux1")

    anterior = parse_price_ar(anterior_el.get_text(" ", strip=True)) if anterior_el else None
    actual = parse_price_ar(actual_el.get_text(" ", strip=True)) if actual_el else None

    if anterior is not None:
        return anterior, actual

    if actual is not None:
        return actual, None

    return None, None


def scrape_pdp(url: str, listado_url: str):
    html = get_html_with_proxy_rotation(url, timeout=25, validator=validator_pdp)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    name_el = soup.select_one("h1.titulo_producto.principal")
    producto = clean_text(name_el.get_text(" ", strip=True)) if name_el else None

    sku = extract_sku_from_pdp(soup)
    imagen_url = extract_image_url_from_pdp(soup)
    precio_base, precio_oferta = extract_prices_from_pdp(soup)
    precio_por_unidad = extract_precio_por_unidad(soup)

    # protección extra por HTML “raro”
    if (precio_base is None and precio_oferta is None) and not producto:
        return None

    return {
        "sku": sku,
        "producto": producto,
        "url": url,
        "imagen_url": imagen_url,
        "precio_base": precio_base,
        "precio_oferta": precio_oferta,
        "precio_por_unidad": precio_por_unidad,
        "pagina_listado": listado_url,
    }


# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--max-pages", type=int, default=0, help="0 = sin límite (hasta página vacía)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--out", default="kilbel_lacteos.xlsx")
    ap.add_argument("--max-empty-pages", type=int, default=1,
                    help="Corta si encuentra N páginas seguidas sin productos (default 1).")
    ap.add_argument("--proxy-identities", type=int, default=PROXY_ID_POOL_SIZE,
                    help="Cantidad de identidades (rotaciones) a probar por URL cuando falla.")
    args = ap.parse_args()

    global PROXY_ID_POOL_SIZE
    PROXY_ID_POOL_SIZE = max(3, int(args.proxy_identities))

    print(f"[INFO] Proxy: {PROXY_HOST}:{PROXY_PORT} (DataImpulse)")
    print(f"[INFO] Rotación de identidades por URL: {PROXY_ID_POOL_SIZE}")

    all_rows = []
    page = args.start_page
    pages_done = 0
    empty_pages = 0

    while True:
        if args.max_pages and pages_done >= args.max_pages:
            break

        listado_url = listing_page_url(page)
        print(f"[LISTADO] {listado_url}")

        html = get_html_with_proxy_rotation(listado_url, timeout=25, validator=validator_listing,
                                            max_identities=PROXY_ID_POOL_SIZE)
        links = extract_product_links_from_listing(html)

        if not links:
            empty_pages += 1
            print(f"[WARN] Página {page} sin productos / html inválido. empty_pages={empty_pages}")
            if empty_pages >= args.max_empty_pages:
                print(f"[STOP] {empty_pages} páginas seguidas sin productos. Fin.")
                break
            page += 1
            pages_done += 1
            time.sleep(args.sleep + random.random() * 0.4)
            continue

        empty_pages = 0
        print(f"  -> productos encontrados: {len(links)}")

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(scrape_pdp, u, listado_url) for u in links]
            for fut in as_completed(futures):
                try:
                    row = fut.result()
                    if row:
                        all_rows.append(row)
                except Exception:
                    pass

        time.sleep(args.sleep + random.random() * 0.4)
        page += 1
        pages_done += 1

    if not all_rows:
        print("No se extrajo nada.")
        return

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)
    df = df.sort_values(["producto", "sku"], na_position="last").reset_index(drop=True)

    with pd.ExcelWriter(args.out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="kilbel_lacteos")

    print(f"\nOK -> {args.out}")
    print(f"Filas: {len(df)}")


if __name__ == "__main__":
    main()
