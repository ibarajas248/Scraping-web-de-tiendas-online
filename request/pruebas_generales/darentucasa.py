#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper para el sitio "DAR en tu casa" (darentucasa.com.ar) con paginación.

- Extrae productos de Ofertas.asp o Articulos.asp
- Sigue paginación probando parámetros comunes (?pag=, ?pagina=, ?page=, ?p=)
- Soporta opcionalmente nl/xl (sucursal/sesión) en la query
- Exporta a XLSX

Ejemplos:
  python dar_scraper.py --out productos_dar.xlsx --source ofertas --max-pages 20
  python dar_scraper.py --out productos_dar.xlsx --source articulos --nl 01070201 --xl ABC123 --max-pages 30
"""

import re
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.darentucasa.com.ar"
OFFERS_PATH = "/Ofertas.asp"
ARTICLES_PATH = "/Articulos.asp"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

def new_session() -> requests.Session:
    """Crea una sesión HTTP con cabeceras adecuadas y cookies iniciales."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": f"{BASE_URL}/Login.asp",
        "X-Requested-With": "XMLHttpRequest",
    })
    retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    try:
        # siembra cookies/sesión
        session.get(f"{BASE_URL}/Login.asp", timeout=20)
    except requests.RequestException:
        pass
    return session

def tidy_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

_price_re = re.compile(
    r"([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})|[0-9]+(?:\.[0-9]{1,2})?)"
)

def parse_price(price_text: str) -> Optional[float]:
    if not price_text:
        return None
    m = _price_re.search(price_text.replace("\xa0", " "))
    if not m:
        return None
    num = m.group(1)
    num = num.replace(".", "").replace(" ", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None

def parse_product_list(html: str) -> List[Dict[str, Any]]:
    """Extrae lista de productos del HTML (Ofertas.asp / Articulos.asp)."""
    soup = BeautifulSoup(html, "html.parser")
    products: List[Dict[str, Any]] = []
    for li in soup.select("li.cuadProd"):
        plu: Optional[str] = None
        img_tag = li.select_one("div.FotoProd img")
        if img_tag:
            onclick = img_tag.get("onclick", "")
            match = re.search(r"'([0-9]+)'", onclick)
            if match:
                plu = match.group(1)

        titulo = None
        desc_tag = li.select_one("div.desc")
        if desc_tag:
            titulo = tidy_space(desc_tag.get_text(strip=True))

        precio_text = ""
        precio_val: Optional[float] = None
        price_div = li.select_one("div.precio div.izq")
        if price_div:
            precio_text = tidy_space(price_div.get_text(" ", strip=True))
            precio_val = parse_price(precio_text)

        oferta = li.select_one("div.OferProd") is not None

        img_url: Optional[str] = None
        if img_tag:
            src = img_tag.get("src")
            if src:
                img_url = src if src.startswith("http") else f"{BASE_URL}/{src.lstrip('/')}"

        products.append({
            "plu": plu,
            "titulo": titulo,
            "precio": precio_val,
            "precio_texto": precio_text,
            "oferta": oferta,
            "imagen": img_url,
            "url_detalle": None,  # el detalle suele requerir POST
        })
    return products

def add_or_replace_qs(url: str, extra_params: Dict[str, str]) -> str:
    """Agrega o reemplaza parámetros de query en una URL."""
    parsed = urlparse(url)
    q = parse_qs(parsed.query, keep_blank_values=True)
    for k, v in extra_params.items():
        q[k] = [str(v)]
    new_q = urlencode(q, doseq=True)
    return urlunparse(parsed._replace(query=new_q))

def fetch_once(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=40)
    r.raise_for_status()
    return r.text

def fetch_paginated(session: requests.Session, base_url: str, max_pages: int = 20) -> List[Dict[str, Any]]:
    """
    Intenta seguir paginación con nombres de parámetro comunes.
    Detiene cuando no encuentra productos nuevos o se llega a max_pages.
    """
    all_products: List[Dict[str, Any]] = []
    seen_keys: Set[str] = set()

    # Página 1 (tal cual)
    html = fetch_once(session, base_url)
    batch = parse_product_list(html)
    for p in batch:
        key = f"{p.get('plu')}|{p.get('titulo')}"
        if key not in seen_keys:
            seen_keys.add(key); all_products.append(p)

    # Heurística de paginación
    param_candidates = ["pag", "pagina", "page", "p"]
    for param in param_candidates:
        empty_streak = 0
        for page in range(2, max_pages + 1):
            url_p = add_or_replace_qs(base_url, {param: str(page)})
            try:
                html_p = fetch_once(session, url_p)
            except requests.RequestException:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue

            batch_p = parse_product_list(html_p)
            added = 0
            for p in batch_p:
                key = f"{p.get('plu')}|{p.get('titulo')}"
                if key not in seen_keys:
                    seen_keys.add(key); all_products.append(p); added += 1

            if added == 0:
                empty_streak += 1
                # dos páginas seguidas sin novedades -> cambiar de estrategia
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0

        # si este nombre de parámetro funcionó (sumó más de la inicial), perfecto
        if len(all_products) > len(batch):
            break

    return all_products

def build_path(base_path: str, nl: Optional[str], xl: Optional[str]) -> str:
    url = f"{BASE_URL}{base_path}"
    extras = {}
    if nl: extras["nl"] = nl
    if xl: extras["xl"] = xl
    if extras:
        url = add_or_replace_qs(url, extras)
    return url

def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper para DAR en tu casa → Excel")
    parser.add_argument("--out", default="Productos_DAR.xlsx", help="Ruta del archivo XLSX de salida")
    parser.add_argument("--source", choices=["ofertas", "articulos"], default="ofertas",
                        help="Fuente de productos: 'ofertas' (Ofertas.asp) o 'articulos' (Articulos.asp)")
    parser.add_argument("--max-pages", type=int, default=20, help="Máximo de páginas a intentar por estrategia")
    parser.add_argument("--nl", help="Parámetro de sucursal NL (opcional)")
    parser.add_argument("--xl", help="Parámetro de sesión XL (opcional)")
    args = parser.parse_args()

    session = new_session()
    base_path = OFFERS_PATH if args.source == "ofertas" else ARTICLES_PATH
    start_url = build_path(base_path, args.nl, args.xl)

    productos = fetch_paginated(session, start_url, max_pages=args.max_pages)

    if not productos:
        print("⚠️ No se extrajeron productos. Verifica ruta, cookies o parámetros nl/xl.")
        return

    df = pd.DataFrame(productos)
    cols = ["plu", "titulo", "precio", "precio_texto", "oferta", "imagen", "url_detalle"]
    df = df.reindex(columns=cols)
    df.drop_duplicates(subset=["plu", "titulo"], inplace=True)

    output = Path(args.out)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Productos")
    print(f"✅ Se exportaron {len(df)} productos a {output.resolve()}")

if __name__ == "__main__":
    main()
