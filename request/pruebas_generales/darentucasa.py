#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper para el sitio "DAR en tu casa" (darentucasa.com.ar).

Este script descarga el listado de productos que el sitio publica en sus páginas
de ofertas o destacados. Los artículos aparecen como elementos <li> con la clase
«cuadProd» e incluyen la foto del producto, el nombre, el precio y una marca
de oferta cuando corresponde.

Uso:
    python dar_scraper.py --out productos_dar.xlsx

Notas:
* Este scraper está pensado como ejemplo de uso. El portal "DAR en tu casa"
  suele requerir otros parámetros (como `xl` y `nl`) para categorías
  específicas. Aquí se toma por defecto la página de ofertas
  (`Ofertas.asp`), que publica una selección de artículos.
"""

import re
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional

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
        session.get(f"{BASE_URL}/Login.asp", timeout=20)  # siembra cookies
    except requests.RequestException:
        pass
    return session

def tidy_space(text: str) -> str:
    """Elimina espacios repetidos y recorta extremos."""
    return re.sub(r"\s+", " ", text or "").strip()

_price_re = re.compile(
    r"([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})|[0-9]+(?:\.[0-9]{1,2})?)"
)

def parse_price(price_text: str) -> Optional[float]:
    """Convierte una cadena de precio (formato argentino) a float."""
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
    """Extrae una lista de productos del HTML de Ofertas.asp o Articulos.asp."""
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
            "url_detalle": None,  # se deja nulo porque el detalle requiere un POST
        })
    return products

def fetch_products(session: requests.Session, path: str = OFFERS_PATH) -> List[Dict[str, Any]]:
    """Realiza la petición HTTP y retorna la lista de productos encontrados."""
    url = f"{BASE_URL}{path}"
    response = session.get(url, timeout=40)
    response.raise_for_status()
    return parse_product_list(response.text)

def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper para DAR en tu casa → Excel")
    parser.add_argument("--out", default="Productos_DAR.xlsx",
                        help="Ruta del archivo XLSX de salida")
    parser.add_argument("--source", choices=["ofertas", "articulos"], default="ofertas",
                        help="Fuente de productos: 'ofertas' para Ofertas.asp o 'articulos' para Articulos.asp")
    args = parser.parse_args()
    session = new_session()
    path = OFFERS_PATH if args.source == "ofertas" else ARTICLES_PATH
    productos = fetch_products(session, path=path)
    if not productos:
        print("⚠️ No se extrajeron productos. Verifica la ruta o los encabezados.")
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
