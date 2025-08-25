#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kilbel (kilbelonline.com) – Almacén n1_1
Scraper con impresión detallada de todo lo encontrado y exportación a Excel.

- Recorre /almacen/n1_1/pag/1/, /2/, /3/ ... hasta que no haya productos.
- De cada card del listado toma: CódigoInterno (prod_####), nombre, URL, imagen,
  SKU/record id de tienda (id_item_####), precios (lista/oferta), promo (XX% OFF),
  precio por Kg y precio sin impuestos si aparecen.
- Entra al detalle y completa: COD ####, nombre, precios, ruta de categorías,
  EAN (si aparece), precio por Kg y sin impuestos (si cambian), etc.
- Imprime TODO en consola y guarda Excel al final.
"""

import re
import time
import random
import argparse
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ------------------ Config ------------------
BASE = "https://www.kilbelonline.com"
LISTING_FMT = "/almacen/n1_1/pag/{page}/"

TIMEOUT = 25
RETRIES = 3
SLEEP_ITEM = (0.35, 0.8)    # espera entre productos (rand)
SLEEP_PAGE = (0.8, 1.6)     # espera entre páginas (rand)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
}

# ------------------ Utilidades ------------------
def clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())

def parse_price(s: Optional[str]) -> Optional[float]:
    """Convierte '$ 16.200,00' -> 16200.00"""
    if not s:
        return None
    s = re.sub(r"[^\d,.\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def get_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=RETRIES)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def get_soup(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    for intent in range(1, RETRIES + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return BeautifulSoup(r.text, "lxml")
            if r.status_code in (403, 404):
                print(f"    ⚠ {r.status_code} en {url} (paro de intentar)")
                return None
            print(f"    ⚠ HTTP {r.status_code} en {url}, reintento {intent}/{RETRIES}...")
            time.sleep(0.8)
        except requests.RequestException as e:
            print(f"    ⚠ Error de red en {url}: {e} (reintento {intent}/{RETRIES})")
            time.sleep(0.8)
    return None

# ------------------ Listado ------------------
def parse_listing_products(soup: BeautifulSoup) -> List[Dict]:
    productos = []
    cards = soup.select("div.producto[id^=prod_]")
    print(f"  • Cards encontradas en listado: {len(cards)}")

    for idx, prod in enumerate(cards, 1):
        pid_match = re.search(r"prod_(\d+)", prod.get("id", ""))
        codigo_interno = pid_match.group(1) if pid_match else None

        a = prod.select_one(".col1_listado .titulo02 a")
        nombre_list = clean(a.get_text()) if a else None
        href = a.get("href") if a else None
        url_detalle = urljoin(BASE, href) if href else None

        img = prod.select_one(".ant_imagen img")
        img_url = img.get("data-src") or (img.get("src") if img else None)

        sku_input = prod.select_one(f"input#id_item_{codigo_interno}") if codigo_interno else None
        sku_tienda = sku_input.get("value") if sku_input else None

        precio_lista_list = None
        precio_oferta_list = None
        el_prev = prod.select_one(".precio_complemento .precio.anterior")
        if el_prev:
            precio_lista_list = parse_price(el_prev.get_text())

        el_actual = prod.select_one(".precio_complemento .precio.aux1")
        if el_actual:
            precio_oferta_list = parse_price(el_actual.get_text())

        promo = None
        promo_span = prod.select_one("span.promocion")
        if promo_span:
            m = re.search(r"promocion(\d+)-off", " ".join(promo_span.get("class", [])))
            if m:
                promo = f"{m.group(1)}% OFF"

        precio_x_kg_list = None
        sin_imp_list = None
        for cod in prod.select(".precio_complemento .codigo"):
            txt = cod.get_text(" ", strip=True)
            if "Precio por" in txt:
                precio_x_kg_list = parse_price(txt)
            if "imp" in txt.lower():  # "Imp.Nac." / "impuestos"
                sin_imp_list = parse_price(txt)

        print(
            f"    [{idx:03}] LISTADO  "
            f"CODINT={codigo_interno}  "
            f"SKU_TIENDA={sku_tienda}  "
            f"NOMBRE='{nombre_list}'  "
            f"URL={url_detalle}"
        )
        print(
            f"           PRECIOS listado -> lista={precio_lista_list}  "
            f"oferta={precio_oferta_list}  promo={promo}  "
            f"porKg={precio_x_kg_list}  sinImp={sin_imp_list}"
        )

        productos.append({
            "CodigoInterno_list": codigo_interno,
            "NombreProducto_list": nombre_list,
            "URL": url_detalle,
            "Imagen": img_url,
            "SKU_Tienda": sku_tienda,
            "RecordId_Tienda": sku_tienda,
            "PrecioLista_list": precio_lista_list,
            "PrecioOferta_list": precio_oferta_list,
            "TipoOferta": promo,
            "PrecioPorKg_list": precio_x_kg_list,
            "PrecioSinImpuestos_list": sin_imp_list,
        })
    return productos

# ------------------ Detalle ------------------
def parse_detail(session: requests.Session, url: str) -> Dict:
    res = {
        "NombreProducto": None,
        "CodigoInterno_det": None,
        "PrecioLista": None,
        "PrecioOferta": None,
        "PrecioPorKg": None,
        "PrecioSinImpuestos": None,
        "Categoria": None,
        "Subcategoria": None,
        "Subsubcategoria": None,
        "EAN": None,
        "Marca": None,
        "Fabricante": None,
    }
    soup = get_soup(session, url)
    if not soup:
        print("           ⚠ No pude cargar el detalle.")
        return res

    h1 = soup.select_one("#detalle_producto h1.titulo_producto")
    if h1:
        res["NombreProducto"] = clean(h1.get_text())

    cod_box = soup.find(string=re.compile(r"COD\.\s*\d+"))
    if cod_box:
        m = re.search(r"COD\.\s*(\d+)", cod_box)
        if m:
            res["CodigoInterno_det"] = m.group(1)

    prev = soup.select_one("#detalle_producto .precio.anterior")
    if prev:
        res["PrecioLista"] = parse_price(prev.get_text())

    act = soup.select_one("#detalle_producto .precio.aux1")
    if act:
        res["PrecioOferta"] = parse_price(act.get_text())

    for div in soup.select("#detalle_producto .codigo"):
        t = div.get_text(" ", strip=True)
        if "Precio por" in t:
            res["PrecioPorKg"] = parse_price(t)
        if "sin impuestos" in t.lower() or "imp.nac" in t.lower():
            res["PrecioSinImpuestos"] = parse_price(t)

    # Ruta de categorías del onclick agregarLista_dataLayerPush('Almacén  > Infusiones  > Café')
    onclick_node = soup.find(attrs={"onclick": re.compile(r"agregarLista_dataLayerPush")})
    if onclick_node:
        on = onclick_node.get("onclick", "")
        m = re.search(r"agregarLista_dataLayerPush\('([^']+)'", on)
        if m:
            ruta = clean(m.group(1).replace("&gt;", ">"))
            partes = [clean(p) for p in ruta.split(">") if p.strip()]
            if partes:
                res["Categoria"] = partes[0] if len(partes) > 0 else None
                res["Subcategoria"] = partes[1] if len(partes) > 1 else None
                res["Subsubcategoria"] = partes[2] if len(partes) > 2 else None

    # EAN: heurística -> número de 13 dígitos en el texto completo
    m_ean = re.search(r"\b(\d{13})\b", soup.get_text(" ", strip=True))
    if m_ean:
        res["EAN"] = m_ean.group(1)

    # Marca / Fabricante: si el sitio los publica con etiquetas, agregá aquí los selectores.
    # Por defecto quedan None.
    return res

# ------------------ Runner ------------------
def run(max_pages: int = 300, start_page: int = 1, outfile: str = "kilbel_almacen_n1_1.xlsx"):
    session = get_session()
    resultados: List[Dict] = []
    vistos = set()  # para evitar duplicados por URL

    for page in range(start_page, max_pages + 1):
        url_list = urljoin(BASE, LISTING_FMT.format(page=page))
        print(f"\n=== Página {page} -> {url_list}")
        soup = get_soup(session, url_list)
        if not soup:
            print("   (fin: sin contenido o HTTP de corte)")
            break

        rows = parse_listing_products(soup)
        if not rows:
            print("   (fin: no hay cards de productos)")
            break

        for r in rows:
            url = r.get("URL")
            if not url:
                print("    - Card sin URL de detalle, salto.")
                continue
            if url in vistos:
                print(f"    - Ya visitado: {url}")
                continue
            vistos.add(url)

            # Detalle
            d = parse_detail(session, url)
            print(
                "           DETALLE -> "
                f"COD={d.get('CodigoInterno_det')}  "
                f"NOMBRE='{d.get('NombreProducto')}'"
            )
            print(
                "                      "
                f"CATEGORÍA='{d.get('Categoria')}'  "
                f"SUBCAT='{d.get('Subcategoria')}'  "
                f"SUBSUB='{d.get('Subsubcategoria')}'"
            )
            print(
                "                      "
                f"PRECIOS detalle -> lista={d.get('PrecioLista')}  "
                f"oferta={d.get('PrecioOferta')}  porKg={d.get('PrecioPorKg')}  "
                f"sinImp={d.get('PrecioSinImpuestos')}  EAN={d.get('EAN')}"
            )

            merged = {
                "EAN": d.get("EAN"),
                "CodigoInterno": d.get("CodigoInterno_det") or r.get("CodigoInterno_list"),
                "NombreProducto": d.get("NombreProducto") or r.get("NombreProducto_list"),
                "Categoria": d.get("Categoria"),
                "Subcategoria": d.get("Subcategoria"),
                "Marca": d.get("Marca"),
                "Fabricante": d.get("Fabricante"),
                "PrecioLista": d.get("PrecioLista") if d.get("PrecioLista") is not None else r.get("PrecioLista_list"),
                "PrecioOferta": d.get("PrecioOferta") if d.get("PrecioOferta") is not None else r.get("PrecioOferta_list"),
                "TipoOferta": r.get("TipoOferta"),
                "PrecioPorKg": d.get("PrecioPorKg") if d.get("PrecioPorKg") is not None else r.get("PrecioPorKg_list"),
                "PrecioSinImpuestos": d.get("PrecioSinImpuestos") if d.get("PrecioSinImpuestos") is not None else r.get("PrecioSinImpuestos_list"),
                "URL": r.get("URL"),
                "Imagen": r.get("Imagen"),
                "SKU_Tienda": r.get("SKU_Tienda"),
                "RecordId_Tienda": r.get("RecordId_Tienda"),
                "Pagina": page,
            }
            resultados.append(merged)

            time.sleep(random.uniform(*SLEEP_ITEM))
        time.sleep(random.uniform(*SLEEP_PAGE))

    if not resultados:
        print("\n⚠ No se obtuvieron productos. No se generará Excel.")
        return

    df = pd.DataFrame(resultados)
    # orden clásico para tus reportes
    cols = [
        "EAN", "CodigoInterno", "NombreProducto",
        "Categoria", "Subcategoria", "Marca", "Fabricante",
        "PrecioLista", "PrecioOferta", "TipoOferta",
        "PrecioPorKg", "PrecioSinImpuestos",
        "URL", "Imagen", "SKU_Tienda", "RecordId_Tienda", "Pagina"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    df.to_excel(outfile, index=False)
    print(f"\n✔ Guardado Excel: {outfile}  (filas: {len(df)})")

# ------------------ CLI ------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scraper Kilbel Almacén n1_1 (verbose).")
    ap.add_argument("--max-pages", type=int, default=300, help="Máximo de páginas a recorrer (se corta solo si no hay más).")
    ap.add_argument("--start-page", type=int, default=1, help="Página inicial (por defecto 1).")
    ap.add_argument("--outfile", type=str, default="kilbel_almacen_n1_1.xlsx", help="Nombre del archivo Excel de salida.")
    args = ap.parse_args()

    run(max_pages=args.max_pages, start_page=args.start_page, outfile=args.outfile)
