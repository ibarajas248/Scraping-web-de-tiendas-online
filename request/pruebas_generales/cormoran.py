#!/usr/bin/env python3
"""
Scraper para cormoran.com.ar
================================================

Este módulo descarga el árbol de categorías de Cormoran (un sitio mayorista
construido sobre la plataforma VTEX), recorre todas las categorías y
recupera los productos con su información más importante, como nombre,
código, precio, estado de stock y URL de imagen.  La salida final se
genera en un archivo Excel (y opcionalmente CSV) que contiene un registro
por SKU.  El script imprime cada producto en la consola a medida que lo
procesa para dar feedback sobre el progreso.

Debido a que el endpoint de búsqueda de productos de VTEX requiere un
"sales channel" válido (y, en algunos comercios, autenticación), el
script implementa dos estrategias:

1. **API de productos**: intenta consultar el endpoint
   ``/api/catalog_system/pub/products/search`` con distintas variantes
   del parámetro ``sc`` (sales channel) para cada categoría.  Si se
   obtiene una respuesta válida, se utiliza esa fuente porque es más
   robusta y precisa (incluye EAN/GTIN, precios de lista y de oferta,
   stock, etc.).

2. **HTML de la categoría**: en caso de que la API devuelva
   "Sales channel not found" o cualquier error, el script realiza un
   scraping del HTML del listado de productos.  Cormoran expone un
   bloque JSON‑LD en cada página de categoría con un ``itemListElement``
   que describe cada producto.  Este JSON incluye el nombre,
   identificador, marca, SKU, imagen y una oferta agregada con
   ``lowPrice`` y ``highPrice`` que usamos como precio.  El script
   extrae ese JSON‑LD mediante expresiones regulares y lo convierte en
   diccionarios Python.

Para ejecutar este script debes instalar las dependencias:

    pip install requests pandas openpyxl

Limitaciones:

* No utiliza Selenium ni BeautifulSoup, solo ``requests`` y las
  librerías estándar.  Por tanto, no ejecuta JavaScript y depende de
  que el HTML generado por el servidor contenga la información
  necesaria.
* El scraping del HTML se basa en la presencia de JSON‑LD con
  ``itemListElement``.  Si Cormoran cambia su plantilla y elimina ese
  bloque, la extracción HTML podría fallar.
* Los endpoints VTEX pueden estar sujetos a restricciones de
  geolocalización o rate‑limiting.  El script implementa una pausa
  configurable entre requests para ser amable con el servidor.

Uso (línea de comandos)::

    python cormoran_scraper.py --outfile productos_cormoran.xlsx

Argumentos principales::

    --base       URL base de la tienda (por defecto
                 https://www.cormoran.com.ar)
    --step       Cantidad de productos por página para las llamadas a
                 la API (VTEX admite hasta 50).  Útil para ajustar
                 desempeño.
    --outfile    Nombre del fichero de salida XLSX.
    --csv        Nombre de fichero para exportar CSV (opcional).
    --pause      Pausa en segundos entre peticiones (por defecto 0.3).
    --maxcats    Limitar el número de categorías procesadas (debug).

"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session() -> requests.Session:
    """Crea una sesión HTTP con un User-Agent realista y reintentos."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
        }
    )
    return s


def fetch_category_tree(session: requests.Session, base: str) -> List[Dict[str, any]]:
    """
    Descarga el árbol de categorías (hasta 50 niveles de profundidad).

    Devuelve una lista de nodos, cada uno de los cuales puede tener un
    campo ``children`` con subcategorías.  Cada nodo tiene al menos
    ``id``, ``name`` y ``url``.
    """
    url = f"{base.rstrip('/')}/api/catalog_system/pub/category/tree/50"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Formato inesperado del árbol de categorías")
    return data


def flatten_categories(tree: List[Dict[str, any]]) -> List[Tuple[int, str, str]]:
    """
    Aplana el árbol de categorías a una lista de tuplas (id, nombre, url).
    Incluye tanto nodos hoja como contenedores.
    """
    flat: List[Tuple[int, str, str]] = []

    def walk(node: Dict[str, any], path: List[str]):
        cid = int(node.get("id"))
        name = str(node.get("name"))
        url = str(node.get("url"))  # la URL absoluta de la categoría
        full_name = " / ".join(path + [name]) if path else name
        flat.append((cid, full_name, url))
        for child in node.get("children", []) or []:
            walk(child, path + [name])

    for n in tree:
        walk(n, [])
    # elimina duplicados manteniendo el orden
    seen = set()
    unique: List[Tuple[int, str, str]] = []
    for tup in flat:
        if tup[0] not in seen:
            seen.add(tup[0])
            unique.append(tup)
    return unique


def try_fetch_products_api(
        session: requests.Session,
        base: str,
        category_id: int,
        step: int = 50,
        max_sc: int = 15,
) -> Optional[List[Dict[str, any]]]:
    """
    Intenta recuperar productos de una categoría mediante la API de búsqueda
    de VTEX.  Prueba distintas variantes del parámetro ``sc`` (sales channel)
    hasta que la respuesta deje de ser "Sales channel not found".

    Devuelve una lista de productos en formato de la API o None si
    ninguna combinación funcionó.
    """
    search_url = f"{base.rstrip('/')}/api/catalog_system/pub/products/search"
    # enumeramos sales channels; la mayoría de tiendas usan 1 o 2
    for sc in range(1, max_sc + 1):
        products: List[Dict[str, any]] = []
        _from = 0
        found_valid = False
        while True:
            params = {
                "fq": f"C:{category_id}",
                "_from": _from,
                "_to": _from + step - 1,
                "sc": sc,
            }
            resp = session.get(search_url, params=params, timeout=30)
            text = resp.text.strip()
            # Si el canal no existe, abortamos y probamos el siguiente
            if "Sales channel not found" in text or resp.status_code == 404:
                break
            if resp.status_code != 200:
                break
            try:
                data = resp.json()
            except Exception:
                break
            if not data:
                break
            # A veces retorna un único producto como dict
            if isinstance(data, dict):
                page = [data]
            else:
                page = list(data)
            products.extend(page)
            found_valid = True
            # si la cantidad recibida es menor que el tamaño de página, terminamos
            if len(page) < step:
                break
            _from += step
            time.sleep(0.1)  # pausa corta entre páginas
        if found_valid and products:
            return products
    return None


def parse_jsonld_from_html(html: str) -> List[Dict[str, any]]:
    """
    Extrae bloques JSON‑LD de tipo Product de una página HTML.

    Busca ``<script type="application/ld+json">`` y procesa aquellos
    cuyo contenido incluye ``"@type":"Product"`` o ``itemListElement``.

    Devuelve una lista de diccionarios correspondientes a productos.
    """
    products: List[Dict[str, any]] = []
    # captura todos los bloques JSON-LD
    for match in re.finditer(
            r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            re.DOTALL | re.IGNORECASE,
    ):
        content = match.group(1)
        # elimina comentarios HTML
        content_clean = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
        try:
            data = json.loads(content_clean)
        except Exception:
            continue
        # si es un listado
        if isinstance(data, dict) and 'itemListElement' in data:
            for elem in data['itemListElement']:
                item = elem.get('item')
                if isinstance(item, dict) and item.get('@type') == 'Product':
                    products.append(item)
        # si es un único producto
        elif isinstance(data, dict) and data.get('@type') == 'Product':
            products.append(data)
    return products


def fetch_products_html(
        session: requests.Session, category_url: str
) -> List[Dict[str, any]]:
    """
    Recupera productos de una página de categoría analizando su JSON‑LD.

    Nota: Cormoran paginaba con scroll infinito en el momento de
    desarrollo.  Este método solo recupera los productos que aparecen en
    la primera carga del HTML.  Es posible que no capture todo el
    catálogo, pero es un buen plan de respaldo.
    """
    r = session.get(category_url, timeout=30)
    r.raise_for_status()
    products = parse_jsonld_from_html(r.text)
    return products


def product_rows_from_vtex_product(
        base: str, prod: Dict[str, any], category_name: str
) -> List[Dict[str, any]]:
    """
    Convierte un producto devuelto por la API de VTEX en una lista de
    filas de salida (una por SKU).

    Cada fila incluye ProductId, SKU, EAN, nombre, marca, categoría,
    precio de lista, precio de oferta, cantidad disponible, URL del
    producto, imagen, unidad, multiplicador y nombre del vendedor.
    """
    rows: List[Dict[str, any]] = []
    product_id = prod.get("productId")
    name = prod.get("productName") or prod.get("productTitle")
    brand = prod.get("brand")
    link_text = prod.get("linkText")
    url = f"{base.rstrip('/')}/{link_text}/p" if link_text else None
    for item in prod.get("items", []) or []:
        sku = item.get("itemId")
        # EAN puede estar en varios lugares
        ean = (
                item.get("ean")
                or next(
            (ref.get("Value") for ref in item.get("referenceId", []) or [] if
             ref.get("Key") in {"EAN", "ean", "GTIN", "RefId"}),
            None,
        )
                or None
        )
        # seller/price info
        sellers = item.get("sellers") or []
        seller_data = sellers[0] if sellers else {}
        offer = seller_data.get("commertialOffer", {}) if seller_data else {}
        price = offer.get("Price")
        list_price = offer.get("ListPrice")
        available_qty = offer.get("AvailableQuantity")
        image_url = None
        images = item.get("images") or []
        if images:
            image_url = images[0].get("imageUrl")
        unit_multiplier = item.get("unitMultiplier")
        measurement_unit = item.get("measurementUnit")
        seller_name = seller_data.get("sellerName") if seller_data else None
        rows.append(
            {
                "ProductId": product_id,
                "SKU": sku,
                "EAN": ean,
                "Nombre": name,
                "Marca": brand,
                "Categoria": category_name,
                "PrecioLista": list_price,
                "PrecioOferta": price,
                "StockDisponible": available_qty,
                "URL": url,
                "Imagen": image_url,
                "UnitMultiplier": unit_multiplier,
                "MeasurementUnit": measurement_unit,
                "Seller": seller_name,
            }
        )
    if not rows:
        rows.append(
            {
                "ProductId": product_id,
                "SKU": None,
                "EAN": None,
                "Nombre": name,
                "Marca": brand,
                "Categoria": category_name,
                "PrecioLista": None,
                "PrecioOferta": None,
                "StockDisponible": None,
                "URL": url,
                "Imagen": None,
                "UnitMultiplier": None,
                "MeasurementUnit": None,
                "Seller": None,
            }
        )
    return rows


def product_rows_from_jsonld(
        prod: Dict[str, any], category_name: str
) -> List[Dict[str, any]]:
    """
    Convierte un objeto Producto extraído del JSON‑LD en filas de salida.

    Toma los campos comunes (nombre, marca, SKU, mpn, imagen, precio,
    disponibilidad, url) y los normaliza a las columnas del DataFrame.
    """
    name = prod.get("name")
    brand = None
    brand_data = prod.get("brand")
    if isinstance(brand_data, dict):
        brand = brand_data.get("name")
    sku = prod.get("sku")
    mpn = prod.get("mpn")
    url = prod.get("@id") or prod.get("url")
    image = prod.get("image")
    offers = prod.get("offers") or {}
    # offers puede ser AggregateOffer o lista de Offer
    price = None
    list_price = None
    available = None
    if isinstance(offers, dict):
        # AggregateOffer
        price = offers.get("lowPrice") or offers.get("price")
        list_price = offers.get("highPrice") or offers.get("price")
        available = 1 if offers.get("offers") else None
    elif isinstance(offers, list) and offers:
        # Use first offer
        offer = offers[0]
        price = offer.get("price")
        list_price = offer.get("price")
        avail = offer.get("availability") or ""
        available = 1 if "InStock" in avail else 0
    return [
        {
            "ProductId": mpn,
            "SKU": sku,
            "EAN": None,
            "Nombre": name,
            "Marca": brand,
            "Categoria": category_name,
            "PrecioLista": list_price,
            "PrecioOferta": price,
            "StockDisponible": available,
            "URL": url,
            "Imagen": image,
            "UnitMultiplier": None,
            "MeasurementUnit": None,
            "Seller": None,
        }
    ]


def crawl_cormoran(
        base: str = "https://www.cormoran.com.ar",
        step: int = 50,
        pause: float = 0.3,
        max_categories: Optional[int] = None,
) -> pd.DataFrame:
    """
    Recorre la tienda de Cormoran obteniendo todos los productos.

    Intenta primero la API de VTEX para cada categoría; si falla, recurre al
    análisis del HTML.  Imprime cada producto encontrado y devuelve un
    DataFrame con todas las filas.
    """
    session = make_session()
    # 1. Obtener árbol de categorías
    tree = fetch_category_tree(session, base)
    categories = flatten_categories(tree)
    if max_categories:
        categories = categories[:max_categories]

    all_rows: List[Dict[str, any]] = []
    seen_keys: set = set()
    total_categories = len(categories)
    for idx, (cid, cname, curl) in enumerate(categories, start=1):
        print(f"=== Categoria {idx}/{total_categories}: {cname} ===")
        # 2. Intentar API
        api_data = None
        try:
            api_data = try_fetch_products_api(session, base, cid, step=step)
        except Exception as e:
            api_data = None
        if api_data:
            for prod in api_data:
                rows = product_rows_from_vtex_product(base, prod, cname)
                for row in rows:
                    key = (row.get("ProductId"), row.get("SKU"))
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    all_rows.append(row)
                    # impresión en consola
                    nombre = row.get("Nombre") or ""
                    precio = row.get("PrecioOferta") or row.get("PrecioLista") or "-"
                    stock_text = "OK" if row.get("StockDisponible") else "SIN STOCK"
                    print(f" - {nombre[:60]} | ${precio} | {stock_text}")
            continue  # ya manejamos esta categoría
        # 3. Fallback: scrape HTML
        try:
            html_products = fetch_products_html(session, curl)
        except Exception as e:
            print(f" [warn] No se pudo procesar {curl}: {e}")
            continue
        for prod in html_products:
            rows = product_rows_from_jsonld(prod, cname)
            for row in rows:
                key = (row.get("ProductId"), row.get("SKU"))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                all_rows.append(row)
                nombre = row.get("Nombre") or ""
                precio = row.get("PrecioOferta") or row.get("PrecioLista") or "-"
                stock_text = "OK" if row.get("StockDisponible") else "SIN STOCK"
                print(f" - {nombre[:60]} | ${precio} | {stock_text}")
        time.sleep(pause)
    df = pd.DataFrame(all_rows)
    # Asegurarse de que todas las columnas existan y ordenar
    cols = [
        "ProductId",
        "SKU",
        "EAN",
        "Nombre",
        "Marca",
        "Categoria",
        "PrecioLista",
        "PrecioOferta",
        "StockDisponible",
        "URL",
        "Imagen",
        "UnitMultiplier",
        "MeasurementUnit",
        "Seller",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].drop_duplicates().reset_index(drop=True)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Dump completo de productos de Cormoran")
    parser.add_argument(
        "--base", default="https://www.cormoran.com.ar", help="URL base de la tienda"
    )
    parser.add_argument(
        "--step",
        type=int,
        default=50,
        help="Tamaño de página para llamadas a la API (máx 50)",
    )
    parser.add_argument(
        "--outfile", default="productos_cormoran.xlsx", help="Archivo de salida XLSX"
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="Archivo CSV de salida adicional (opcional)",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=0.3,
        help="Pausa entre requests a páginas HTML (segundos)",
    )
    parser.add_argument(
        "--maxcats",
        type=int,
        default=None,
        help="Limitar el número de categorías a procesar (debug)",
    )
    args = parser.parse_args()
    df = crawl_cormoran(
        base=args.base,
        step=args.step,
        pause=args.pause,
        max_categories=args.maxcats,
    )
    print(f"\nTotal de filas (SKU) obtenidas: {len(df)}")
    # Exportar
    df.to_excel(args.outfile, index=False)
    if args.csv:
        df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    print(f"Datos exportados a {args.outfile}" + (f" y {args.csv}" if args.csv else ""))


if __name__ == "__main__":
    main()