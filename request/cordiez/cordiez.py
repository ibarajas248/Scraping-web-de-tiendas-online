#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cordiez (VTEX) – Descarga completa multi-categoría y exporta a un Excel único.

Mejoras:
- Manejo de HTTP 206 como respuesta aceptable si el JSON es válido.
- Sesión con Retry/Backoff y timeouts por defecto.
- Headers reforzados (Accept-Encoding: identity, Referer dinámico por categoría).
- Logs y deduplicación más robustos.

Requisitos:
  pip install requests pandas tenacity xlsxwriter
"""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ------------- Config editable -------------
BASE = "https://www.cordiez.com.ar"
ORDER = "OrderByScoreDESC"   # Ej.: OrderByTopSaleDESC, OrderByPriceASC, etc.
STEP = 50                    # _from.._to (to es inclusivo). 50 funciona bien en VTEX (0..49, 50..99, etc.)
SLEEP = 0.20                 # pausa entre page-calls
TIMEOUT = 25                 # timeout HTTP segundos
OUT_XLSX = f"cordiez_todas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

# Pega aquí las URLs de las categorías a recorrer
CATEGORY_URLS = [
    "https://www.cordiez.com.ar/sin-gluten-y-diet",
    "https://www.cordiez.com.ar/almacen",
    "https://www.cordiez.com.ar/bazar/automotor",
    "https://www.cordiez.com.ar/bazar/platos-copas-y-cubiertos",
    "https://www.cordiez.com.ar/bebidas",
    "https://www.cordiez.com.ar/bebes-y-ninos",
    "https://www.cordiez.com.ar/carnes",
    "https://www.cordiez.com.ar/congelados",
    "https://www.cordiez.com.ar/cuidado-personal",
    "https://www.cordiez.com.ar/cuidado-de-la-ropa",
    "https://www.cordiez.com.ar/desayuno-y-merienda",
    "https://www.cordiez.com.ar/electrodomesticos",
    "https://www.cordiez.com.ar/fiambres-y-quesos",
    "https://www.cordiez.com.ar/frutas-y-verduras",
    "https://www.cordiez.com.ar/kiosco",
    "https://www.cordiez.com.ar/bazar/libreria",
    "https://www.cordiez.com.ar/limpieza-y-hogar",
    "https://www.cordiez.com.ar/lacteos",
    "https://www.cordiez.com.ar/mascotas",
    "https://www.cordiez.com.ar/panaderia",
    "https://www.cordiez.com.ar/pastas",
    "https://www.cordiez.com.ar/reposteria",
    "https://www.cordiez.com.ar/varios",
]

# Headers base; agregaremos Referer dinámico por categoría y Accept-Encoding: identity
HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "identity",  # evita compresión con rangos que disparan 206 en algunos CDNs
    "Connection": "keep-alive",
}

# ------------- Helpers -------------
class HTTPError(Exception):
    pass

def path_from_category_url(url: str) -> str:
    """
    Convierte la URL de catálogo en la 'ruta' VTEX para usar en la API de search.
    Ej.: https://www.cordiez.com.ar/bazar/automotor -> 'bazar/automotor'
    """
    p = urlparse(url)
    return p.path.strip('/')

def humanize_path(path: str) -> str:
    """
    Convierte 'bazar/platos-copas-y-cubiertos' -> 'Bazar / Platos Copas Y Cubiertos'
    """
    def pretty(seg: str) -> str:
        return seg.replace('-', ' ').strip().title()
    parts = [pretty(seg) for seg in path.split('/') if seg]
    return " / ".join(parts)

def build_url(category_path: str, start: int, end: int) -> str:
    # VTEX: _to es inclusivo. Paginamos 0..49, 50..99, etc.
    return f"{BASE}/api/catalog_system/pub/products/search/{category_path}?&_from={start}&_to={end}&O={ORDER}"

def make_session() -> requests.Session:
    """
    Sesión con Retry y timeouts por defecto.
    """
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)

    # wrap .get para poner timeout por defecto
    _orig_get = s.get
    def _get(url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = TIMEOUT
        return _orig_get(url, **kwargs)
    s.get = _get  # type: ignore
    return s

def try_parse_json(resp: requests.Response) -> Any:
    """
    Intenta parsear JSON tolerando application/json o texto.
    """
    try:
        return resp.json()
    except Exception:
        return json.loads(resp.text)

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
    retry=retry_if_exception_type((requests.RequestException, HTTPError)),
)
def fetch_range(session: requests.Session, url: str, referer: str) -> List[Dict[str, Any]]:
    """
    Llama a la API VTEX. Acepta 200 o 206 si el cuerpo es JSON válido.
    """
    headers = dict(HEADERS_BASE)
    headers["Referer"] = referer
    resp = session.get(url, headers=headers)

    if resp.status_code not in (200, 206):
        raise HTTPError(f"HTTP {resp.status_code} for {url}")

    try:
        data = try_parse_json(resp)
    except Exception as e:
        # A veces 206 trae cuerpo parcial o texto inesperado
        raise HTTPError(f"JSON parse error for {url}: {e}")

    if not isinstance(data, list):
        # En VTEX, este endpoint debe devolver lista de productos; si no, lo consideramos fallo
        raise HTTPError(f"Unexpected payload (not a list) for {url}")

    return data

def extract_rows(product: Dict[str, Any], fuente_categoria: str) -> List[Dict[str, Any]]:
    """
    Una fila por SKU (items[*]), preservando EAN, precios, disponibilidad y promos.
    """
    rows: List[Dict[str, Any]] = []

    prod_name = product.get("productName") or product.get("productTitle")
    brand = product.get("brand")
    link = product.get("link")  # PDP SEO-friendly del producto

    # Categoría/subcategoría a partir del primer path de categories si existe
    categories = product.get("categories") or []
    categoria = subcategoria = ""
    if categories:
        parts = [p for p in (categories[0] or "").split("/") if p]
        if len(parts) >= 1: categoria = parts[0]
        if len(parts) >= 2: subcategoria = parts[1]

    items = product.get("items") or []
    for it in items:
        ean = it.get("ean") or None
        sku = it.get("itemId") or None
        name_it = it.get("name") or prod_name

        sellers = it.get("sellers") or []
        price = list_price = None
        is_available = None
        oferta_tipo = None

        if sellers:
            offer = sellers[0].get("commertialOffer") or {}
            price = offer.get("Price")
            list_price = offer.get("ListPrice")
            is_available = offer.get("IsAvailable")
            teasers = offer.get("PromotionTeasers") or []
            if teasers:
                nombres = []
                for t in teasers:
                    n = (t.get("name") or t.get("Name") or "").strip()
                    if n: nombres.append(n)
                oferta_tipo = "; ".join(nombres) if nombres else None

        rows.append({
            "EAN": ean,
            "Código Interno": sku,
            "Nombre Producto": name_it,
            "Categoría": categoria,
            "Subcategoría": subcategoria,
            "Marca": brand,
            "Precio de Lista": list_price,
            "Precio de Oferta": price,
            "Tipo de Oferta": oferta_tipo,
            "URL": link,
            "Disponible": is_available,
            "Categoría Fuente": fuente_categoria,  # derivada de la URL recorrida
        })
    return rows

def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Elimina duplicados por (SKU, EAN). Conserva la primera aparición.
    """
    seen = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        key = (r.get("Código Interno"), r.get("EAN"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def fetch_category(session: requests.Session, category_url: str) -> List[Dict[str, Any]]:
    """
    Paginación completa de una categoría (ruta VTEX), retorna filas (una por SKU).
    """
    category_path = path_from_category_url(category_url)
    if not category_path:
        raise HTTPError(f"URL inválida: {category_url}")

    filas: List[Dict[str, Any]] = []
    fuente_categoria = humanize_path(category_path)

    print(f"\n=== Explorando categoría: {fuente_categoria} ===")
    time.sleep(1.5)  # pequeña espera para no golpear demasiado

    start = 0
    while True:
        end = start + STEP - 1
        url = build_url(category_path, start, end)
        chunk = fetch_range(session, url, referer=category_url)
        n = len(chunk)
        print(f"[{fuente_categoria}] _from={start} _to={end} -> {n} productos")

        if n == 0:
            break

        for prod in chunk:
            try:
                filas.extend(extract_rows(prod, fuente_categoria))
            except Exception as e:
                # si un producto viene malformado, lo saltamos
                print(f"  - Warning: item malformado en {fuente_categoria}: {e}")

        if n < STEP:
            # última página de esta categoría
            break

        start += STEP
        time.sleep(SLEEP)

    return filas

def main():
    session = make_session()
    all_rows: List[Dict[str, Any]] = []

    for cat_url in CATEGORY_URLS:
        try:
            filas = fetch_category(session, cat_url)
            if filas:
                print(f"✔ {len(filas)} filas obtenidas en '{cat_url}'")
                all_rows.extend(filas)
            else:
                print(f"⚠ Sin resultados en '{cat_url}'")
        except Exception as e:
            print(f"❌ Error en categoría '{cat_url}': {e}")

    if not all_rows:
        print("No se encontraron productos en ninguna categoría.")
        return

    # Deduplicar globalmente por (SKU, EAN)
    all_rows = dedupe_rows(all_rows)

    # DataFrame final
    df = pd.DataFrame(all_rows)
    cols = [
        "EAN", "Código Interno", "Nombre Producto",
        "Categoría", "Subcategoría", "Marca",
        "Precio de Lista", "Precio de Oferta", "Tipo de Oferta",
        "URL", "Disponible", "Categoría Fuente"
    ]
    df = df.reindex(columns=cols)

    # Exportar a Excel único
    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="cordiez_todo")
        wb = writer.book
        ws = writer.sheets["cordiez_todo"]
        money_fmt = wb.add_format({"num_format": "#,##0.00"})
        ws.set_column("A:A", 16)   # EAN
        ws.set_column("B:B", 14)   # Código Interno
        ws.set_column("C:C", 50)   # Nombre
        ws.set_column("D:E", 22)   # Cat/Sub
        ws.set_column("F:F", 18)   # Marca
        ws.set_column("G:H", 16, money_fmt)  # Precios
        ws.set_column("I:I", 30)   # Tipo de Oferta
        ws.set_column("J:J", 70)   # URL
        ws.set_column("K:K", 12)   # Disponible
        ws.set_column("L:L", 40)   # Categoría Fuente

    print(f"✅ Exportado: {OUT_XLSX} | Filas: {len(df)} | Categorías: {len(CATEGORY_URLS)}")

if __name__ == "__main__":
    main()
