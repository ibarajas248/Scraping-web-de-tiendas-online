#!/usr/bin/env python3
"""
Reporte multi‑supermercado (VTEX) por EAN — Script standalone.

Este script lee un archivo Excel llamado `maestro.xlsx` ubicado en el mismo
directorio que el script. A partir de los EAN contenidos en ese archivo,
consulta el catálogo de varias tiendas basadas en la plataforma VTEX para
obtener información de productos (nombre, marca, categoría, disponibilidad y
precios). Las tiendas se consultan secuencialmente por EAN pero en paralelo
entre sí para optimizar el tiempo total de ejecución. Al finalizar, genera
un Excel consolidado con el resultado y lo guarda en la misma carpeta.

Además, este script realiza una ingesta de los datos recolectados en una
base de datos MySQL utilizando funciones de `base_datos.get_conn()`.

Principales características y mejoras respecto a implementaciones simples:

* **Sesiones HTTP robustas** con reintentos, cabeceras tipo navegador y
  uso de `Referer` dinámico para evitar bloqueos de WAF.
* **Detección automática del `sales channel` (`sc`)** por tienda a través
  del endpoint `/api/sessions`. Si no se detecta, se prueba una lista de
  canales frecuentes.
* **Búsqueda por EAN con estrategias de fallback:** primero por
  `alternateIds_Ean`, luego por fulltext (`ft`) y, si es necesario,
  iterando sobre distintos `sc`.
* **Obtención de precios mediante simulación de checkout:** cuando los
  precios devueltos por `catalog_system` son nulos o cero, se realiza una
  llamada a `/api/checkout/pub/orderForms/simulation` para obtener el precio
  vigente y el precio de lista en función del código postal y país.
* **Retrasos con jitter** entre peticiones para mitigar bloqueos por scraping.
* **Inserción masiva en MySQL** con reintentos ante bloqueos o errores y
  manejo cuidadoso de valores nulos y tipos de datos.

Uso:

```
python reporte_multi_super_vtex_por_ean.py --ean-column EAN
```

Se pueden proporcionar parámetros opcionales para ajustar dominios extra,
canal de venta (`sc`), número de tiendas en paralelo y pausas entre
solicitudes. La ingesta en la base de datos está forzada a `True`.
"""

import io
import os
import sys
import json
import time
import random
import argparse
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======== MySQL =========
import mysql.connector
from mysql.connector import errors as myerr

# Añade la carpeta raíz (2 niveles arriba) al sys.path para importar tu helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- tu conexión MySQL

# =========================================
#  CANDIDATOS (mismos que en la app) — default_sc ajustado a "1"
# =========================================
VTEX_CANDIDATES_AR = [
    {"name": "Carrefour", "base": "https://www.carrefour.com.ar", "default_sc": "1"},
    {"name": "Jumbo",     "base": "https://www.jumbo.com.ar",     "default_sc": "1"},
    {"name": "Mas online",    "base": "https://www.masonline.com.ar", "default_sc": "1"},
]

# Ruta del endpoint de catálogo VTEX
SEARCH_PATH = "/api/catalog_system/pub/products/search"
TIMEOUT = 25

# ====== Headers base para las solicitudes HTTP ======
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    # El Referer se asigna dinámicamente según el dominio consultado
}

def make_session(retries: int = 2) -> requests.Session:
    """Crea una sesión HTTP configurada con reintentos y cabeceras apropiadas."""
    s = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=0.35,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=False  # reintenta cualquier método HTTP
    )
    s.headers.update(HEADERS)
    s.mount("https://", HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=retry))
    s.mount("http://",  HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=retry))
    return s

def jitter_delay(base: float) -> float:
    """Devuelve un valor de delay con variación aleatoria (±33%)."""
    return max(0.05, base * random.uniform(0.66, 1.33))

def _safe_get(d: Dict, path: List[Any], default=None):
    """Acceso seguro a estructuras anidadas (dict/list)."""
    cur = d
    try:
        for p in path:
            if isinstance(cur, list):
                cur = cur[p] if isinstance(p, int) else None
            else:
                cur = cur.get(p)
            if cur is None:
                return default
        return cur
    except Exception:
        return default

def normalizar_columna_ean(df: pd.DataFrame, col: str) -> pd.Series:
    """Convierte la columna de EAN a una serie limpia de cadenas numéricas."""
    ser = df[col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    ser = ser.str.replace(r"[^\d]", "", regex=True)
    return ser.replace({"": None})

# ============== Descubrimiento VTEX / sc / región ==============

def is_vtex_store(session: requests.Session, base_url: str) -> bool:
    """Comprueba si un dominio responde como una tienda VTEX."""
    try:
        url = base_url.rstrip("/") + SEARCH_PATH
        params = {"_from": 0, "_to": 0, "ft": "a"}
        host = urlparse(base_url).netloc or base_url
        session.headers["Referer"] = f"https://{host}/"
        r = session.get(url, params=params, timeout=TIMEOUT)
        txt = (r.text or "").strip()
        return (r.status_code in (200, 206)) and (
            txt.startswith("[") or "vtex" in (r.headers.get("x-powered-by", "") + r.headers.get("server", "")).lower()
        )
    except Exception:
        return False

def discover_sc(session: requests.Session, base_url: str) -> Optional[str]:
    """Intenta descubrir el sales channel (`sc`) de una tienda consultando /api/sessions."""
    try:
        host = urlparse(base_url).netloc or base_url
        session.headers["Referer"] = f"https://{host}/"
        r = session.get(base_url.rstrip("/") + "/api/sessions", timeout=12)
        if r.ok and r.headers.get("content-type", "").startswith("application/json"):
            js = r.json() or {}
            ns = (js.get("namespaces") or {}).get("store") or {}
            ch = ns.get("channel") or {}
            sc = ch.get("value") or ch.get("Value")
            return str(sc).strip() if sc else None
    except Exception:
        pass
    return None

def discover_region_country(session: requests.Session, base_url: str) -> Tuple[str, str]:
    """
    Obtiene un código postal y un país por defecto para la simulación de precios.
    Se consulta /api/sessions y, si no se encuentra nada, se devuelve ("C1000", "ARG").
    """
    try:
        host = urlparse(base_url).netloc or base_url
        session.headers["Referer"] = f"https://{host}/"
        r = session.get(base_url.rstrip("/") + "/api/sessions", timeout=12)
        if r.ok and r.headers.get("content-type", "").startswith("application/json"):
            js = r.json() or {}
            ns = js.get("namespaces") or {}
            public = ns.get("public") or {}
            addr = ns.get("storefront") or {}
            pc = (public.get("postalCode", {}) or {}).get("value") or (addr.get("postalCode", {}) or {}).get("value")
            co = (public.get("country", {}) or {}).get("value") or "ARG"
            if pc and isinstance(pc, str) and len(pc) >= 4:
                return pc.strip(), co.strip()
            return ("C1000", co.strip() if isinstance(co, str) else "ARG")
    except Exception:
        pass
    return ("C1000", "ARG")

# ============== Búsqueda robusta por EAN (con fallbacks) ==============

# Lista de sales channels a probar cuando no se detecta uno válido
SC_TRY = ["1", "2", "3", "5", "7", "8", "10", "12", "20", "43"]

def vtex_search_product(session: requests.Session, base_url: str, params: List[Tuple[str, str]]):
    """Ejecuta una búsqueda en catalog_system devolviendo estado y JSON si hay lista."""
    url = base_url.rstrip("/") + SEARCH_PATH
    r = session.get(url, params=params, timeout=TIMEOUT)
    txt = (r.text or "").strip()
    if r.status_code in (200, 206) and txt.startswith("["):
        try:
            return "OK", r.json()
        except Exception:
            return "NO_JSON", None
    return ("NO_JSON", None) if r.status_code in (200, 206) else (f"HTTP_{r.status_code}", None)

def vtex_buscar_por_ean_session(session: requests.Session, base_url: str, ean: str, sc: Optional[str]) -> Tuple[str, Optional[List[Dict[str, Any]]]]:
    """
    Busca productos por EAN utilizando varias estrategias:
      1. `fq=alternateIds_Ean` con sc (si hay)
      2. `fq=alternateIds_Ean` sin sc
      3. `ft` simple (búsqueda fulltext)
      4. brute force de sc con ambas estrategias si no se detectó sc
    Devuelve un estado y la lista de productos (o None).
    """
    host = urlparse(base_url).netloc or base_url
    session.headers["Referer"] = f"https://{host}/"

    # 1) fq con sc directo
    params = [("fq", f"alternateIds_Ean:{ean}")]
    if sc:
        params.append(("sc", sc))
    estado, prods = vtex_search_product(session, base_url, params)
    if estado == "OK" and prods:
        return "OK", prods

    # 2) fq sin sc
    estado2, prods2 = vtex_search_product(session, base_url, [("fq", f"alternateIds_Ean:{ean}")])
    if estado2 == "OK" and prods2:
        return "OK", prods2

    # 3) ft como fallback
    estado3, prods3 = vtex_search_product(session, base_url, [("ft", ean)])
    if estado3 == "OK" and prods3:
        return "OK", prods3

    # 4) brute-force de sc si no se proporcionó sc
    if not sc:
        for sc_try in SC_TRY:
            estado4, prods4 = vtex_search_product(session, base_url, [("fq", f"alternateIds_Ean:{ean}"), ("sc", sc_try)])
            if estado4 == "OK" and prods4:
                return "OK", prods4
            estado5, prods5 = vtex_search_product(session, base_url, [("ft", ean), ("sc", sc_try)])
            if estado5 == "OK" and prods5:
                return "OK", prods5

    # Devuelve el último estado obtenido
    return estado3 or estado2 or estado, None

# ============== Simulación de precios ==============

def simulate_price(session: requests.Session, base_url: str, sku_id: str, seller_id: str, sc: Optional[str], postal_code: str, country: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Llama al endpoint de simulación de checkout para obtener los precios de lista y
    de oferta. Devuelve una tupla `(precio_lista, precio_oferta)` en la divisa de
    la tienda o `(None, None)` si no se pudo determinar.

    El endpoint usado es `/api/checkout/pub/orderForms/simulation` con un body
    mínimo. Los precios regresan en centavos y se convierten a float.
    """
    try:
        host = urlparse(base_url).netloc or base_url
        session.headers["Referer"] = f"https://{host}/"
        url = base_url.rstrip("/") + "/api/checkout/pub/orderForms/simulation"
        params = []
        if sc:
            params.append(("sc", str(sc)))
        payload = {
            "items": [
                {
                    "id": str(sku_id),
                    "quantity": 1,
                    "seller": str(seller_id or "1")
                }
            ],
            "country": country or "ARG",
            "postalCode": postal_code or "C1000"
        }
        r = session.post(url, params=params, json=payload, timeout=20)
        if not r.ok:
            return None, None
        js = r.json() or {}
        items = js.get("items") or []
        if not items:
            return None, None
        it = items[0]
        p = it.get("price")
        lp = it.get("listPrice")

        def cv(x):
            try:
                return round(float(x) / 100.0, 2) if x is not None else None
            except Exception:
                return None
        return cv(lp), cv(p)
    except Exception:
        return None, None

# ============== Parseo de productos VTEX ==============

def _derive_prices(co: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Calcula precios de lista y de oferta a partir del bloque commertialOffer."""
    def _f(x):
        try:
            if x is None or (isinstance(x, str) and not x.strip()):
                return None
            return float(x)
        except Exception:
            return None

    p = _f(co.get("Price"))
    l = _f(co.get("ListPrice"))
    pwd = _f(co.get("PriceWithoutDiscount"))

    lista = oferta = None
    promo_tipo = None
    # Preferir PriceWithoutDiscount si es mayor o igual que Price
    if pwd is not None and p is not None and pwd >= p and p > 0:
        lista, oferta, promo_tipo = pwd, p, "promo_pwd"
    elif l is not None and p is not None and l >= p and p > 0:
        lista, oferta, promo_tipo = l, p, "promo_listprice"
    elif p is not None and p > 0:
        lista, oferta, promo_tipo = p, None, None
    elif l is not None and l > 0:
        lista, oferta, promo_tipo = l, None, None
    else:
        lista, oferta, promo_tipo = None, None, None
    return lista, oferta, promo_tipo

def parsear_producto_vtex(producto: Dict[str, Any], ean_consultado: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Normaliza la respuesta de un producto VTEX en una lista de filas.
    Cada fila corresponde a una combinación de SKU/Seller.
    """
    filas = []
    product_id = producto.get("productId")
    product_name = producto.get("productName")
    brand = producto.get("brand")
    link = producto.get("link") or producto.get("linkText")

    # Categorías
    cat1 = cat2 = None
    cat_tree = producto.get("categories") or []
    if not cat_tree:
        ct = producto.get("categoryTree") or []
        if ct:
            cat1 = _safe_get(ct, [0, "Name"])
            cat2 = _safe_get(ct, [1, "Name"])
    else:
        try:
            parts = [p for p in cat_tree[0].split("/") if p]
            cat1 = parts[0] if len(parts) > 0 else None
            cat2 = parts[1] if len(parts) > 1 else None
        except Exception:
            pass

    # Promos a nivel producto
    promo_tags = None
    if producto.get("clusterHighlights"):
        promo_tags = ", ".join([str(v) for v in producto.get("clusterHighlights", {}).values() if v])
    elif producto.get("productClusters"):
        promo_tags = ", ".join([str(v) for v in producto.get("productClusters", {}).values() if v])

    items = producto.get("items", []) or []
    if not items:
        filas.append({
            "product_id": product_id, "sku_id": None,
            "nombre": product_name, "marca": brand,
            "categoria": cat1, "subcategoria": cat2,
            "url": (base_url + link) if link and isinstance(link, str) and link.startswith("/") else link,
            "precio_lista": None, "precio_oferta": None, "disponible": None,
            "oferta_tags": promo_tags, "ean_reportado": None, "seller_id": None
        })
        return filas

    for it in items:
        sku_id = it.get("itemId") or it.get("id")
        ean_item = it.get("ean") or it.get("Ean")
        sellers = it.get("sellers") or []

        if not sellers:
            filas.append({
                "product_id": product_id, "sku_id": sku_id,
                "nombre": product_name, "marca": brand,
                "categoria": cat1, "subcategoria": cat2,
                "url": (base_url + link) if link and isinstance(link, str) and link.startswith("/") else link,
                "precio_lista": None, "precio_oferta": None, "disponible": None,
                "oferta_tags": promo_tags, "ean_reportado": ean_item, "seller_id": None
            })
            continue

        for s in sellers:
            sid = s.get("sellerId") or s.get("id")
            co = s.get("commertialOffer") or {}
            lista, oferta, _promo_tipo = _derive_prices(co)
            available = co.get("AvailableQuantity")
            teasers = co.get("Teasers") or co.get("DiscountHighLight") or []
            if isinstance(teasers, list) and teasers:
                teasers_txt = ", ".join([
                    t.get("name") or json.dumps(t, ensure_ascii=False) for t in teasers if isinstance(t, dict)
                ])
            elif isinstance(teasers, list):
                teasers_txt = None
            else:
                teasers_txt = str(teasers) if teasers else None
            filas.append({
                "product_id": product_id, "sku_id": sku_id,
                "nombre": product_name, "marca": brand,
                "categoria": cat1, "subcategoria": cat2,
                "url": (base_url + link) if link and isinstance(link, str) and link.startswith("/") else link,
                "precio_lista": lista, "precio_oferta": oferta, "disponible": available,
                "oferta_tags": teasers_txt or promo_tags, "ean_reportado": ean_item, "seller_id": sid
            })
    return filas

# ============== Detección automática (sin cache streamlit) ==============

def detectar_tiendas_vtex(candidatos: List[Dict[str, str]], extras: List[str], retries: int) -> List[Dict[str, str]]:
    """Retorna la lista de dominios VTEX detectados entre candidatos y extras."""
    session = make_session(retries=retries)
    final: List[Dict[str, str]] = []

    for d in extras:
        d = d.strip()
        if not d:
            continue
        if not (d.startswith("http://") or d.startswith("https://")):
            d = "https://" + d
        final.append({"name": d, "base": d, "default_sc": ""})

    final = candidatos + final
    found: List[Dict[str, str]] = []

    def probe(entry: Dict[str, str]) -> Optional[Dict[str, str]]:
        try:
            ok = is_vtex_store(session, entry["base"])
            return entry if ok else None
        except Exception:
            return None

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(final) or 1)) as ex:
        futs = [ex.submit(probe, e) for e in final]
        for f in concurrent.futures.as_completed(futs):
            r = f.result()
            if r:
                found.append(r)

    uniq = {e["base"]: e for e in found}
    return list(uniq.values())

# ============== Helpers DB (con esquema en código tienda) ==============
MAXLEN_TIPO_OFERTA = 64
MAXLEN_COMENTARIOS = 255
MAXLEN_NOMBRE = 255
MAXLEN_CATEGORIA = 120
MAXLEN_SUBCATEGORIA = 200
MAXLEN_NOMBRE_TIENDA = 255

LOCK_ERRNOS = {1205, 1213}
RETRYABLE_ERRNOS = {1205, 1213}  # lock wait timeout, deadlock

def should_retry(e) -> bool:
    return getattr(e, "errno", None) in RETRYABLE_ERRNOS

def _truncate(s: Optional[str], n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    return s if len(s) <= n else s[:n]

def _price_str(val) -> Optional[str]:
    """Normaliza un valor de precio a string con dos decimales o None."""
    if val is None:
        return None
    try:
        f = float(val)
        if pd.isna(f):
            return None
        if abs(f) > 999999999:
            return None
        return f"{round(f, 2):.2f}"
    except Exception:
        return None

def _domain_with_scheme(url: str) -> str:
    """Devuelve la URL normalizada en forma scheme://host sin path."""
    try:
        if not (url.startswith("http://") or url.startswith("https://")):
            url = "https://" + url
        p = urlparse(url)
        host = p.netloc or p.path
        scheme = p.scheme or "https"
        return f"{scheme}://{host}"
    except Exception:
        return url

def exec_with_retry(cur, sql, params=None, max_retries=5, base_sleep=0.4):
    attempt = 0
    while True:
        try:
            cur.execute(sql, params or ())
            return
        except myerr.DatabaseError as e:
            code = getattr(e, 'errno', None)
            if code in LOCK_ERRNOS and attempt < max_retries:
                wait = base_sleep * (2 ** attempt)
                time.sleep(wait)
                attempt += 1
                continue
            raise

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_with_retry(cur,
                    "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
                    (codigo, _truncate(nombre, MAXLEN_NOMBRE))
                    )
    exec_with_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def split_categoria_sub(cat1: Optional[str], cat2: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    categoria = _truncate(cat1, MAXLEN_CATEGORIA) if cat1 else None
    sub = _truncate(cat2, MAXLEN_SUBCATEGORIA) if cat2 else None
    return categoria, sub

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = r.get("ean_reportado") or r.get("ean_consultado") or None
    nombre = _truncate(r.get("nombre"), MAXLEN_NOMBRE)
    marca = _truncate(r.get("marca"), MAXLEN_NOMBRE)
    categoria, subcategoria = split_categoria_sub(r.get("categoria"), r.get("subcategoria"))
    # 1) Buscar por EAN
    if ean:
        exec_with_retry(cur, "SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_with_retry(cur, """
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (nombre or "", marca or "", categoria, subcategoria, pid))
            return pid
    # 2) Fallback por nombre y marca
    if nombre:
        exec_with_retry(cur, "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
                        (nombre, marca or ""))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_with_retry(cur, """
                UPDATE productos SET
                  ean = COALESCE(%s, ean),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (ean, categoria, subcategoria, pid))
            return pid
    # 3) Insertar
    exec_with_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), NULLIF(%s,''), %s, %s, %s)
    """, (ean, nombre or "", marca or "", None, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    sku = _truncate(str(r.get("sku_id") or r.get("product_id") or "") or "", MAXLEN_NOMBRE)
    record_id = sku
    url = _truncate(r.get("url"), MAXLEN_NOMBRE)
    nombre_tienda = _truncate(r.get("nombre"), MAXLEN_NOMBRE_TIENDA)
    if sku:
        exec_with_retry(cur, """
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid
    exec_with_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en):
    pl = _price_str(r.get("precio_lista"))
    po = _price_str(r.get("precio_oferta") or r.get("precio_lista"))  # usa lista si no hay oferta
    tipo = None
    promo_tipo = None
    promo_texto = _truncate(r.get("oferta_tags"), MAXLEN_COMENTARIOS)
    exec_with_retry(cur, """
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = VALUES(promo_tipo),
          promo_texto_regular = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios = VALUES(promo_comentarios)
    """, (tienda_id, producto_tienda_id, capturado_en, pl, po, tipo, promo_tipo, None, None, promo_texto))

def ingest_to_mysql(df: pd.DataFrame, batch_size: int = 100):
    """Ingresa el DataFrame resultante en las tablas de la base de datos."""
    if df is None or df.empty:
        print("[DB] No hay filas para ingestar.")
        return
    conn = None
    total = 0
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cset:
                cset.execute("SET SESSION innodb_lock_wait_timeout = 5")
                cset.execute("SET SESSION net_read_timeout  = 60")
                cset.execute("SET SESSION net_write_timeout = 60")
                cset.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception:
            pass
        conn.autocommit = False
        cur = conn.cursor(buffered=True)
        capturado_en = dt.now()
        grp_cols = ["supermercado", "dominio"]
        if not all(c in df.columns for c in grp_cols):
            print("[DB] Faltan columnas de tienda en el DataFrame.")
            return
        for (supername, base), df_g in df.groupby(grp_cols):
            tienda_codigo = _domain_with_scheme(base) or base
            tienda_nombre = supername or tienda_codigo
            # upsert tienda con reintento
            for attempt in range(6):
                try:
                    tienda_id = upsert_tienda(cur, tienda_codigo, tienda_nombre)
                    conn.commit()
                    break
                except mysql.connector.Error as e:
                    if should_retry(e) and attempt < 5:
                        conn.rollback()
                        time.sleep(0.25 * (2 ** attempt))
                        continue
                    raise
                except Exception:
                    conn.rollback()
                    raise
            batch = 0
            for _, r in df_g.iterrows():
                rec = r.to_dict()
                rec["nombre"] = rec.get("nombre") or ""
                for attempt in range(6):
                    try:
                        pid = find_or_create_producto(cur, rec)
                        ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)
                        insert_historico(cur, tienda_id, ptid, rec, capturado_en)
                        total += 1
                        batch += 1
                        if batch >= min(20, batch_size):
                            conn.commit()
                            batch = 0
                        break
                    except myerr.DatabaseError as e:
                        errno = getattr(e, "errno", None)
                        if errno == 1264:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            rec2 = dict(rec)
                            rec2["precio_lista"] = None
                            rec2["precio_oferta"] = None
                            try:
                                pid = find_or_create_producto(cur, rec2)
                                ptid = upsert_producto_tienda(cur, tienda_id, pid, rec2)
                                insert_historico(cur, tienda_id, ptid, rec2, capturado_en)
                                total += 1
                                batch += 1
                                if batch >= min(20, batch_size):
                                    conn.commit()
                                    batch = 0
                                break
                            except Exception as e2:
                                if should_retry(e2) and attempt < 5:
                                    conn.rollback()
                                    time.sleep(0.25 * (2 ** attempt))
                                    continue
                                conn.rollback()
                                break
                        if should_retry(e) and attempt < 5:
                            conn.rollback()
                            time.sleep(0.25 * (2 ** attempt))
                            continue
                        conn.rollback()
                        break
                    except Exception:
                        conn.rollback()
                        break
            if batch:
                conn.commit()
        print(f"[DB] ✅ Inserciones/updates en historico_precios: {total}")
    except mysql.connector.Error as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"[DB] ❌ MySQL error {getattr(e, 'errno', None)}: {e}")
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"[DB] ❌ Error de ingesta: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

# ============== Flujo principal ==============

def main():
    parser = argparse.ArgumentParser(description="Reporte multi‑supermercado (VTEX) por EAN — Script standalone")
    parser.add_argument("--ean-column", dest="ean_column", default=None, help="Nombre de la columna con EAN (opcional).")
    parser.add_argument("--extra-domains", dest="extra_domains", default="", help="Dominios extra separados por espacios o líneas.")
    parser.add_argument("--sc-overrides", dest="sc_overrides", default="", help='JSON opcional {dominio: sc}.')
    parser.add_argument("--stores-in-parallel", type=int, default=3, help="Tiendas en paralelo (default: 3).")
    parser.add_argument("--per-store-delay", type=float, default=0.15, help="Pausa por solicitud y tienda (default: 0.15s).")
    parser.add_argument("--retries-req", type=int, default=2, help="Reintentos HTTP leves (default: 2).")
    parser.add_argument("--force-all", action="store_true", default=True, help="Forzar consulta a todos los candidatos (default: True).")
    parser.add_argument("--no-force-all", dest="force_all", action="store_false", help="Desactivar 'force-all'.")
    parser.add_argument("--save-to-db", action="store_true", default=False, help="Guardar en MySQL al finalizar.")
    parser.add_argument("--batch-size", type=int, default=100, help="Tamaño de mini-lote (commit) para DB.")
    args = parser.parse_args()

    # 🔴 Forzar ingesta en DB SIEMPRE
    args.save_to_db = True
    print("[DB] Ingesta a MySQL: FORZADA (save_to_db=True)")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, "maestro.xlsx")
    out_path = os.path.join(script_dir, "reporte_multi_super_vtex_por_ean.xlsx")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"No encontré {input_path}. Coloca 'maestro.xlsx' en la misma carpeta del script.")
    print(f"[IO] Leyendo: {input_path}")
    try:
        df_in = pd.read_excel(input_path)
    except Exception as e:
        raise RuntimeError(f"No pude leer el Excel maestro.xlsx: {e}")

    candidatos_cols = {
        "ean", "codigo", "codigo_barras", "codigo_barra", "barcode",
        "codigo de barras", "cod_barras", "cod_barra"
    }
    if args.ean_column:
        col_ean = args.ean_column
        if col_ean not in df_in.columns:
            raise ValueError(f"La columna '{col_ean}' no existe en el archivo.")
    else:
        posibles = [c for c in df_in.columns if str(c).lower() in candidatos_cols]
        col_ean = posibles[0] if posibles else df_in.columns[0]
        print(f"[INFO] Columna EAN seleccionada: '{col_ean}'")
    # Procesar dominios extra
    extra_domains = []
    if args.extra_domains.strip():
        extra_domains = [x.strip() for x in args.extra_domains.replace("\r", "\n").split() if x.strip()]
    # Overrides de sc
    sc_map = {}
    if args.sc_overrides.strip():
        try:
            sc_map = json.loads(args.sc_overrides)
            if not isinstance(sc_map, dict):
                print("[WARN] El JSON de overrides debe ser {dominio: sc}. Ignorando.")
                sc_map = {}
        except Exception as e:
            print(f"[WARN] No pude parsear overrides: {e}")
            sc_map = {}
    print("[INFO] Detectando qué dominios responden como VTEX…")
    detected = detectar_tiendas_vtex(VTEX_CANDIDATES_AR, extra_domains, retries=args.retries_req)
    def _norm_extra(d: str) -> Dict[str, str]:
        d = d.strip()
        if not (d.startswith("http://") or d.startswith("https://")):
            d = "https://" + d
        return {"name": d, "base": d, "default_sc": ""}
    candidates_all = VTEX_CANDIDATES_AR + [_norm_extra(x) for x in extra_domains]
    vtex_stores = candidates_all if args.force_all else detected
    if not args.force_all and len(vtex_stores) <= 1:
        print("[WARN] Pocas tiendas detectadas. Fallback: se consultarán todos los candidatos.")
        vtex_stores = candidates_all
    # Elimina duplicados por base
    vtex_stores = list({e["base"]: e for e in vtex_stores}.values())
    if not vtex_stores:
        raise RuntimeError("No hay tiendas para consultar. Revisa la lista de candidatos o agrega dominios.")
    print("[INFO] Tiendas que se consultarán (cada una secuencial):")
    for s in vtex_stores:
        print(f"  - {s.get('name','')} :: {s['base']} (sc={s.get('default_sc','')})")
    # EANs únicos
    eans_all = normalizar_columna_ean(df_in, col_ean).dropna().unique().tolist()
    if not eans_all:
        raise RuntimeError("No encontré EANs válidos en esa columna.")
    # Crear sesiones por tienda
    SESSIONS: Dict[str, requests.Session] = {s["base"]: make_session(retries=args.retries_req) for s in vtex_stores}
    total_estimado = len(eans_all) * len(vtex_stores)
    done = 0
    rows: List[Dict[str, Any]] = []
    errores: List[Tuple[str, str, str]] = []
    def progreso():
        pct = (done / max(1, total_estimado)) * 100.0
        print(f"\r[RUN] Progreso: {done}/{total_estimado} ({pct:5.1f}%)", end="", flush=True)
    def add_empty_row(store, base, e, estado: str):
        rows.append({
            "supermercado": store.get("name", base),
            "dominio": base,
            "estado_llamada": estado,
            "ean_consultado": e,
            "ean_reportado": None,
            "nombre": None, "marca": None,
            "categoria": None, "subcategoria": None,
            "precio_lista": None, "precio_oferta": None, "disponible": None,
            "oferta_tags": None,
            "product_id": None, "sku_id": None, "seller_id": None, "url": None
        })
    def worker_store(store):
        nonlocal done
        base = store["base"]
        session = SESSIONS[base]
        # Detectar sc por override o auto
        sc = sc_map.get(base) or store.get("default_sc") or discover_sc(session, base)
        # Obtener cp y país para simulación
        postal_code, country = discover_region_country(session, base)
        for ean in eans_all:
            estado, productos = vtex_buscar_por_ean_session(session, base, ean, sc=sc)
            try:
                if estado == "OK" and productos:
                    for prod in productos:
                        filas = parsear_producto_vtex(prod, ean, base)
                        for f in filas:
                            # Si no hay precio válido, simular
                            pl = f.get("precio_lista")
                            po = f.get("precio_oferta")
                            # Considera 0 o None como inválido
                            if (pl is None or pl == 0) and (po is None or po == 0):
                                sku = f.get("sku_id")
                                seller = f.get("seller_id") or "1"
                                if sku:
                                    sim_lista, sim_oferta = simulate_price(session, base, str(sku), str(seller), sc, postal_code, country)
                                    if sim_lista or sim_oferta:
                                        f["precio_lista"] = sim_lista
                                        f["precio_oferta"] = sim_oferta or sim_lista
                            rows.append({
                                "supermercado": store.get("name", base),
                                "dominio": base,
                                "estado_llamada": "OK",
                                "ean_consultado": ean,
                                "ean_reportado": f.get("ean_reportado"),
                                "nombre": f.get("nombre"),
                                "marca": f.get("marca"),
                                "categoria": f.get("categoria"),
                                "subcategoria": f.get("subcategoria"),
                                "precio_lista": f.get("precio_lista"),
                                "precio_oferta": f.get("precio_oferta"),
                                "disponible": f.get("disponible"),
                                "oferta_tags": f.get("oferta_tags"),
                                "product_id": f.get("product_id"),
                                "sku_id": f.get("sku_id"),
                                "seller_id": f.get("seller_id"),
                                "url": f.get("url"),
                            })
                else:
                    # Cuando no hay coincidencia o hay un error, agrega fila vacía
                    add_empty_row(store, base, ean, estado if estado else "NO_MATCH")
            except Exception as ex:
                errores.append((base, ean, str(ex)))
                add_empty_row(store, base, ean, "ERROR")
            done += 1
            progreso()
            if args.per_store_delay > 0:
                time.sleep(jitter_delay(args.per_store_delay))
    print(f"[RUN] Tiendas en paralelo: {args.stores_in_parallel} | Delay por tienda: {args.per_store_delay}s | Retries: {args.retries_req}")
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.stores_in_parallel) as executor:
        futs = [executor.submit(worker_store, store) for store in vtex_stores]
        for f in concurrent.futures.as_completed(futs):
            try:
                _ = f.result()
            except Exception as ex:
                print(f"\n[WARN] Worker con excepción: {ex}")
    print("\n[OK] Recolección finalizada.")
    df_out = pd.DataFrame(rows)
    order_cols = [
        "supermercado", "dominio", "estado_llamada",
        "ean_consultado", "ean_reportado",
        "nombre", "marca",
        "categoria", "subcategoria",
        "precio_lista", "precio_oferta", "disponible",
        "oferta_tags",
        "product_id", "sku_id", "seller_id",
        "url"
    ]
    cols_presentes = [c for c in order_cols if c in df_out.columns] + [c for c in df_out.columns if c not in order_cols]
    if not df_out.empty:
        df_out = df_out[cols_presentes]
    out_path = os.path.join(script_dir, "reporte_multi_super_vtex_por_ean.xlsx")
    print(f"[IO] Escribiendo Excel: {out_path}")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_in.assign(**{f"{col_ean}_normalizado": normalizar_columna_ean(df_in, col_ean)}).to_excel(
            writer, index=False, sheet_name="entrada"
        )
        if not df_out.empty:
            df_out.to_excel(writer, index=False, sheet_name="reporte")
            for dom, df_g in df_out.groupby(["supermercado", "dominio"]):
                sheet = f"{dom[0][:20]}".strip() or "tienda"
                try:
                    df_g.to_excel(writer, index=False, sheet_name=sheet)
                except Exception:
                    short = dom[1].replace("https://", "").replace("http://", "").split("/")[0]
                    sheet = (f"{dom[0][:15]}-{short[:10]}")[:31]
                    df_g.to_excel(writer, index=False, sheet_name=sheet)
            (
                df_out.groupby(["supermercado", "dominio", "estado_llamada"], dropna=False)["ean_consultado"]
                .nunique()
                .rename("EANs_consultados_unicos")
                .reset_index()
                .to_excel(writer, index=False, sheet_name="resumen")
            )
    print(f"[OK] Excel generado: {out_path}")
    if not df_out.empty:
        print("\n[QA] Resumen por tienda/estado:")
        resumen = (
            df_out.groupby(["supermercado", "dominio", "estado_llamada"], dropna=False)["ean_consultado"]
            .nunique().rename("EANs_unicos").reset_index()
        )
        for _, r in resumen.iterrows():
            print(f" - {r['supermercado']} [{r['dominio']}]: {r['estado_llamada']} -> {r['EANs_unicos']}")
    if not df_out.empty:
        print("[DB] Guardando en MySQL…")
        ingest_to_mysql(df_out, batch_size=args.batch_size)
    else:
        print("[DB] No hay filas para ingestar.")
    print("[DONE] Proceso completado.")

if __name__ == "__main__":
    main()