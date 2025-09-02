#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Alvear Online → MySQL

- Baja catálogo via API (GetCatalagoSeleccionado).
- Aplana items y calcula precio efectivo.
- Inserta/actualiza:
  - tiendas (codigo='alvear', nombre='Alvear Online')
  - productos (EAN = NULL)
  - producto_tienda (clave natural: tienda_id + codigoInterno)
  - historico_precios (precios como VARCHAR, según tu preferencia)

Requisitos:
  pip install requests pandas certifi mysql-connector-python
y un módulo base_datos.py con get_conn() que devuelva una conexión MySQL.
"""

import os
import math
import time
import random
import warnings
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import requests
import pandas as pd
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning
from mysql.connector import Error as MySQLError

import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

# ===================== Config de negocio =====================
TIENDA_CODIGO = "alvear"
TIENDA_NOMBRE = "Alvear Online"

# Catálogo a relevar
ID_CATALOGO = 1042
PAGE_SIZE = 20
START_PAGE = 0
MAX_PAGES_CAP = 1000
ID_INSTALACION = 3
ES_RUBRO = False
VISTA_FAVORITOS = False
SLEEP_BETWEEN_PAGES = 0.3
FORCE_INSECURE = True  # desactiva verificación TLS si tu entorno rompe certs

# Endpoint
BASE = "https://www.alvearonline.com.ar/BackOnline/api/Catalogo/GetCatalagoSeleccionado"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.alvearonline.com.ar",
    "Referer": "https://www.alvearonline.com.ar/",
    "Connection": "keep-alive",
}

# ===================== Red/SSL =====================
def resolve_verify() -> bool | str:
    if FORCE_INSECURE or os.environ.get("NO_SSL_VERIFY", "").strip() == "1":
        warnings.simplefilter("ignore", InsecureRequestWarning)
        return False
    bundle = os.environ.get("REQUESTS_CA_BUNDLE")
    return bundle if bundle else certifi.where()

def make_session(
    retries: int = 5,
    backoff: float = 1.0,
    connect_timeout: int = 20,
    read_timeout: int = 90,
) -> Tuple[requests.Session, Tuple[int, int]]:
    s = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.headers.update(DEFAULT_HEADERS)
    s.verify = resolve_verify()
    s.trust_env = True
    default_timeout = (connect_timeout, read_timeout)

    orig_request = s.request
    def _wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = default_timeout
        return orig_request(method, url, **kwargs)
    s.request = _wrapped
    return s, default_timeout

def jitter_delay(base: float, factor: float = 0.3) -> float:
    return base * (1.0 + random.uniform(-factor, factor))

def get_json_resilient(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: Tuple[int, int],
    attempts: int = 5,
    base_backoff: float = 1.5,
) -> Dict[str, Any]:
    last_exc = None
    for i in range(1, attempts + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.SSLError as e:
            last_exc = e
            try:
                warnings.simplefilter("ignore", InsecureRequestWarning)
                r = session.get(url, params=params, timeout=timeout, verify=False)
                r.raise_for_status()
                return r.json()
            except Exception as e2:
                last_exc = e2
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError) as e:
            last_exc = e
            sleep_s = jitter_delay(base_backoff ** i)
            print(f"[NET] Reintento {i}/{attempts} tras '{type(e).__name__}': {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue
        except requests.HTTPError as e:
            last_exc = e
            if 500 <= e.response.status_code < 600 and i < attempts:
                sleep_s = jitter_delay(base_backoff ** i)
                print(f"[HTTP {e.response.status_code}] Reintento {i}/{attempts} en {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise
    raise requests.exceptions.RequestException(f"Fallo tras múltiples intentos: {last_exc}")

# ===================== Helpers de datos =====================
def norm_img_path(p: Optional[str]) -> Optional[str]:
    if not p:
        return None
    p = str(p).replace("\\", "/")
    if p.startswith("http://") or p.startswith("https://"):
        return p
    if p.startswith("//"):
        return "https:" + p
    return "https://www.alvearonline.com.ar" + ("" if p.startswith("/") else "/") + p

def precio_efectivo(precio_lista: Optional[float], precio_promocional: Optional[float]) -> Optional[float]:
    if precio_promocional is not None and precio_promocional > 0:
        if precio_lista is None:
            return precio_promocional
        return min(precio_lista, precio_promocional)
    return precio_lista

def parse_page_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    secciones = data.get("listadoSecciones") or []
    if isinstance(secciones, list):
        for sec in secciones:
            lista = sec.get("listaArticulos") or []
            if isinstance(lista, list):
                items.extend(lista)
    return items

def total_pages_from_payload(data: Dict[str, Any], page_size: int) -> Optional[int]:
    total = data.get("cantidadArticulosFiltrados")
    if isinstance(total, int) and page_size > 0:
        return math.ceil(total / page_size)
    return None

def flatten_item(it: Dict[str, Any]) -> Dict[str, Any]:
    img_url = None
    for im in (it.get("listaImagenesArticulos") or []):
        if isinstance(im, dict) and im.get("path"):
            img_url = norm_img_path(im["path"])
            if img_url:
                break
    row = {
        "idArticulo": it.get("idArticulo"),
        "idCatalogoEntrada": it.get("idCatalogoEntrada"),
        "orden": it.get("orden"),
        "nombre": it.get("nombre"),
        "datosExtra": it.get("datosExtra"),
        "codigoInterno": it.get("codigoInterno"),  # SKU tienda
        "modelo": it.get("modelo"),
        "precioLista": it.get("precioLista"),
        "porcentajeImpuestosNacionales": it.get("porcentajeImpuestosNacionales"),
        "precioSinImpuestos": it.get("precioSinImpuestos"),
        "precioPromocional": it.get("precioPromocional"),
        "porcentajeDescuento": it.get("porcentajeDescuento"),
        "cantidadLimMax": it.get("cantidadLimMax"),
        "cantidadLimMin": it.get("cantidadLimMin"),
        "incremento": it.get("incremento"),
        "descripcion": it.get("descripcion"),
        "fechaRegistro": it.get("fechaRegistro"),
        "activo": it.get("activo"),
        "catalogoEntradaActivo": it.get("catalogoEntradaActivo"),
        "stockDisponible": it.get("stockDisponible"),
        "productoPesable": it.get("productoPesable"),
        "idRubro": it.get("idRubro"),
        "idSubrubro": it.get("idSubrubro"),
        "idOferta": it.get("idOferta"),
        "idMarca": it.get("idMarca"),
        "imagen": img_url,
        "pathSello": norm_img_path(it.get("pathSello")),
    }
    row["precioEfectivo"] = precio_efectivo(row["precioLista"], row["precioPromocional"])
    return row

# ===================== Descarga con paginación =====================
def fetch_catalogo_all(session: requests.Session, timeout: Tuple[int, int]) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    total_pages: Optional[int] = None
    page = START_PAGE

    while True:
        params = {
            "idCatalogo": ID_CATALOGO,
            "subfiltros": "",
            "page": page,
            "pageSize": PAGE_SIZE,
            "idInstalacion": ID_INSTALACION,
            "esRubro": str(ES_RUBRO).lower(),
            "vistaFavoritos": str(VISTA_FAVORITOS).lower(),
        }

        data = get_json_resilient(session, BASE, params, timeout=timeout, attempts=5, base_backoff=1.5)

        if total_pages is None:
            total_pages = total_pages_from_payload(data, PAGE_SIZE)
            total_reg = data.get("cantidadArticulosFiltrados")
            if total_pages:
                print(f"[INFO] Total estimado: {total_pages} páginas (~{total_reg} items)")

        items = parse_page_items(data)
        if not items:
            print(f"[INFO] Página {page}: sin items. Fin.")
            break

        for it in items:
            row = flatten_item(it)
            all_rows.append(row)
            print(f"[P{page}] {row.get('nombre')}  |  ${row.get('precioEfectivo')}  |  SKU:{row.get('codigoInterno')}")

        page += 1

        if total_pages is not None and page >= total_pages:
            break
        if (page - START_PAGE) >= MAX_PAGES_CAP:
            print(f"[WARN] Cap de páginas alcanzado ({MAX_PAGES_CAP}). Corto.")
            break

        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_rows

# ===================== MySQL helpers =====================
def clean_txt(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    return s

def parse_price_to_varchar(x: Any) -> Optional[str]:
    """Devuelve precio como VARCHAR (o None) según tu preferencia de almacenar texto."""
    if x is None:
        return None
    try:
        v = float(x)
        if np.isnan(v):
            return None
        return f"{round(v, 2)}"
    except Exception:
        # si viene string ya bien, déjalo
        s = str(x).strip()
        return s if s else None

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, row: Dict[str, Any]) -> int:
    """
    En Alvear no hay EAN → guardamos NULL.
    Intentamos evitar dupli
    cados usando (nombre, idMarca) si ambos existen;
    si no, solo nombre (riesgo aceptado).
    """
    ean = None  # explícito
    nombre = clean_txt(row.get("nombre"))
    marca_id = row.get("idMarca")

    if nombre and marca_id is not None:
        cur.execute(
            "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
            (nombre, str(marca_id))
        )
        r = cur.fetchone()
        if r:
            pid = r[0]
            # Actualizamos metadatos suaves (categorías por ids si quieres)
            cur.execute("""
                UPDATE productos SET
                  fabricante = fabricante,
                  categoria = COALESCE(categoria, %s),
                  subcategoria = COALESCE(subcategoria, %s)
                WHERE id=%s
            """, (str(row.get("idRubro") or "") or None,
                  str(row.get("idSubrubro") or "") or None,
                  pid))
            return pid

    if nombre:
        cur.execute(
            "SELECT id FROM productos WHERE nombre=%s LIMIT 1",
            (nombre,)
        )
        r = cur.fetchone()
        if r:
            return r[0]

    # Insert nuevo
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULLIF(%s,''), NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (
        nombre or "",
        str(marca_id) if marca_id is not None else "",
        str(row.get("idRubro") or ""),
        str(row.get("idSubrubro") or "")
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, row: Dict[str, Any]) -> int:
    """
    Clave natural preferida: (tienda_id, codigoInterno) como sku_tienda.
    Respaldo: (tienda_id, record_id_tienda) con idArticulo.
    """
    sku = clean_txt(row.get("codigoInterno"))
    record_id = clean_txt(row.get("idArticulo"))
    url = None  # API no trae URL de PDP directa
    nombre_tienda = clean_txt(row.get("nombre"))

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    if record_id:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    # Último recurso (sin llaves naturales)
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, row: Dict[str, Any], capturado_en: datetime):
    precio_lista = parse_price_to_varchar(row.get("precioLista"))
    precio_oferta = parse_price_to_varchar(row.get("precioPromocional"))
    # Si quieres guardar precio_efectivo también, calcula y guarda en promo_texto_regular, por ejemplo
    precio_efectivo_txt = parse_price_to_varchar(precio_efectivo(row.get("precioLista"), row.get("precioPromocional")))
    promo_tipo = None
    promo_txt_regular = precio_efectivo_txt  # opcional: almacenar aquí el efectivo
    promo_txt_desc = None
    promo_comentarios = None

    cur.execute("""
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
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista, precio_oferta, None,
        promo_tipo, promo_txt_regular, promo_txt_desc, promo_comentarios
    ))

# ===================== Main =====================
def main():
    print(f"[INFO] Descargando catálogo {ID_CATALOGO} (pageSize={PAGE_SIZE})...")
    if resolve_verify() is False:
        print("[WARN] TLS sin verificación (solo pruebas).")

    session, default_timeout = make_session()
    rows = fetch_catalogo_all(session, timeout=default_timeout)

    if not rows:
        print("[INFO] No se descargaron productos.")
        return

    capturado_en = datetime.now()

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for row in rows:
            producto_id = find_or_create_producto(cur, row)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, row)
            insert_historico(cur, tienda_id, pt_id, row, capturado_en)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
