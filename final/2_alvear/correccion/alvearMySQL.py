#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Alvear Online → MySQL (robusto por horarios)

Objetivo: que NO se muera si a partir de cierta hora el backend empieza a tirar 500/403/429 o responde HTML.

Cambios clave:
- PAGE_SIZE más chico + sleep más grande (evita rate-limit / saturación).
- get_json_resilient “infinito controlado”: nunca crashea por 5xx/429/403; enfría y reintenta.
- Detecta no-JSON (HTML) y reintenta (muy común en WAF).
- No corta por 1 página vacía (grace).
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
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning
from mysql.connector import Error as MySQLError

import sys

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

# ===================== Config de negocio =====================
TIENDA_CODIGO = "alvear"
TIENDA_NOMBRE = "Alvear Online"

ID_CATALOGO = 1042
ID_INSTALACION = 3
ES_RUBRO = False
VISTA_FAVORITOS = False

# ↓↓↓ Ajustes anti-“después de las 5 AM” ↓↓↓
PAGE_SIZE = int(os.environ.get("ALVEAR_PAGE_SIZE", "12"))          # antes 20
SLEEP_BETWEEN_PAGES = float(os.environ.get("ALVEAR_SLEEP", "1.2")) # antes 0.3

START_PAGE = int(os.environ.get("ALVEAR_START_PAGE", "0"))
MAX_PAGES_CAP = int(os.environ.get("ALVEAR_MAX_PAGES", "1000"))

# Si tu entorno rompe certs
FORCE_INSECURE = True

BASE = "https://www.alvearonline.com.ar/BackOnline/api/Catalogo/GetCatalagoSeleccionado"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    ),
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

def jitter_delay(base: float, factor: float = 0.35) -> float:
    return base * (1.0 + random.uniform(-factor, factor))

def _safe_snippet(txt: str, n: int = 250) -> str:
    if not txt:
        return ""
    return txt[:n].replace("\n", " ").replace("\r", " ")

def make_session(
    retries: int = 2,             # poco acá; el retry serio está en get_json_resilient
    backoff: float = 0.8,
    connect_timeout: int = 30,
    read_timeout: int = 240,
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
        respect_retry_after_header=True,
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

def get_json_resilient(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: Tuple[int, int],
    attempts: int = 8,
    base_backoff: float = 1.7,
    max_total_wait_s: int = 20 * 60,   # 20 min tolerancia por página (ajustable)
) -> Dict[str, Any]:
    """
    Devuelve dict JSON.
    - Reintenta 403/408/429/5xx y errores de red.
    - Si se agotan attempts, entra en cooldown largo y sigue reintentando.
    - Solo abandona tras max_total_wait_s acumulado intentando esa misma página.
    """
    start = time.time()
    cycle = 0
    last_exc: Optional[Exception] = None

    while True:
        cycle += 1

        for i in range(1, attempts + 1):
            try:
                r = session.get(url, params=params, timeout=timeout)

                status = r.status_code
                ct = (r.headers.get("Content-Type") or "").lower()

                # Si viene HTML/no-json, log y tratar como retry
                if "application/json" not in ct:
                    print(f"[WARN] page={params.get('page')} HTTP {status} CT={ct} snip='{_safe_snippet(r.text)}'")
                    # muchas veces es WAF/edge; forzamos retry
                    raise ValueError(f"Respuesta no-JSON (CT={ct})")

                # Si status “problemático”, retry
                if status in (403, 408, 429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"HTTP {status}", response=r)

                # OK
                data = r.json()
                if not isinstance(data, dict):
                    raise ValueError("JSON no es dict")
                return data

            except requests.exceptions.SSLError as e:
                last_exc = e
                # fallback insecure
                try:
                    warnings.simplefilter("ignore", InsecureRequestWarning)
                    r = session.get(url, params=params, timeout=timeout, verify=False)
                    status = r.status_code
                    if status in (403, 408, 429, 500, 502, 503, 504):
                        raise requests.HTTPError(f"HTTP {status}", response=r)
                    data = r.json()
                    if isinstance(data, dict):
                        return data
                    raise ValueError("JSON no es dict (insecure)")
                except Exception as e2:
                    last_exc = e2

            except (
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                ValueError,
            ) as e:
                last_exc = e
                code = getattr(getattr(e, "response", None), "status_code", None)

                # Backoff
                sleep_s = jitter_delay(base_backoff ** i)

                # Si es 500 repetido después de las 5am, enfriar más
                if code == 500:
                    sleep_s = max(sleep_s, jitter_delay(20.0))

                # Si es 403/429, enfriar más (posible rate-limit / WAF)
                if code in (403, 429):
                    sleep_s = max(sleep_s, jitter_delay(45.0))

                print(
                    f"[RETRY] ciclo={cycle} intento={i}/{attempts} page={params.get('page')} "
                    f"status={code} err={type(e).__name__} sleep={sleep_s:.1f}s"
                )
                time.sleep(sleep_s)
                continue

            except Exception as e:
                last_exc = e
                sleep_s = jitter_delay(base_backoff ** i)
                print(f"[RETRY] ciclo={cycle} intento={i}/{attempts} page={params.get('page')} err={type(e).__name__} sleep={sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue

        # Se agotaron intentos -> cooldown y seguir sin morir
        elapsed = time.time() - start
        if elapsed >= max_total_wait_s:
            raise requests.exceptions.RequestException(
                f"Fallo sostenido en page={params.get('page')} tras {int(elapsed)}s. Último error: {last_exc}"
            )

        cooldown = jitter_delay(90.0)  # 1.5 min aprox
        print(f"[COOLDOWN] Agoté {attempts} intentos. Espero {cooldown:.1f}s y reintento page={params.get('page')}.")
        time.sleep(cooldown)

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

    empty_pages_in_a_row = 0
    MAX_EMPTY_PAGES_GRACE = 3

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

        try:
            data = get_json_resilient(session, BASE, params, timeout=timeout, attempts=8, base_backoff=1.7)
        except Exception as e:
            # Si una página está “rota”, no mates la corrida
            print(f"[WARN] page={page} fallo sostenido ({type(e).__name__}). Espero 120s y reintento la misma página.")
            time.sleep(jitter_delay(120.0))
            continue

        if total_pages is None:
            total_pages = total_pages_from_payload(data, PAGE_SIZE)
            total_reg = data.get("cantidadArticulosFiltrados")
            if total_pages:
                print(f"[INFO] Total estimado: {total_pages} páginas (~{total_reg} items)")

        # payload inesperado -> reintentar
        if not isinstance(data, dict) or "listadoSecciones" not in data:
            print(f"[WARN] Payload inesperado en page={page}. Espero 60s y reintento.")
            time.sleep(jitter_delay(60.0))
            continue

        items = parse_page_items(data)

        if not items:
            empty_pages_in_a_row += 1
            print(f"[WARN] page={page}: sin items (vacías seguidas={empty_pages_in_a_row}/{MAX_EMPTY_PAGES_GRACE}).")
            if empty_pages_in_a_row >= MAX_EMPTY_PAGES_GRACE:
                print("[INFO] Varias páginas vacías seguidas. Fin real.")
                break
            time.sleep(jitter_delay(10.0))
            continue

        empty_pages_in_a_row = 0

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

        time.sleep(jitter_delay(SLEEP_BETWEEN_PAGES))

    return all_rows

# ===================== MySQL helpers =====================
def clean_txt(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def parse_price_to_varchar(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        v = float(x)
        if np.isnan(v):
            return None
        return f"{round(v, 2)}"
    except Exception:
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
            cur.execute("""
                UPDATE productos SET
                  categoria = COALESCE(categoria, %s),
                  subcategoria = COALESCE(subcategoria, %s)
                WHERE id=%s
            """, (
                (str(row.get("idRubro") or "") or None),
                (str(row.get("idSubrubro") or "") or None),
                pid
            ))
            return pid

    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        r = cur.fetchone()
        if r:
            return r[0]

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
    sku = clean_txt(row.get("codigoInterno"))
    record_id = clean_txt(row.get("idArticulo"))
    url = None
    nombre_tienda = clean_txt(row.get("nombre"))

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
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

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, row: Dict[str, Any], capturado_en: datetime):
    precio_lista = parse_price_to_varchar(row.get("precioLista"))
    precio_oferta = parse_price_to_varchar(row.get("precioPromocional"))
    precio_efectivo_txt = parse_price_to_varchar(
        precio_efectivo(row.get("precioLista"), row.get("precioPromocional"))
    )

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
        None, precio_efectivo_txt, None, None
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
        if conn:
            conn.rollback()
        print(f"❌ Error MySQL: {e}")

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
