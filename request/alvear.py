#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Alvear Online – Catálogo 1042

- Paginación desde page=0 hasta totalPages (con cantidadArticulosFiltrados/pageSize).
- La API devuelve productos dentro de listadoSecciones[].listaArticulos[] → se aplanan.
- Imprime en consola cada producto (página, nombre, precio efectivo, SKU).
- Normaliza rutas de imagen (backslashes → slashes).
- Exporta a Excel.

Robustez de red:
- Reintentos sobre SSLError / ReadTimeout / ConnectionError.
- Tiempo de espera ampliado (connect=20s, read=90s).
- Backoff exponencial con jitter.
- Modo inseguro por defecto (TLS verify desactivado) para redes con MITM.

Dependencias:
  pip install requests pandas openpyxl certifi
(En Windows con proxy/antivirus TLS, opcional: pip install certifi-win32)
"""

import os
import math
import time
import random
import warnings
from typing import List, Dict, Any, Optional, Tuple

import requests
import pandas as pd
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning

# ===================== Config por defecto =====================
ID_CATALOGO = 1042
PAGE_SIZE = 20
START_PAGE = 0
MAX_PAGES_CAP = 1000
ID_INSTALACION = 3
ES_RUBRO = False
VISTA_FAVORITOS = False
SLEEP_BETWEEN_PAGES = 0.3          # pausa suave entre páginas
OUT_PATH = f"alvear_catalogo_{ID_CATALOGO}.xlsx"

# ⚠️ Por tu entorno actual, dejo inseguro por defecto.
# Cuando soluciones el CA (REQUESTS_CA_BUNDLE o certifi-win32), cambia a False.
FORCE_INSECURE = True

# Endpoint (usar www para evitar mismatch de cert)
BASE = "https://www.alvearonline.com.ar/BackOnline/api/Catalogo/GetCatalagoSeleccionado"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.alvearonline.com.ar",
    "Referer": "https://www.alvearonline.com.ar/",
    "Connection": "keep-alive",
}

# ===================== Helpers de red/SSL =====================
def resolve_verify() -> bool | str:
    """
    Prioridad de verificación TLS:
      1) FORCE_INSECURE=True  → False (inseguro).
      2) NO_SSL_VERIFY=1      → False (inseguro).
      3) REQUESTS_CA_BUNDLE   → usar ruta custom.
      4) certifi.where()      → bundle por defecto.
    """
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
    s.trust_env = True  # respeta variables de entorno de proxy

    default_timeout = (connect_timeout, read_timeout)

    # Timeout por defecto
    orig_request = s.request
    def _wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = default_timeout
        return orig_request(method, url, **kwargs)
    s.request = _wrapped

    return s, default_timeout

def jitter_delay(base: float, factor: float = 0.3) -> float:
    """Devuelve un delay con jitter para evitar sincronías."""
    return base * (1.0 + random.uniform(-factor, factor))

def get_json_resilient(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: Tuple[int, int],
    attempts: int = 4,
    base_backoff: float = 1.2,
) -> Dict[str, Any]:
    """
    GET con reintentos manuales ante SSLError / ReadTimeout / ConnectionError.
    - Primer intento respetando verify configurado.
    - Si SSLError: reintenta inseguro (verify=False).
    - Si timeouts/conexión: reintenta con backoff exponencial + jitter.
    """
    last_exc = None
    for i in range(1, attempts + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.SSLError as e:
            last_exc = e
            # Reintento inmediato en modo inseguro
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
            # Backoff exponencial con jitter
            sleep_s = jitter_delay(base_backoff ** i)
            print(f"[NET] Reintento {i}/{attempts} tras error '{type(e).__name__}': durmiendo {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue
        except requests.HTTPError as e:
            # Errores HTTP (4xx/5xx). Si la API a veces da 5xx, el Retry del adapter ya ayudó.
            last_exc = e
            # Para 5xx, probamos de nuevo; para 4xx, no insistas mucho.
            if 500 <= e.response.status_code < 600 and i < attempts:
                sleep_s = jitter_delay(base_backoff ** i)
                print(f"[HTTP {e.response.status_code}] Reintento {i}/{attempts} en {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise
    # Si llegamos aquí, agotamos reintentos
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
        "codigoInterno": it.get("codigoInterno"),
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

# ===================== Exportar a Excel =====================
def export_excel(rows: List[Dict[str, Any]], out_path: str) -> None:
    if not rows:
        print("[INFO] No se encontraron productos para exportar.")
        return
    df = pd.DataFrame(rows)
    cols = [
        "idArticulo","idCatalogoEntrada","orden","nombre","datosExtra","codigoInterno","modelo",
        "precioLista","precioPromocional","precioEfectivo","porcentajeImpuestosNacionales",
        "precioSinImpuestos","porcentajeDescuento","cantidadLimMax","cantidadLimMin","incremento",
        "descripcion","fechaRegistro","activo","catalogoEntradaActivo","stockDisponible",
        "productoPesable","idRubro","idSubrubro","idOferta","idMarca","imagen","pathSello",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]
    df.to_excel(out_path, index=False)
    print(f"[OK] Exportado {len(df):,} productos a {out_path}")

# ===================== Main =====================
if __name__ == "__main__":
    print(f"[INFO] Descargando catálogo {ID_CATALOGO} (pageSize={PAGE_SIZE})...")
    if FORCE_INSECURE or os.environ.get("NO_SSL_VERIFY", "").strip() == "1":
        print("[WARN] TLS sin verificación (solo pruebas).")
    session, default_timeout = make_session()
    rows = fetch_catalogo_all(session, timeout=default_timeout)
    export_excel(rows, OUT_PATH)
