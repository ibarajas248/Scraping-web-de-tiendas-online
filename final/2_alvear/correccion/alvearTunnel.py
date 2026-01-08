#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Alvear Online → MySQL (24/7 ultra-resiliente) + SSH Tunnel

✅ Igual a tu script, pero en vez de usar base_datos.get_conn() conecta a MySQL
   a través de un túnel SSH (SSHTunnelForwarder).

Requisitos:
    pip install sshtunnel mysql-connector-python requests certifi numpy
"""

import os
import math
import time
import random
import json
import warnings
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import requests
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning

import mysql.connector
from mysql.connector import Error as MySQLError

from sshtunnel import SSHTunnelForwarder

# =================== Config SSH / MySQL ===================
SSH_HOST = "scrap.intelligenceblue.com.ar"
SSH_USER = "scrap-ssh"
SSH_PASS = "gLqqVHswm42QjbdvitJ0"

# OJO: este DB_HOST es el host VISTO DESDE EL SERVIDOR (remote_bind_address)
DB_HOST = "127.0.0.1"
DB_USER = "userscrap"
DB_PASS = "UY8rMSGcHUunSsyJE4c7"
DB_NAME = "scrap"
DB_PORT = 3306

# ===================== Config de negocio =====================
TIENDA_CODIGO = "alvear"
TIENDA_NOMBRE = "Alvear Online"

ID_CATALOGO = 1042
ID_INSTALACION = 3
ES_RUBRO = False
VISTA_FAVORITOS = False

# Ajustes anti-rate-limit
PAGE_SIZE = int(os.environ.get("ALVEAR_PAGE_SIZE", "12"))
SLEEP_BASE = float(os.environ.get("ALVEAR_SLEEP", "1.2"))

START_PAGE = int(os.environ.get("ALVEAR_START_PAGE", "0"))
MAX_PAGES_CAP = int(os.environ.get("ALVEAR_MAX_PAGES", "1000"))

# TLS
FORCE_INSECURE = os.environ.get("ALVEAR_FORCE_INSECURE", "1").strip() == "1"

# Control de tolerancia por página
MAX_WAIT_PER_PAGE_S = int(os.environ.get("ALVEAR_MAX_WAIT_PER_PAGE_S", str(20 * 60)))  # 20 min
MAX_EMPTY_PAGES_GRACE = int(os.environ.get("ALVEAR_EMPTY_GRACE", "3"))

# Checkpoint
CHECKPOINT_PATH = os.environ.get("ALVEAR_CHECKPOINT", "alvear_checkpoint.json")

BASE = "https://www.alvearonline.com.ar/BackOnline/api/Catalogo/GetCatalagoSeleccionado"

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
]

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.alvearonline.com.ar",
    "Referer": "https://www.alvearonline.com.ar/",
    "Connection": "keep-alive",
}

# ===================== Utils =====================
def resolve_verify() -> bool | str:
    if FORCE_INSECURE or os.environ.get("NO_SSL_VERIFY", "").strip() == "1":
        warnings.simplefilter("ignore", InsecureRequestWarning)
        return False
    bundle = os.environ.get("REQUESTS_CA_BUNDLE")
    return bundle if bundle else certifi.where()

def jitter_delay(base: float, factor: float = 0.35) -> float:
    return max(0.0, base * (1.0 + random.uniform(-factor, factor)))

def _safe_snippet(txt: str, n: int = 250) -> str:
    if not txt:
        return ""
    return txt[:n].replace("\n", " ").replace("\r", " ")

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def load_checkpoint() -> Dict[str, Any]:
    try:
        if os.path.exists(CHECKPOINT_PATH):
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_checkpoint(page: int, extra: Optional[Dict[str, Any]] = None) -> None:
    payload = {"page": page, "ts": now_iso()}
    if extra:
        payload.update(extra)
    tmp = CHECKPOINT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CHECKPOINT_PATH)

def pick_ua() -> str:
    return random.choice(UA_LIST)

def is_likely_html(ct: str, text: str) -> bool:
    ct = (ct or "").lower()
    if "text/html" in ct:
        return True
    t = (text or "").lstrip().lower()
    return t.startswith("<!doctype html") or t.startswith("<html") or "<head" in t[:300]

# ===================== Sesión HTTP =====================
def make_session(
    retries: int = 1,
    backoff: float = 0.4,
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
    s.headers["User-Agent"] = pick_ua()
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

def refresh_session(old: Optional[requests.Session]) -> requests.Session:
    try:
        if old:
            old.close()
    except Exception:
        pass
    s, _ = make_session()
    return s

# ===================== HTTP resiliente (24/7) =====================
def get_json_247(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: Tuple[int, int],
    attempts: int = 10,
    base_backoff: float = 1.7,
    max_total_wait_s: int = MAX_WAIT_PER_PAGE_S,
) -> Tuple[Optional[Dict[str, Any]], requests.Session, Dict[str, Any]]:
    start = time.time()
    cycle = 0
    last_exc: Optional[Exception] = None
    meta = {"blocked": False, "status": None, "html": False}

    while True:
        cycle += 1
        for i in range(1, attempts + 1):
            try:
                if i == 1 and cycle > 1:
                    session.headers["User-Agent"] = pick_ua()

                r = session.get(url, params=params, timeout=timeout)
                status = r.status_code
                ct = (r.headers.get("Content-Type") or "")
                txt = r.text or ""

                meta["status"] = status
                meta["html"] = is_likely_html(ct, txt)

                if meta["html"] or ("application/json" not in ct.lower()):
                    meta["blocked"] = True
                    print(f"[WARN] page={params.get('page')} HTTP {status} CT={ct} snip='{_safe_snippet(txt)}'")
                    raise ValueError(f"Respuesta no-JSON/HTML (CT={ct})")

                if status in (403, 408, 429, 500, 502, 503, 504):
                    if status in (403, 429):
                        meta["blocked"] = True
                    raise requests.HTTPError(f"HTTP {status}", response=r)

                data = r.json()
                if not isinstance(data, dict):
                    raise ValueError("JSON no es dict")
                return data, session, meta

            except requests.exceptions.SSLError as e:
                last_exc = e
                try:
                    warnings.simplefilter("ignore", InsecureRequestWarning)
                    r = session.get(url, params=params, timeout=timeout, verify=False)
                    status = r.status_code
                    meta["status"] = status
                    if status in (403, 408, 429, 500, 502, 503, 504):
                        raise requests.HTTPError(f"HTTP {status}", response=r)
                    data = r.json()
                    if isinstance(data, dict):
                        return data, session, meta
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
                meta["status"] = code

                sleep_s = jitter_delay(base_backoff ** i)
                if code == 500:
                    sleep_s = max(sleep_s, jitter_delay(20.0))

                if code in (403, 429) or meta.get("html"):
                    sleep_s = max(sleep_s, jitter_delay(60.0))
                    if i >= 3:
                        session = refresh_session(session)

                print(
                    f"[RETRY] ciclo={cycle} intento={i}/{attempts} page={params.get('page')} "
                    f"status={code} err={type(e).__name__} sleep={sleep_s:.1f}s"
                )
                time.sleep(sleep_s)

                if time.time() - start >= max_total_wait_s:
                    print(
                        f"[SKIP] page={params.get('page')} superó max_wait={max_total_wait_s}s. "
                        f"Último error: {last_exc}"
                    )
                    return None, session, meta
                continue

            except Exception as e:
                last_exc = e
                sleep_s = jitter_delay(base_backoff ** i)
                print(f"[RETRY] ciclo={cycle} intento={i}/{attempts} page={params.get('page')} err={type(e).__name__} sleep={sleep_s:.1f}s")
                time.sleep(sleep_s)
                if time.time() - start >= max_total_wait_s:
                    print(
                        f"[SKIP] page={params.get('page')} superó max_wait={max_total_wait_s}s. "
                        f"Último error: {last_exc}"
                    )
                    return None, session, meta
                continue

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
        "precioSinImpuestos": it.get("precioSinImpuestos"),
        "precioPromocional": it.get("precioPromocional"),
        "porcentajeDescuento": it.get("porcentajeDescuento"),
        "descripcion": it.get("descripcion"),
        "fechaRegistro": it.get("fechaRegistro"),
        "activo": it.get("activo"),
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

# ===================== Descarga paginada =====================
def fetch_catalogo_all(session: requests.Session, timeout: Tuple[int, int]) -> Tuple[List[Dict[str, Any]], requests.Session]:
    all_rows: List[Dict[str, Any]] = []
    total_pages: Optional[int] = None

    ck = load_checkpoint()
    page = int(ck.get("page", START_PAGE))
    if page < START_PAGE:
        page = START_PAGE

    empty_pages_in_a_row = 0
    sleep_dynamic = SLEEP_BASE
    ok_streak = 0

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

        data, session, meta = get_json_247(
            session, BASE, params, timeout=timeout, attempts=10, base_backoff=1.7, max_total_wait_s=MAX_WAIT_PER_PAGE_S
        )

        if data is None:
            sleep_dynamic = min(max(sleep_dynamic, SLEEP_BASE) * 1.3, 30.0)
            ok_streak = 0
            print(f"[WARN] page={page} salteada por bloqueo sostenido. sleep_dynamic={sleep_dynamic:.1f}s")
            page += 1
            save_checkpoint(page, {"skipped_last": True, "sleep_dynamic": sleep_dynamic})
            time.sleep(jitter_delay(sleep_dynamic))
            continue

        if total_pages is None:
            total_pages = total_pages_from_payload(data, PAGE_SIZE)
            total_reg = data.get("cantidadArticulosFiltrados")
            if total_pages:
                print(f"[INFO] Total estimado: {total_pages} páginas (~{total_reg} items)")

        if not isinstance(data, dict) or "listadoSecciones" not in data:
            print(f"[WARN] Payload inesperado en page={page}. Reseteo sesión y reintento.")
            session = refresh_session(session)
            time.sleep(jitter_delay(max(10.0, sleep_dynamic)))
            continue

        items = parse_page_items(data)

        if not items:
            empty_pages_in_a_row += 1
            print(f"[WARN] page={page}: sin items (vacías seguidas={empty_pages_in_a_row}/{MAX_EMPTY_PAGES_GRACE}).")
            if empty_pages_in_a_row >= MAX_EMPTY_PAGES_GRACE:
                print("[INFO] Varias páginas vacías seguidas. Fin real.")
                break
            time.sleep(jitter_delay(max(8.0, sleep_dynamic)))
            page += 1
            save_checkpoint(page, {"empty_grace": empty_pages_in_a_row, "sleep_dynamic": sleep_dynamic})
            continue

        empty_pages_in_a_row = 0

        for it in items:
            row = flatten_item(it)
            all_rows.append(row)
            print(f"[P{page}] {row.get('nombre')}  |  ${row.get('precioEfectivo')}  |  SKU:{row.get('codigoInterno')}")

        if meta.get("blocked"):
            sleep_dynamic = min(max(sleep_dynamic, SLEEP_BASE) * 1.25, 30.0)
            ok_streak = 0
        else:
            ok_streak += 1
            if ok_streak >= 3:
                sleep_dynamic = max(SLEEP_BASE, sleep_dynamic * 0.85)
                ok_streak = 0

        page += 1
        save_checkpoint(page, {"sleep_dynamic": sleep_dynamic, "last_ok": True})

        if total_pages is not None and page >= total_pages:
            break
        if (page - START_PAGE) >= MAX_PAGES_CAP:
            print(f"[WARN] Cap de páginas alcanzado ({MAX_PAGES_CAP}). Corto.")
            break

        time.sleep(jitter_delay(sleep_dynamic))

    return all_rows, session

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
        return f"{v:.2f}"
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

# ===================== MySQL retry (1205/1213) =====================
def mysql_run_with_retry(conn, fn, max_tries: int = 6):
    last = None
    for i in range(1, max_tries + 1):
        try:
            return fn()
        except MySQLError as e:
            last = e
            msg = str(e)
            if ("1205" in msg) or ("1213" in msg):
                sleep_s = jitter_delay(min(2 ** i, 30))
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"[MySQL RETRY] intento={i}/{max_tries} {msg} sleep={sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise
    raise last

# ===================== SSH Tunnel + DB connect =====================
def open_ssh_tunnel() -> SSHTunnelForwarder:
    """
    Abre túnel local -> remoto: 127.0.0.1:<LOCAL_PORT> ==> (DB_HOST:DB_PORT) en el servidor.
    """
    print(f"[SSH] Conectando a {SSH_HOST} como {SSH_USER} ...")
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, 22),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASS,
        remote_bind_address=(DB_HOST, DB_PORT),
        local_bind_address=("127.0.0.1", 0),  # 0 = puerto libre automático
        set_keepalive=30,
    )
    tunnel.start()
    print(f"[SSH] Tunnel OK: 127.0.0.1:{tunnel.local_bind_port} -> {DB_HOST}:{DB_PORT}")
    return tunnel

def get_conn_via_tunnel(local_port: int):
    return mysql.connector.connect(
        host="127.0.0.1",
        port=local_port,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=False,
        connection_timeout=30,
    )

# ===================== Main =====================
def main():
    print(f"[INFO] Descargando catálogo {ID_CATALOGO} (pageSize={PAGE_SIZE})...")
    if resolve_verify() is False:
        print("[WARN] TLS sin verificación (solo si tu entorno rompe certs).")

    session, default_timeout = make_session()

    try:
        rows, session = fetch_catalogo_all(session, timeout=default_timeout)
    finally:
        try:
            session.close()
        except Exception:
            pass

    if not rows:
        print("[INFO] No se descargaron productos.")
        return

    capturado_en = datetime.now()

    tunnel = None
    conn = None
    try:
        # ---- SSH tunnel ----
        tunnel = open_ssh_tunnel()

        # ---- MySQL over tunnel ----
        conn = get_conn_via_tunnel(tunnel.local_bind_port)
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        BATCH = int(os.environ.get("ALVEAR_MYSQL_BATCH", "200"))
        insertados = 0

        def process_one(rw: Dict[str, Any]):
            nonlocal insertados
            producto_id = find_or_create_producto(cur, rw)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, rw)
            insert_historico(cur, tienda_id, pt_id, rw, capturado_en)
            insertados += 1

        for idx, row in enumerate(rows, 1):
            mysql_run_with_retry(conn, lambda: process_one(row))

            if idx % BATCH == 0:
                mysql_run_with_retry(conn, lambda: conn.commit())
                print(f"[INFO] commit batch: {idx}/{len(rows)}")

        conn.commit()
        print(f"💾 Guardado en MySQL (túnel): {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"❌ Error MySQL: {e}")

    except Exception as e:
        print(f"❌ Error general: {type(e).__name__}: {e}")

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        try:
            if tunnel:
                tunnel.stop()
                print("[SSH] Tunnel cerrado.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
