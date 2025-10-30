#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reporte multi-supermercado (VTEX) por EAN — Script standalone (sin Streamlit)

- Lee maestro.xlsx desde la MISMA carpeta donde está este archivo.
- Recorre EANs y consulta tiendas VTEX (secuencial dentro de cada tienda, varias tiendas en paralelo).
- Genera un Excel consolidado en la misma carpeta.
- Ingesta opcional en MySQL (mismas tablas/campos que la versión UI).

Parámetros opcionales (todos tienen defaults equivalentes a la app):
  --ean-column NOMBRE          Nombre de la columna EAN si quieres forzarlo.
  --extra-domains "url1 url2"  Dominios extra separados por espacios o nueva línea.
  --sc-overrides JSON          Overrides de Sales Channel: {"https://dominio":"1", ...}
  --stores-in-parallel INT     Tiendas en paralelo (default: 3).
  --per-store-delay FLOAT      Pausa por solicitud dentro de cada tienda (default: 0.15s).
  --retries-req INT            Reintentos HTTP leves (default: 2).
  --force-all                  Consultar TODOS los candidatos, aún si no detecta VTEX (default: True).
  --save-to-db                 Guardar en MySQL al finalizar (default: False).  <-- se fuerza a True abajo
  --batch-size INT             Tamaño de mini-lote para commits (default: 100).
"""

import io
import os
import sys
import json
import time
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
#  CANDIDATOS (mismos que en la app)
# =========================================
VTEX_CANDIDATES_AR = [
    # Cencosud (descomentables si quieres)
    # {"name": "jumbo_ar", "base": "https://www.jumbo.com.ar", "default_sc": ""},
    # {"name": "vea", "base": "https://www.vea.com.ar", "default_sc": ""},
    # {"name": "disco", "base": "https://www.disco.com.ar", "default_sc": ""},
    # DIA (varía el host; ambos serán probados)
    # {"name": "dia", "base": "https://diaonline.supermercadosdia.com.ar", "default_sc": ""},
    # Carrefour
    {"name": "Carrefour Argentina", "base": "https://www.carrefour.com.ar", "default_sc": ""},
    {"name": "Jumbo Argentina", "base": "https://www.jumbo.com.ar", "default_sc": ""},
    # Más candidatos:
    # {"name": "Mi Super VTEX", "base": "https://www.misuper.com.ar", "default_sc": "1"},
]

HEADERS = {"User-Agent": "Mozilla/5.0"}
SEARCH_PATH = "/api/catalog_system/pub/products/search"
TIMEOUT = 25

# ============== Utilidades HTTP/VTEX ==============
def make_session(retries: int = 2) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=retries, backoff_factor=0.25, status_forcelist=[429, 500, 502, 503, 504])
    s.headers.update(HEADERS)
    s.mount("https://", HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=retry))
    s.mount("http://", HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=retry))
    return s


def _safe_get(d: Dict, path: List[Any], default=None):
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
    ser = df[col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    ser = ser.str.replace(r"[^\d]", "", regex=True)
    return ser.replace({"": None})


def is_vtex_store(session: requests.Session, base_url: str) -> bool:
    try:
        url = base_url.rstrip("/") + SEARCH_PATH
        params = {"_from": 0, "_to": 0, "ft": "a"}
        r = session.get(url, params=params, timeout=TIMEOUT)
        return (r.status_code in (200, 206)) and r.text.strip().startswith("[")
    except Exception:
        return False


def vtex_buscar_por_ean_session(session: requests.Session, base_url: str, ean: str, sc: Optional[str]) -> Tuple[str, Optional[List[Dict[str, Any]]]]:
    """
    Retorna (estado_llamada, data):
      - "OK" si devolvió JSON lista.
      - "NO_JSON" si respondió algo distinto a lista JSON.
      - "ERROR" si hubo excepción de red.
    """
    try:
        url = base_url.rstrip("/") + SEARCH_PATH
        params = [("fq", f"alternateIds_Ean:{ean}")]
        if sc:
            params.append(("sc", sc))
        r = session.get(url, params=params, timeout=TIMEOUT)
        txt = r.text.strip()
        if r.status_code in (200, 206) and txt.startswith("["):
            return "OK", r.json()
        return "NO_JSON", None
    except Exception:
        return "ERROR", None


def extraer_eans_de_producto(prod: Dict[str, Any]) -> set:
    eans = set()
    for it in prod.get("items", []) or []:
        e = it.get("ean") or it.get("Ean")
        if isinstance(e, str) and e.isdigit():
            eans.add(e)
    return eans


def parsear_producto_vtex(producto: Dict[str, Any], ean_consultado: str, base_url: str) -> List[Dict[str, Any]]:
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
            "url": (base_url + link) if link and link.startswith("/") else link,
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
                "url": (base_url + link) if link and link.startswith("/") else link,
                "precio_lista": None, "precio_oferta": None, "disponible": None,
                "oferta_tags": promo_tags, "ean_reportado": ean_item, "seller_id": None
            })
            continue

    # ... (continúa igual, omito para ahorrar espacio visual)
    # NOTE: No modifiques nada aquí; dejo todo idéntico al bloque que ya compartiste.
    # === COPIA EXACTA HASTA EL FINAL DEL ARCHIVO ===

    for it in items:
        sku_id = it.get("itemId") or it.get("id")
        ean_item = it.get("ean") or it.get("Ean")
        sellers = it.get("sellers") or []
        if not sellers:
            filas.append({
                "product_id": product_id, "sku_id": sku_id,
                "nombre": product_name, "marca": brand,
                "categoria": cat1, "subcategoria": cat2,
                "url": (base_url + link) if link and link.startswith("/") else link,
                "precio_lista": None, "precio_oferta": None, "disponible": None,
                "oferta_tags": promo_tags, "ean_reportado": ean_item, "seller_id": None
            })
            continue

        for s in sellers:
            sid = s.get("sellerId") or s.get("id")
            co = s.get("commertialOffer") or {}
            list_price = co.get("ListPrice")
            price = co.get("Price")
            available = co.get("AvailableQuantity")
            teasers = co.get("Teasers") or co.get("DiscountHighLight") or []
            if isinstance(teasers, list) and teasers:
                teasers_txt = ", ".join(
                    [t.get("name") or json.dumps(t, ensure_ascii=False) for t in teasers if isinstance(t, dict)]
                )
            elif isinstance(teasers, list):
                teasers_txt = None
            else:
                teasers_txt = str(teasers) if teasers else None

            filas.append({
                "product_id": product_id, "sku_id": sku_id,
                "nombre": product_name, "marca": brand,
                "categoria": cat1, "subcategoria": cat2,
                "url": (base_url + link) if link and link.startswith("/") else link,
                "precio_lista": list_price, "precio_oferta": price, "disponible": available,
                "oferta_tags": teasers_txt or promo_tags, "ean_reportado": ean_item, "seller_id": sid
            })
    return filas


# ============== Detección automática (sin cache streamlit) ==============
def detectar_tiendas_vtex(candidatos: List[Dict[str, str]], extras: List[str], retries: int) -> List[Dict[str, str]]:
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


# ============== Helpers DB (idénticos en lógica) ==============
MAXLEN_TIPO_OFERTA = 64
MAXLEN_COMENTARIOS = 255
MAXLEN_NOMBRE = 255
MAXLEN_CATEGORIA = 120
MAXLEN_SUBCATEGORIA = 200
MAXLEN_NOMBRE_TIENDA = 255

LOCK_ERRNOS = {1205, 1213}


def _truncate(s: Optional[str], n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    return s if len(s) <= n else s[:n]


def _price_str(val) -> Optional[str]:
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


def _domain_host(url: str) -> str:
    try:
        return urlparse(url).netloc or url
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

    # 1) Preferir EAN
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

    # 2) Fallback por (nombre, marca)
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

    # 3) Insert
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

    # Sin SKU, aún guardamos vínculo
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
    # VTEX expone list y oferta; si falta alguno, usamos el disponible
    pl = _price_str(r.get("precio_lista"))
    po = _price_str(r.get("precio_oferta") if r.get("precio_oferta") not in (None, 0) else r.get("precio_lista"))
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
    if df is None or df.empty:
        print("[DB] No hay filas para ingestar.")
        return

    conn = None
    total = 0
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cset:
                cset.execute("SET SESSION innodb_lock_wait_timeout = 8")
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
            tienda_codigo = _domain_host(base) or base
            tienda_nombre = supername or tienda_codigo
            tienda_id = upsert_tienda(cur, tienda_codigo, tienda_nombre)
            conn.commit()

            batch = 0
            for _, r in df_g.iterrows():
                rec = r.to_dict()
                rec["nombre"] = rec.get("nombre") or ""
                try:
                    pid = find_or_create_producto(cur, rec)
                    ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)
                    insert_historico(cur, tienda_id, ptid, rec, capturado_en)
                    total += 1
                    batch += 1
                    if batch >= batch_size:
                        conn.commit()
                        batch = 0
                except myerr.DatabaseError as e:
                    errno = getattr(e, "errno", None)
                    if errno in LOCK_ERRNOS:
                        conn.rollback()
                        continue
                    elif errno == 1264:
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
                            if batch >= batch_size:
                                conn.commit()
                                batch = 0
                        except Exception:
                            conn.rollback()
                            continue
                    else:
                        conn.rollback()
                        continue

            if batch:
                conn.commit()

        print(f"[DB] ✅ Inserciones/updates en historico_precios: {total}")

    except mysql.connector.Error as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        print(f"[DB] ❌ MySQL error {getattr(e, 'errno', None)}: {e}")
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        print(f"[DB] ❌ Error de ingesta: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass


# ============== Flujo principal ==============
def main():
    parser = argparse.ArgumentParser(description="Reporte multi-supermercado (VTEX) por EAN — Script standalone")
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

    # 🔴 Forzar ingesta en DB SIEMPRE (mínimo cambio, todo lo demás igual)
    args.save_to_db = True
    print("[DB] Ingesta a MySQL: FORZADA (save_to_db=True)")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, "maestro.xlsx")
    out_path = os.path.join(script_dir, "reporte_multi_super_vtex_por_ean.xlsx")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"No encontré {input_path}. Coloca 'maestro.xlsx' en la misma carpeta del script.")

    # Leer Excel
    print(f"[IO] Leyendo: {input_path}")
    try:
        df_in = pd.read_excel(input_path)
    except Exception as e:
        raise RuntimeError(f"No pude leer el Excel maestro.xlsx: {e}")

    # Detectar columna EAN (o usar la indicada)
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

    # Extras y overrides
    extra_domains = []
    if args.extra_domains.strip():
        extra_domains = [x.strip() for x in args.extra_domains.replace("\r", "\n").split() if x.strip()]

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

    # Detección de tiendas
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

    # Unicas por base
    vtex_stores = list({e["base"]: e for e in vtex_stores}.values())

    if not vtex_stores:
        raise RuntimeError("No hay tiendas para consultar. Revisa la lista de candidatos o agrega dominios.")

    print("[INFO] Tiendas que se consultarán (cada una secuencial):")
    for s in vtex_stores:
        print(f"  - {s.get('name','')} :: {s['base']} (sc={s.get('default_sc','')})")

    # EANs
    eans_all = normalizar_columna_ean(df_in, col_ean).dropna().unique().tolist()
    if not eans_all:
        raise RuntimeError("No encontré EANs válidos en esa columna.")

    # Sesiones por dominio
    SESSIONS: Dict[str, requests.Session] = {s["base"]: make_session(retries=args.retries_req) for s in vtex_stores}

    total_estimado = len(eans_all) * len(vtex_stores)
    done = 0

    rows: List[Dict[str, Any]] = []
    errores: List[Tuple[str, str, str]] = []

    def progreso():
        pct = (done / max(1, total_estimado)) * 100.0
        print(f"\r[RUN] Progreso: {done}/{total_estimado} ({pct:5.1f}%)", end="", flush=True)

    def procesar_producto(store, base, ean_consultado, producto, estado: str):
        for fila in parsear_producto_vtex(producto, ean_consultado, base):
            rows.append({
                "supermercado": store.get("name", base),
                "dominio": base,
                "estado_llamada": estado,
                "ean_consultado": ean_consultado,
                "ean_reportado": fila.get("ean_reportado"),
                "nombre": fila.get("nombre"),
                "marca": fila.get("marca"),
                "categoria": fila.get("categoria"),
                "subcategoria": fila.get("subcategoria"),
                "precio_lista": fila.get("precio_lista"),
                "precio_oferta": fila.get("precio_oferta"),
                "disponible": fila.get("disponible"),
                "oferta_tags": fila.get("oferta_tags"),
                "product_id": fila.get("product_id"),
                "sku_id": fila.get("sku_id"),
                "seller_id": fila.get("seller_id"),
                "url": fila.get("url"),
            })

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

    # —— Worker por tienda (SECUENCIAL dentro de la tienda) ——
    def worker_store(store):
        base = store["base"]
        sc = sc_map.get(base, store.get("default_sc") or None)
        session = SESSIONS[base]

        processed = 0
        for e in eans_all:
            estado, prods = vtex_buscar_por_ean_session(session, base, e, sc=sc)
            try:
                if estado == "OK" and prods:
                    for p in prods:
                        procesar_producto(store, base, e, p, estado="OK")
                elif estado == "OK" and not prods:
                    add_empty_row(store, base, e, estado="NO_MATCH")
                else:
                    add_empty_row(store, base, e, estado=estado)
            except Exception as ex:
                errores.append((base, e, str(ex)))
                add_empty_row(store, base, e, estado="ERROR")

            nonlocal done
            done += 1
            progreso()

            if args.per_store_delay > 0:
                time.sleep(args.per_store_delay)
        return processed

    # —— Ejecutar varias tiendas en paralelo (cada una secuencial) ——
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

    # ======================
    #  Output Excel
    # ======================
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
    if errores:
        print(f"[INFO] Errores capturados: {len(errores)} (continuó con filas vacías).")

    # ===== Ingesta a MySQL (FORZADA) =====
    if not df_out.empty:
        print("[DB] Guardando en MySQL…")
        ingest_to_mysql(df_out, batch_size=args.batch_size)
    else:
        print("[DB] No hay filas para ingestar.")

    print("[DONE] Proceso completado.")


if __name__ == "__main__":
    main()
