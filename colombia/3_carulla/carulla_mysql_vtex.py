#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Éxito (VTEX) — Scraper FULL catálogo (MUCHO más coverage) + Persistencia MySQL (1 solo archivo)

Salida DF columnas:
EAN, Código Interno, Nombre Producto, Categoría, Subcategoría, Marca,
Fabricante, Precio de Lista, Precio de Oferta, Tipo de Oferta, URL

MEJORA CLAVE vs barrido global simple:
- Barrido por semillas: ft=a..z + 0..9
  (VTEX a veces NO entrega todo el catálogo sin ft/fq; con seeds se cubre mucho más)
- Páginas en paralelo por seed.
- Dedupe por productId.
- Sin print por producto (usa --verbose para logs).

MySQL:
- fit() NO trunca, omite si excede (evita 1406).
- upserts con LAST_INSERT_ID para evitar SELECT extra.
- histórico con executemany (batch) para acelerar.
- Regla Kilbel: si hay SKU, NO pisa producto_id en ON DUPLICATE.

Requiere:
- pip install requests pandas mysql-connector-python xlsxwriter
- base_datos_local.get_conn
"""

import os
import re
import sys
import time
import logging
import argparse
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError


# =========================
# Import conexión MySQL (tu helper)
# =========================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos_local import get_conn  # <- tu conexión MySQL


# =========================
# CONFIG
# =========================
BASE_API = "https://www.carulla.com/api/catalog_system/pub/products/search"
BASE_WEB = "https://www.carulla.com/"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

TIENDA_CODIGO = "https://www.carulla.com"
TIENDA_NOMBRE = "Carulla"

# Disponibilidad
REQUIRE_IS_AVAILABLE = True
REQUIRE_QTY_POSITIVE = True          # acepta 99999
REQUIRE_PRICE_POSITIVE = True

# Excel sanitize
ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# =========================
# HELPERS
# =========================
def sanitize_for_excel(value):
    if isinstance(value, str):
        return ILLEGAL_XLSX.sub("", value)
    return value

def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if pd.isna(x):
            return None
        v = float(x)
        if np.isnan(v):
            return None
        return v
    except Exception:
        return None

def clean(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    s = str(val).strip()
    return s if s else None

def to_price_txt_2dec(x) -> Optional[str]:
    v = safe_float(x)
    if v is None:
        return None
    return f"{float(v):.2f}"


# =========================
# HTTP: session por hilo + retries
# =========================
_tls = threading.local()

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=4,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def get_session() -> requests.Session:
    if not hasattr(_tls, "session"):
        _tls.session = make_session()
    return _tls.session


# =========================
# VTEX parse
# =========================
def tipo_de_oferta(offer: dict, list_price: float, price: float) -> str:
    try:
        dh = offer.get("DiscountHighLight") or []
        if dh and isinstance(dh, list):
            name = (dh[0].get("Name") or dh[0].get("\u003CName\u003Ek__BackingField") or "").strip()
            if name:
                return name
    except Exception:
        pass
    return "Descuento" if (price or 0) < (list_price or 0) else "Precio regular"

def pick_available_offer(items: List[dict]):
    for it in (items or []):
        for s in (it.get("sellers") or []):
            co = s.get("commertialOffer") or {}
            ok = True

            if REQUIRE_IS_AVAILABLE:
                ok = ok and bool(co.get("IsAvailable"))

            if REQUIRE_QTY_POSITIVE:
                qty = co.get("AvailableQuantity")
                try:
                    qty = int(qty)
                except Exception:
                    qty = 0
                ok = ok and (qty > 0 or qty == 99999)

            if REQUIRE_PRICE_POSITIVE:
                price = co.get("FullSellingPrice", co.get("Price", 0))
                try:
                    price = float(price or 0)
                except Exception:
                    price = 0.0
                ok = ok and (price > 0)

            if ok:
                return it, s, co

    return None, None, None

def parse_product_min(p: Dict) -> Optional[Dict]:
    """
    Parser mínimo y rápido.
    """
    def first_val(v, default=None):
        if v is None:
            return default
        if isinstance(v, (list, tuple)):
            return v[0] if v else default
        return v

    def to_str(x):
        return "" if x is None else str(x).strip()

    def to_float(x, default=0.0):
        try:
            if isinstance(x, (int, float)):
                return float(x)
            s = str(x).strip()
            s = s.replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
            return float(s)
        except Exception:
            return float(default)

    def split_cat_path(path: str) -> Tuple[str, str]:
        if not isinstance(path, str):
            return "", ""
        parts = [seg for seg in path.split("/") if seg]
        if not parts:
            return "", ""
        cat = parts[0]
        sub = parts[1] if len(parts) > 1 else ""
        return cat, sub

    def build_url(p: Dict) -> str:
        link_text = p.get("linkText")
        link = p.get("link") or ""
        if link_text:
            return f"{BASE_WEB.rstrip('/')}/{link_text}/p"
        if isinstance(link, str) and (link.startswith("http://") or link.startswith("https://")):
            return link
        if BASE_WEB and isinstance(link, str):
            return f"{BASE_WEB.rstrip('/')}/{link.lstrip('/')}"
        return to_str(link)

    items = p.get("items") or []
    item0, seller0, offer = pick_available_offer(items)
    if not offer:
        return None

    # EAN
    ean = (item0.get("ean") if item0 else None) or first_val(p.get("EAN"))
    if not ean and item0:
        for rid in (item0.get("referenceId") or []):
            k = (rid.get("Key") or "").upper()
            if k in ("EAN", "EAN13", "GTIN", "REFID"):
                ean = rid.get("Value")
                if ean:
                    break
    ean = to_str(ean)

    # Código Interno
    codigo_interno = to_str((item0 or {}).get("itemId") or p.get("productId"))

    # Categoría/subcategoría desde path
    categories = p.get("categories") or []
    cat, sub = ("", "")
    if categories and isinstance(categories, list) and isinstance(categories[0], str):
        cat, sub = split_cat_path(categories[0])

    url_prod = build_url(p)

    # Precios
    list_price = offer.get("ListPrice")
    pwd = offer.get("PriceWithoutDiscount")
    price = offer.get("FullSellingPrice", offer.get("Price"))

    list_price = to_float(list_price) if list_price else (to_float(pwd) if pwd else to_float(price))
    price = to_float(price)

    brand = p.get("brand") or ""
    manufacturer = p.get("manufacturer") or ""

    return {
        "EAN": ean,
        "Código Interno": codigo_interno,
        "Nombre Producto": to_str(p.get("productName")),
        "Categoría": to_str(cat),
        "Subcategoría": to_str(sub),
        "Marca": to_str(brand),
        "Fabricante": to_str(manufacturer),
        "Precio de Lista": list_price,
        "Precio de Oferta": price,
        "Tipo de Oferta": tipo_de_oferta(offer, list_price, price),
        "URL": url_prod,
        "_productId": to_str(p.get("productId") or ""),
    }


# =========================
# SCRAPE: barrido por SEEDS ft=a..z + 0..9
# =========================
COLS_FINAL = [
    "EAN", "Código Interno", "Nombre Producto", "Categoría", "Subcategoría",
    "Marca", "Fabricante", "Precio de Lista", "Precio de Oferta", "Tipo de Oferta", "URL"
]

def fetch_page_seed(seed: str, offset: int, step: int, timeout: int = 25) -> List[Dict]:
    """
    Descarga 1 página usando ft=seed.
    """
    sess = get_session()
    url = f"{BASE_API}?ft={seed}&_from={offset}&_to={offset + step - 1}"
    r = sess.get(url, timeout=timeout)
    if r.status_code not in (200, 206):
        return []
    try:
        data = r.json()
    except Exception:
        return []
    if not data:
        return []
    out = []
    for p in data:
        prod = parse_product_min(p)
        if prod:
            out.append(prod)
    return out

def fetch_catalog_by_seeds_fast(
    step: int = 50,
    workers: int = 16,
    max_offset: int = 250000,
    empty_pages_stop: int = 8,
    window_pages: int = 60,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Recorre seeds: a..z y 0..9
    - Por cada seed: pagina offsets en ventanas paralelas.
    - Corta por seed cuando hay muchas ventanas vacías seguidas.
    - Dedupe global por productId.
    """
    t0 = time.time()

    seeds = [chr(c) for c in range(ord("a"), ord("z") + 1)] + [str(d) for d in range(0, 10)]

    seen_pid = set()
    all_rows: List[Dict] = []

    logging.info("🚀 Barrido por seeds ft=a..z + 0..9 | workers=%d step=%d", workers, step)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for seed in seeds:
            logging.info("🔤 Seed ft=%s", seed)

            offset = 0
            empty_streak = 0

            while offset < max_offset and empty_streak < empty_pages_stop:
                futures = {}
                base = offset
                for k in range(window_pages):
                    off = base + k * step
                    if off >= max_offset:
                        break
                    futures[ex.submit(fetch_page_seed, seed, off, step)] = off

                if not futures:
                    break

                any_nonempty = False

                for fut in as_completed(futures):
                    off = futures[fut]
                    try:
                        rows = fut.result()
                    except Exception:
                        rows = []

                    if not rows:
                        continue

                    any_nonempty = True

                    for prod in rows:
                        pid = prod.get("_productId") or ""
                        if pid and pid in seen_pid:
                            continue
                        if pid:
                            seen_pid.add(pid)
                        prod.pop("_productId", None)
                        all_rows.append(prod)

                    if verbose:
                        logging.info("ft=%s offset=%d -> %d útiles (acum=%d)", seed, off, len(rows), len(all_rows))

                if not any_nonempty:
                    empty_streak += 1
                else:
                    empty_streak = 0

                offset += step * window_pages

    if not all_rows:
        logging.warning("No se obtuvieron filas del barrido por seeds.")
        return pd.DataFrame(columns=COLS_FINAL)

    df = pd.DataFrame(all_rows)
    for c in COLS_FINAL:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[COLS_FINAL].drop_duplicates(keep="last")

    elapsed = time.time() - t0
    logging.info("✅ Total productos (dedupe): %d | ⏱️ %.1fs", len(df), elapsed)
    return df


# =========================
# Guardado opcional
# =========================
def save_xlsx(df: pd.DataFrame, path: str):
    df2 = df.applymap(sanitize_for_excel)
    with pd.ExcelWriter(
        path,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    ) as writer:
        df2.to_excel(writer, index=False, sheet_name="productos")
        wb = writer.book
        ws = writer.sheets["productos"]

        money = wb.add_format({"num_format": "0.00"})
        text = wb.add_format({"num_format": "@"})

        col_idx = {name: i for i, name in enumerate(df2.columns)}
        if "EAN" in col_idx:
            ws.set_column(col_idx["EAN"], col_idx["EAN"], 18, text)
        if "Nombre Producto" in col_idx:
            ws.set_column(col_idx["Nombre Producto"], col_idx["Nombre Producto"], 52)
        for c in ["Categoría", "Subcategoría", "Marca", "Fabricante"]:
            if c in col_idx:
                ws.set_column(col_idx[c], col_idx[c], 20)
        for c in ["Precio de Lista", "Precio de Oferta"]:
            if c in col_idx:
                ws.set_column(col_idx[c], col_idx[c], 14, money)
        if "URL" in col_idx:
            ws.set_column(col_idx["URL"], col_idx["URL"], 46)

    logging.info("📗 XLSX guardado: %s (%d filas)", path, len(df))

def save_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)
    logging.info("💾 CSV guardado: %s (%d filas)", path, len(df))


# =========================
# MySQL: límites de columnas + fit
# =========================
def load_schema_limits(cur):
    targets = ("tiendas", "productos", "producto_tienda", "historico_precios")
    q = """
    SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME IN (%s,%s,%s,%s)
    """
    cur.execute(q, targets)
    limits: Dict[str, Dict[str, Optional[int]]] = {}
    for t, c, maxlen in cur.fetchall():
        limits.setdefault(t, {})[c] = maxlen
    return limits

def make_fit_fn(limits):
    def fit(table: str, column: str, val: Any, *, digits_only: bool = False) -> Optional[str]:
        v = clean(val)
        if v is None:
            return None
        if digits_only:
            v = "".join(ch for ch in str(v) if ch.isdigit())
            if not v:
                return None
        maxlen = limits.get(table, {}).get(column)
        if isinstance(v, str) and maxlen is not None and maxlen > 0:
            if len(v) > maxlen:
                return None  # NO truncamos
        return v
    return fit


# =========================
# MySQL: upserts optimizados
# =========================
def upsert_tienda(cur, codigo: Optional[str], nombre: Optional[str]) -> int:
    cur.execute("""
        INSERT INTO tiendas (codigo, nombre)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          nombre = VALUES(nombre)
    """, (clean(codigo), clean(nombre)))
    return cur.lastrowid

def find_or_create_producto(cur, p: Dict[str, Any],
                           cache_ean_to_pid: Dict[str, int],
                           cache_nb_to_pid: Dict[Tuple[str, str], int]) -> int:
    ean = clean(p.get("ean")) or ""
    nombre = clean(p.get("nombre")) or ""
    marca = clean(p.get("marca")) or ""
    fabricante = clean(p.get("fabricante")) or ""
    categoria = clean(p.get("categoria")) or ""
    subcategoria = clean(p.get("subcategoria")) or ""

    if ean and ean in cache_ean_to_pid:
        return cache_ean_to_pid[ean]

    key_nb = (nombre, marca)
    if key_nb in cache_nb_to_pid:
        return cache_nb_to_pid[key_nb]

    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (nombre, marca, fabricante, categoria, subcategoria, pid))
            cache_ean_to_pid[ean] = pid
            cache_nb_to_pid[key_nb] = pid
            return pid

    # fallback (nombre,marca) solo si marca no está vacía
    if nombre and marca:
        cur.execute("SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1", (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (ean or "", fabricante, categoria, subcategoria, pid))
            if ean:
                cache_ean_to_pid[ean] = pid
            cache_nb_to_pid[key_nb] = pid
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (ean or "", nombre, marca, fabricante, categoria, subcategoria))
    pid = cur.lastrowid
    if ean:
        cache_ean_to_pid[ean] = pid
    cache_nb_to_pid[key_nb] = pid
    return pid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    """
    Regla Kilbel:
    - Si hay SKU, NO actualizar producto_id si ya existe por esa key.
    """
    sku = clean(p.get("sku"))
    record_id = clean(p.get("record_id"))
    url = clean(p.get("url")) or ""
    nombre_tienda = clean(p.get("nombre_tienda")) or (clean(p.get("nombre")) or "")

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              -- NO pisa producto_id cuando hay SKU
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    if record_id:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid


# =========================
# MySQL: histórico batch
# =========================
def flush_historico_batch(cur, batch_params: List[Tuple]):
    if not batch_params:
        return
    cur.executemany("""
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
    """, batch_params)
    batch_params.clear()


# =========================
# Pipeline: DF -> MySQL
# =========================
def persist_df_to_mysql(df: pd.DataFrame, hist_batch_size: int = 1500):
    if df is None or df.empty:
        print("⚠️ DataFrame vacío; nada que insertar.")
        return

    df = df.copy()
    if "EAN" in df.columns:
        df["EAN"] = df["EAN"].astype("string")
    df = df.where(pd.notna(df), None)

    cols = list(df.columns)
    col = {name: i for i, name in enumerate(cols)}

    capturado_en = datetime.now()

    cache_ean_to_pid: Dict[str, int] = {}
    cache_nb_to_pid: Dict[Tuple[str, str], int] = {}

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        limits = load_schema_limits(cur)
        fit = make_fit_fn(limits)

        tienda_id = upsert_tienda(
            cur,
            fit("tiendas", "codigo", TIENDA_CODIGO),
            fit("tiendas", "nombre", TIENDA_NOMBRE),
        )

        hist_batch: List[Tuple] = []
        inserted_hist = 0

        for row in df.itertuples(index=False, name=None):
            def val(name):
                i = col.get(name)
                return row[i] if i is not None else None

            p = {
                "ean":          fit("productos", "ean", val("EAN"), digits_only=True),
                "nombre":       fit("productos", "nombre", val("Nombre Producto")),
                "marca":        fit("productos", "marca", val("Marca")),
                "fabricante":   fit("productos", "fabricante", val("Fabricante")),
                "categoria":    fit("productos", "categoria", val("Categoría")),
                "subcategoria": fit("productos", "subcategoria", val("Subcategoría")),

                "url":          fit("producto_tienda", "url_tienda", val("URL")),
                "sku":          fit("producto_tienda", "sku_tienda", val("Código Interno")),
                "record_id":    None,
                "nombre_tienda": fit("producto_tienda", "nombre_tienda", val("Nombre Producto")),

                "precio_lista":  safe_float(val("Precio de Lista")),
                "precio_oferta": safe_float(val("Precio de Oferta")),
                "tipo_oferta":   fit("historico_precios", "tipo_oferta", val("Tipo de Oferta")),
            }

            producto_id = find_or_create_producto(cur, p, cache_ean_to_pid, cache_nb_to_pid)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)

            precio_lista_txt = fit("historico_precios", "precio_lista", to_price_txt_2dec(p.get("precio_lista")))
            precio_oferta_txt = fit("historico_precios", "precio_oferta", to_price_txt_2dec(p.get("precio_oferta")))

            hist_batch.append((
                tienda_id, pt_id, capturado_en,
                precio_lista_txt, precio_oferta_txt,
                p.get("tipo_oferta"),
                None, None, None, None
            ))
            inserted_hist += 1

            if len(hist_batch) >= hist_batch_size:
                flush_historico_batch(cur, hist_batch)

        flush_historico_batch(cur, hist_batch)
        conn.commit()
        print(f"💾 Guardado en MySQL: {inserted_hist} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

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


# =========================
# MAIN
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", type=int, default=50, help="Items por página VTEX (50 suele ser ok).")
    ap.add_argument("--workers", type=int, default=16, help="Hilos para páginas en paralelo (8-24 típico).")
    ap.add_argument("--max-offset", type=int, default=300000, help="Salvavidas por seed: offset máximo.")
    ap.add_argument("--window-pages", type=int, default=60, help="Páginas por ventana paralela (por seed).")
    ap.add_argument("--empty-stop", type=int, default=8, help="Ventanas vacías seguidas para cortar cada seed.")
    ap.add_argument("--verbose", action="store_true", help="Log por offset.")
    ap.add_argument("--save-xlsx", type=str, default="", help="Path xlsx opcional.")
    ap.add_argument("--save-csv", type=str, default="", help="Path csv opcional.")
    ap.add_argument("--no-mysql", action="store_true", help="No persistir en MySQL.")
    ap.add_argument("--hist-batch", type=int, default=1500, help="Batch insert historico.")
    args = ap.parse_args()

    t0 = time.time()

    df = fetch_catalog_by_seeds_fast(
        step=args.step,
        workers=args.workers,
        max_offset=args.max_offset,
        empty_pages_stop=args.empty_stop,
        window_pages=args.window_pages,
        verbose=args.verbose,
    )

    if not df.empty:
        if args.save_csv.strip():
            save_csv(df, args.save_csv.strip())
        if args.save_xlsx.strip():
            save_xlsx(df, args.save_xlsx.strip())

    if args.no_mysql:
        logging.info("🧠 --no-mysql: no se inserta en DB.")
    else:
        if df.empty:
            print("No se obtuvieron productos.")
        else:
            persist_df_to_mysql(df, hist_batch_size=args.hist_batch)

    elapsed = time.time() - t0
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)
    logging.info("⏱️ Tiempo total: %dh %dm %ds", h, m, s)

if __name__ == "__main__":
    main()
