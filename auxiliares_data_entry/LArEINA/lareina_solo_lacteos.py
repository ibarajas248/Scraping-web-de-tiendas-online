#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Reina (lareinaonline.com.ar) — SOLO 1 NL (categoría/subcategoría) + páginas pg + MySQL

Itera:
  https://www.lareinaonline.com.ar/productosnl.asp?pg=1&nl=03030000
  https://www.lareinaonline.com.ar/productosnl.asp?pg=2&nl=03030000
  ...

En cada página extrae por <li class="cuadProd">:
  - EAN (desde Pr=... o onclick PCompra('...'))
  - precio (final)
  - precio_base (si hay 2 precios: base=max, precio=min; si hay 1: base=precio)
  - url (detalle absoluta + P=...)
  - url_imagen

Inserta en MySQL con tu patrón:
  upsert_tienda -> find_or_create_producto -> upsert_producto_tienda -> insert_historico
  - precio_lista = precio_base
  - precio_oferta = precio (solo si base > precio, si no NULL)
  - capturado_en común por corrida
  - ON DUPLICATE KEY UPDATE

Uso:
  python lareina_solo_lacteos.py
  python lareina_solo_lacteos.py --nl 03030000 --pg-from 1 --pg-to 3 --P 7
  python lareina_solo_lacteos.py --pg-from 1 --P 7  (auto-stop cuando una pg venga vacía)
"""

import re
import os
import sys
import time
import argparse
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- tu conexión MySQL


# ================== Config ==================
BASE = "https://www.lareinaonline.com.ar"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 25
SLEEP_BETWEEN = 0.20

TIENDA_CODIGO = "lareina"
TIENDA_NOMBRE = "La Reina"

# Regex / utils
_NULLLIKE = {"", "null", "none", "nan", "na"}
SPACES_RX = re.compile(r"\s+")
MONEY_RX = re.compile(r"\$\s*[\d\.]+(?:,\s*\d{2})?")  # $2.087,00 | $ 1.234, 56
_price_clean_re = re.compile(r"[^\d,.\-]")


# ================== Utilidades ==================
def norm_text(s: str) -> str:
    return SPACES_RX.sub(" ", (s or "").strip())

def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

def get_qp(url: str, key: str) -> Optional[str]:
    try:
        return parse_qs(urlparse(url).query).get(key, [None])[0]
    except Exception:
        return None

def set_qp(url: str, key: str, value: str) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    q[key] = [str(value)]
    new_q = urlencode({k: v[0] for k, v in q.items()}, doseq=False)
    return u._replace(query=new_q).geturl()

def ensure_p(url: str, pval: int) -> str:
    """Asegura P=pval en la URL (si ya lo tiene y coincide, no cambia)."""
    cur = get_qp(url, "P")
    if cur and str(cur) == str(pval):
        return url
    return set_qp(url, "P", str(pval))

def mk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def fetch(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "latin-1"):
        r.encoding = r.apparent_encoding
    time.sleep(SLEEP_BETWEEN)
    return r.text

def money_to_float(txt: str) -> Optional[float]:
    if not txt:
        return None
    m = MONEY_RX.search(txt)
    if not m:
        return None
    val = (
        m.group(0)
        .replace("$", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", ".")
        .strip()
    )
    try:
        return float(val)
    except Exception:
        return None

def extract_all_prices(text: str) -> List[float]:
    """Devuelve lista de floats encontrados en un texto con $."""
    if not text:
        return []
    text = norm_text(text)
    out = []
    for m in MONEY_RX.finditer(text):
        v = money_to_float(m.group(0))
        if v is not None:
            out.append(v)
    return out

def parse_price(val) -> float:
    """Convierte a float o np.nan, aceptando formatos con coma/punto."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return np.nan
    s = _price_clean_re.sub("", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

def to_varchar_2(x) -> Optional[str]:
    v = parse_price(x)
    if x is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    return f"{float(v):.2f}"


# ================== Scraping ==================
def build_list_url(nl: str, pg: int, pval: int) -> str:
    url = f"{BASE}/productosnl.asp?pg={pg}&nl={nl}"
    return ensure_p(url, pval)

def extract_items_from_page(html: str, pval: int) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for li in soup.select("li.cuadProd"):
        a = li.select_one(".FotoProd a[href*='productosdet.asp']")
        if not a:
            continue

        # URL detalle
        det_url = urljoin(BASE, a.get("href", ""))
        det_url = ensure_p(det_url, pval)

        # EAN: preferimos Pr=...
        ean = get_qp(det_url, "Pr")
        if not (ean and ean.isdigit()):
            # fallback onclick PCompra('...')
            agrega = li.select_one(".Agrega[onclick]")
            if agrega:
                m = re.search(r"PCompra\('(\d+)'\)", agrega.get("onclick", ""))
                if m:
                    ean = m.group(1)

        if not (ean and ean.isdigit()):
            # fallback final: buscar un 8/13 dígitos dentro del li
            raw = li.get_text(" ", strip=True)
            m = re.search(r"\b(\d{8}|\d{13})\b", raw)
            ean = m.group(1) if m else None

        # Nombre
        name_el = li.select_one(".InfoProd .desc")
        nombre = norm_text(name_el.get_text(" ", strip=True)) if name_el else None
        if not nombre:
            img_alt = li.select_one(".FotoProd img[alt]")
            if img_alt and img_alt.get("alt"):
                nombre = norm_text(img_alt.get("alt"))

        # Imagen
        img = li.select_one(".FotoProd img[src]")
        url_img = urljoin(BASE, img.get("src")) if img else None

        # Precio(s)
        price_block = li.select_one(".InfoProd .precio") or li.select_one(".InfoProd") or li
        price_text = norm_text(price_block.get_text(" ", strip=True))
        price_text = re.sub(r",\s*(\d{2})", r",\1", price_text)  # ", 00" -> ",00"

        prices = extract_all_prices(price_text)
        if not prices:
            continue

        if len(prices) == 1:
            precio = prices[0]
            precio_base = prices[0]
        else:
            precio_base = max(prices)
            precio = min(prices)

        items.append({
            "EAN": ean,
            "Nombre Producto": nombre,
            "Precio": precio,                 # precio final
            "Precio Base": precio_base,       # base
            "URL": det_url,
            "URL Imagen": url_img,
        })

    return items

def scrape_nl(nl: str, pg_from: int, pg_to: int, pval: int, max_pages_auto: int = 500) -> pd.DataFrame:
    """
    Si pg_to <= 0: auto-stop cuando una página venga sin items.
    Si pg_to > 0: itera fijo pg_from..pg_to.
    """
    session = mk_session()
    all_rows: List[Dict[str, Any]] = []
    seen = set()

    pg = pg_from
    stop_on_empty = (pg_to <= 0)

    while True:
        if (not stop_on_empty) and (pg > pg_to):
            break
        if stop_on_empty and (pg - pg_from) >= max_pages_auto:
            print(f"[WARN] Auto-stop por límite max_pages_auto={max_pages_auto}.")
            break

        url = build_list_url(nl, pg, pval)
        try:
            html = fetch(session, url)
        except Exception as e:
            print(f"[WARN] fallo leyendo pg={pg}: {e}")
            # si estamos en auto y falla, cortamos para no loop infinito
            if stop_on_empty:
                break
            pg += 1
            continue

        rows = extract_items_from_page(html, pval)
        print(f"[OK] nl={nl} P={pval} pg={pg} -> {len(rows)} items")

        if stop_on_empty and len(rows) == 0:
            break

        for r in rows:
            k = (r.get("EAN") or "").strip() or (r.get("URL") or "").strip()
            if not k or k in seen:
                continue
            seen.add(k)
            all_rows.append(r)

        pg += 1

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    # Dedupe extra por EAN (fallback URL)
    df["_k"] = df["EAN"].fillna("").astype(str).str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "URL"].fillna("").astype(str).str.strip()
    df = df.drop_duplicates(subset=["_k"]).drop(columns=["_k"]).reset_index(drop=True)
    return df


# ================== MySQL helpers (tu lógica) ==================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
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
            """, (
                p.get("nombre") or "", p.get("marca") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    nombre = clean(p.get("nombre")) or ""
    marca  = clean(p.get("marca")) or ""
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca))
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
            """, (
                p.get("ean") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", nombre, marca,
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    """
    Para este caso:
      - sku_tienda = EAN (estable)
      - record_id_tienda = None
      - url_tienda = URL detalle
      - nombre_tienda = nombre en tienda
    """
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre") or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid

    if rec:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
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
          promo_tipo = COALESCE(VALUES(promo_tipo), promo_tipo),
          promo_texto_regular = COALESCE(VALUES(promo_texto_regular), promo_texto_regular),
          promo_texto_descuento = COALESCE(VALUES(promo_texto_descuento), promo_texto_descuento),
          promo_comentarios = COALESCE(VALUES(promo_comentarios), promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        to_varchar_2(p.get("precio_lista")),
        to_varchar_2(p.get("precio_oferta")),
        p.get("tipo_oferta") or None,
        p.get("promo_tipo") or None,
        p.get("promo_texto_regular") or None,
        p.get("promo_texto_descuento") or None,
        p.get("promo_comentarios") or None
    ))


# ================== Main ==================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nl", default="03030000", help="Código nl (default: 03030000)")
    ap.add_argument("--pg-from", type=int, default=1, help="Página inicial (default: 1)")
    ap.add_argument("--pg-to", type=int, default=0,
                    help="Página final. Si 0 o negativo: auto-stop cuando pg venga vacía (default: 0)")
    ap.add_argument("--P", type=int, default=7, help="Plaza/sucursal P (default: 7)")
    ap.add_argument("--out", default="",
                    help="CSV salida. Si vacío, se arma automáticamente con fecha.")
    ap.add_argument("--no-mysql", action="store_true", help="No inserta en MySQL (solo CSV)")
    args = ap.parse_args()

    nl = str(args.nl).strip()
    pval = int(args.P)

    df = scrape_nl(nl=nl, pg_from=args.pg_from, pg_to=args.pg_to, pval=pval)

    print(f"\n✅ Filas obtenidas: {len(df)}")
    if df.empty:
        print("Sin datos, fin.")
        return

    # CSV de respaldo
    if args.out.strip():
        out_csv = args.out.strip()
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_csv = f"lareina_nl_{nl}_P{pval}_{stamp}.csv"

    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print("🧾 CSV:", out_csv)

    if args.no_mysql:
        return

    # Inserción MySQL
    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for _, r in df.iterrows():
            ean = clean(r.get("EAN"))
            nombre = clean(r.get("Nombre Producto"))
            url = clean(r.get("URL"))

            precio = r.get("Precio")          # final
            precio_base = r.get("Precio Base")  # base

            # MySQL: precio_lista = base, precio_oferta solo si hay descuento real
            if precio_base is not None and precio is not None and float(precio_base) > float(precio):
                precio_oferta = precio
                tipo_oferta = "Oferta"
            else:
                precio_oferta = None
                tipo_oferta = "Precio regular"

            p = {
                # sku_tienda = EAN
                "sku": ean,
                "record_id": None,

                "ean": ean,
                "nombre": nombre,
                "marca": None,
                "fabricante": None,
                "categoria": None,
                "subcategoria": None,

                "precio_lista": precio_base,
                "precio_oferta": precio_oferta,
                "tipo_oferta": tipo_oferta,

                "promo_tipo": None,
                "promo_texto_regular": None,
                "promo_texto_descuento": None,
                "promo_comentarios": None,

                "url": url,
            }

            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} históricos para {TIENDA_NOMBRE} ({capturado_en})")

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
