#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re, time, unicodedata
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime
from typing import Dict, Any, List, Optional

import numpy as np
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

# ===================== Config =====================
CATEGORIES = [
    "https://www.lagallega.com.ar/productosnl.asp?nl=03000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=05000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=07000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=13000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=15000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=09000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=06000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=04000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=02000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=19000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=11000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=08000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=10000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=16000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=18000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=17000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=14000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=21000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=20000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=12000000"
]

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
TIMEOUT = 20
RETRIES = 3
SLEEP_PAGE = 0.30
MAX_EMPTY_PAGES = 2
MAX_PAGES = 200
PAGE_SIZE = 50

TIENDA_CODIGO = "lagallega"
TIENDA_NOMBRE = "La Gallega"

# ===================== Helpers comunes =====================
_price_clean_re = re.compile(r"[^\d,.\-]")
_slug_nonword = re.compile(r"[^a-zA-Z0-9\s-]")
_slug_spaces = re.compile(r"[\s\-]+")
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

def parse_price(val) -> float:
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

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = _slug_nonword.sub("", text)
    return _slug_spaces.sub("-", text.strip().lower())

# ===================== HTTP session =====================
def build_session():
    s = requests.Session()
    retry = Retry(total=RETRIES, backoff_factor=0.5,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET"], raise_on_status=False)
    ad = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update(HEADERS)
    try:
        s.cookies.set("cantP", str(PAGE_SIZE), domain="www.lagallega.com.ar", path="/")
    except Exception:
        pass
    return s

# ===================== Scraper específico La Gallega =====================
def parse_price_ar(texto: str) -> float:
    if not texto: return 0.0
    t = (texto.replace("$","").replace(".","").replace("\xa0","").strip()
               .replace(",", "."))
    try: return float(t)
    except Exception: return 0.0

def extract_ean_from_alt(alt_text: str) -> str:
    if not alt_text: return ""
    m = re.match(r"(\d{8,14})\s*-\s*", alt_text.strip())
    return m.group(1) if m else ""

def list_products_on_page(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for li in soup.select("li.cuadProd"):
        a = li.select_one(".FotoProd a[href]")
        if not a:
            continue
        href = urljoin(base_url, a["href"])
        img = li.select_one(".FotoProd img[alt]")
        ean_hint = extract_ean_from_alt(img.get("alt","")) if img else ""
        nombre_el = li.select_one(".InfoProd .desc")
        precio_el = li.select_one(".InfoProd .precio .izq")
        nombre = nombre_el.get_text(strip=True) if nombre_el else ""
        precio = parse_price_ar(precio_el.get_text(strip=True) if precio_el else "")
        out.append({"detail_url": href, "ean_hint": ean_hint,
                    "nombre_list": nombre, "precio_list": precio})
    return out

def parse_detail(html, detail_url, ean_hint=""):
    soup = BeautifulSoup(html, "html.parser")
    pr = ""
    try:
        q = parse_qs(urlparse(detail_url).query)
        pr = (q.get("Pr") or q.get("pr") or [""])[0]
    except Exception:
        pass

    tile = soup.select_one("#ContainerDesc .DetallIzq .tile")
    ean = extract_ean_from_alt(tile.get("alt","")) if tile else ""
    if not ean: ean = ean_hint

    nombre_el = soup.select_one(".DetallDer .DetallDesc > b")
    marca_el  = soup.select_one(".DetallDer .DetallMarc")
    precio_el = soup.select_one(".DetallDer .DetallPrec .izq")

    nombre = nombre_el.get_text(strip=True) if nombre_el else ""
    marca  = marca_el.get_text(strip=True) if marca_el else ""
    precio = parse_price_ar(precio_el.get_text(strip=True) if precio_el else "")

    return {
        "EAN": ean or "",
        "Código Interno": pr or "",
        "Nombre Producto": nombre or "",
        "Marca": marca or "",
        "Precio": precio,
        "URL": detail_url,
    }

def set_query(url: str, **params) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in params.items():
        q[k] = [str(v)]
    new_q = urlencode({k: v[0] if isinstance(v, list) else v for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def scrape_one_category(url: str, session: requests.Session):
    base = "{u.scheme}://{u.netloc}/".format(u=urlparse(url))
    try:
        cat_code = parse_qs(urlparse(url).query).get("nl", [""])[0]
    except Exception:
        cat_code = ""

    page = 1
    empty_streak = 0
    rows = []
    seen_page_signatures = set()
    seen_detail_urls = set()

    while True:
        if page > MAX_PAGES:
            print(f"⛔️ Corte por MAX_PAGES en {cat_code} (>{MAX_PAGES})")
            break

        page_url = set_query(url, pg=page)
        r = session.get(page_url, timeout=TIMEOUT)
        if r.status_code != 200:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES: break
            page += 1; continue

        items = list_products_on_page(r.text, base)
        if not items:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES: break
            page += 1; continue

        page_signature = tuple(it["detail_url"] for it in items)
        if page_signature in seen_page_signatures:
            print(f"🔁 Página repetida detectada en {cat_code} (pg={page}). Corto paginación.")
            break
        seen_page_signatures.add(page_signature)

        empty_streak = 0
        print(f"📄 {cat_code} — pg {page}: {len(items)} productos (URL: {page_url})")

        for it in items:
            durl = it["detail_url"]
            if durl in seen_detail_urls:
                continue
            seen_detail_urls.add(durl)

            rd = session.get(durl, timeout=TIMEOUT)
            if rd.status_code != 200:
                continue
            det = parse_detail(rd.text, durl, ean_hint=it.get("ean_hint",""))

            if not det["Nombre Producto"] and it.get("nombre_list"):
                det["Nombre Producto"] = it["nombre_list"]
            if det["Precio"] == 0 and it.get("precio_list"):
                det["Precio"] = it["precio_list"]

            row = {
                "EAN": det["EAN"],
                "Código Interno": det["Código Interno"],
                "Nombre Producto": det["Nombre Producto"],
                "Categoría": cat_code,
                "Subcategoría": "",
                "Marca": det["Marca"],
                "Fabricante": "",
                "Precio de Lista": det["Precio"],
                "Precio de Oferta": det["Precio"],
                "Tipo de Oferta": "Precio regular",
                "URL": det["URL"],
            }
            print(f"  🛒 {row['EAN']} | {row['Nombre Producto']} | ${row['Precio de Lista']:.2f} | {row['URL']}")
            rows.append(row)

        page += 1
        time.sleep(SLEEP_PAGE)

    return rows

# ===================== MySQL upserts (mismo estilo que Coto) =====================
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
    sku = clean(p.get("sku"))           # aquí usamos el "Código Interno" como SKU
    rec = clean(p.get("record_id"))     # no tenemos record_id real -> lo dejamos None
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
    def to_txt_or_none(x):
        v = parse_price(x)
        if x is None: return None
        if isinstance(v, float) and np.isnan(v): return None
        return f"{round(float(v), 2)}"

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
        to_txt_or_none(p.get("precio_lista")), to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None, p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None, p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# ===================== Utilidad local =====================
def unique_in_order(seq):
    seen = set(); out = []
    for s in seq:
        if s not in seen:
            out.append(s); seen.add(s)
    return out

# ===================== Runner (scrape + inserción) =====================
def main():
    t0 = time.time()
    session = build_session()
    urls = unique_in_order(CATEGORIES)

    # 1) Scraping
    all_rows: List[Dict[str, Any]] = []
    for u in urls:
        try:
            all_rows.extend(scrape_one_category(u, session))
        except Exception as e:
            print(f"⚠️ Error en categoría {u}: {e}")

    if not all_rows:
        print("⚠️ Sin datos.")
        return

    df = pd.DataFrame(all_rows)

    # Dedupe priorizado: EAN → Código Interno → URL
    df["EAN"] = df["EAN"].astype("string")
    df["_k"] = df["EAN"].fillna("").str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "Código Interno"].fillna("").astype(str).str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "URL"].fillna("").astype(str).str.strip()
    before = len(df)
    df = df.drop_duplicates(subset=["_k"]).drop(columns=["_k"])
    print(f"🧹 Dedupe: -{before - len(df)} duplicados → {len(df)} únicos")

    # 2) Inserción en MySQL (igual estilo que Coto)
    capturado_en = datetime.now()
    conn = None

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for _, r in df.iterrows():
            # Mapear a la estructura p usada por los upserts
            p = {
                "sku": clean(r.get("Código Interno")),       # usamos Código Interno como SKU
                "record_id": None,                           # no existe para este sitio
                "ean": clean(r.get("EAN")),
                "nombre": clean(r.get("Nombre Producto")),
                "marca": clean(r.get("Marca")),
                "fabricante": clean(r.get("Fabricante")),
                "categoria": clean(r.get("Categoría")),
                "subcategoria": clean(r.get("Subcategoría")),
                "precio_lista": r.get("Precio de Lista"),
                "precio_oferta": r.get("Precio de Oferta"),
                "tipo_oferta": clean(r.get("Tipo de Oferta") or "Precio regular"),
                "promo_tipo": None,
                "precio_regular_promo": None,
                "precio_descuento": None,
                "comentarios_promo": None,
                "url": clean(r.get("URL")),
            }

            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
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

    print(f"⏱️ Tiempo total: {time.time() - t0:.2f} s")

if __name__ == "__main__":
    main()
