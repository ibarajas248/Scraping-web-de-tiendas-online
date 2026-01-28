#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re, time
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime
from typing import Dict, Any, List

import numpy as np
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error as MySQLError
import sys, os

# ===================== IMPORT DB (opcional) =====================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # tu conexión MySQL

# ===================== MODO PRUEBA =====================
TEST_MODE = True
TEST_CATEGORIES = [
    "https://www.lagallega.com.ar/productosnl.asp?nl=03000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=05000000",
]
TEST_MAX_PAGES = 1          # solo 1 página por categoría
TEST_MAX_DETAILS = 8        # solo N productos por categoría
TEST_SLEEP = 0.1

# si querés probar DB rápido:
ENABLE_DB_WRITE = False     # <- ponelo True si querés que inserte
DB_MAX_ROWS = 20            # límite de filas a insertar en prueba

# ===================== HTTP =====================
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
TIMEOUT = 15
RETRIES = 2
PAGE_SIZE = 50

# ===================== Helpers =====================
_price_clean_re = re.compile(r"[^\d,.\-]")

def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

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

def parse_price_ar(texto: str) -> float:
    if not texto:
        return 0.0
    t = (
        texto.replace("$", "")
        .replace(".", "")
        .replace("\xa0", "")
        .strip()
        .replace(",", ".")
    )
    try:
        return float(t)
    except Exception:
        return 0.0

def extract_ean_from_alt(alt_text: str) -> str:
    if not alt_text:
        return ""
    m = re.match(r"(\d{8,14})\s*-\s*", alt_text.strip())
    return m.group(1) if m else ""

def build_session():
    s = requests.Session()
    retry = Retry(
        total=RETRIES,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    ad = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
    s.mount("https://", ad)
    s.mount("http://", ad)
    s.headers.update(HEADERS)
    try:
        s.cookies.set("cantP", str(PAGE_SIZE), domain="www.lagallega.com.ar", path="/")
    except Exception:
        pass
    return s

def set_query(url: str, **params) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in params.items():
        q[k] = [str(v)]
    new_q = urlencode({k: v[0] if isinstance(v, list) else v for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def list_products_on_page(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for li in soup.select("li.cuadProd"):
        a = li.select_one(".FotoProd a[href]")
        if not a:
            continue
        href = urljoin(base_url, a["href"])
        img = li.select_one(".FotoProd img[alt]")
        ean_hint = extract_ean_from_alt(img.get("alt", "")) if img else ""
        nombre_el = li.select_one(".InfoProd .desc")
        precio_el = li.select_one(".InfoProd .precio .izq")
        nombre = nombre_el.get_text(strip=True) if nombre_el else ""
        precio = parse_price_ar(precio_el.get_text(strip=True) if precio_el else "")
        out.append(
            {
                "detail_url": href,
                "ean_hint": ean_hint,
                "nombre_list": nombre,
                "precio_list": precio,
            }
        )
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
    ean = extract_ean_from_alt(tile.get("alt", "")) if tile else ""
    if not ean:
        ean = ean_hint

    nombre_el = soup.select_one(".DetallDer .DetallDesc > b")
    marca_el = soup.select_one(".DetallDer .DetallMarc")

    precio_el = soup.select_one(".DetallDer .DetallPrec .izq")
    precio_actual = parse_price_ar(precio_el.get_text(strip=True) if precio_el else "")

    has_offer = soup.select_one("div.OferProd") is not None

    if has_offer:
        precio_lista = None
        precio_oferta = precio_actual if precio_actual > 0 else None
        tipo_oferta = "Oferta"
    else:
        precio_lista = precio_actual if precio_actual > 0 else None
        precio_oferta = precio_actual if precio_actual > 0 else None
        tipo_oferta = "Precio regular"

    nombre = nombre_el.get_text(strip=True) if nombre_el else ""
    marca = marca_el.get_text(strip=True) if marca_el else ""

    return {
        "EAN": ean or "",
        "Código Interno": pr or "",
        "Nombre Producto": nombre or "",
        "Marca": marca or "",
        "Precio Lista": precio_lista,
        "Precio Oferta": precio_oferta,
        "Tipo Oferta": tipo_oferta,
        "URL": detail_url,
        "HasOffer": has_offer,
    }

# ===================== DB (reusa tu patrón) =====================
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
    sku = clean(p.get("sku"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre") or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    def to_txt_or_none(x):
        v = parse_price(x)
        if x is None:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        return f"{round(float(v), 2)}"

    cur.execute("""
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        to_txt_or_none(p.get("precio_lista")),
        to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None,
    ))

# ===================== Smoke test runner =====================
def main():
    s = build_session()
    all_rows = []

    for cat_url in TEST_CATEGORIES:
        base = "{u.scheme}://{u.netloc}/".format(u=urlparse(cat_url))
        cat_code = parse_qs(urlparse(cat_url).query).get("nl", [""])[0]

        print(f"\n=== TEST categoría nl={cat_code} ===")

        # solo 1 página
        page_url = set_query(cat_url, pg=1)
        r = s.get(page_url, timeout=TIMEOUT)
        print(f"LIST: {page_url} -> {r.status_code}")
        if r.status_code != 200:
            continue

        items = list_products_on_page(r.text, base)
        print(f"  encontrados en listado: {len(items)}")
        items = items[:TEST_MAX_DETAILS]

        ok_offer = 0
        ok_regular = 0

        for i, it in enumerate(items, 1):
            durl = it["detail_url"]
            rd = s.get(durl, timeout=TIMEOUT)
            if rd.status_code != 200:
                print(f"  [{i}] detail FAIL {rd.status_code}: {durl}")
                continue

            det = parse_detail(rd.text, durl, ean_hint=it.get("ean_hint", ""))

            # fallbacks
            if not det.get("Nombre Producto") and it.get("nombre_list"):
                det["Nombre Producto"] = it["nombre_list"]
            if (det.get("Precio Oferta") is None or det.get("Precio Oferta") == 0) and it.get("precio_list"):
                det["Precio Oferta"] = it["precio_list"]
                if det.get("Tipo Oferta") == "Precio regular":
                    det["Precio Lista"] = it["precio_list"]

            # checks de consistencia
            if det["HasOffer"]:
                cond = (det.get("Precio Lista") is None) and (det.get("Precio Oferta") not in (None, 0))
                ok_offer += int(cond)
            else:
                cond = (det.get("Precio Lista") not in (None, 0)) and (det.get("Precio Oferta") not in (None, 0))
                ok_regular += int(cond)

            pl = det.get("Precio Lista")
            po = det.get("Precio Oferta")
            pl_txt = "NULL" if pl is None else f"{float(pl):.2f}"
            po_txt = "NULL" if po is None else f"{float(po):.2f}"

            print(
                f"  [{i}] offer={det['HasOffer']} | {det.get('EAN','')} | "
                f"{det.get('Nombre Producto','')[:60]} | L:{pl_txt} O:{po_txt}"
            )

            all_rows.append({
                "EAN": det.get("EAN",""),
                "Código Interno": det.get("Código Interno",""),
                "Nombre Producto": det.get("Nombre Producto",""),
                "Categoría": cat_code,
                "Subcategoría": "",
                "Marca": det.get("Marca",""),
                "Fabricante": "",
                "Precio de Lista": det.get("Precio Lista"),
                "Precio de Oferta": det.get("Precio Oferta"),
                "Tipo de Oferta": det.get("Tipo Oferta"),
                "URL": det.get("URL", durl),
            })

            time.sleep(TEST_SLEEP)

        print(f"  ✅ checks: oferta_ok={ok_offer} regular_ok={ok_regular} (sobre {len(items)})")

    if not all_rows:
        print("\n⚠️ No se recolectó nada en el smoke test.")
        return

    df = pd.DataFrame(all_rows)
    print("\n=== RESUMEN DF ===")
    print(df.head(10).to_string(index=False))

    if not ENABLE_DB_WRITE:
        print("\n(DB) ENABLE_DB_WRITE=False → no inserta en MySQL.")
        return

    # Insert rápido (limitado)
    df = df.head(DB_MAX_ROWS).copy()
    capturado_en = datetime.now()

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, "lagallega", "La Gallega")

        inserted = 0
        for _, r in df.iterrows():
            p = {
                "sku": clean(r.get("Código Interno")),
                "ean": clean(r.get("EAN")),
                "nombre": clean(r.get("Nombre Producto")),
                "marca": clean(r.get("Marca")),
                "fabricante": clean(r.get("Fabricante")),
                "categoria": clean(r.get("Categoría")),
                "subcategoria": clean(r.get("Subcategoría")),
                "precio_lista": r.get("Precio de Lista"),
                "precio_oferta": r.get("Precio de Oferta"),
                "tipo_oferta": clean(r.get("Tipo de Oferta")),
                "url": clean(r.get("URL")),
            }

            pid = find_or_create_producto(cur, p)
            ptid = upsert_producto_tienda(cur, tienda_id, pid, p)
            insert_historico(cur, tienda_id, ptid, p, capturado_en)
            inserted += 1

        conn.commit()
        print(f"\n💾 DB OK: insertadas {inserted} filas (limit={DB_MAX_ROWS}) capturado_en={capturado_en}")

    except MySQLError as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"\n❌ MySQL error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
