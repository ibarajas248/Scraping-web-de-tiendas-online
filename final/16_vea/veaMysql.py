#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests, pandas as pd, time, re, unicodedata
from html import unescape
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import numpy as np
from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# =========================
# Config
# =========================
CATEGORIAS: List[str] = [
    "electro","tiempo-libre","bebidas","carnes","almacen","frutas-y-verduras",
    "lacteos","perfumeria","bebes-y-ninos","limpieza","quesos-y-fiambres",
    "congelados","panaderia-y-pasteleria","comidas-preparadas","mascotas","hogar-y-textil",
]

BASE_HOST = "https://www.vea.com.ar"
BASE_URL  = f"{BASE_HOST}/api/catalog_system/pub/products/search"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
STEP = 50
SLEEP = 0.5
MAX_PAGES = 500
MAX_RETRIES = 4
RETRY_BACKOFF = 1.5

# Identidad de tienda en tu DB
TIENDA_CODIGO = "vea"
TIENDA_NOMBRE = "Vea Argentina"

inicio = time.time()

# =========================
# Helpers comunes / limpieza
# =========================
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]')
_price_clean_re = re.compile(r"[^\d,.\-]")
_slug_nonword = re.compile(r"[^a-zA-Z0-9\s-]")
_slug_spaces = re.compile(r"[\s\-]+")
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean_html(html_text: Optional[str]) -> str:
    if not html_text:
        return ""
    text = unescape(html_text)
    try:
        return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    except Exception:
        return text

def sanitize_excel(value: Any) -> Any:
    if isinstance(value, str):
        return ILLEGAL_XLSX.sub('', value)
    return value

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
    if not s: return np.nan
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

def ensure_abs_url(link: Optional[str]) -> str:
    if not link: return ""
    if link.startswith("http://") or link.startswith("https://"):
        return link
    if not link.startswith("/"):
        link = "/" + link
    return f"{BASE_HOST}{link}"

def best_category_from_vtex(categories_list: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    VTEX entrega rutas tipo '/Almacén/Arroz/'. Tomamos [0] y [-1] como cat/subcat.
    """
    segs: List[str] = []
    for path in categories_list or []:
        # divide por '/', quita vacíos y espacios
        parts = [p.strip() for p in path.split("/") if p.strip()]
        segs.extend(parts)
    if not segs:
        return None, None
    if len(segs) == 1:
        return segs[0], None
    return segs[0], segs[-1]

# =========================
# Parsing
# =========================
def parse_product(p: Dict[str, Any]) -> Dict[str, Any]:
    items = p.get("items") or []
    first_item = items[0] if items else {}

    ean = first_item.get("ean")
    sellers = first_item.get("sellers") or []
    first_seller = sellers[0] if sellers else {}
    offer = first_seller.get("commertialOffer") or {}

    images = first_item.get("images") or []
    image_url = images[0].get("imageUrl") if images else None

    # Guardo también la lista original de categorías (para cat/subcat)
    categories_list = p.get("categories") or []
    categories_str = " > ".join([c for c in categories_list if c])
    categories_ids_str = " > ".join([c for c in (p.get("categoriesIds") or []) if c])

    # Teasers/promos si están
    teasers = offer.get("Teasers") or []
    teaser_texts = []
    for t in teasers:
        # VTEX puede tener distintos formatos; tomamos texto común si existe
        txt = t.get("name") or t.get("teaserType") or t.get("id")
        if txt: teaser_texts.append(str(txt).strip())
    promo_tipo = "; ".join(teaser_texts) if teaser_texts else None

    link = p.get("link") or ("/" + p.get("linkText", ""))  # fallback sencillo
    link = ensure_abs_url(link)

    return {
        "productId": str(p.get("productId") or ""),
        "productName": p.get("productName"),
        "brand": p.get("brand"),
        "productReference": str(p.get("productReference") or "") or None,  # SKU de VTEX
        "ean": ean,
        "price": offer.get("Price"),
        "priceWithoutDiscount": offer.get("PriceWithoutDiscount"),
        "listPrice": offer.get("ListPrice"),
        "priceValidUntil": offer.get("PriceValidUntil"),
        "isAvailable": offer.get("IsAvailable"),
        "availableQty": offer.get("AvailableQuantity"),
        "categoryId": p.get("categoryId"),
        "categories_raw": categories_list,
        "categories": categories_str,
        "categoriesIds": categories_ids_str,
        "releaseDate": p.get("releaseDate"),
        "linkText": p.get("linkText"),
        "link": link,
        "imageUrl": image_url,
        "description": clean_html(p.get("description")),
        "promo_tipo": promo_tipo,
    }

# =========================
# Requests con reintentos
# =========================
def fetch_page(categoria: str, offset: int, step: int = STEP) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/{categoria}"
    params = {"_from": offset, "_to": offset + step - 1}

    backoff = 1.0
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code in (200, 206):
                try:
                    return r.json() or []
                except ValueError:
                    print(f"⚠️ Respuesta no JSON en {categoria} offset={offset}")
                    return []
            if r.status_code in (400, 404):
                print(f"⚠️ HTTP {r.status_code} en {categoria} offset={offset}")
                return []
            if r.status_code in (429, 500, 502, 503, 504):
                print(f"⏳ HTTP {r.status_code} en {categoria} offset={offset} (reintento {intento}/{MAX_RETRIES})")
                time.sleep(backoff)
                backoff *= RETRY_BACKOFF
                continue
            print(f"⚠️ HTTP {r.status_code} en {categoria} offset={offset}")
            return []
        except requests.RequestException as e:
            if intento == MAX_RETRIES:
                print(f"❌ Error de red en {categoria} offset={offset}: {e}")
                return []
            print(f"⏳ Error de red en {categoria} offset={offset}: {e} (reintento {intento}/{MAX_RETRIES})")
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF
    return []

# =========================
# Scrape por categoría
# =========================
def scrape_categoria(categoria: str) -> List[Dict[str, Any]]:
    productos_rows: List[Dict[str, Any]] = []
    seen_ids: set = set()
    offset = 0
    page = 1
    pages_without_new = 0

    while page <= MAX_PAGES:
        print(f"🔎 {categoria}: {offset}–{offset+STEP-1} (página {page})")
        data = fetch_page(categoria, offset, STEP)
        if not data:
            break

        prev_count = len(seen_ids)
        for p in data:
            pid = str(p.get("productId") or "")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                productos_rows.append(parse_product(p))

        nuevos = len(seen_ids) - prev_count
        if nuevos == 0:
            pages_without_new += 1
        else:
            pages_without_new = 0

        if pages_without_new >= 2 or len(data) < STEP:
            break

        offset += STEP
        page += 1
        time.sleep(SLEEP)

    return productos_rows

# =========================
# MySQL upserts (estilo Coto)
# =========================
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
    Usamos:
      - sku_tienda = productReference (si existe)
      - record_id_tienda = productId (fallback universal en VTEX)
    Requiere UNIQUE(tienda_id, sku_tienda) y/o UNIQUE(tienda_id, record_id_tienda).
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
    def to_txt_or_none(x):
        v = parse_price(x)
        if x is None: return None
        if isinstance(v, float) and np.isnan(v): return None
        return f"{round(float(v), 2)}"  # se guarda como VARCHAR

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
        to_txt_or_none(p.get("precio_lista")), to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None, p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None, p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# =========================
# Main (scrape + inserción)
# =========================
if __name__ == "__main__":
    all_rows: List[Dict[str, Any]] = []
    for cat in CATEGORIAS:
        cat_rows = scrape_categoria(cat)
        if cat_rows:
            all_rows.extend(cat_rows)

    if not all_rows:
        print("No se extrajeron productos.")
        exit(0)

    # Dedupe global por productId
    df = pd.DataFrame(all_rows)
    if "productId" in df.columns:
        df = df.drop_duplicates(subset=["productId"])
    else:
        df = df.drop_duplicates()

    # ===== Inserción en MySQL =====
    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for _, r in df.iterrows():
            # Derivar categoría/subcategoría desde lista VTEX
            cat, subcat = best_category_from_vtex(r.get("categories_raw") or [])

            # Determinar precios y tipo de oferta
            price = r.get("price")
            list_price = r.get("priceWithoutDiscount") or r.get("listPrice")
            tipo_oferta = "Oferta" if (parse_price(list_price) > parse_price(price)) else "Precio regular"

            # Map a estructura p
            p = {
                "sku": clean(r.get("productReference")),          # SKU VTEX (si existe)
                "record_id": clean(r.get("productId")),           # productId VTEX
                "ean": clean(r.get("ean")),
                "nombre": clean(r.get("productName")),
                "marca": clean(r.get("brand")),
                "fabricante": None,                                # no disponible en este endpoint
                "categoria": clean(cat),
                "subcategoria": clean(subcat),
                "precio_lista": list_price,
                "precio_oferta": price,
                "tipo_oferta": tipo_oferta,
                "promo_tipo": clean(r.get("promo_tipo")),
                "precio_regular_promo": None,
                "precio_descuento": None,
                "comentarios_promo": None,
                "url": clean(r.get("link")),
                "nombre_tienda": clean(r.get("productName")),      # opcional
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

    fin = time.time()
    print(f"⏱️ Tiempo total de ejecución: {fin - inicio:.2f} s")
