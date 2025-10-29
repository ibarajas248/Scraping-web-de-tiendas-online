#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jumbo (VTEX público) → MySQL — Barrido de Cobertura Máxima (sin appKey/appToken)

Estrategia sin APIs privadas:
- Recorre árbol de categorías (TREE_DEPTH configurable).
- Pagina con _from/_to (STEP=100) y detecta "estancamiento" por productId.
- Si detecta tope/estancamiento → particiona por FACETES de marca (brand) usando /facets.
- Si no hay facets de marca o siguen topes → particiona alfabéticamente por ft=a..z,0..9.
- Si aún faltan productos → particiona por rangos de precio (P:[min TO max]) usando buckets.
- Loop de sales channel (SC_LIST) para descubrir vistas alternas.
- Fallback a detalle por productId cuando un producto no trae sellers en listado.
- Producer–Consumer: múltiples scrapers en paralelo + 1 writer único a DB.
- Mini-commits, tolerancia 1205/1264, dumps crudos JSON por página (raw_jumbo/).

Requisitos:
  pip install requests beautifulsoup4 lxml mysql-connector-python

Nota: Ajusta nombres de índices/UNIQUE según tu esquema (comentarios en upsert_producto_tienda).

Autor: Tú :) (con ayuda de ChatGPT)
"""

import os, sys, re, json, time, random, threading
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set
from urllib.parse import quote
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import numpy as np
from mysql.connector import Error as MySQLError, errors as myerr

# ========= Conexión =========
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- tu conexión MySQL

# ========= Config General =========
BASE = "https://www.jumbo.com.ar"
TIMEOUT = 25
RETRIES = 4
STEP = 100                    # VTEX suele tolerar 100
TREE_DEPTH = 8                # más profundo para no perder hojas
SLEEP_OK = 0.25
MAX_EMPTY = 10                # tolerancia a huecos

SCRAPER_WORKERS = 4           # hilos de scraping por categoría/partición
QUEUE_MAX = 5000              # cola para writer
WRITER_BATCH = 250            # tamaño de batch para insert_batch
COMMIT_EVERY = 150

# Sales Channels a intentar (parar si ya no aporta)
SC_LIST = [1, 2, 3, 4, 5]

# Particiones
ENABLE_BRAND_PARTITION = True
ENABLE_ALPHA_PARTITION = True
ENABLE_PRICE_PARTITION = True

ALPHA_TOKENS = list("abcdefghijklmnopqrstuvwxyz0123456789")
PRICE_BUCKETS = [(0, 999), (1000, 1999), (2000, 4999), (5000, 9999), (10000, 19999), (20000, 49999), (50000, 99999), (100000, 199999), (200000, 9999999)]

# UA/proxy
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15",
]
PROXIES = []  # e.g., ["http://user:pass@ip:port"]

# Tienda
TIENDA_CODIGO = "jumbo_ar"
TIENDA_NOMBRE = "Jumbo Argentina"

# Column limits
MAXLEN_NOMBRE       = 255
MAXLEN_URL          = 512
MAXLEN_TIPO_OFERTA  = 191
MAXLEN_PROMO_TXT    = 191
MAXLEN_PROMO_COMENT = 255

# Errores MySQL a manejar
LOCK_ERRNOS = {1205, 1213}
OUT_OF_RANGE_ERRNO = 1264

# Dumps crudos
RAW_DIR = "raw_jumbo"
os.makedirs(RAW_DIR, exist_ok=True)

# ========= Parada por ENTER =========
class StopController:
    def __init__(self):
        self._ev = threading.Event()
        self._t = threading.Thread(target=self._wait_enter, daemon=True)

    def _wait_enter(self):
        try:
            print("🛑 Presiona ENTER en cualquier momento para PARAR y guardar lo procesado…")
            _ = sys.stdin.readline()
            self._ev.set()
        except Exception:
            pass

    def start(self):
        self._t.start()

    def tripped(self) -> bool:
        return self._ev.is_set()

# ========= Utils =========
ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
_price_clean_re = re.compile(r"[^\d,.\-]")
_NULLLIKE = {"", "null", "none", "nan", "na"}

HEADERS_BASE = {"Accept": "application/json"}

def jitter(a=0.05, b=0.2):
    return random.uniform(a, b)

def new_session():
    s = requests.Session()
    s.headers.update({"User-Agent": random.choice(UAS), **HEADERS_BASE})
    if PROXIES:
        p = random.choice(PROXIES)
        s.proxies.update({"http": p, "https": p})
    return s

def req_json(url, session, params=None, max_retries=RETRIES):
    for i in range(max_retries):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    time.sleep(0.2 + jitter())
            elif r.status_code in (429, 408, 500, 502, 503, 504):
                time.sleep(0.5 + 0.4 * i + jitter())
            else:
                time.sleep(0.2 + jitter())
        except requests.RequestException:
            time.sleep(0.4 + 0.4 * i + jitter())
    return None

# ========= Limpieza =========
from html import unescape

def clean_text(v):
    if v is None:
        return ""
    if not isinstance(v, str):
        return v
    try:
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", v)

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

def _truncate(s, n):
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]

def _price_txt_or_none(x):
    v = parse_price(x)
    if x is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    return f"{round(float(v), 2)}"

# ========= Categorías & Facets =========

def get_category_tree(session, depth=TREE_DEPTH):
    url = f"{BASE}/api/catalog_system/pub/category/tree/{depth}"
    return req_json(url, session) or []

def iter_paths(tree) -> List[str]:
    out = []
    def walk(node, path):
        slug = node.get("url", "").strip("/").split("/")[-1] or node.get("slug") or node.get("Name")
        if not slug:
            return
        new_path = path + [slug]
        out.append("/".join(new_path))
        for ch in (node.get("children") or []):
            walk(ch, new_path)
    for n in tree:
        walk(n, [])
    uniq, seen = [], set()
    for p in out:
        ps = p.strip("/").lower()
        if ps and ps not in seen:
            seen.add(ps)
            uniq.append(ps)
    return uniq

def map_for_path(path_str: str) -> str:
    depth = len([p for p in path_str.split("/") if p])
    return ",".join(["c"] * depth)

# Facets (para extraer marcas, price ranges, etc.)

def get_facets(session, cat_path: str, sc: Optional[int] = None) -> Dict[str, Any]:
    encoded_path = quote(cat_path, safe="/")
    params = {"map": map_for_path(cat_path)}
    if sc is not None:
        params["sc"] = sc
    url = f"{BASE}/api/catalog_system/pub/facets/search/{encoded_path}"
    data = req_json(url, session, params=params)
    return data or {}


def extract_brand_facets(facets: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Devuelve lista de dicts con {Map, Value, Name} para marcas.
    Estructuras pueden variar por cuenta, intentamos detectar claves comunes.
    """
    out = []
    # Formato típico: {"Departments":[], "Brands":[{"Map":"b","Name":"Coca-Cola","Value":"2000001", ...}], ...}
    # También puede venir como "Brand" o en "SpecificationFilters". Buscamos por claves que contengan 'brand'.
    def scan(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k.lower() in ("brands", "brand") and isinstance(v, list):
                    for it in v:
                        if isinstance(it, dict) and it.get("Map") and it.get("Value"):
                            out.append({
                                "Map": it.get("Map"),
                                "Value": str(it.get("Value")),
                                "Name": it.get("Name") or it.get("Label") or str(it.get("Value")),
                            })
                else:
                    scan(v)
        elif isinstance(obj, list):
            for it in obj:
                scan(it)
    scan(facets)
    # dedup por (Map, Value)
    seen = set(); res = []
    for b in out:
        key = (b["Map"], b["Value"])
        if key not in seen:
            seen.add(key); res.append(b)
    return res

# ========= DB helpers (patrón Coto endurecido) =========

def exec_retry(cur, sql, params=(), max_retries=5, base_sleep=0.4):
    att = 0
    while True:
        try:
            cur.execute(sql, params)
            return
        except myerr.DatabaseError as e:
            code = getattr(e, "errno", None)
            if code in LOCK_ERRNOS and att < max_retries:
                wait = base_sleep * (2 ** att) + jitter(0.05, 0.2)
                print(f"[LOCK] errno={code} retry {att+1}/{max_retries} in {wait:.2f}s")
                time.sleep(wait)
                att += 1
                continue
            raise

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    exec_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
    nombre = _truncate((clean(p.get("nombre")) or ""), MAXLEN_NOMBRE)
    marca  = clean(p.get("marca")) or ""
    categoria   = clean(p.get("categoria"))
    subcategoria= clean(p.get("subcategoria"))
    fabricante  = clean(p.get("fabricante"))

    if ean:
        exec_retry(cur, "SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_retry(cur, """
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (nombre, marca or "", fabricante or "", categoria or "", subcategoria or "", pid))
            return pid

    if nombre and marca:
        exec_retry(cur, """SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                   (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_retry(cur, """
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (ean or "", fabricante or "", categoria or "", subcategoria or "", pid))
            return pid

    exec_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (ean or "", nombre, marca or "", fabricante or "", categoria or "", subcategoria or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = _truncate(clean(p.get("url")), MAXLEN_URL)
    nombre_tienda = _truncate((clean(p.get("nombre")) or ""), MAXLEN_NOMBRE)

    # Ajusta UNIQUE en DB: típica combinación UNIQUE(tienda_id, sku_tienda, record_id_tienda)
    if sku:
        exec_retry(cur, """
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
        exec_retry(cur, """
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,'')) 
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    exec_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en):
    precio_lista_txt  = _price_txt_or_none(p.get("precio_lista"))
    precio_oferta_txt = _price_txt_or_none(p.get("precio_oferta"))

    tipo_oferta = _truncate(clean(p.get("tipo_oferta")), MAXLEN_TIPO_OFERTA)
    promo_tipo  = _truncate(clean(p.get("promo_tipo")),  MAXLEN_PROMO_TXT)

    promo_texto_regular   = _truncate(clean(p.get("precio_regular_promo")), MAXLEN_PROMO_TXT)
    promo_texto_descuento = _truncate(clean(p.get("precio_descuento")),     MAXLEN_PROMO_TXT)

    comentarios = []
    if p.get("categoria"):   comentarios.append(f"cat={p['categoria']}")
    if p.get("subcategoria"):comentarios.append(f"sub={p['subcategoria']}")
    promo_comentarios = _truncate(" | ".join(comentarios), MAXLEN_PROMO_COMENT) if comentarios else None

    sql = """
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
    """
    params = (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista_txt, precio_oferta_txt, tipo_oferta,
        promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios
    )
    try:
        exec_retry(cur, sql, params)
        return
    except myerr.DatabaseError as e:
        if getattr(e, "errno", None) == OUT_OF_RANGE_ERRNO:
            print(f"[WARN] 1264 out-of-range (pid_tienda={producto_tienda_id}) → reintento con NULL")
            params_null = (
                tienda_id, producto_tienda_id, capturado_en,
                None, None, tipo_oferta,
                promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios
            )
            try:
                exec_retry(cur, sql, params_null)
                return
            except Exception as e2:
                print(f"[WARN] No se pudo insertar ni con NULL (pid_tienda={producto_tienda_id}). Omito. {e2}")
                return
        raise

# ========= Parsing producto =========

def parse_rows_from_product_and_ps(p: Dict[str, Any], base: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    ps: List[Dict[str, Any]] = []

    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = p.get("brand")
    brand_id = p.get("brandId")
    link_text = p.get("linkText")
    link = f"{base}/{link_text}/p" if link_text else ""
    categories = [c.strip("/") for c in (p.get("categories") or [])]
    category_path = " > ".join(categories[:1]) if categories else ""
    full_category_path = " > ".join(categories)

    parts = [x.strip() for x in full_category_path.split(">")] if full_category_path else []
    parts = [x for x in parts if x]
    categoria = parts[0] if parts else None
    subcategoria = parts[-1] if len(parts) >= 2 else None

    specs = {}
    for grp in (p.get("specificationGroups") or []):
        for it in (grp.get("specifications") or []):
            k = it.get("name"); v = it.get("value")
            if k and v: specs[k] = v

    cluster = p.get("clusterHighlights") or {}
    props = p.get("properties") or {}

    desc = clean_text(p.get("description") or p.get("descriptionShort") or p.get("metaTagDescription") or "")

    items = p.get("items") or []
    for it in items:
        sku_id = it.get("itemId")
        sku_name = clean_text(it.get("name"))
        ean = ""
        for ref in (it.get("referenceId") or []):
            if ref.get("Value"): ean = ref["Value"]; break

        measurement_unit = it.get("measurementUnit")
        unit_multiplier = it.get("unitMultiplier")
        images = ", ".join(img.get("imageUrl", "") for img in (it.get("images") or []))

        sellers = it.get("sellers") or []

        if not sellers:
            rows.append({
                "productId": product_id, "skuId": sku_id,
                "sellerId": "", "sellerName": "", "availableQty": None,
                "price": None, "listPrice": None, "priceWithoutDiscount": None,
                "installments_json": "", "teasers_json": "",
                "tax": None, "rewardValue": None, "spotPrice": None,
                "name": name, "skuName": sku_name, "brand": brand, "brandId": brand_id,
                "ean": ean, "categoryTop": category_path, "categoryFull": full_category_path,
                "link": link, "linkText": link_text,
                "measurementUnit": measurement_unit, "unitMultiplier": unit_multiplier,
                "images": images, "description": desc,
                "specs_json": json.dumps(specs, ensure_ascii=False),
                "cluster_json": json.dumps(cluster, ensure_ascii=False),
                "properties_json": json.dumps(props, ensure_ascii=False),
            })
            ps.append({
                "sku": clean(sku_id),
                "record_id": clean(product_id),
                "ean": clean(ean),
                "nombre": _truncate(clean(name), MAXLEN_NOMBRE),
                "marca": clean(brand),
                "fabricante": None,
                "precio_lista": None,
                "precio_oferta": None,
                "tipo_oferta": None,
                "promo_tipo": None,
                "precio_regular_promo": None,
                "precio_descuento": None,
                "comentarios_promo": None,
                "categoria": clean(categoria),
                "subcategoria": clean(subcategoria),
                "url": _truncate(clean(link), MAXLEN_URL),
            })
            continue

        for s in sellers:
            s_id = s.get("sellerId"); s_name = s.get("sellerName")
            offer = s.get("commertialOffer") or {}
            price = offer.get("Price")
            list_price = offer.get("ListPrice")
            pwd = offer.get("PriceWithoutDiscount")
            avail = offer.get("AvailableQuantity")
            tax = offer.get("Tax")
            reward = offer.get("RewardValue")
            installments = offer.get("Installments") or []
            teasers = offer.get("Teasers") or []
            spot = offer.get("spotPrice", None)

            rows.append({
                "productId": product_id, "skuId": sku_id,
                "sellerId": s_id, "sellerName": s_name,
                "availableQty": avail, "price": price, "listPrice": list_price,
                "priceWithoutDiscount": pwd,
                "installments_json": json.dumps(installments, ensure_ascii=False),
                "teasers_json": json.dumps(teasers, ensure_ascii=False),
                "tax": tax, "rewardValue": reward, "spotPrice": spot,
                "name": name, "skuName": sku_name, "brand": brand, "brandId": brand_id,
                "ean": ean, "categoryTop": category_path, "categoryFull": full_category_path,
                "link": link, "linkText": link_text,
                "measurementUnit": measurement_unit, "unitMultiplier": unit_multiplier,
                "images": images, "description": desc,
                "specs_json": json.dumps(specs, ensure_ascii=False),
                "cluster_json": json.dumps(cluster, ensure_ascii=False),
                "properties_json": json.dumps(props, ensure_ascii=False),
            })

            promo_txts = []
            for t in teasers:
                nm = t.get("name") or t.get("title") or ""
                if nm: promo_txts.append(str(nm))
            promo_tipo = "; ".join(promo_txts) if promo_txts else None

            ps.append({
                "sku": clean(sku_id),
                "record_id": clean(product_id),
                "ean": clean(ean),
                "nombre": _truncate(clean(name), MAXLEN_NOMBRE),
                "marca": clean(brand),
                "fabricante": None,
                "precio_lista": list_price,
                "precio_oferta": (spot if spot not in (None, "") else price),
                "tipo_oferta": None,
                "promo_tipo": promo_tipo,
                "precio_regular_promo": None,
                "precio_descuento": None,
                "comentarios_promo": None,
                "categoria": clean(categoria),
                "subcategoria": clean(subcategoria),
                "url": _truncate(clean(link), MAXLEN_URL),
            })

    return rows, ps

# ========= Fallbacks =========

def product_has_sellers(p: Dict[str, Any]) -> bool:
    for it in (p.get("items") or []):
        if it.get("sellers"): return True
    return False


def fetch_product_detail(session, product_id: str, sc: Optional[int] = None) -> Optional[Dict[str, Any]]:
    params = {"fq": f"productId:{product_id}"}
    if sc is not None: params["sc"] = sc
    url = f"{BASE}/api/catalog_system/pub/products/search"
    data = req_json(url, session, params=params)
    if isinstance(data, list) and data:
        return data[0]
    return None

# ========= Scrape de una consulta =========

def make_search_url(cat_path: str, _from: int, _to: int, sc: Optional[int] = None, extra_params: Optional[Dict[str, str]] = None) -> Tuple[str, Dict[str, Any]]:
    encoded_path = quote(cat_path, safe="/")
    params = {"map": map_for_path(cat_path), "_from": _from, "_to": _to}
    if sc is not None: params["sc"] = sc
    if extra_params:
        params.update(extra_params)
    url = f"{BASE}/api/catalog_system/pub/products/search/{encoded_path}"
    return url, params


def stream_products(session, cat_path: str, sc: Optional[int], extra_params: Optional[Dict[str, str]], stopper: StopController, seen_products: Set[str]) -> Iterable[Dict[str, Any]]:
    offset, empty_streak = 0, 0
    stagnation = 0
    last_seen_count = len(seen_products)

    while not stopper.tripped():
        url, params = make_search_url(cat_path, offset, offset + STEP - 1, sc, extra_params)
        data = req_json(url, session, params=params)
        # dump crudo
        try:
            tag = "base"
            if extra_params:
                parts = [f"{k}-{v}" for k,v in sorted(extra_params.items()) if k in ("fq","ft","P","priceRange")]
                if parts: tag = ("_".join(parts))[:80]
            fname = os.path.join(RAW_DIR, f"{cat_path.replace('/','_')}_sc{sc or 0}_{offset}_{tag}.json")
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(data if data is not None else [], f, ensure_ascii=False)
        except Exception as e:
            print(f"[RAW WARN] {e}")

        if data is None:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY: break
            offset += STEP
            time.sleep(SLEEP_OK + jitter()); continue

        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY: break
            offset += STEP
            time.sleep(SLEEP_OK + jitter()); continue

        empty_streak = 0

        new_in_this_page = 0
        for p in data:
            pid = str(p.get("productId")) if p.get("productId") is not None else None
            if pid and pid not in seen_products:
                seen_products.add(pid)
                new_in_this_page += 1
            yield p

        # detección de estancamiento (no crece el set de productIds)
        if len(seen_products) == last_seen_count:
            stagnation += 1
        else:
            stagnation = 0
            last_seen_count = len(seen_products)

        if stagnation >= 2:  # 2 páginas seguidas sin nuevos productId → tope probable
            print(f"[STALL] {cat_path} sc={sc} params={extra_params} parece topeado (sin nuevos productId).")
            break

        offset += STEP
        time.sleep(SLEEP_OK + jitter())

# ========= Lógica de categoría con particiones =========

def process_products(products: Iterable[Dict[str, Any]], session, sc: Optional[int], enqueue_fn, stopper: StopController):
    for p in products:
        if stopper.tripped(): break
        # Fallback a detalle si no hay sellers
        if not product_has_sellers(p) and p.get("productId"):
            p_det = fetch_product_detail(session, str(p["productId"]), sc)
            if p_det and product_has_sellers(p_det):
                p = p_det
        _, ps = parse_rows_from_product_and_ps(p, BASE)
        for one in ps:
            enqueue_fn(one)


def fetch_category_with_partitions(session, cat_path: str, stopper: StopController, enqueue_fn):
    seen_products_global: Set[str] = set()

    # 1) Loop por sales channels
    for sc in SC_LIST:
        if stopper.tripped(): break
        print(f"[CAT] {cat_path} | sc={sc} | base")
        base_stream = stream_products(session, cat_path, sc, None, stopper, seen_products_global)
        process_products(base_stream, session, sc, enqueue_fn, stopper)

        # Si ya vimos muchos y aún parece topeado, activamos particiones
        if stopper.tripped(): break

        # 2) Partición por brand via facets
        if ENABLE_BRAND_PARTITION:
            facets = get_facets(session, cat_path, sc)
            brands = extract_brand_facets(facets)
            if brands:
                print(f"[FACETS] {cat_path} sc={sc} brands={len(brands)}")
                for b in brands:
                    if stopper.tripped(): break
                    # Map suele ser 'b' para brandId; Value es el id; para brand por nombre podría ser 'B'
                    fq = f"{b['Map']}:{b['Value']}"
                    extra = {"fq": fq}
                    print(f"[CAT] {cat_path} | sc={sc} | brand={b['Name']} ({fq})")
                    stream_b = stream_products(session, cat_path, sc, extra, stopper, seen_products_global)
                    process_products(stream_b, session, sc, enqueue_fn, stopper)

        # 3) Partición alfabética por ft=
        if ENABLE_ALPHA_PARTITION:
            for token in ALPHA_TOKENS:
                if stopper.tripped(): break
                extra = {"ft": token}
                print(f"[CAT] {cat_path} | sc={sc} | ft={token}")
                stream_ft = stream_products(session, cat_path, sc, extra, stopper, seen_products_global)
                process_products(stream_ft, session, sc, enqueue_fn, stopper)

        # 4) Partición por precio (buckets)
        if ENABLE_PRICE_PARTITION:
            for (lo, hi) in PRICE_BUCKETS:
                if stopper.tripped(): break
                # VTEX price range fq: P:[lo TO hi]
                extra = {"fq": f"P:[{lo} TO {hi}]"}
                print(f"[CAT] {cat_path} | sc={sc} | price=[{lo},{hi}]")
                stream_p = stream_products(session, cat_path, sc, extra, stopper, seen_products_global)
                process_products(stream_p, session, sc, enqueue_fn, stopper)

# ========= Inserción incremental =========

def insert_batch(conn, tienda_id: int, ps: List[Dict[str, Any]], capturado_en) -> int:
    if not ps: return 0
    total = 0
    seen = set()

    def _new_cursor():
        try:
            return conn.cursor()
        except Exception:
            conn.ping(reconnect=True, attempts=3, delay=1)
            return conn.cursor()

    cur = _new_cursor()
    done_in_batch = 0

    for p in ps:
        key = (p.get("sku"), p.get("record_id"), p.get("url"), p.get("precio_oferta"))
        if key in seen: continue
        seen.add(key)

        for attempt in range(2):
            try:
                producto_id = find_or_create_producto(cur, p)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                total += 1
                done_in_batch += 1
                break
            except MySQLError as e:
                errno = getattr(e, "errno", None)
                if errno == 1205:
                    try: conn.rollback()
                    except Exception: pass
                    wait = 0.7 * (2 ** attempt) + jitter()
                    print(f"[WARN] 1205 fila (sku={p.get('sku')}, rec={p.get('record_id')}). Retry en {wait:.2f}s…")
                    time.sleep(wait)
                    try: cur.close()
                    except Exception: pass
                    cur = _new_cursor()
                    if attempt == 1:
                        print(f"[SKIP] Fila omitida por lock persistente (sku={p.get('sku')}, rec={p.get('record_id')}).")
                    continue
                else:
                    raise

        if done_in_batch >= COMMIT_EVERY:
            try:
                conn.commit(); print(f"[mini-commit] +{done_in_batch} filas")
            except Exception as e:
                print(f"[WARN] Commit mini-lote falló: {e}; rollback…")
                try: conn.rollback()
                except Exception: pass
            done_in_batch = 0

    if done_in_batch:
        try:
            conn.commit(); print(f"[mini-commit] +{done_in_batch} filas (final)")
        except Exception as e:
            print(f"[WARN] Commit final mini-lote falló: {e}; rollback…")
            try: conn.rollback()
            except Exception: pass

    try: cur.close()
    except Exception: pass
    return total

# ========= Producer–Consumer =========

def db_writer(conn, tienda_id, stopper: StopController, q: Queue):
    from datetime import datetime as dt
    buffer: List[Dict[str, Any]] = []
    while not stopper.tripped() or not q.empty():
        try:
            p = q.get(timeout=1)
            buffer.append(p)
            if len(buffer) >= WRITER_BATCH:
                insert_batch(conn, tienda_id, buffer, dt.now())
                buffer = []
        except Empty:
            if buffer:
                insert_batch(conn, tienda_id, buffer, dt.now())
                buffer = []
    if buffer:
        insert_batch(conn, tienda_id, buffer, dt.now())

# ========= Main =========

def main():
    from datetime import datetime as dt
    stopper = StopController(); stopper.start()
    s0 = new_session()

    print("Descubriendo categorías…")
    tree = get_category_tree(s0, TREE_DEPTH)
    cat_paths = iter_paths(tree)
    print(f"Categorías detectadas: {len(cat_paths)}")

    # Conexión DB y tienda
    conn = get_conn()
    try:
        with conn.cursor() as cset:
            cset.execute("SET SESSION innodb_lock_wait_timeout = 15")
            cset.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
    except Exception:
        pass
    conn.autocommit = False
    cur = conn.cursor()
    tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
    conn.commit(); cur.close()

    q: Queue = Queue(maxsize=QUEUE_MAX)
    writer_t = threading.Thread(target=db_writer, args=(conn, tienda_id, stopper, q), daemon=True)
    writer_t.start()

    started = time.time()

    def enqueue_fn(pdict):
        while not stopper.tripped():
            try:
                q.put(pdict, timeout=0.5)
                break
            except:
                time.sleep(0.05)

    # Worker de categoría
    def one_category_worker(path):
        if stopper.tripped(): return 0
        session = new_session()
        before = time.time()
        try:
            fetch_category_with_partitions(session, path, stopper, enqueue_fn)
        finally:
            dt_s = time.time() - before
            print(f"[CAT DONE] {path} en {dt_s:.1f}s")
        return 1

    try:
        with ThreadPoolExecutor(max_workers=SCRAPER_WORKERS) as ex:
            futures = [ex.submit(one_category_worker, p) for p in cat_paths]
            for f in as_completed(futures):
                _ = f.result()
                if stopper.tripped():
                    break
    except KeyboardInterrupt:
        stopper._ev.set(); print("🛑 Ctrl+C detectado — cerrando…")
    finally:
        stopper._ev.set()
        writer_t.join(timeout=120)
        try:
            conn.commit()
        except Exception:
            try: conn.rollback()
            except Exception: pass
        try: conn.close()
        except Exception: pass
        elapsed = time.time() - started
        print(f"🏁 Finalizado {TIENDA_NOMBRE}. Tiempo total: {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
