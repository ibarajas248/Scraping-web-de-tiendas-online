#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Jumbo (VTEX) → MySQL con el patrón de inserción "Coto"
- Inserta en: tiendas, productos, producto_tienda, historico_precios
- Mapea VTEX → dict p con mismas claves que Coto
- PARADA POR ENTER para insertar parcial y salir.
- Robusto: árbol profundo, paginación, OOS visibles, orden estable,
  segmentación por marcas (facets) + Fallback alfabético por ft (fuzzy=0),
  dedupe por (sku, record_id, seller_id, url, precio_oferta),
  sales channel (sc) auto, pool HTTP, reintento 206, clean_text rápido,
  cache de producto_id, corte por offset y por errores.
- Manejo de errores VTEX 400 por tokens alfa “raros” (ü, ñ, etc.): se marcan
  como inválidos y se saltan sin detener la categoría.
- MySQL robusto ante 1205/1213: reintentos con SAVEPOINT, micro-commits.
"""

import requests, time, re, json, sys, os, threading
from html import unescape
from urllib.parse import quote
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
from mysql.connector import Error as MySQLError, errorcode
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========= Conexión =========
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- tu conexión MySQL

# ========= Config =========
BASE = "https://www.jumbo.com.ar"
STEP = 50
SLEEP_OK = 0.15
TIMEOUT = 10
RETRIES = 3
MAX_EMPTY = 8
TREE_DEPTH = 20                   # más profundo por si acaso
MAX_WORKERS_BRANDS = 4
MAX_OFFSET = 6000                 # evita 400 por rangos gigantes
FT_FALLBACK_TOKENS = (
    ["0","1","2","3","4","5","6","7","8","9"] +
    [chr(c) for c in range(ord("a"), ord("z")+1)] +
    ["á","é","í","ó","ú","ñ","ü"]               # español
)
# Tokens problemáticos detectados; se van añadiendo dinámicamente
BAD_ALPHA_TOKENS = set()

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

TIENDA_CODIGO = "jumbo_ar"
TIENDA_NOMBRE = "Jumbo Argentina"

ACTIVE_SC: Optional[int] = None  # detectado en runtime

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
    def start(self): self._t.start()
    def tripped(self) -> bool: return self._ev.is_set()

# ========= HTTP Session con pool =========
def make_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0)
    s.mount("https://", adapter); s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s

# ========= Helpers scraping =========
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
_price_clean_re = re.compile(r"[^\d,.\-]")
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean_text(v):
    if v is None: return ""
    if not isinstance(v, str): return v
    s = v.strip()
    if "<" not in s and "&" not in s:
        return ILLEGAL_XLSX.sub("", s)
    try:
        s = unescape(s)
        if "<" in s and ">" in s:
            from bs4 import BeautifulSoup
            s = BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", s)

def clean(val):
    if val is None: return None
    s = re.sub(r"\s+", " ", str(val).strip())
    return None if s.lower() in _NULLLIKE else s

def parse_price(val) -> float:
    if val is None or (isinstance(val, float) and np.isnan(val)): return np.nan
    if isinstance(val, (int, float)): return float(val)
    s = _price_clean_re.sub("", str(val).strip())
    if not s: return np.nan
    if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
    elif "," in s:           s = s.replace(",", ".")
    try: return float(s)
    except: return np.nan

def req_json(url, session, params=None):
    for i in range(RETRIES):
        try:
            r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
            try: print(f"🔎 GET {r.url} → {r.status_code}")
            except: pass
            if r.status_code == 200:
                try: return r.json()
                except Exception as e:
                    print(f"⚠️ Error parseando JSON (intento {i+1}): {e}"); time.sleep(0.6)
            elif r.status_code == 206:
                print("⚠️ 206 Partial Content; reintentando rápido…"); time.sleep(0.4); continue
            elif r.status_code in (429, 408, 500, 502, 503, 504):
                time.sleep(0.5 + 0.5*i)
            elif r.status_code == 400:
                # devolvemos None para que el caller incremente err_streak
                time.sleep(0.2 + 0.2*i); return None
            else:
                time.sleep(0.2 + 0.2*i)
        except requests.RequestException as e:
            print(f"⚠️ Error HTTP (intento {i+1}): {e}"); time.sleep(0.5 + 0.5*i)
    return None

# ========= Detección de sales channel (sc) =========
def _resp_is_inactive_sc(obj) -> bool:
    if obj is None: return False
    if isinstance(obj, dict):
        msg = (obj.get("message") or obj.get("Message") or "").lower()
        if "sc is inactive" in msg or "inactive sales channel" in msg: return True
    return False

def detect_active_sc(session) -> Optional[int]:
    test_url = f"{BASE}/api/catalog_system/pub/products/search/"
    for sc in [None,1,2,3,4,5,6]:
        params = {"ft":"a","_from":0,"_to":0,"hideUnavailableItems":"false","O":"OrderByNameASC"}
        if sc is not None: params["sc"]=str(sc)
        data = req_json(test_url, session, params=params)
        if _resp_is_inactive_sc(data):
            print(f"⚠️  sc={sc} inactivo. Probando otro…"); continue
        if isinstance(data, (list, dict)):
            print(f"✅ Usando sc={sc if sc is not None else '(sin sc)'}"); return sc
    print("⚠️  Ningún sc válido detectado. Seguiremos sin 'sc'."); return None

# ========= Categorías =========
def get_category_tree(session, depth=TREE_DEPTH):
    url = f"{BASE}/api/catalog_system/pub/category/tree/{depth}"
    return req_json(url, session) or []

def _slug_from_node(node):
    return (node.get("url","").strip("/").split("/")[-1] or node.get("slug") or node.get("Name"))

def iter_all_paths_and_leaves(tree):
    """Devuelve (all_paths, leaf_paths) ambos como rutas 'a/b/c' normalizadas."""
    all_paths, leaf_paths = [], []
    def walk(node, path):
        slug = _slug_from_node(node)
        if not slug: return
        new_path = path + [slug]
        joined = "/".join(new_path).strip("/").lower()
        all_paths.append(joined)
        children = node.get("children") or []
        if not children:
            leaf_paths.append(joined)
        for ch in children:
            walk(ch, new_path)
    for n in tree: walk(n, [])
    # dedup preservando orden
    def uniq(seq):
        out, seen = [], set()
        for x in seq:
            if x and x not in seen:
                seen.add(x); out.append(x)
        return out
    return uniq(all_paths), uniq(leaf_paths)

def map_for_path(path_str):
    depth = len([p for p in path_str.split("/") if p])
    return ",".join(["c"] * depth)

# ========= MySQL helpers =========
LOCK_ERRNOS = {1205, 1213}  # 1205=Lock wait timeout, 1213=Deadlock

def is_lock_or_deadlock(e: MySQLError) -> bool:
    try:
        return getattr(e, 'errno', None) in LOCK_ERRNOS or \
               'Lock wait timeout' in str(e) or 'Deadlock found' in str(e)
    except:
        return False

def mysql_exec_with_retry(conn, func, *, max_retries=5, backoff_base=0.4, savepoint_name=None):
    """
    Ejecuta func(cur) con reintentos ante 1205/1213.
    Usa SAVEPOINT si se proporciona, para evitar perder toda la transacción.
    """
    attempt = 0
    while True:
        try:
            cur = conn.cursor()
            if savepoint_name:
                cur.execute(f"SAVEPOINT {savepoint_name}")
            res = func(cur)
            if savepoint_name:
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            return res
        except MySQLError as e:
            if is_lock_or_deadlock(e) and attempt < max_retries:
                wait = backoff_base * (2 ** attempt)
                print(f"⏳ MySQL lock/deadlock (intent {attempt+1}/{max_retries}). Retrying in {wait:.2f}s …")
                try:
                    if savepoint_name:
                        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                except Exception:
                    pass
                time.sleep(wait)
                attempt += 1
                continue
            # fuera de reintentos → relanzar
            raise

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)", (codigo, nombre)
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
                  nombre=COALESCE(NULLIF(%s,''),nombre),
                  marca=COALESCE(NULLIF(%s,''),marca),
                  fabricante=COALESCE(NULLIF(%s,''),fabricante),
                  categoria=COALESCE(NULLIF(%s,''),categoria),
                  subcategoria=COALESCE(NULLIF(%s,''),subcategoria)
                WHERE id=%s
            """, (p.get("nombre") or "", p.get("marca") or "", p.get("fabricante") or "",
                  p.get("categoria") or "", p.get("subcategoria") or "", pid))
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
                  ean=COALESCE(NULLIF(%s,''),ean),
                  fabricante=COALESCE(NULLIF(%s,''),fabricante),
                  categoria=COALESCE(NULLIF(%s,''),categoria),
                  subcategoria=COALESCE(NULLIF(%s,''),subcategoria)
                WHERE id=%s
            """, (p.get("ean") or "", p.get("fabricante") or "",
                  p.get("categoria") or "", p.get("subcategoria") or "", pid))
            return pid
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (p.get("ean") or "", nombre, marca, p.get("fabricante") or "",
          p.get("categoria") or "", p.get("subcategoria") or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku")); rec = clean(p.get("record_id"))
    url = p.get("url") or ""; nombre_tienda = p.get("nombre") or ""
    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id=LAST_INSERT_ID(id),
              producto_id=VALUES(producto_id),
              record_id_tienda=COALESCE(VALUES(record_id_tienda),record_id_tienda),
              url_tienda=COALESCE(VALUES(url_tienda),url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda),nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid
    if rec:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s,%s,NULL,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id=LAST_INSERT_ID(id),
              producto_id=VALUES(producto_id),
              url_tienda=COALESCE(VALUES(url_tienda),url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda),nombre_tienda)
        """, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''))
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
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          precio_lista=VALUES(precio_lista),
          precio_oferta=VALUES(precio_oferta),
          tipo_oferta=VALUES(tipo_oferta),
          promo_tipo=VALUES(promo_tipo),
          promo_texto_regular=VALUES(promo_texto_regular),
          promo_texto_descuento=VALUES(promo_texto_descuento),
          promo_comentarios=VALUES(promo_comentarios)
    """, (tienda_id, producto_tienda_id, capturado_en,
          to_txt_or_none(p.get("precio_lista")), to_txt_or_none(p.get("precio_oferta")),
          p.get("tipo_oferta") or None, p.get("promo_tipo") or None,
          p.get("precio_regular_promo") or None, p.get("precio_descuento") or None,
          p.get("comentarios_promo") or None))

# ========= Cache producto_id =========
_producto_cache_by_ean: Dict[str, int] = {}
_producto_cache_by_nm: Dict[Tuple[str, str], int] = {}
def _cache_get_producto_id(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
    if ean and ean in _producto_cache_by_ean: return _producto_cache_by_ean[ean]
    nombre = clean(p.get("nombre")) or ""; marca  = clean(p.get("marca")) or ""
    key_nm = (nombre, marca) if (nombre and marca) else None
    if key_nm and key_nm in _producto_cache_by_nm: return _producto_cache_by_nm[key_nm]
    pid = find_or_create_producto(cur, p)
    if ean: _producto_cache_by_ean[ean] = pid
    if key_nm: _producto_cache_by_nm[key_nm] = pid
    return pid

# ========= Parsing =========
def parse_rows_from_product_and_ps(p, base):
    rows, ps = [], []
    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = p.get("brand"); brand_id = p.get("brandId")
    link_text = p.get("linkText"); link = f"{base}/{link_text}/p" if link_text else ""
    categories = [c.strip("/") for c in (p.get("categories") or [])]
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

    desc = clean_text(p.get("description") or p.get("descriptionShort") or p.get("metaTagDescription") or "")
    items = p.get("items") or []
    for it in items:
        sku_id = it.get("itemId")
        sku_name = clean_text(it.get("name"))
        ean = ""
        for ref in (it.get("referenceId") or []):
            if ref.get("Value"): ean = ref["Value"]; break
        measurement_unit = it.get("measurementUnit"); unit_multiplier = it.get("unitMultiplier")
        images = ", ".join(img.get("imageUrl", "") for img in (it.get("images") or []))

        sellers = it.get("sellers") or []
        if not sellers:
            rows.append({
                "productId": product_id, "skuId": sku_id, "sellerId": "", "sellerName": "",
                "availableQty": None, "price": None, "listPrice": None, "priceWithoutDiscount": None,
                "installments_json": "", "teasers_json": "", "tax": None, "rewardValue": None,
                "spotPrice": None, "name": name, "skuName": sku_name, "brand": brand, "brandId": brand_id,
                "ean": ean, "categoryFull": full_category_path, "link": link, "linkText": link_text,
                "measurementUnit": measurement_unit, "unitMultiplier": unit_multiplier,
                "images": images, "description": desc, "specs_json": json.dumps(specs, ensure_ascii=False),
            })
            ps.append({
                "sku": clean(sku_id), "record_id": clean(product_id), "seller_id": None,
                "ean": clean(ean), "nombre": clean(name), "marca": clean(brand),
                "fabricante": None, "precio_lista": None, "precio_oferta": None,
                "tipo_oferta": None, "promo_tipo": None,
                "precio_regular_promo": None, "precio_descuento": None, "comentarios_promo": None,
                "categoria": clean(categoria), "subcategoria": clean(subcategoria), "url": clean(link),
            })
            continue

        for s in sellers:
            s_id = s.get("sellerId"); s_name = s.get("sellerName")
            offer = s.get("commertialOffer") or {}
            price = offer.get("Price"); list_price = offer.get("ListPrice")
            pwd = offer.get("PriceWithoutDiscount"); avail = offer.get("AvailableQuantity")
            tax = offer.get("Tax"); reward = offer.get("RewardValue")
            installments = offer.get("Installments") or []; teasers = offer.get("Teasers") or []
            spot = offer.get("spotPrice", None)

            rows.append({
                "productId": product_id, "skuId": sku_id, "sellerId": s_id, "sellerName": s_name,
                "availableQty": avail, "price": price, "listPrice": list_price,
                "priceWithoutDiscount": pwd, "installments_json": json.dumps(installments, ensure_ascii=False),
                "teasers_json": json.dumps(teasers, ensure_ascii=False), "tax": tax, "rewardValue": reward,
                "spotPrice": spot, "name": name, "skuName": sku_name, "brand": brand, "brandId": brand_id,
                "ean": ean, "categoryFull": full_category_path, "link": link, "linkText": link_text,
                "measurementUnit": measurement_unit, "unitMultiplier": unit_multiplier,
                "images": images, "description": desc, "specs_json": json.dumps(specs, ensure_ascii=False),
            })

            promo_txts = []
            for t in teasers:
                nm = t.get("name") or t.get("title") or ""
                if nm: promo_txts.append(str(nm))
            promo_tipo = "; ".join(promo_txts) if promo_txts else None

            ps.append({
                "sku": clean(sku_id), "record_id": clean(product_id), "seller_id": clean(s_id),
                "ean": clean(ean), "nombre": clean(name), "marca": clean(brand),
                "fabricante": None, "precio_lista": list_price,
                "precio_oferta": (spot if spot not in (None, "") else price),
                "tipo_oferta": None, "promo_tipo": promo_tipo,
                "precio_regular_promo": None, "precio_descuento": None, "comentarios_promo": None,
                "categoria": clean(categoria), "subcategoria": clean(subcategoria), "url": clean(link),
            })

    return rows, ps

# ========= Facets (Brands) =========
def _collect_brand_ids(facets_json) -> List[int]:
    out = set()
    if not facets_json: return []
    for b in (facets_json.get("Brands") or []):
        bid = b.get("Id") or b.get("Value") or b.get("id")
        try:
            if bid is not None: out.add(int(bid))
        except: pass
    for grp in (facets_json.get("Facets") or []):
        nm = (grp.get("Name") or grp.get("name") or "").lower()
        if nm in ("marcas", "brand", "brands"):
            for b in (grp.get("Values") or grp.get("values") or []):
                bid = b.get("Id") or b.get("Value") or b.get("id")
                try:
                    if bid is not None: out.add(int(bid))
                except: pass
    return sorted(out)

def get_brands_for_path(session, encoded_path, map_str) -> List[int]:
    params = {"map": map_str, "hideUnavailableItems": "false"}
    if ACTIVE_SC is not None: params["sc"] = str(ACTIVE_SC)
    url_facets = f"{BASE}/api/catalog_system/pub/facets/search/{encoded_path}"
    f = req_json(url_facets, session, params=params) or {}
    return _collect_brand_ids(f)

# ========= Fallback alfabético (ft) =========
def _fetch_alpha_shards(session, encoded_path, map_str, bid, stopper: StopController):
    """Divide por tokens alfabéticos (ft, fuzzy=0) para vaciar el resto."""
    rows, ps_all = [], []
    for tok in FT_FALLBACK_TOKENS:
        if stopper.tripped(): break
        if tok in BAD_ALPHA_TOKENS:
            # Token previamente marcado como problemático
            continue

        offset, empty_streak, last_page_len, err_streak = 0, 0, STEP, 0
        while not stopper.tripped():
            if offset > MAX_OFFSET:
                print(f"   · α-shard '{tok}' corte por MAX_OFFSET={MAX_OFFSET} (brand={bid})")
                break
            params = {
                "map": map_str,
                "_from": offset, "_to": offset + STEP - 1,
                "hideUnavailableItems": "false",
                "O": "OrderByNameASC",
                "ft": tok, "fuzzy": "0",
            }
            if ACTIVE_SC is not None: params["sc"] = str(ACTIVE_SC)
            if bid is not None: params["fq"] = f"B:{bid}"
            url = f"{BASE}/api/catalog_system/pub/products/search/{encoded_path}"
            data = req_json(url, session, params=params)
            if _resp_is_inactive_sc(data):
                print("❌ 'sc' inactivo en α-shard."); break

            if data is None:
                err_streak += 1
                if err_streak >= 3:
                    print(f"   · α-shard '{tok}' marcado INVÁLIDO (x{err_streak}). Se omite y seguimos.")
                    BAD_ALPHA_TOKENS.add(tok)
                    break  # salimos de este token y vamos al siguiente
                offset += STEP; time.sleep(SLEEP_OK); continue

            err_streak = 0
            if not data:
                empty_streak += 1
                if empty_streak >= MAX_EMPTY and last_page_len < STEP: break
                offset += STEP; time.sleep(SLEEP_OK); continue

            empty_streak = 0; last_page_len = len(data)
            for p in data:
                r, ps = parse_rows_from_product_and_ps(p, BASE)
                rows.extend(r); ps_all.extend(ps)
            offset += STEP; time.sleep(SLEEP_OK)
    return rows, ps_all

# ========= Fetch por marca (shard) =========
def _fetch_brand_shard(session, encoded_path, map_str, bid, stopper: StopController):
    rows, ps_all = [], []
    offset, empty_streak, last_page_len, err_streak = 0, 0, STEP, 0
    used_alpha_fallback = False

    while not stopper.tripped():
        if offset > MAX_OFFSET:
            print(f"⚠️ Corte por MAX_OFFSET={MAX_OFFSET} en brand={bid} → activando fallback alfabético")
            used_alpha_fallback = True
            r2, p2 = _fetch_alpha_shards(session, encoded_path, map_str, bid, stopper)
            rows.extend(r2); ps_all.extend(p2)
            break

        params = {
            "map": map_str,
            "_from": offset, "_to": offset + STEP - 1,
            "hideUnavailableItems": "false", "O": "OrderByNameASC",
        }
        if ACTIVE_SC is not None: params["sc"] = str(ACTIVE_SC)
        if bid is not None: params["fq"] = f"B:{bid}"

        url = f"{BASE}/api/catalog_system/pub/products/search/{encoded_path}"
        data = req_json(url, session, params=params)

        if _resp_is_inactive_sc(data):
            print(f"❌ 'sc' inactivo para brand {bid}."); break

        if data is None:
            err_streak += 1
            if err_streak >= 3:
                print(f"⚠️ Errores repetidos (x{err_streak}) en brand={bid}, offset={offset} → fallback alfabético")
                used_alpha_fallback = True
                r2, p2 = _fetch_alpha_shards(session, encoded_path, map_str, bid, stopper)
                rows.extend(r2); ps_all.extend(p2)
                break
            offset += STEP; time.sleep(SLEEP_OK); continue

        err_streak = 0

        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY and last_page_len < STEP:
                break
            offset += STEP; time.sleep(SLEEP_OK); continue

        empty_streak = 0
        last_page_len = len(data)
        for p in data:
            r, ps = parse_rows_from_product_and_ps(p, BASE)
            rows.extend(r); ps_all.extend(ps)

        offset += STEP; time.sleep(SLEEP_OK)

    if not used_alpha_fallback and last_page_len == STEP and not stopper.tripped():
        print(f"ℹ️ brand={bid}: cola llena; ejecutando α-shards por seguridad…")
        r2, p2 = _fetch_alpha_shards(session, encoded_path, map_str, bid, stopper)
        rows.extend(r2); ps_all.extend(p2)

    return rows, ps_all

# ========= Scrape por categoría (paralelizando por marca) =========
def fetch_category(session, cat_path, stopper: StopController):
    rows, ps_all = [], []
    map_str = map_for_path(cat_path)
    encoded_path = quote(cat_path, safe="/")

    brand_ids = get_brands_for_path(session, encoded_path, map_str)
    brand_ids_or_none = brand_ids or [None]
    print(f"   · {cat_path}: {len(brand_ids) if brand_ids else 0} marcas; paralelo={MAX_WORKERS_BRANDS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_BRANDS) as ex:
        futs = [ex.submit(_fetch_brand_shard, session, encoded_path, map_str, bid, stopper)
                for bid in brand_ids_or_none]
        for fut in as_completed(futs):
            r, ps = fut.result()
            rows.extend(r); ps_all.extend(ps)

    if not rows and not ps_all:
        print(f"⚠️  Categoría '{cat_path}' sin resultados, continuando…")

    return rows, ps_all

# ========= Inserción en DB =========
def insert_batch(conn, tienda_id: int, ps: List[Dict[str, Any]], capturado_en: datetime, chunk_size: int = 200) -> int:
    """
    Inserta con micro-commits y reintentos por fila ante 1205/1213.
    """
    if not ps: return 0
    total, seen = 0, set()
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT sp_batch")
    except Exception:
        pass

    def _upsert_one(cur, p):
        producto_id = _cache_get_producto_id(cur, p)
        pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
        insert_historico(cur, tienda_id, pt_id, p, capturado_en)

    pending = 0
    for p in ps:
        key = (p.get("sku"), p.get("record_id"), p.get("seller_id"), p.get("url"), p.get("precio_oferta"))
        if key in seen:
            continue
        seen.add(key)

        try:
            mysql_exec_with_retry(
                conn,
                lambda c: _upsert_one(c, p),
                max_retries=5,
                backoff_base=0.35,
                savepoint_name="sp_row"
            )
        except MySQLError as e:
            print(f"⚠️ Saltando fila por MySQL ({str(e)[:160]}…) → seguimos")
            try:
                cur.execute("ROLLBACK TO SAVEPOINT sp_row")
            except Exception:
                pass
            continue

        total += 1
        pending += 1

        if pending >= chunk_size:
            # micro-commit para acortar transacciones y liberar locks
            try:
                conn.commit()
                try:
                    cur.execute("SAVEPOINT sp_batch")
                except Exception:
                    pass
            except MySQLError as e:
                print(f"⚠️ Commit parcial falló: {e}. Reintentando…")
                time.sleep(0.6)
            pending = 0

    return total

# ========= Main =========
def main():
    stopper = StopController(); stopper.start()
    session = make_session()

    global ACTIVE_SC
    ACTIVE_SC = detect_active_sc(session)

    print("Descubriendo categorías…")
    tree = get_category_tree(session, TREE_DEPTH)
    all_paths, leaf_paths = iter_all_paths_and_leaves(tree)
    print(f"Total rutas: {len(all_paths)} | Hojas reales: {len(leaf_paths)}")

    # Trabaja SOLO hojas reales
    cat_paths = leaf_paths

    conn = None
    capturado_en = datetime.now()
    total_insertados = 0

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        # Fallar rápido ante bloqueos y reducir contención
        try:
            cur.execute("SET SESSION innodb_lock_wait_timeout=6")
            cur.execute("SET SESSION transaction_isolation='READ-COMMITTED'")
        except Exception:
            pass

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        conn.commit()

        for i, path in enumerate(cat_paths, 1):
            if stopper.tripped():
                print("🛑 Parada solicitada. Guardando lo acumulado…"); break

            print(f"[{i}/{len(cat_paths)}] {path}")
            rows, ps = fetch_category(session, path, stopper)

            if ps:
                try:
                    inc = insert_batch(conn, tienda_id, ps, capturado_en)
                    conn.commit()  # commit de seguridad tras la categoría
                    total_insertados += inc
                    print(f"💾 Commit categoría '{path}' → +{inc} registros (acum: {total_insertados})")
                except MySQLError as e:
                    print(f"❌ Error MySQL en categoría '{path}': {e}")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    # Reabrir conexión y seguir con la siguiente categoría
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = get_conn()
                    conn.autocommit = False
                    c2 = conn.cursor()
                    try:
                        c2.execute("SET SESSION innodb_lock_wait_timeout=6")
                        c2.execute("SET SESSION transaction_isolation='READ-COMMITTED'")
                    except Exception:
                        pass

            if stopper.tripped(): break
            time.sleep(0.25)

    except KeyboardInterrupt:
        print("🛑 Interrumpido por usuario (Ctrl+C). Guardando lo acumulado…")
        try:
            if conn: conn.commit()
        except Exception:
            pass
    finally:
        if conn:
            try: conn.close()
            except: pass
        print(f"🏁 Finalizado. Histórico insertado: {total_insertados} filas para {TIENDA_NOMBRE} ({capturado_en})")

if __name__ == "__main__":
    main()
