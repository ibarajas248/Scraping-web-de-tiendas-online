#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Jumbo (VTEX) → MySQL (patrón "Coto" endurecido) + XLSX
# - Igual mapeo de datos y política de precios que tu “reporte multi-super por EAN”
# - Ajusta nombres de tablas/columnas en MAPEO si tu esquema difiere.

import os, sys, re, json, time, threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from html import unescape
from mysql.connector import Error as MySQLError, errors as myerr

# ========= Conexión (usa TU helper) =========
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- Debe retornar mysql.connector.connect(...)

# ========= MAPEO DE TABLAS/ COLUMNAS (ajustable) =========
T_TAB_TIENDAS          = "tiendas"
T_COL_TIENDA_ID        = "id"
T_COL_TIENDA_CODIGO    = "codigo"
T_COL_TIENDA_NOMBRE    = "nombre"

T_TAB_PRODUCTOS        = "productos"
T_COL_PROD_ID          = "id"
T_COL_PROD_EAN         = "ean"
T_COL_PROD_NOMBRE      = "nombre"
T_COL_PROD_MARCA       = "marca"
T_COL_PROD_FAB         = "fabricante"
T_COL_PROD_CAT         = "categoria"
T_COL_PROD_SUBCAT      = "subcategoria"

T_TAB_PROD_TIENDA      = "producto_tienda"
T_COL_PT_ID            = "id"
T_COL_PT_TIENDA_ID     = "tienda_id"
T_COL_PT_PROD_ID       = "producto_id"
T_COL_PT_SKU           = "sku_tienda"
T_COL_PT_RECORD        = "record_id_tienda"
T_COL_PT_URL           = "url_tienda"
T_COL_PT_NOMBRE        = "nombre_tienda"

T_TAB_HISTORICO        = "historico_precios"
T_COL_H_TIENDA_ID      = "tienda_id"
T_COL_H_PT_ID          = "producto_tienda_id"
T_COL_H_CAPTURADO      = "capturado_en"
T_COL_H_PRECIO_LISTA   = "precio_lista"
T_COL_H_PRECIO_OFERTA  = "precio_oferta"
T_COL_H_TIPO_OFERTA    = "tipo_oferta"
T_COL_H_PROMO_TIPO     = "promo_tipo"
T_COL_H_PROMO_TXT_REG  = "promo_texto_regular"
T_COL_H_PROMO_TXT_DESC = "promo_texto_descuento"
T_COL_H_PROMO_COMENT   = "promo_comentarios"

# ========= Config VTEX =========
BASE = "https://www.jumbo.com.ar"
TREE_DEPTH = 5
STEP = 50                    # paginación VTEX: 0-49, 50-99, ...
TIMEOUT = 25
RETRIES = 3
SLEEP_OK = 0.25
MAX_EMPTY = 8

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# Identidad tienda
TIENDA_CODIGO = BASE
TIENDA_NOMBRE = "Jumbo Argentina"

# ========= Límites/Batch =========
MAXLEN_NOMBRE       = 255
MAXLEN_URL          = 512
MAXLEN_TIPO_OFERTA  = 191
MAXLEN_PROMO_TXT    = 191
MAXLEN_PROMO_COMENT = 255
LOCK_ERRNOS = {1205, 1213}
ERRNO_OUT_OF_RANGE = 1264
COMMIT_EVERY = 150

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

# ========= Helpers texto/precio =========
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
_price_clean_re = re.compile(r"[^\d,.\-]")
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean_text(v):
    if v is None: return ""
    if not isinstance(v, str): return v
    try:
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", v)

def clean(val):
    if val is None: return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

def parse_price(val) -> float:
    if val is None or (isinstance(val, float) and np.isnan(val)): return np.nan
    if isinstance(val, (int, float)): return float(val)
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

def _truncate(s, n):
    if s is None: return None
    s = str(s)
    return s if len(s) <= n else s[:n]

def _price_txt_or_none(x):
    v = parse_price(x)
    if x is None: return None
    if isinstance(v, float) and np.isnan(v): return None
    return f"{round(float(v), 2)}"

# ========= HTTP JSON con reintentos =========
def req_json(url, session, params=None):
    for i in range(RETRIES):
        r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                time.sleep(0.6)
        elif r.status_code in (429, 408, 500, 502, 503, 504):
            time.sleep(0.6 + 0.4*i)
        else:
            time.sleep(0.3)
    return None

# ========= EAN y precios (idénticos criterios a tu app VTEX) =========
def _is_ean_candidate(s: str) -> bool:
    if not s: return False
    s = re.sub(r"\D", "", str(s))
    return len(s) in (8, 12, 13, 14)

def extract_ean_from_item(it: Dict[str, Any]) -> Optional[str]:
    direct = (it.get("ean") or it.get("EAN") or it.get("Ean"))
    if direct and _is_ean_candidate(direct):
        return re.sub(r"\D", "", str(direct))
    for ref in (it.get("referenceId") or []):
        k = (ref.get("Key") or ref.get("key") or "").strip().lower()
        v = (ref.get("Value") or ref.get("value") or "").strip()
        if k in ("ean", "barcode", "bar-code") and _is_ean_candidate(v):
            return re.sub(r"\D", "", v)
    for ref in (it.get("referenceId") or []):
        v = (ref.get("Value") or ref.get("value") or "").strip()
        if _is_ean_candidate(v):
            return re.sub(r"\D", "", v)
    return None

def _fnum(x):
    try:
        if x is None or (isinstance(x, str) and not x.strip()): return None
        f = float(x)
        return f if f > 0 else None
    except Exception:
        return None

def _derive_prices(co: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    p   = _fnum(co.get("Price"))
    l   = _fnum(co.get("ListPrice"))
    pwd = _fnum(co.get("PriceWithoutDiscount"))
    lista = oferta = None
    promo_tipo = None
    if pwd is not None and p is not None and pwd >= p and p > 0:
        lista, oferta, promo_tipo = pwd, p, "promo_pwd"
    elif l is not None and p is not None and l >= p and p > 0:
        lista, oferta, promo_tipo = l, p, "promo_listprice"
    elif p is not None and p > 0:
        lista, oferta, promo_tipo = p, None, None
    elif l is not None and l > 0:
        lista, oferta, promo_tipo = l, None, None
    else:
        lista, oferta, promo_tipo = None, None, None
    sp = _fnum(co.get("spotPrice"))
    if sp is not None and (oferta is None or sp < oferta):
        oferta = sp
    return lista, oferta, promo_tipo

# ========= Árbol de categorías =========
def get_category_tree(session, depth=TREE_DEPTH):
    url = f"{BASE}/api/catalog_system/pub/category/tree/{depth}"
    return req_json(url, session) or []

def iter_paths(tree):
    out = []
    def walk(node, path):
        slug = node.get("url", "").strip("/").split("/")[-1] or node.get("slug") or node.get("Name")
        if not slug: return
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
            seen.add(ps); uniq.append(ps)
    return uniq

def map_for_path(path_str): return ",".join(["c"] * len([p for p in path_str.split("/") if p]))

# ========= SQL con reintentos =========
def exec_retry(cur, sql, params=(), max_retries=5, base_sleep=0.5):
    att = 0
    while True:
        try:
            cur.execute(sql, params); return
        except myerr.DatabaseError as e:
            code = getattr(e, "errno", None)
            if code in LOCK_ERRNOS and att < max_retries:
                time.sleep(base_sleep * (2 ** att)); att += 1; continue
            raise

# ========= Helpers MySQL (respetan el MAPEO) =========
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    sql_ins = f"""
        INSERT INTO {T_TAB_TIENDAS} ({T_COL_TIENDA_CODIGO}, {T_COL_TIENDA_NOMBRE})
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE {T_COL_TIENDA_NOMBRE}=VALUES({T_COL_TIENDA_NOMBRE})
    """
    exec_retry(cur, sql_ins, (codigo, nombre))
    sql_get = f"SELECT {T_COL_TIENDA_ID} FROM {T_TAB_TIENDAS} WHERE {T_COL_TIENDA_CODIGO}=%s LIMIT 1"
    exec_retry(cur, sql_get, (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
    nombre = _truncate((clean(p.get("nombre")) or ""), MAXLEN_NOMBRE)
    marca  = clean(p.get("marca")) or ""
    categoria   = clean(p.get("categoria"))
    subcategoria= clean(p.get("subcategoria"))
    fabricante  = clean(p.get("fabricante"))

    if ean:
        sql_get = f"SELECT {T_COL_PROD_ID} FROM {T_TAB_PRODUCTOS} WHERE {T_COL_PROD_EAN}=%s LIMIT 1"
        exec_retry(cur, sql_get, (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            sql_up = f"""
                UPDATE {T_TAB_PRODUCTOS} SET
                  {T_COL_PROD_NOMBRE} = COALESCE(NULLIF(%s,''), {T_COL_PROD_NOMBRE}),
                  {T_COL_PROD_MARCA}  = COALESCE(NULLIF(%s,''), {T_COL_PROD_MARCA}),
                  {T_COL_PROD_FAB}    = COALESCE(NULLIF(%s,''), {T_COL_PROD_FAB}),
                  {T_COL_PROD_CAT}    = COALESCE(NULLIF(%s,''), {T_COL_PROD_CAT}),
                  {T_COL_PROD_SUBCAT} = COALESCE(NULLIF(%s,''), {T_COL_PROD_SUBCAT})
                WHERE {T_COL_PROD_ID}=%s
            """
            exec_retry(cur, sql_up, (nombre, marca or "", fabricante or "", categoria or "", subcategoria or "", pid))
            return pid

    if nombre and marca:
        sql_find_nm = f"""
            SELECT {T_COL_PROD_ID} FROM {T_TAB_PRODUCTOS}
            WHERE {T_COL_PROD_NOMBRE}=%s AND IFNULL({T_COL_PROD_MARCA},'')=%s LIMIT 1
        """
        exec_retry(cur, sql_find_nm, (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            sql_up2 = f"""
                UPDATE {T_TAB_PRODUCTOS} SET
                  {T_COL_PROD_EAN}    = COALESCE(NULLIF(%s,''), {T_COL_PROD_EAN}),
                  {T_COL_PROD_FAB}    = COALESCE(NULLIF(%s,''), {T_COL_PROD_FAB}),
                  {T_COL_PROD_CAT}    = COALESCE(NULLIF(%s,''), {T_COL_PROD_CAT}),
                  {T_COL_PROD_SUBCAT} = COALESCE(NULLIF(%s,''), {T_COL_PROD_SUBCAT})
                WHERE {T_COL_PROD_ID}=%s
            """
            exec_retry(cur, sql_up2, (ean or "", fabricante or "", categoria or "", subcategoria or "", pid))
            return pid

    sql_ins = f"""
        INSERT INTO {T_TAB_PRODUCTOS}
          ({T_COL_PROD_EAN},{T_COL_PROD_NOMBRE},{T_COL_PROD_MARCA},{T_COL_PROD_FAB},{T_COL_PROD_CAT},{T_COL_PROD_SUBCAT})
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """
    exec_retry(cur, sql_ins, (ean or "", nombre, marca or "", fabricante or "", categoria or "", subcategoria or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = _truncate(clean(p.get("url")), MAXLEN_URL)
    nombre_tienda = _truncate((clean(p.get("nombre")) or ""), MAXLEN_NOMBRE)

    if sku:
        sql = f"""
            INSERT INTO {T_TAB_PROD_TIENDA}
              ({T_COL_PT_TIENDA_ID},{T_COL_PT_PROD_ID},{T_COL_PT_SKU},{T_COL_PT_RECORD},{T_COL_PT_URL},{T_COL_PT_NOMBRE})
            VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              {T_COL_PT_ID} = LAST_INSERT_ID({T_COL_PT_ID}),
              {T_COL_PT_PROD_ID} = VALUES({T_COL_PT_PROD_ID}),
              {T_COL_PT_URL} = COALESCE(VALUES({T_COL_PT_URL}), {T_COL_PT_URL}),
              {T_COL_PT_NOMBRE} = COALESCE(VALUES({T_COL_PT_NOMBRE}), {T_COL_PT_NOMBRE})
        """
        exec_retry(cur, sql, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid

    if rec:
        sql2 = f"""
            INSERT INTO {T_TAB_PROD_TIENDA}
              ({T_COL_PT_TIENDA_ID},{T_COL_PT_PROD_ID},{T_COL_PT_SKU},{T_COL_PT_RECORD},{T_COL_PT_URL},{T_COL_PT_NOMBRE})
            VALUES (%s,%s,NULL,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              {T_COL_PT_ID} = LAST_INSERT_ID({T_COL_PT_ID}),
              {T_COL_PT_PROD_ID} = VALUES({T_COL_PT_PROD_ID}),
              {T_COL_PT_URL} = COALESCE(VALUES({T_COL_PT_URL}), {T_COL_PT_URL}),
              {T_COL_PT_NOMBRE} = COALESCE(VALUES({T_COL_PT_NOMBRE}), {T_COL_PT_NOMBRE})
        """
        exec_retry(cur, sql2, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    sql3 = f"""
        INSERT INTO {T_TAB_PROD_TIENDA}
          ({T_COL_PT_TIENDA_ID},{T_COL_PT_PROD_ID},{T_COL_PT_URL},{T_COL_PT_NOMBRE})
        VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''))
    """
    exec_retry(cur, sql3, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    precio_lista_txt  = _price_txt_or_none(p.get("precio_lista"))
    oferta_val = p.get("precio_oferta")
    precio_oferta_txt = _price_txt_or_none(oferta_val if oferta_val not in (None, "") else p.get("precio_lista"))
    tipo_oferta = None
    promo_tipo  = _truncate(clean(p.get("promo_tipo")), MAXLEN_PROMO_TXT)
    promo_regular = None
    promo_desc    = None
    comentarios = []
    if p.get("categoria"):   comentarios.append(f"cat={p['categoria']}")
    if p.get("subcategoria"):comentarios.append(f"sub={p['subcategoria']}")
    oferta_tags = clean(p.get("oferta_tags"))
    if oferta_tags: comentarios.append(f"tags={oferta_tags}")
    promo_comentarios = _truncate(" | ".join(comentarios), MAXLEN_PROMO_COMENT) if comentarios else None

    sql = f"""
        INSERT INTO {T_TAB_HISTORICO}
          ({T_COL_H_TIENDA_ID},{T_COL_H_PT_ID},{T_COL_H_CAPTURADO},
           {T_COL_H_PRECIO_LISTA},{T_COL_H_PRECIO_OFERTA},{T_COL_H_TIPO_OFERTA},
           {T_COL_H_PROMO_TIPO},{T_COL_H_PROMO_TXT_REG},{T_COL_H_PROMO_TXT_DESC},{T_COL_H_PROMO_COMENT})
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          {T_COL_H_PRECIO_LISTA}=VALUES({T_COL_H_PRECIO_LISTA}),
          {T_COL_H_PRECIO_OFERTA}=VALUES({T_COL_H_PRECIO_OFERTA}),
          {T_COL_H_TIPO_OFERTA}=VALUES({T_COL_H_TIPO_OFERTA}),
          {T_COL_H_PROMO_TIPO}=VALUES({T_COL_H_PROMO_TIPO}),
          {T_COL_H_PROMO_TXT_REG}=VALUES({T_COL_H_PROMO_TXT_REG}),
          {T_COL_H_PROMO_TXT_DESC}=VALUES({T_COL_H_PROMO_TXT_DESC}),
          {T_COL_H_PROMO_COMENT}=VALUES({T_COL_H_PROMO_COMENT})
    """
    params = (tienda_id, producto_tienda_id, capturado_en,
              precio_lista_txt, precio_oferta_txt, tipo_oferta,
              promo_tipo, promo_regular, promo_desc, promo_comentarios)
    try:
        exec_retry(cur, sql, params); return
    except myerr.DatabaseError as e:
        if getattr(e, "errno", None) == ERRNO_OUT_OF_RANGE:
            params_null = (tienda_id, producto_tienda_id, capturado_en,
                           None, None, tipo_oferta, promo_tipo, promo_regular, promo_desc, promo_comentarios)
            exec_retry(cur, sql, params_null); return
        raise

# ========= Parsing VTEX → filas DB + filas de reporte =========
def parse_rows_from_product_and_ps(p, base):
    ps_db: List[Dict[str, Any]] = []
    report_rows: List[Dict[str, Any]] = []

    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = p.get("brand")
    link_text = p.get("linkText")
    link = f"{base}/{link_text}/p" if link_text else (p.get("link") or "")
    categories = [c.strip("/") for c in (p.get("categories") or [])]
    cat1 = cat2 = None
    if categories:
        try:
            parts = [x for x in categories[0].split("/") if x]
            cat1 = parts[0] if len(parts) > 0 else None
            cat2 = parts[1] if len(parts) > 1 else None
        except Exception:
            pass

    items = p.get("items") or []
    if not items:
        report_rows.append({
            "supermercado": TIENDA_NOMBRE, "dominio": BASE, "estado_llamada": "OK",
            "ean_consultado": None, "ean_reportado": None,
            "nombre": name, "marca": brand, "categoria": cat1, "subcategoria": cat2,
            "precio_lista": None, "precio_oferta": None, "disponible": None,
            "oferta_tags": None, "product_id": product_id, "sku_id": None, "seller_id": None,
            "url": link
        })
        ps_db.append({
            "sku": None, "record_id": clean(product_id),
            "ean": None, "nombre": _truncate(clean(name), MAXLEN_NOMBRE),
            "marca": clean(brand), "fabricante": None,
            "precio_lista": None, "precio_oferta": None,
            "promo_tipo": None,
            "categoria": clean(cat1), "subcategoria": clean(cat2),
            "url": _truncate(clean(link), MAXLEN_URL), "oferta_tags": None
        })
        return ps_db, report_rows

    for it in items:
        sku_id = it.get("itemId")
        ean = extract_ean_from_item(it) or ""
        sellers = it.get("sellers") or []

        if not sellers:
            report_rows.append({
                "supermercado": TIENDA_NOMBRE, "dominio": BASE, "estado_llamada": "OK",
                "ean_consultado": None, "ean_reportado": ean or None,
                "nombre": name, "marca": brand, "categoria": cat1, "subcategoria": cat2,
                "precio_lista": None, "precio_oferta": None, "disponible": None,
                "oferta_tags": None, "product_id": product_id, "sku_id": sku_id, "seller_id": None,
                "url": link
            })
            ps_db.append({
                "sku": clean(sku_id), "record_id": clean(product_id),
                "ean": clean(ean), "nombre": _truncate(clean(name), MAXLEN_NOMBRE),
                "marca": clean(brand), "fabricante": None,
                "precio_lista": None, "precio_oferta": None,
                "promo_tipo": None,
                "categoria": clean(cat1), "subcategoria": clean(cat2),
                "url": _truncate(clean(link), MAXLEN_URL), "oferta_tags": None
            })
            continue

        for s in sellers:
            s_id = s.get("sellerId")
            offer = s.get("commertialOffer") or {}
            lista, oferta, promo_tipo_rule = _derive_prices(offer)
            available = offer.get("AvailableQuantity")
            teasers = offer.get("Teasers") or offer.get("DiscountHighLight") or []
            if isinstance(teasers, list) and teasers:
                teaser_txt = ", ".join([t.get("name") or t.get("title") or json.dumps(t, ensure_ascii=False)
                                        for t in teasers if isinstance(t, dict)])
            elif isinstance(teasers, list):
                teaser_txt = None
            else:
                teaser_txt = str(teasers) if teasers else None

            report_rows.append({
                "supermercado": TIENDA_NOMBRE, "dominio": BASE, "estado_llamada": "OK",
                "ean_consultado": None, "ean_reportado": (ean or None),
                "nombre": name, "marca": brand, "categoria": cat1, "subcategoria": cat2,
                "precio_lista": lista, "precio_oferta": oferta, "disponible": available,
                "oferta_tags": teaser_txt, "product_id": product_id, "sku_id": sku_id, "seller_id": s_id,
                "url": link
            })

            ps_db.append({
                "sku": clean(sku_id), "record_id": clean(product_id),
                "ean": clean(ean), "nombre": _truncate(clean(name), MAXLEN_NOMBRE),
                "marca": clean(brand), "fabricante": None,
                "precio_lista": lista, "precio_oferta": oferta,
                "promo_tipo": promo_tipo_rule,
                "categoria": clean(cat1), "subcategoria": clean(cat2),
                "url": _truncate(clean(link), MAXLEN_URL), "oferta_tags": teaser_txt
            })

    return ps_db, report_rows

# ========= Scrape por categoría =========
def fetch_category(session, cat_path, stopper: StopController):
    ps_all, report_all = [], []
    offset, empty_streak = 0, 0
    map_str = map_for_path(cat_path)
    encoded_path = quote(cat_path, safe="/")

    while True:
        if stopper.tripped(): break
        url = f"{BASE}/api/catalog_system/pub/products/search/{encoded_path}?map={map_str}&_from={offset}&_to={offset+STEP-1}"
        data = req_json(url, session)

        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY: break
            offset += STEP; time.sleep(SLEEP_OK); continue

        empty_streak = 0
        for p in data:
            ps, rep = parse_rows_from_product_and_ps(p, BASE)
            ps_all.extend(ps); report_all.extend(rep)
            if stopper.tripped(): break

        offset += STEP
        if stopper.tripped(): break
        time.sleep(SLEEP_OK)

    return ps_all, report_all

# ========= Inserción incremental =========
def insert_batch(conn, tienda_id: int, ps: List[Dict[str, Any]], capturado_en: datetime) -> int:
    if not ps: return 0
    total = 0
    seen = set()
    def _new_cursor():
        try: return conn.cursor()
        except Exception:
            conn.ping(reconnect=True, attempts=3, delay=1); return conn.cursor()
    cur = _new_cursor()
    done_in_batch = 0

    for p in ps:
        key = (p.get("sku"), p.get("record_id"), p.get("ean"), p.get("url"), p.get("precio_oferta"))
        if key in seen: continue
        seen.add(key)
        for attempt in range(2):
            try:
                producto_id = find_or_create_producto(cur, p)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                total += 1; done_in_batch += 1; break
            except MySQLError as e:
                errno = getattr(e, "errno", None)
                if errno == 1205:
                    try: conn.rollback()
                    except Exception: pass
                    time.sleep(0.7 * (2 ** attempt))
                    try: cur.close()
                    except Exception: pass
                    cur = _new_cursor()
                    if attempt == 1:
                        print(f"[SKIP] Fila omitida por lock persistente (sku={p.get('sku')}, rec={p.get('record_id')}).")
                    continue
                elif errno == ERRNO_OUT_OF_RANGE:
                    # Reintenta sin precios
                    try:
                        conn.rollback()
                    except Exception: pass
                    p2 = dict(p); p2["precio_lista"] = None; p2["precio_oferta"] = None
                    try:
                        producto_id = find_or_create_producto(cur, p2)
                        pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p2)
                        insert_historico(cur, tienda_id, pt_id, p2, capturado_en)
                        total += 1; done_in_batch += 1; break
                    except Exception:
                        try: conn.rollback()
                        except Exception: pass
                        break
                else:
                    try: conn.rollback()
                    except Exception: pass
                    break

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
            print(f"[WARN] Commit final falló: {e}; rollback…")
            try: conn.rollback()
            except Exception: pass

    try: cur.close()
    except Exception: pass
    return total

# ========= XLSX (mismo layout de tu app VTEX) =========
ORDER_COLS = [
    "supermercado","dominio","estado_llamada",
    "ean_consultado","ean_reportado",
    "nombre","marca","categoria","subcategoria",
    "precio_lista","precio_oferta","disponible",
    "oferta_tags","product_id","sku_id","seller_id","url"
]

def save_xlsx_report(rows_report: List[Dict[str, Any]]) -> Optional[str]:
    df = pd.DataFrame(rows_report or [])
    if not df.empty:
        cols = [c for c in ORDER_COLS if c in df.columns] + [c for c in df.columns if c not in ORDER_COLS]
        df = df[cols]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"reporte_jumbo_vtex_{ts}.xlsx"
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), fname)
    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="reporte")
        print(f"📄 XLSX generado: {out_path} (filas: {len(df)})")
        return out_path
    except Exception as e:
        print(f"[WARN] No se pudo escribir el XLSX: {e}")
        return None

# ========= Main =========
def main():
    stopper = StopController(); stopper.start()
    session = requests.Session()

    print("🔎 Descubriendo categorías…")
    tree = get_category_tree(session, TREE_DEPTH)
    cat_paths = iter_paths(tree)
    print(f"📁 Categorías detectadas: {len(cat_paths)}")

    conn = None
    capturado_en = datetime.now()
    total_insertados = 0
    report_rows: List[Dict[str, Any]] = []

    try:
        conn = get_conn()
        # ajustes de sesión para menos bloqueos
        try:
            with conn.cursor() as cset:
                cset.execute("SET SESSION innodb_lock_wait_timeout = 15")
                cset.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
        except Exception:
            pass

        conn.autocommit = False
        cur = conn.cursor()
        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        conn.commit()

        for i, path in enumerate(cat_paths, 1):
            if stopper.tripped():
                print("🛑 Parada solicitada. Guardando lo acumulado…")
                break
            print(f"[{i}/{len(cat_paths)}] {path}")

            ps, rep = fetch_category(session, path, stopper)
            if rep: report_rows.extend(rep)

            if ps:
                inc = insert_batch(conn, tienda_id, ps, capturado_en)
                conn.commit()
                total_insertados += inc
                print(f"💾 Commit categoría '{path}' → +{inc} registros (acum: {total_insertados})")

            if stopper.tripped():
                break
            time.sleep(0.25)

    except KeyboardInterrupt:
        print("🛑 Interrumpido por usuario (Ctrl+C). Guardando lo acumulado…")
        try:
            if conn: conn.commit()
        except Exception:
            pass
    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
        raise
    finally:
        try:
            save_xlsx_report(report_rows)
        except Exception as e:
            print(f"[WARN] Error al generar XLSX: {e}")
        if conn:
            try: conn.close()
            except Exception:
                pass
        print(f"🏁 Finalizado. Histórico insertado: {total_insertados} filas para {TIENDA_NOMBRE} ({capturado_en})")

if __name__ == "__main__":
    main()
