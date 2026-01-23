#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Jumbo (VTEX) -> MySQL (solo rescate por EANs) + XLSX
- Lee EANs separados por coma
- Consulta: /api/catalog_system/pub/products/search?fq=alternateIds_Ean:<ean>  (forzando sc=32)
- Inserta solo si IsAvailable == True (fallback qty>0 si IsAvailable no viene)
- Los no disponibles quedan en XLSX con estado_llamada="SKIP_NOT_AVAILABLE"
"""

import os, sys, re, json, time, logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from html import unescape
from mysql.connector import Error as MySQLError, errors as myerr

# ========= Conexión (usa TU helper) =========
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- Debe retornar mysql.connector.connect(...)

# ===================== LOGGING =====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("jumbo_ean_only")

# ========= MAPEO TABLAS =========
T_TAB_TIENDAS     = "tiendas"
T_COL_TIENDA_ID   = "id"
T_COL_TIENDA_COD  = "codigo"
T_COL_TIENDA_NOM  = "nombre"

T_TAB_PRODUCTOS   = "productos"
T_COL_PROD_ID     = "id"
T_COL_PROD_EAN    = "ean"
T_COL_PROD_NOMBRE = "nombre"
T_COL_PROD_MARCA  = "marca"
T_COL_PROD_FAB    = "fabricante"
T_COL_PROD_CAT    = "categoria"
T_COL_PROD_SUBCAT = "subcategoria"

T_TAB_PROD_TIENDA = "producto_tienda"
T_COL_PT_ID       = "id"
T_COL_PT_TIENDAID = "tienda_id"
T_COL_PT_PRODID   = "producto_id"
T_COL_PT_SKU      = "sku_tienda"
T_COL_PT_RECORD   = "record_id_tienda"
T_COL_PT_URL      = "url_tienda"
T_COL_PT_NOMBRE   = "nombre_tienda"

T_TAB_HISTORICO   = "historico_precios"
T_COL_H_TIENDAID  = "tienda_id"
T_COL_H_PTID      = "producto_tienda_id"
T_COL_H_CAPT      = "capturado_en"
T_COL_H_PLISTA    = "precio_lista"
T_COL_H_POFERTA   = "precio_oferta"
T_COL_H_TIPO      = "tipo_oferta"
T_COL_H_PROMO_T   = "promo_tipo"
T_COL_H_TXT_REG   = "promo_texto_regular"
T_COL_H_TXT_DESC  = "promo_texto_descuento"
T_COL_H_COMENT    = "promo_comentarios"

# ========= Config VTEX =========
BASE = "https://www.jumbo.com.ar"
SALES_CHANNEL = int(os.getenv("VTEX_SC", "32"))
TIMEOUT = 25
RETRIES = 3

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# Identidad tienda
TIENDA_CODIGO = BASE
TIENDA_NOMBRE = "Jumbo"

# ========= Límites =========
MAXLEN_NOMBRE       = 255
MAXLEN_URL          = 512
MAXLEN_PROMO_TXT    = 191
MAXLEN_PROMO_COMENT = 255

LOCK_ERRNOS = {1205, 1213}
ERRNO_OUT_OF_RANGE = 1264

# ========= EANs: por ahora SOLO este =========
EANS_CSV = os.getenv("JUMBO_EANS", "7795917008104")  # separados por coma
EAN_LIST = [x.strip() for x in EANS_CSV.split(",") if x.strip()]

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

# ========= HTTP JSON con reintentos (FORZANDO sc) =========
def req_json(url, session, params=None):
    params = dict(params or {})
    params.setdefault("sc", SALES_CHANNEL)

    for i in range(RETRIES):
        try:
            r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        except requests.RequestException:
            time.sleep(0.6 + 0.4*i)
            continue

        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                time.sleep(0.6)
        elif r.status_code in (429, 408, 500, 502, 503, 504):
            time.sleep(0.6 + 0.4*i)
        else:
            if i == RETRIES - 1:
                log.debug("HTTP %s %s params=%s", r.status_code, url, params)
            time.sleep(0.3)
    return None

# ========= EAN =========
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
        if x is None or (isinstance(x, str) and not x.strip()):
            return None
        f = float(x)
        return f if f > 0 else None
    except Exception:
        return None

def _derive_prices(co: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    lista = _fnum(co.get("PriceWithoutDiscount"))
    oferta = _fnum(co.get("FullSellingPrice"))
    promo_tipo = None

    if lista is not None and oferta is not None:
        promo_tipo = "vtex_pwd_full"
    elif lista is not None and oferta is None:
        p = _fnum(co.get("Price"))
        oferta = p if p is not None else lista
        promo_tipo = "vtex_pwd_price" if p is not None and p != lista else "vtex_pwd_sin_full"
    elif lista is None and oferta is not None:
        p = _fnum(co.get("Price"))
        lista = _fnum(co.get("PriceWithoutDiscount")) or p or oferta
        promo_tipo = "vtex_full_only"
    else:
        p = _fnum(co.get("Price"))
        if p is not None:
            lista = p
            oferta = p
        promo_tipo = None

    return lista, oferta, promo_tipo

# ========= DISPONIBILIDAD =========
def is_available_product(product_obj: Dict[str, Any], offer: Dict[str, Any]) -> bool:
    if isinstance(offer.get("IsAvailable"), bool):
        return offer["IsAvailable"]
    if isinstance(product_obj.get("IsAvailable"), bool):
        return product_obj["IsAvailable"]
    qty = offer.get("AvailableQuantity")
    if isinstance(qty, (int, float)):
        return qty > 0
    return True

# ========= SQL con reintentos =========
def exec_retry(cur, sql, params=(), max_retries=5, base_sleep=0.5):
    att = 0
    while True:
        try:
            cur.execute(sql, params)
            return
        except myerr.DatabaseError as e:
            code = getattr(e, "errno", None)
            if code in LOCK_ERRNOS and att < max_retries:
                time.sleep(base_sleep * (2 ** att))
                att += 1
                continue
            raise

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    sql_ins = f"""
        INSERT INTO {T_TAB_TIENDAS} ({T_COL_TIENDA_COD}, {T_COL_TIENDA_NOM})
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE {T_COL_TIENDA_NOM}=VALUES({T_COL_TIENDA_NOM})
    """
    exec_retry(cur, sql_ins, (codigo, nombre))
    sql_get = f"SELECT {T_COL_TIENDA_ID} FROM {T_TAB_TIENDAS} WHERE {T_COL_TIENDA_COD}=%s LIMIT 1"
    exec_retry(cur, sql_get, (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
    nombre = _truncate((clean(p.get("nombre")) or ""), MAXLEN_NOMBRE)
    marca  = clean(p.get("marca")) or ""
    categoria    = clean(p.get("categoria"))
    subcategoria = clean(p.get("subcategoria"))
    fabricante   = clean(p.get("fabricante"))

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
              ({T_COL_PT_TIENDAID},{T_COL_PT_PRODID},{T_COL_PT_SKU},{T_COL_PT_RECORD},{T_COL_PT_URL},{T_COL_PT_NOMBRE})
            VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              {T_COL_PT_ID} = LAST_INSERT_ID({T_COL_PT_ID}),
              {T_COL_PT_PRODID} = VALUES({T_COL_PT_PRODID}),
              {T_COL_PT_RECORD} = COALESCE(VALUES({T_COL_PT_RECORD}), {T_COL_PT_RECORD}),
              {T_COL_PT_URL} = COALESCE(VALUES({T_COL_PT_URL}), {T_COL_PT_URL}),
              {T_COL_PT_NOMBRE} = COALESCE(VALUES({T_COL_PT_NOMBRE}), {T_COL_PT_NOMBRE})
        """
        exec_retry(cur, sql, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid

    if rec:
        sql2 = f"""
            INSERT INTO {T_TAB_PROD_TIENDA}
              ({T_COL_PT_TIENDAID},{T_COL_PT_PRODID},{T_COL_PT_SKU},{T_COL_PT_RECORD},{T_COL_PT_URL},{T_COL_PT_NOMBRE})
            VALUES (%s,%s,NULL,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              {T_COL_PT_ID} = LAST_INSERT_ID({T_COL_PT_ID}),
              {T_COL_PT_PRODID} = VALUES({T_COL_PT_PRODID}),
              {T_COL_PT_URL} = COALESCE(VALUES({T_COL_PT_URL}), {T_COL_PT_URL}),
              {T_COL_PT_NOMBRE} = COALESCE(VALUES({T_COL_PT_NOMBRE}), {T_COL_PT_NOMBRE})
        """
        exec_retry(cur, sql2, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    sql3 = f"""
        INSERT INTO {T_TAB_PROD_TIENDA}
          ({T_COL_PT_TIENDAID},{T_COL_PT_PRODID},{T_COL_PT_URL},{T_COL_PT_NOMBRE})
        VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''))
    """
    exec_retry(cur, sql3, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    precio_lista_txt = _price_txt_or_none(p.get("precio_lista"))
    oferta_val = p.get("precio_oferta")
    precio_oferta_txt = _price_txt_or_none(oferta_val if oferta_val not in (None, "") else p.get("precio_lista"))

    tipo_oferta = None
    if precio_lista_txt is not None and precio_oferta_txt is not None and precio_lista_txt != precio_oferta_txt:
        tipo_oferta = "con_descuento"

    promo_tipo = _truncate(clean(p.get("promo_tipo")), MAXLEN_PROMO_TXT)

    comentarios = []
    if p.get("categoria"): comentarios.append(f"cat={p['categoria']}")
    if p.get("subcategoria"): comentarios.append(f"sub={p['subcategoria']}")
    oferta_tags = clean(p.get("oferta_tags"))
    if oferta_tags: comentarios.append(f"tags={oferta_tags}")
    promo_coment = _truncate(" | ".join(comentarios), MAXLEN_PROMO_COMENT) if comentarios else None

    sql = f"""
        INSERT INTO {T_TAB_HISTORICO}
          ({T_COL_H_TIENDAID},{T_COL_H_PTID},{T_COL_H_CAPT},
           {T_COL_H_PLISTA},{T_COL_H_POFERTA},{T_COL_H_TIPO},
           {T_COL_H_PROMO_T},{T_COL_H_TXT_REG},{T_COL_H_TXT_DESC},{T_COL_H_COMENT})
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          {T_COL_H_PLISTA}=VALUES({T_COL_H_PLISTA}),
          {T_COL_H_POFERTA}=VALUES({T_COL_H_POFERTA}),
          {T_COL_H_TIPO}=VALUES({T_COL_H_TIPO}),
          {T_COL_H_PROMO_T}=VALUES({T_COL_H_PROMO_T}),
          {T_COL_H_TXT_REG}=VALUES({T_COL_H_TXT_REG}),
          {T_COL_H_TXT_DESC}=VALUES({T_COL_H_TXT_DESC}),
          {T_COL_H_COMENT}=VALUES({T_COL_H_COMENT})
    """
    params = (tienda_id, producto_tienda_id, capturado_en,
              precio_lista_txt, precio_oferta_txt, tipo_oferta,
              promo_tipo, None, None, promo_coment)
    try:
        exec_retry(cur, sql, params)
    except myerr.DatabaseError as e:
        if getattr(e, "errno", None) == ERRNO_OUT_OF_RANGE:
            params_null = (tienda_id, producto_tienda_id, capturado_en,
                           None, None, tipo_oferta, promo_tipo, None, None, promo_coment)
            exec_retry(cur, sql, params_null)
        else:
            raise

# ========= VTEX: fetch por EAN =========
def fetch_by_ean(session, ean: str) -> List[Dict[str, Any]]:
    url = f"{BASE}/api/catalog_system/pub/products/search"
    params = {"fq": f"alternateIds_Ean:{ean}"}
    return req_json(url, session, params=params) or []

# ========= Parsing (genera: ps_db + reporte) =========
def parse_rows_from_product(p: Dict[str, Any], ean_consultado: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    ps_db: List[Dict[str, Any]] = []
    report_rows: List[Dict[str, Any]] = []

    product_id = p.get("productId")
    name = clean_text(p.get("productName"))
    brand = p.get("brand")
    link_text = p.get("linkText")
    link = f"{BASE}/{link_text}/p" if link_text else (p.get("link") or "")

    categories = [c.strip("/") for c in (p.get("categories") or [])]
    cat1 = cat2 = None
    if categories:
        parts = [x for x in categories[0].split("/") if x]
        cat1 = parts[0] if len(parts) > 0 else None
        cat2 = parts[1] if len(parts) > 1 else None

    items = p.get("items") or []
    if not items:
        report_rows.append({
            "supermercado": TIENDA_NOMBRE, "dominio": BASE, "estado_llamada": "NO_ITEMS",
            "ean_consultado": ean_consultado, "ean_reportado": None,
            "nombre": name, "marca": brand, "categoria": cat1, "subcategoria": cat2,
            "precio_lista": None, "precio_oferta": None, "disponible": None,
            "oferta_tags": None, "product_id": product_id, "sku_id": None, "seller_id": None,
            "url": link
        })
        return ps_db, report_rows

    for it in items:
        sku_id = it.get("itemId")
        ean_rep = extract_ean_from_item(it) or ""
        sellers = it.get("sellers") or []

        if not sellers:
            report_rows.append({
                "supermercado": TIENDA_NOMBRE, "dominio": BASE, "estado_llamada": "NO_SELLERS",
                "ean_consultado": ean_consultado, "ean_reportado": ean_rep or None,
                "nombre": name, "marca": brand, "categoria": cat1, "subcategoria": cat2,
                "precio_lista": None, "precio_oferta": None, "disponible": None,
                "oferta_tags": None, "product_id": product_id, "sku_id": sku_id, "seller_id": None,
                "url": link
            })
            continue

        for s in sellers:
            s_id = s.get("sellerId")
            offer = s.get("commertialOffer") or {}

            available_bool = is_available_product(p, offer)
            lista, oferta, promo_tipo_rule = _derive_prices(offer)

            teasers = offer.get("Teasers") or offer.get("DiscountHighLight") or []
            if isinstance(teasers, list) and teasers:
                teaser_txt = ", ".join([
                    t.get("name") or t.get("title") or json.dumps(t, ensure_ascii=False)
                    for t in teasers if isinstance(t, dict)
                ])
            elif isinstance(teasers, list):
                teaser_txt = None
            else:
                teaser_txt = str(teasers) if teasers else None

            report_rows.append({
                "supermercado": TIENDA_NOMBRE, "dominio": BASE,
                "estado_llamada": "OK" if available_bool else "SKIP_NOT_AVAILABLE",
                "ean_consultado": ean_consultado, "ean_reportado": (ean_rep or None),
                "nombre": name, "marca": brand, "categoria": cat1, "subcategoria": cat2,
                "precio_lista": lista, "precio_oferta": oferta, "disponible": available_bool,
                "oferta_tags": teaser_txt, "product_id": product_id, "sku_id": sku_id, "seller_id": s_id,
                "url": link
            })

            if not available_bool:
                continue

            ps_db.append({
                "sku": clean(sku_id),
                "record_id": clean(product_id),
                "ean": clean(ean_rep) or clean(ean_consultado),
                "nombre": _truncate(clean(name), MAXLEN_NOMBRE),
                "marca": clean(brand),
                "fabricante": None,
                "precio_lista": lista,
                "precio_oferta": oferta,
                "promo_tipo": promo_tipo_rule,
                "categoria": clean(cat1),
                "subcategoria": clean(cat2),
                "url": _truncate(clean(link), MAXLEN_URL),
                "oferta_tags": teaser_txt
            })

    return ps_db, report_rows

# ========= XLSX =========
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
    fname = f"reporte_jumbo_eans_{ts}.xlsx"
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), fname)
    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            #df.to_excel(w, index=False, sheet_name="reporte")
            print("correcto la generacion del dataframe")
        log.info("📄 XLSX generado: %s (filas: %s)", out_path, len(df))
        return out_path
    except Exception as e:
        log.warning("No se pudo escribir el XLSX: %s", e)
        return None

# ========= Main =========
def main():
    if not EAN_LIST:
        log.error("No hay EANs. Define JUMBO_EANS='ean1,ean2,...'")
        return

    session = requests.Session()
    capturado_en = datetime.now()
    report_rows: List[Dict[str, Any]] = []
    total_insertados = 0

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False

        # ajustes de sesión (opcionales)
        try:
            cset = conn.cursor()
            cset.execute("SET SESSION innodb_lock_wait_timeout = 15")
            cset.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
            cset.close()
        except Exception:
            pass

        cur = conn.cursor()
        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        conn.commit()
        cur.close()

        log.info("VTEX sc=%s | EANs=%s", SALES_CHANNEL, EAN_LIST)

        for ean in EAN_LIST:
            ean_clean = re.sub(r"\D", "", ean)
            if not _is_ean_candidate(ean_clean):
                log.warning("EAN inválido: %s (skip)", ean)
                continue

            log.info("🔎 EAN %s ...", ean_clean)
            data = fetch_by_ean(session, ean_clean)
            if not data:
                report_rows.append({
                    "supermercado": TIENDA_NOMBRE, "dominio": BASE, "estado_llamada": "NO_RESULTS",
                    "ean_consultado": ean_clean, "ean_reportado": None,
                    "nombre": None, "marca": None, "categoria": None, "subcategoria": None,
                    "precio_lista": None, "precio_oferta": None, "disponible": None,
                    "oferta_tags": None, "product_id": None, "sku_id": None, "seller_id": None,
                    "url": None
                })
                log.warning("EAN %s: sin resultados", ean_clean)
                continue

            # parse + insertar
            ps_to_insert: List[Dict[str, Any]] = []
            for prod in data:
                ps, rep = parse_rows_from_product(prod, ean_clean)
                report_rows.extend(rep)
                ps_to_insert.extend(ps)

            if not ps_to_insert:
                log.info("EAN %s: 0 insertados (posible IsAvailable=false)", ean_clean)
                continue

            # inserción (simple, sin batch enorme porque son pocos EAN)
            cur = conn.cursor()
            inserted_here = 0
            for p in ps_to_insert:
                try:
                    producto_id = find_or_create_producto(cur, p)
                    pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                    insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                    inserted_here += 1
                except MySQLError as e:
                    errno = getattr(e, "errno", None)
                    if errno in LOCK_ERRNOS:
                        conn.rollback()
                        log.warning("Lock MySQL (errno=%s) EAN=%s sku=%s rec=%s", errno, ean_clean, p.get("sku"), p.get("record_id"))
                        continue
                    elif errno == ERRNO_OUT_OF_RANGE:
                        conn.rollback()
                        p2 = dict(p); p2["precio_lista"] = None; p2["precio_oferta"] = None
                        try:
                            producto_id = find_or_create_producto(cur, p2)
                            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p2)
                            insert_historico(cur, tienda_id, pt_id, p2, capturado_en)
                            inserted_here += 1
                        except Exception:
                            conn.rollback()
                        continue
                    else:
                        conn.rollback()
                        log.warning("MySQL error errno=%s EAN=%s: %s", errno, ean_clean, e)
                        continue

            conn.commit()
            cur.close()

            total_insertados += inserted_here
            log.info("✅ EAN %s -> +%s insertados (acum: %s)", ean_clean, inserted_here, total_insertados)
            time.sleep(0.25)

    except KeyboardInterrupt:
        log.info("🛑 Interrumpido por usuario (Ctrl+C). Guardando...")
        try:
            if conn: conn.commit()
        except Exception:
            pass
    finally:
        try:
            save_xlsx_report(report_rows)
        except Exception as e:
            log.warning("Error generando XLSX: %s", e)

        if conn:
            try: conn.close()
            except Exception: pass

        log.info("🏁 Finalizado. Insertados=%s | capturado_en=%s", total_insertados, capturado_en)

if __name__ == "__main__":
    main()
