#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re, json, time, unicodedata
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import requests
from mysql.connector import Error as MySQLError

from base_datos import get_conn  # asegúrate de configurarlo

# ------------ Config ------------ #
SITE_BASE = "https://www.cotodigital.com.ar"
URL_BASE = f"{SITE_BASE}/sitios/cdigi/categoria"
NRPP = 50
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}
SLEEP = 0.35
TIMEOUT = 25
MAX_PAGINAS = 5
RETRIES = 3

TIENDA_CODIGO = "coto"
TIENDA_NOMBRE = "Coto Digital"

BASE_PARAMS = {
    "Dy": "1",
    "Nrpp": str(NRPP),
    "format": "json"
}

def clean(val):
    if val is None: return None
    s = str(val).strip()
    return s if s else None

def get_attr(attrs: dict, key: str, default: str = "") -> str:
    if not isinstance(attrs, dict): return default
    v = attrs.get(key, [""])
    if isinstance(v, list) and v:
        return v[0]
    return default

def parse_json_field(value):
    if value is None: return value
    if isinstance(value, (dict, list)): return value
    try:
        return json.loads(value)
    except Exception:
        return value

_price_clean_re = re.compile(r"[^\d,.\-]")

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

def extract_records_tree(root) -> List[dict]:
    found = []
    def walk(node):
        if isinstance(node, dict):
            if "records" in node and isinstance(node["records"], list):
                found.extend(node["records"])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(root)
    return found

def extract_fabricante(dto_caract: Any) -> str:
    if not isinstance(dto_caract, list): return ""
    keys = {"FABRICANTE", "ELABORADO POR", "FABRICADO POR", "PROVEEDOR"}
    for c in dto_caract:
        n = str(c.get("nombre", "")).strip().upper()
        d = str(c.get("descripcion", "")).strip()
        if n in keys and d:
            return d
    return ""

_slug_nonword = re.compile(r"[^a-zA-Z0-9\s-]")
_slug_spaces = re.compile(r"[\s\-]+")

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = _slug_nonword.sub("", text)
    return _slug_spaces.sub("-", text.strip().lower())

def best_category(attrs: Dict[str, Any]) -> Tuple[str, str]:
    anc = attrs.get("allAncestors.displayName", [])
    if isinstance(anc, list) and anc:
        clean_list = [str(x).strip() for x in anc if str(x).strip().lower() != "cotodigital"]
        if len(clean_list) >= 2:
            return clean_list[0], clean_list[-1]
        if len(clean_list) == 1:
            return clean_list[0], ""
    cat = get_attr(attrs, "product.LDEPAR") or get_attr(attrs, "product.category")
    sub = get_attr(attrs, "product.FAMILIA") or get_attr(attrs, "parentCategory.displayName") or ""
    return cat, sub

def build_product_url(attrs: Dict[str, Any], rec: Dict[str, Any]) -> str:
    name = get_attr(attrs, "product.displayName")
    record_id = get_attr(attrs, "record.id")
    if name and record_id:
        return f"{SITE_BASE}/sitios/cdigi/productos/{slugify(name)}/_/R-{record_id}?Dy=1&idSucursal=200"
    record_state = ((rec.get("detailsAction", {}) or {}).get("recordState") or "").split("?", 1)[0]
    if record_state:
        path = record_state if record_state.startswith("/") else f"/{record_state}"
        return f"{SITE_BASE}/sitios/cdigi/producto{path}"
    return ""

def parse_record(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(rec, dict) or "attributes" not in rec:
        return None
    attrs = rec["attributes"]

    dto_price  = parse_json_field(get_attr(attrs, "sku.dtoPrice"))
    dto_caract = parse_json_field(get_attr(attrs, "product.dtoCaracteristicas"))
    dto_desc   = parse_json_field(get_attr(attrs, "product.dtoDescuentos"))

    precio_lista = dto_price.get("precioLista") if isinstance(dto_price, dict) else ""
    precio_final = dto_price.get("precio") if isinstance(dto_price, dict) else ""

    promo_tipo = ""
    precio_regular_promo = ""
    precio_descuento = ""
    comentarios_promo = ""
    if isinstance(dto_desc, list) and dto_desc:
        promos_txt = [str(d.get("textoDescuento", "")).strip() for d in dto_desc if d]
        promo_tipo = "; ".join([p for p in promos_txt if p])
        d0 = (dto_desc[0] or {})
        precio_regular_promo = str(d0.get("textoPrecioRegular", "")).replace("Precio Contado:", "").strip()
        precio_descuento = d0.get("precioDescuento", "")
        comentarios_promo = str(d0.get("comentarios", "")).strip()

    fabricante = extract_fabricante(dto_caract)
    categoria, subcategoria = best_category(attrs)

    p = {
        "sku": clean(get_attr(attrs, "sku.repositoryId")),
        "record_id": clean(get_attr(attrs, "record.id")),
        "ean": clean(get_attr(attrs, "product.eanPrincipal")),
        "nombre": clean(get_attr(attrs, "product.displayName")),
        "marca": clean(get_attr(attrs, "product.brand") or get_attr(attrs, "product.MARCA")),
        "fabricante": clean(fabricante),
        "precio_lista": clean(precio_lista),
        "precio_oferta": clean(precio_final),
        "tipo_oferta": clean(get_attr(attrs, "product.tipoOferta")),
        "promo_tipo": clean(promo_tipo),
        "precio_regular_promo": clean(precio_regular_promo),
        "precio_descuento": clean(precio_descuento),
        "comentarios_promo": clean(comentarios_promo),
        "categoria": clean(categoria),
        "subcategoria": clean(subcategoria),
        "url": clean(build_product_url(attrs, rec)),
    }
    if p["sku"] or p["precio_lista"] or p["precio_oferta"]:
        return p
    return None

def fetch_json(params: Dict[str, str]) -> Dict[str, Any]:
    last_exc = None
    for _ in range(RETRIES):
        try:
            r = requests.get(URL_BASE, params=params, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            last_exc = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_exc = e
        time.sleep(0.4)
    raise last_exc or RuntimeError("Error de red")

# ------------ Helpers BD (upserts) ------------ #
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

    cur.execute("""
        SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1
    """, (p.get("nombre") or "", p.get("marca") or ""))
    row = cur.fetchone()
    if row:
        pid = row[0]
        cur.execute("""
            UPDATE productos SET
              ean = COALESCE(NULLIF(%s,''), ean),
              marca = COALESCE(NULLIF(%s,''), marca),
              fabricante = COALESCE(NULLIF(%s,''), fabricante),
              categoria = COALESCE(NULLIF(%s,''), categoria),
              subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
            WHERE id=%s
        """, (
            p.get("ean") or "", p.get("marca") or "", p.get("fabricante") or "",
            p.get("categoria") or "", p.get("subcategoria") or "", pid
        ))
        return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", p.get("nombre") or "", p.get("marca") or "",
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              record_id_tienda=COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, p.get("url") or "", p.get("nombre") or ""))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND sku_tienda=%s LIMIT 1",
                    (tienda_id, sku))
        return cur.fetchone()[0]

    if rec:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, p.get("url") or "", p.get("nombre") or ""))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND record_id_tienda=%s LIMIT 1",
                    (tienda_id, rec))
        return cur.fetchone()[0]

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, p.get("url") or "", p.get("nombre") or ""))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    def to_txt_or_none(x):
        v = parse_price(x)
        if x is None: return None
        if isinstance(v, float) and np.isnan(v): return None
        return f"{round(float(v), 2)}"  # guardamos como texto (VARCHAR)

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

def main():
    productos: List[Dict[str, Any]] = []
    seen_keys = set()

    offset = 0
    pagina = 0
    t0 = time.time()
    total = None

    while pagina < MAX_PAGINAS:
        params = dict(BASE_PARAMS)
        params["No"] = str(offset)
        params["Nrpp"] = str(NRPP)

        try:
            data = fetch_json(params)
        except Exception as e:
            print(f"[{offset}] ⚠️ {e}")
            break

        if total is None:
            total = data.get("totalNumRecs") or data.get("totalNumRecords") or None

        nuevos = 0
        for rec in extract_records_tree(data):
            p = parse_record(rec)
            if not p: continue
            key = p.get("sku") or p.get("record_id") or (p.get("nombre"), p.get("url"))
            if key in seen_keys: continue
            seen_keys.add(key)
            productos.append(p)
            nuevos += 1

        acum = len(productos)
        print(f"[{offset}] +{nuevos} nuevos (acum: {acum})")

        if nuevos == 0:
            print("ℹ️ Página sin nuevos; deteniendo.")
            break
        if total and acum >= int(total):
            print(f"ℹ️ Alcanzado total declarado ({int(total)}).")
            break

        offset += NRPP
        pagina += 1
        time.sleep(SLEEP)

    if not productos:
        print("⚠️ No se descargaron productos.")
        return

    # ====== Inserción directa en MySQL ======
    capturado_en = datetime.now()

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for p in productos:
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
