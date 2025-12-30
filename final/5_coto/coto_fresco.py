#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coto Digital (Endeca/ATG) -> SOLO categoría "Frescos" -> MySQL

✅ Corrige el problema de "0 records" al filtrar por Frescos:
- Endeca devuelve Nr URL-encoded (con %28 %3A etc).
- Si se lo pasas así a requests params, requests lo vuelve a encodear (doble encoding) -> 0 records.
- Solución: unquote() al Nr/N antes de usarlo en params.

Qué hace:
1) Seed request a /categoria?format=json
2) Detecta "Frescos" y extrae su estado (Nr/N) desde navegación
3) Decodifica unquote() los params encontrados
4) Pagina con No/Nrpp y guarda a MySQL con tu lógica estándar

Ajusta:
- ID_SUCURSAL si corresponde
"""

import re, json, time, unicodedata
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
import sys, os

import numpy as np
import requests
from mysql.connector import Error as MySQLError
from urllib.parse import unquote

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

# =================== Config ===================
SITE_BASE = "https://www.cotodigital.com.ar"
URL_BASE = f"{SITE_BASE}/sitios/cdigi/categoria"
NRPP = 50

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": SITE_BASE,
}

SLEEP = 0.35
TIMEOUT = 25
RETRIES = 3
MAX_PAGINAS = 5000

TIENDA_CODIGO = "coto"
TIENDA_NOMBRE = "Coto Digital"

# Sucursal: ajusta si corresponde
ID_SUCURSAL = "200"

BASE_PARAMS = {
    "Dy": "1",
    "Nrpp": str(NRPP),
    "format": "json",
    "idSucursal": ID_SUCURSAL,
}

SESSION = requests.Session()

# =================== Utils ===================
_price_clean_re = re.compile(r"[^\d,.\-]")
_slug_nonword = re.compile(r"[^a-zA-Z0-9\s-]")
_slug_spaces = re.compile(r"[\s\-]+")
_NULLLIKE = {"", "null", "none", "nan", "na"}


def clean(val):
    """Normaliza texto: trim, colapsa espacios, filtra null-likes."""
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s


def get_first(x, default=""):
    """Devuelve string desde list/tuple/str/None, tomando el primer elemento si es lista."""
    if x is None:
        return default
    if isinstance(x, (list, tuple)):
        return str(x[0]) if x else default
    return str(x)


def get_attr(attrs: dict, key: str, default: str = "") -> str:
    """Lee keys tipo 'product.displayName' desde attributes (Endeca)."""
    if not isinstance(attrs, dict):
        return default
    v = attrs.get(key)
    return get_first(v, default)


def get_any(attrs: dict, keys: List[str], default: str = "") -> str:
    """Devuelve el primer valor no vacío para las keys dadas."""
    for k in keys:
        v = get_attr(attrs, k, None)
        if v not in (None, ""):
            return v
    return default


def parse_json_field(value):
    """Si el valor es str y contiene JSON, lo decodifica; si ya es dict/list, lo devuelve."""
    if value is None:
        return value
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def parse_price(val) -> float:
    """Parsea números con separadores locales; devuelve float o np.nan."""
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


def best_category(attrs: Dict[str, Any]) -> Tuple[str, str]:
    """Intenta usar allAncestors.displayName y cae a campos de respaldo."""
    anc = attrs.get("allAncestors.displayName", [])
    if isinstance(anc, list) and anc:
        clean_list = [str(x).strip() for x in anc if str(x).strip().lower() != "cotodigital"]
        if len(clean_list) >= 2:
            return clean_list[0], clean_list[-1]
        if len(clean_list) == 1:
            return clean_list[0], ""
    cat = get_any(attrs, ["product.LDEPAR", "product.category", "parentCategory.displayName"])
    sub = get_any(attrs, ["product.FAMILIA", "product.subcategory", "product.category"])
    return clean(cat), clean(sub)


def extract_fabricante(dto_caract: Any) -> str:
    """Busca FABRICANTE/ELABORADO POR/FABRICADO POR/PROVEEDOR en dtoCaracteristicas."""
    if not isinstance(dto_caract, list):
        return ""
    keys = {"FABRICANTE", "ELABORADO POR", "FABRICADO POR", "PROVEEDOR"}
    for c in dto_caract:
        if not isinstance(c, dict):
            continue
        n = str(c.get("nombre", "")).strip().upper()
        d = str(c.get("descripcion", "")).strip()
        if n in keys and d:
            return d
    return ""


def build_product_url(attrs: Dict[str, Any], rec: Dict[str, Any]) -> str:
    name = get_attr(attrs, "product.displayName")
    record_id = get_attr(attrs, "record.id")
    if name and record_id:
        return f"{SITE_BASE}/sitios/cdigi/productos/{slugify(name)}/_/R-{record_id}?Dy=1&idSucursal={ID_SUCURSAL}"
    record_state = (((rec.get("detailsAction") or {}).get("recordState") or "")).split("?", 1)[0]
    if record_state:
        path = record_state if record_state.startswith("/") else f"/{record_state}"
        return f"{SITE_BASE}{path}"
    return ""


def extract_records_tree(root) -> List[dict]:
    """Extrae todos los 'records' recorriendo el árbol JSON (Endeca)."""
    found: List[dict] = []

    def walk(node):
        if isinstance(node, dict):
            if "records" in node and isinstance(node["records"], list):
                found.extend([r for r in node["records"] if isinstance(r, dict)])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(root)
    return found


def parse_record(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Construye el dict del producto desde un 'record' Endeca."""
    if not isinstance(rec, dict) or "attributes" not in rec:
        return None
    attrs = rec["attributes"]
    if not isinstance(attrs, dict):
        return None

    dto_price = parse_json_field(get_attr(attrs, "sku.dtoPrice"))
    dto_caract = parse_json_field(get_attr(attrs, "product.dtoCaracteristicas"))
    dto_desc = parse_json_field(get_attr(attrs, "product.dtoDescuentos"))

    ean_any = get_any(attrs, [
        "product.eanPrincipal",
        "product.EAN",
        "product.ean",
        "sku.ean",
        "product.dtoCodigosBarras"
    ])
    ean_parsed = parse_json_field(ean_any)
    if isinstance(ean_parsed, list) and ean_parsed:
        ean = str(ean_parsed[0])
    else:
        ean = str(ean_any or "").strip()

    precio_lista = ""
    precio_final = ""

    if isinstance(dto_price, dict):
        precio_lista = dto_price.get("precioLista", "")

    promo_tipo = ""
    precio_regular_promo = ""
    precio_descuento = ""
    comentarios_promo = ""

    if isinstance(dto_desc, list) and dto_desc:
        promos_txt = [str(d.get("textoDescuento", "")).strip() for d in dto_desc if isinstance(d, dict)]
        promo_tipo = "; ".join([p for p in promos_txt if p])

        d0 = dto_desc[0] if isinstance(dto_desc[0], dict) else {}

        precio_regular_promo = str(d0.get("textoPrecioRegular", "")).replace("Precio Contado:", "").strip()

        raw_desc = d0.get("precioDescuento", "")
        precio_desc_num = parse_price(raw_desc)
        if isinstance(precio_desc_num, float) and not np.isnan(precio_desc_num):
            precio_descuento = f"{round(precio_desc_num, 2)}"
            precio_final = precio_descuento
        else:
            precio_final = dto_price.get("precio", "") if isinstance(dto_price, dict) else ""

        comentarios_promo = str(d0.get("comentarios", "")).strip()
    else:
        precio_final = dto_price.get("precio", "") if isinstance(dto_price, dict) else ""

    fabricante = extract_fabricante(dto_caract)
    categoria, subcategoria = best_category(attrs)

    p = {
        "sku": clean(get_any(attrs, ["sku.repositoryId", "sku.id", "sku.repositoryid"])),
        "record_id": clean(get_attr(attrs, "record.id")),
        "ean": clean(ean),
        "nombre": clean(get_any(attrs, ["product.displayName", "product.description"])),
        "marca": clean(get_any(attrs, ["product.brand", "product.MARCA"])),
        "fabricante": clean(fabricante),
        "precio_lista": clean(precio_lista),
        "precio_oferta": clean(precio_final),
        "tipo_oferta": clean(get_any(attrs, ["product.tipoOferta", "product.TipoOferta"])),
        "promo_tipo": clean(promo_tipo),
        "precio_regular_promo": clean(precio_regular_promo),
        "precio_descuento": clean(precio_descuento),
        "comentarios_promo": clean(comentarios_promo),
        "categoria": clean(categoria),
        "subcategoria": clean(subcategoria),
        "url": clean(build_product_url(attrs, rec)),
    }

    if p["sku"] or p["record_id"] or p["precio_lista"] or p["precio_oferta"]:
        return p
    return None


def fetch_json(params: Dict[str, str]) -> Dict[str, Any]:
    """GET con reintentos; devuelve JSON de Endeca."""
    last_exc = None
    for _ in range(RETRIES):
        try:
            # DEBUG URL final (descomenta si necesitas):
            # print("GET:", requests.Request("GET", URL_BASE, params=params).prepare().url)

            r = SESSION.get(URL_BASE, params=params, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            last_exc = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            last_exc = e
        time.sleep(0.4)
    raise last_exc or RuntimeError("Error de red")


# ----------------- Descubrimiento de filtro "Frescos" -----------------
def find_frescos_params(data: Dict[str, Any]) -> Dict[str, str]:
    """
    Encuentra el estado navegable (Nr / N) para la categoría 'Frescos' dentro del JSON.
    Devuelve params listos para meter a BASE_PARAMS, pero OJO: puede venir URL-encoded.
    """
    candidates: List[Tuple[str, str]] = []

    def walk(node):
        if isinstance(node, dict):
            label = ""
            for lk in ("displayName", "label", "name", "dimensionName", "value"):
                if lk in node:
                    label = get_first(node.get(lk), "")
                    break

            nav = ""
            for nk in ("navigationState", "recordState", "action", "link", "url"):
                if nk in node:
                    nav = get_first(node.get(nk), "")
                    break

            if label and "fresco" in str(label).lower():
                candidates.append((label, nav))

            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(data)

    def parse_N_Nr_from_string(s: str) -> Dict[str, str]:
        if not s:
            return {}
        qs = s.split("?", 1)[1] if "?" in s else s
        out: Dict[str, str] = {}
        for part in qs.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                if k in ("N", "Nr"):
                    out[k] = v
        return out

    for label, nav in candidates:
        params = parse_N_Nr_from_string(nav)
        if params:
            print(f"✅ Frescos detectado: '{label}' -> {params}")
            return params

    raise RuntimeError(
        "No pude encontrar el filtro de categoría 'Frescos' en el JSON. "
        "Abre Frescos en el navegador y pega aquí la URL completa."
    )


# ================ MySQL helpers (upserts) ================
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
    marca = clean(p.get("marca")) or ""
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
    """Upsert que devuelve ID con LAST_INSERT_ID para evitar SELECT extra."""
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
        if x is None:
            return None
        v = parse_price(x)
        if isinstance(v, float) and np.isnan(v):
            return None
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


# =================== Main ===================
def main():
    productos: List[Dict[str, Any]] = []
    seen_keys = set()

    offset = 0
    pagina = 0
    total = None
    t0 = time.time()

    # 1) Seed para descubrir Frescos
    seed_params = dict(BASE_PARAMS)
    seed_params["No"] = "0"
    seed_params["Nrpp"] = str(NRPP)

    try:
        seed_data = fetch_json(seed_params)
    except Exception as e:
        print(f"[SEED] ⚠️ {e}")
        return

    try:
        frescos_params = find_frescos_params(seed_data)

        # ✅ FIX: evitar doble-encoding (Nr/N vienen URL-encoded a veces)
        frescos_params = {k: unquote(v) for k, v in frescos_params.items()}

        BASE_PARAMS.update(frescos_params)
        print(f"🎯 Filtrando SOLO Frescos con params (decoded): {frescos_params}")

    except Exception as e:
        print(f"❌ {e}")
        return

    # 2) Paginado de Frescos
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

        recs = extract_records_tree(data)
        if not recs:
            print("ℹ️ Página sin records; deteniendo.")
            break

        nuevos = 0
        for rec in recs:
            p = parse_record(rec)
            if not p:
                continue
            key = (p.get("record_id"), p.get("sku"), p.get("url"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            productos.append(p)
            nuevos += 1

        print(f"[{offset}] records={len(recs)} +{nuevos} nuevos (acum: {len(productos)})")

        if total and len(productos) >= int(total):
            print(f"ℹ️ Alcanzado total declarado ({int(total)}).")
            break

        offset += NRPP
        pagina += 1
        time.sleep(SLEEP)

    if not productos:
        print("⚠️ No se descargaron productos de Frescos.")
        return

    # 3) Guardado MySQL
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
        print(f"💾 Guardado en MySQL: {insertados} filas (Frescos) para {TIENDA_NOMBRE} ({capturado_en})")

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

    print(f"⏱️ Tiempo total: {time.time() - t0:.2f} s")


if __name__ == "__main__":
    main()
