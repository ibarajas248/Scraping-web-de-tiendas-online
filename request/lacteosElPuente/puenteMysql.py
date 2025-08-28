#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
El Puente (text-only) -> MySQL con huella/fingerprint como sku_tienda

Estrategia:
- No hay EAN/SKU/ID -> generamos sku_tienda = sha1(normalizar(nombre)|presentacion|subcategoria)
- productos: ean=NULL; coincidimos suave por (nombre, marca=NULL) o por nombre
- producto_tienda: UNIQUE (tienda_id, sku_tienda) usando la huella estable
- historico_precios: guardamos precio del día

Requiere:
  pip install mysql-connector-python beautifulsoup4 pandas requests
  y tu base_datos.get_conn()
"""

import re, html, time, hashlib
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from mysql.connector import Error as MySQLError
from base_datos import get_conn  # <-- tu conexión

# ================== Config scraping (igual a tu script) ==================
BASE = "https://ofertas.lacteoselpuente.com.ar"
ENDPOINT = "/productos/get/{id}"

RUBROS = {
    1: "Quesos Blandos",
    2: "Quesos semiduros",
    3: "Quesos duros",
    4: "Tablas y picadas",
    5: "Lacteos",
    6: "Dulces",
    7: "Marca propia",
    12: "Otros Productos",
}

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
}

RE_PRICE_CAPTURE = re.compile(r"\$\s*([0-9.\s]+,\d{2})")
RE_HAS_PRICE     = re.compile(r"\$\s*\d")

TIENDA_CODIGO = "elpuente"
TIENDA_NOMBRE = "Lácteos El Puente"

# ================== Helpers scraping ==================
def clean_html_text(s: str) -> str:
    if not s:
        return ""
    s = BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    s = html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def fetch_text(rubro_id: int, session: requests.Session) -> str:
    url = f"{BASE}{ENDPOINT.format(id=rubro_id)}"
    r = session.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text

def split_nombre_presentacion(desc: str) -> Tuple[str, str]:
    original = clean_html_text(desc)
    if "." in original:
        nombre, resto = original.split(".", 1)
        nombre, presentacion = nombre.strip(), resto.strip()
    else:
        m = re.search(r"\s(x|por)\s", original, flags=re.IGNORECASE)
        if m:
            nombre = original[:m.start()].strip()
            presentacion = original[m.start():].strip()
        else:
            nombre, presentacion = original, ""
    presentacion = re.sub(r"^\.*\s*", "", presentacion)
    presentacion = re.sub(r"^\b[Vv]alor\b[:\s]*", "", presentacion)
    presentacion = re.sub(r"\s{2,}", " ", presentacion).strip()
    if not nombre:
        nombre = original
    return nombre, presentacion

def parse_text_to_rows(rubro_nombre: str, text: str) -> List[Tuple[str, str, str, str, str]]:
    rows = []
    subcat = rubro_nombre
    carry_desc: Optional[str] = None
    for raw in text.splitlines():
        line = clean_html_text(raw)
        if not line:
            continue
        if line.startswith("#"):
            sub = line.lstrip("#").strip().rstrip(".")
            if sub:
                subcat = sub
            continue
        if RE_HAS_PRICE.search(line):
            m = RE_PRICE_CAPTURE.search(line)
            precio = m.group(1).strip() if m else ""
            before = line.split("$", 1)[0].strip()
            desc = (f"{carry_desc} {before}".strip() if carry_desc else before) or (carry_desc or "")
            nombre, presentacion = split_nombre_presentacion(desc)
            rows.append(("lacteos", subcat, nombre, presentacion, precio))
            carry_desc = None
        else:
            carry_desc = f"{carry_desc} {line}".strip() if carry_desc else line
    return rows

def scrape_elpuente() -> List[Dict[str, Any]]:
    s = requests.Session(); s.headers.update(HEADERS)
    all_rows: List[Dict[str, Any]] = []
    for rid, rubro_nombre in RUBROS.items():
        try:
            txt = fetch_text(rid, s)
        except requests.HTTPError as e:
            print(f"[WARN] {rid} {rubro_nombre}: {e}")
            continue
        rows = parse_text_to_rows(rubro_nombre, txt)
        for (categoria, subcat, nombre, presentacion, precio) in rows:
            all_rows.append({
                "categoria": categoria,
                "subcategoria": subcat,
                "nombre": nombre,
                "presentacion": presentacion,
                "precio_text": precio,
            })
        time.sleep(0.5)
    return all_rows

# ================== Normalización / Fingerprint ==================
_norm_ws = re.compile(r"\s+")
_nonword = re.compile(r"[^a-z0-9]+")

def _norm(s: Optional[str]) -> str:
    if not s: return ""
    s = s.strip().lower()
    s = _norm_ws.sub(" ", s)
    s = s.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
    s = _nonword.sub(" ", s)
    s = _norm_ws.sub(" ", s).strip()
    return s

def make_fingerprint(nombre: str, presentacion: str, subcategoria: str) -> str:
    base = f"{_norm(nombre)}|{_norm(presentacion)}|{_norm(subcategoria)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:24]  # 24 hex = compacto y suficiente

def parse_price(text: Optional[str]) -> Optional[float]:
    if not text: return None
    t = re.sub(r"[^\d,.\-]", "", text)
    if "," in t and "." in t: t = t.replace(".", "").replace(",", ".")
    elif "," in t: t = t.replace(",", ".")
    try: return float(t)
    except Exception: return None

def price_to_varchar(x: Any) -> Optional[str]:
    if x is None: return None
    try:
        v = float(x)
        if np.isnan(v): return None
        return f"{round(v, 2)}"
    except Exception:
        s = str(x).strip()
        return s or None

def clean_txt(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s or None

# ================== Upserts MySQL ==================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    """
    Sin EAN. Intentamos por (nombre, marca=NULL) o por nombre.
    Guardamos categoria/subcategoria/presentacion en producto (opcional).
    """
    ean = None
    nombre = clean_txt(r.get("nombre"))
    marca = None
    fabricante = None
    categoria = clean_txt(r.get("categoria"))
    sub = clean_txt(r.get("subcategoria"))

    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (categoria or "", sub or "", pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (nombre or "", categoria or "", sub or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    """
    Clave natural: (tienda_id, sku_tienda = fingerprint).
    Guardamos nombre_tienda y (opcional) presentacion en url_tienda si no la usas.
    """
    sku = r["sku_tienda"]  # fingerprint
    nombre_tienda = clean_txt(r.get("nombre"))
    # Podés usar url_tienda para guardar alguna referencia si quisieras; aquí NULL.
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, NULL, NULL, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, sku, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: datetime):
    precio = price_to_varchar(r.get("precio_float"))
    # No hay oferta explícita, lo guardamos como precio_lista.
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
        precio, None, None,
        None, None, None,
        # Dejo la presentación como comentario de promo para auditar (opcional)
        clean_txt(r.get("presentacion"))
    ))

# ================== Runner ==================
def main():
    print("[INFO] Descargando El Puente…")
    raw_rows = scrape_elpuente()
    if not raw_rows:
        print("[INFO] Sin filas."); return

    # Enriquecer con fingerprint y precio parseado
    rows: List[Dict[str, Any]] = []
    for r in raw_rows:
        nombre = r["nombre"]
        present = r.get("presentacion") or ""
        subcat = r.get("subcategoria") or ""
        sku = make_fingerprint(nombre, present, subcat)
        precio_f = parse_price(r.get("precio_text"))
        rows.append({
            **r,
            "sku_tienda": sku,
            "precio_float": precio_f,
        })

    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for r in rows:
            producto_id = find_or_create_producto(cur, r)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, r)
            insert_historico(cur, tienda_id, pt_id, r, capturado_en)
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

if __name__ == "__main__":
    main()
