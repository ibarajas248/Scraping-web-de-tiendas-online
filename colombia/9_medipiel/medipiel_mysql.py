#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Inserción en MySQL para el scraper de Carrefour (VTEX)
Tablas: tiendas, productos, producto_tienda, historico_precios

Cambios clave (FIX 1406: Data too long):
- Se leen los límites reales de columnas desde INFORMATION_SCHEMA.
- Función fit(table, col, val, digits_only=False): si el valor excede el largo permitido, se omite (None).
- EAN: se fuerzan solo dígitos y si excede el largo de productos.ean => None.
- Normalización robusta de NaN/pd.NA/None con clean() y safe_float().
- df = df.where(pd.notna(df), None) para evitar "boolean value of NA is ambiguous".
- Todos los textos de entrada a SQL ya vienen “encajados” por fit().
"""

import time
from datetime import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# --------- Config tienda ---------
TIENDA_CODIGO = "www.medipiel.com.co"
TIENDA_NOMBRE = "Medipiel"

# --------- Helpers numéricos ---------
def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if pd.isna(x):
            return None
        v = float(x)
        if np.isnan(v):
            return None
        return v
    except Exception:
        return None

# --------- Normalización valores texto ---------
def clean(val):
    """Devuelve None si val es None, pd.NA, NaN o string vacío; si no, string strip."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    s = str(val).strip()
    return s if s else None

# ================== Helpers BD: límites de columnas ==================
def load_schema_limits(cur):
    """
    Retorna un dict:
       limits[tabla][columna] = CHARACTER_MAXIMUM_LENGTH (o None si no aplica)
    Solo carga las tablas involucradas.
    """
    targets = ("tiendas", "productos", "producto_tienda", "historico_precios")
    q = """
    SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME IN (%s,%s,%s,%s)
    """
    cur.execute(q, targets)
    limits: Dict[str, Dict[str, Optional[int]]] = {}
    for t, c, maxlen in cur.fetchall():
        limits.setdefault(t, {})[c] = maxlen  # None para numerics/textos sin límite explícito
    return limits

def make_fit_fn(limits):
    """
    Crea una función fit(table, column, val, digits_only=False) que:
    - Limpia y normaliza val -> str o None.
    - Si digits_only=True, conserva solo dígitos (útil para EAN).
    - Si el largo de val supera el CHARACTER_MAXIMUM_LENGTH -> devuelve None (omite).
    """
    def fit(table: str, column: str, val: Any, *, digits_only: bool = False) -> Optional[str]:
        v = clean(val)
        if v is None:
            return None
        if digits_only:
            v = "".join(ch for ch in str(v) if ch.isdigit())
            if not v:
                return None
        try:
            maxlen = limits.get(table, {}).get(column)
        except Exception:
            maxlen = None
        if isinstance(v, str) and maxlen is not None and maxlen > 0:
            if len(v) > maxlen:
                # Política: NO truncar; omitir el valor para evitar 1406
                return None
        return v
    return fit

# ================== Helpers BD (upserts) ==================
def upsert_tienda(cur, codigo: Optional[str], nombre: Optional[str]) -> int:
    """
    Recibe ya valores “encajados” por fit(). Si alguno es None y tu esquema
    requiere NOT NULL, ajusta la fuente (TIENDA_*) para que no exceda.
    """
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (clean(codigo), clean(nombre))
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (clean(codigo),))
    row = cur.fetchone()
    if not row:
        # fallback muy defensivo: si no logró crear/ubicar, intenta con el nombre
        cur.execute("SELECT id FROM tiendas WHERE nombre=%s LIMIT 1", (clean(nombre),))
        row = cur.fetchone()
    return row[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
    nombre = clean(p.get("nombre")) or ""
    marca = clean(p.get("marca")) or ""
    fabricante = clean(p.get("fabricante")) or ""
    categoria = clean(p.get("categoria")) or ""
    subcategoria = clean(p.get("subcategoria")) or ""

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
            """, (nombre, marca, fabricante, categoria, subcategoria, pid))
            return pid

    # Fallback por (nombre, marca)
    cur.execute("""
        SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1
    """, (nombre, marca))
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
        """, (ean or "", marca, fabricante, categoria, subcategoria, pid))
        return pid

    # Insert nuevo
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (ean or "", nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))                 # "Código Interno" mapeado a sku_tienda
    record_id = clean(p.get("record_id"))     # si se usa más adelante
    url = clean(p.get("url")) or ""
    nombre_tienda = clean(p.get("nombre")) or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              record_id_tienda=COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND sku_tienda=%s LIMIT 1",
                    (tienda_id, sku))
        return cur.fetchone()[0]

    if record_id:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND record_id_tienda=%s LIMIT 1",
                    (tienda_id, record_id))
        return cur.fetchone()[0]

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime, fit=None):
    def to_txt_or_none(x):
        v = safe_float(x)
        if v is None:
            return None
        return f"{round(float(v), 2)}"

    precio_lista_txt  = to_txt_or_none(p.get("precio_lista"))
    precio_oferta_txt = to_txt_or_none(p.get("precio_oferta"))

    if fit is not None:
        precio_lista_txt  = fit("historico_precios", "precio_lista", precio_lista_txt)
        precio_oferta_txt = fit("historico_precios", "precio_oferta", precio_oferta_txt)
        tipo_oferta       = fit("historico_precios", "tipo_oferta", p.get("tipo_oferta"))
        promo_tipo        = fit("historico_precios", "promo_tipo", p.get("promo_tipo"))
        promo_reg         = fit("historico_precios", "promo_texto_regular", p.get("precio_regular_promo"))
        promo_desc        = fit("historico_precios", "promo_texto_descuento", p.get("precio_descuento"))
        promo_comm        = fit("historico_precios", "promo_comentarios", p.get("comentarios_promo"))
    else:
        tipo_oferta = clean(p.get("tipo_oferta"))
        promo_tipo  = clean(p.get("promo_tipo"))
        promo_reg   = clean(p.get("precio_regular_promo"))
        promo_desc  = clean(p.get("precio_descuento"))
        promo_comm  = clean(p.get("comentarios_promo"))

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
        precio_lista_txt, precio_oferta_txt,
        tipo_oferta, promo_tipo, promo_reg, promo_desc, promo_comm
    ))

# ================== Pipeline: DF de Carrefour → MySQL ==================
def persist_carrefour_df_to_mysql(df: pd.DataFrame):
    """
    Espera un DataFrame con columnas:
    ['EAN','Código Interno','Nombre Producto','Categoría','Subcategoría',
     'Marca','Fabricante','Precio de Lista','Precio de Oferta','Tipo de Oferta','URL']
    """
    if df is None or df.empty:
        print("⚠️ DataFrame vacío; nada que insertar.")
        return

    # --- Normaliza tipos y nulos ---
    df = df.copy()

    # EAN como texto para no perder ceros a la izquierda
    if "EAN" in df.columns:
        df["EAN"] = df["EAN"].astype("string")

    # Cambia pd.NA/NaN por None (evita boolean ambiguity y simplifica SQL)
    df = df.where(pd.notna(df), None)

    # --- Mapeo por posición para tolerar variaciones de encabezados ---
    cols = list(df.columns)
    col = {name: i for i, name in enumerate(cols)}  # acceso por índice

    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        # Cargar límites de columnas y crear fit()
        limits = load_schema_limits(cur)
        fit = make_fit_fn(limits)

        # Asegurar que TIENDA_* también cumplen largo
        tienda_codigo_fit = fit("tiendas", "codigo", TIENDA_CODIGO)
        tienda_nombre_fit = fit("tiendas", "nombre", TIENDA_NOMBRE)

        tienda_id = upsert_tienda(cur, tienda_codigo_fit, tienda_nombre_fit)

        inserted_hist = 0

        # itertuples(..., name=None) devuelve tuplas simples (sin atributos)
        for row in df.itertuples(index=False, name=None):
            # Helper para leer por nombre usando índice
            def val(name):
                i = col.get(name)
                return row[i] if i is not None else None

            # Construye p ya encajado/limpio contra límites de esquema
            p = {
                # productos.*
                "ean":          fit("productos", "ean", val("EAN"), digits_only=True),
                "nombre":       fit("productos", "nombre", val("Nombre Producto")),
                "marca":        fit("productos", "marca", val("Marca")),
                "fabricante":   fit("productos", "fabricante", val("Fabricante")),
                "categoria":    fit("productos", "categoria", val("Categoría")),
                "subcategoria": fit("productos", "subcategoria", val("Subcategoría")),

                # precios (se convertirán a texto 2 decimales dentro de insert_historico)
                "precio_lista":  safe_float(val("Precio de Lista")),
                "precio_oferta": safe_float(val("Precio de Oferta")),
                "tipo_oferta":   fit("historico_precios", "tipo_oferta", val("Tipo de Oferta")),

                # producto_tienda.*
                "url":           fit("producto_tienda", "url_tienda", val("URL")),
                "sku":           fit("producto_tienda", "sku_tienda", val("Código Interno")),
                "record_id":     None,  # si algún día lo usas: fit("producto_tienda","record_id_tienda", ...),
                "nombre_tienda": fit("producto_tienda", "nombre_tienda", val("Nombre Producto")),

                # promos (si las traes del scraper, aplica fit para evitar 1406)
                "promo_tipo":             fit("historico_precios", "promo_tipo", None),
                "precio_regular_promo":   fit("historico_precios", "promo_texto_regular", None),
                "precio_descuento":       fit("historico_precios", "promo_texto_descuento", None),
                "comentarios_promo":      fit("historico_precios", "promo_comentarios", None),
            }

            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en, fit=fit)
            inserted_hist += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {inserted_hist} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

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

# ================== Ejemplo de uso (integra con tu scraper) ==================
if __name__ == "__main__":
    import logging
    from medipiel import fetch_all_categories  # <-- importa tu función existente

    t0 = time.time()
    logging.getLogger().setLevel(logging.INFO)

    df = fetch_all_categories(depth=10)  # tu scraper actual
    if df is None or df.empty:
        print("No se obtuvieron productos.")
    else:
        # Inserta directo en MySQL con protección de longitudes
        persist_carrefour_df_to_mysql(df)

    elapsed = time.time() - t0
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)

    print(f"⏱️ Tiempo total: {hours}h {minutes}m {seconds}s")
