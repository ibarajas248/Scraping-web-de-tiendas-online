# =================== MySQL: upsert para Cormoran ===================
# Requiere: pip install mysql-connector-python
# y un base_datos.get_conn() que devuelva una conexión mysql.connector.connect(...)
from datetime import datetime
import numpy as np
import pandas as pd
from mysql.connector import Error as MySQLError
import sys, os
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu conexión MySQL

TIENDA_CODIGO = "cormoran"
TIENDA_NOMBRE = "Cormoran"

def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    return s

def _price_or_none(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return str(round(float(v), 2))
    try:
        x = float(str(v).replace(",", "."))
        return str(round(x, 2))
    except Exception:
        return None

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: dict) -> int:
    """
    p: {"ean","nombre","marca","categoria","subcategoria","fabricante"}
    Estrategia:
      1) si hay EAN -> match por EAN
      2) sino, match por (nombre, marca)
      3) si no existe -> insert
    """
    ean = _clean(p.get("ean"))
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        r = cur.fetchone()
        if r:
            pid = r[0]
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

    nombre = _clean(p.get("nombre")) or ""
    marca  = _clean(p.get("marca")) or ""
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca))
        r = cur.fetchone()
        if r:
            pid = r[0]
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

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: dict) -> int:
    """
    p: {"sku_tienda","record_id_tienda","url_tienda","nombre_tienda"}
    Clave preferida: SKU tienda. Si no hay, usa record_id (ProductId VTEX).
    """
    sku = _clean(p.get("sku_tienda"))
    rec = _clean(p.get("record_id_tienda"))
    url = p.get("url_tienda") or ""
    nombre_tienda = p.get("nombre_tienda") or ""

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

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: dict, capturado_en: datetime):
    """
    Guarda precios en historico_precios (VARCHARs de precio para tolerar nulos/formato).
    """
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
        _price_or_none(p.get("precio_lista")), _price_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None,
        None, None, None, None
    ))

def upload_cormoran_df_to_mysql(df: pd.DataFrame) -> None:
    """
    Mapea columnas del DF de Cormoran y sube:
      productos (por EAN o nombre+marca)
      producto_tienda (SKU / ProductId)
      historico_precios (precio lista/oferta)
    DF esperado con columnas: ProductId, SKU, EAN, Nombre, Marca, Categoria,
      PrecioLista, PrecioOferta, URL, Imagen, UnitMultiplier, MeasurementUnit, Seller
    """
    if df is None or df.empty:
        print("⚠️ DataFrame vacío: no hay nada para subir.")
        return

    now = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for _, r in df.iterrows():
            # Producto “base”
            prod = {
                "ean": _clean(r.get("EAN")),
                "nombre": _clean(r.get("Nombre")),
                "marca": _clean(r.get("Marca")),
                "fabricante": None,
                "categoria": _clean(r.get("Categoria")),
                "subcategoria": None,
            }
            pid = find_or_create_producto(cur, prod)

            # Producto-Tienda
            pt = {
                "sku_tienda": _clean(r.get("SKU")),
                "record_id_tienda": _clean(r.get("ProductId")),
                "url_tienda": _clean(r.get("URL")),
                "nombre_tienda": _clean(r.get("Nombre")),
            }
            pt_id = upsert_producto_tienda(cur, tienda_id, pid, pt)

            # Histórico
            hist = {
                "precio_lista": r.get("PrecioLista"),
                "precio_oferta": r.get("PrecioOferta"),
                "tipo_oferta": None,  # VTEX lo trae como teasers; si lo agregás al DF, mapealo acá
            }
            insert_historico(cur, tienda_id, pt_id, hist, now)
            insertados += 1

        conn.commit()
        print(f"💾 MySQL OK: {insertados} registros de histórico para {TIENDA_NOMBRE} ({now})")

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
