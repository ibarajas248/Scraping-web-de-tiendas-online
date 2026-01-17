#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lee un Excel con una columna 'URLs', visita cada producto de Átomo (Prestashop)
y extrae información de la página de detalle, incluyendo el JSON del atributo
data-product en #product-details.

Extras:
- Inserta/actualiza en MySQL siguiendo TU PATRÓN estándar:
    * tiendas (codigo='atomo', nombre='Átomo Conviene')
    * productos (preferencia por EAN; match suave por (nombre, marca))
    * producto_tienda (sku_tienda = referencia, record_id_tienda = id_producto)
    * historico_precios (precios como VARCHAR)

Lógica de precios:
- Si existe price_without_reduction (precio normal) y es distinto de price_tax_exc/price_amount:
    precio_lista   = price_without_reduction
    precio_oferta  = precio_numero (o price_amount)
- Si no, solo precio_lista = precio_numero y precio_oferta = NULL

Además sigue guardando atomo_detalle.xlsx para depuración.
"""

import time
import re
import json
from html import unescape
from datetime import datetime
from typing import Dict, Any, List

import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

import sys
import os
from mysql.connector import Error as MySQLError

# ============ Conexión MySQL ============
# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL


# ==========================
# Configuración básica
# ==========================

# Directorio donde está este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_XLSX  = os.path.join(BASE_DIR, "atomo.xlsx")          # archivo de entrada
OUTPUT_XLSX = os.path.join(BASE_DIR, "atomo_detalle.xlsx")  # archivo de salida

TIMEOUT = 20        # segundos de timeout por request
SLEEP_BETWEEN = 0.5 # pausa entre requests para no pegarle muy fuerte al sitio

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

TIENDA_CODIGO = "atomo"
TIENDA_NOMBRE = "Atomo"


# ==========================
# Helpers de parsing
# ==========================

def limpiar_texto(s):
    """Limpia espacios y saltos. Devuelve cadena o None."""
    if s is None:
        return None
    s = str(s)
    s = s.replace("\xa0", " ")
    return " ".join(s.split()).strip() or None


def extraer_ean_desde_link(link):
    """
    Intenta extraer un EAN desde una URL que termina en ...-XXXXXXXXXXXX.html
    (8 a 14 dígitos).
    """
    if not link:
        return None
    m = re.search(r"(\d{8,14})\.html", link)
    return m.group(1) if m else None


def parsear_data_product_attr(attr_val):
    """
    attr_val viene como texto con &quot; etc.
    Lo convertimos a dict usando html.unescape + json.loads.
    """
    if not attr_val:
        return None
    try:
        raw = unescape(attr_val)
        return json.loads(raw)
    except Exception:
        return None


def scrape_producto(url):
    """
    Descarga la página de producto y extrae campos relevantes.
    Devuelve un dict con todos los datos (incluyendo precios).
    """
    fila = {
        "url_origen": url,
        "nombre": None,
        "precio_texto": None,
        "precio_numero": None,
        "moneda": None,
        "stock_cantidad": None,
        "stock_texto": None,
        "marca": None,
        "referencia": None,
        "id_producto": None,
        "categoria_slug": None,
        "categoria_nombre": None,
        "ean": None,
        "unidad": None,
        "available_date": None,
        "availability_message": None,
        "tax_name": None,
        "tax_rate": None,
        "price_tax_exc": None,
        "price_without_reduction": None,
        "date_add": None,
        "date_upd": None,
        "imagen_url": None,
        "link_canonico": None,
        "error": None,
    }

    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        fila["error"] = f"REQUEST_ERROR: {e}"
        return fila

    soup = BeautifulSoup(resp.text, "html.parser")

    # --------------------------
    # Título (h1.h1)
    # --------------------------
    h1 = soup.select_one("h1.h1")
    if h1:
        fila["nombre"] = limpiar_texto(h1.get_text())

    # --------------------------
    # Precio visible en la página
    # --------------------------
    price_span = soup.select_one(".product-prices .current-price .price")
    if price_span:
        precio_texto = limpiar_texto(price_span.get_text())
        fila["precio_texto"] = precio_texto

        if precio_texto:
            tmp = precio_texto
            tmp = tmp.replace("$", "").strip()
            tmp = tmp.replace(".", "").replace(",", ".")
            try:
                fila["precio_numero"] = float(tmp)
            except ValueError:
                fila["precio_numero"] = None

        if "$" in price_span.get_text():
            fila["moneda"] = "$"

    # --------------------------
    # Marca (bloque "product-manufacturer")
    # --------------------------
    brand_el = soup.select_one(".product-manufacturer span a")
    if brand_el:
        fila["marca"] = limpiar_texto(brand_el.get_text())

    # --------------------------
    # Stock (cantidad + texto)
    # --------------------------
    qty_span = soup.select_one(".product-quantities span[data-stock]")
    if qty_span and qty_span.has_attr("data-stock"):
        try:
            fila["stock_cantidad"] = int(qty_span["data-stock"])
        except ValueError:
            fila["stock_cantidad"] = None

    avail_span = soup.select_one("#product-availability span, #product-details span[aria-label='En stock']")
    if avail_span:
        fila["stock_texto"] = limpiar_texto(avail_span.get_text())

    # --------------------------
    # JSON en data-product de #product-details
    # --------------------------
    details_div = soup.select_one("#product-details[data-product]")
    product_data = None
    if details_div and details_div.has_attr("data-product"):
        product_data = parsear_data_product_attr(details_div["data-product"])

    if product_data:
        if not fila["nombre"]:
            fila["nombre"] = limpiar_texto(product_data.get("name"))

        fila["referencia"] = limpiar_texto(product_data.get("reference"))
        fila["id_producto"] = product_data.get("id_product") or product_data.get("id")

        fila["categoria_slug"] = limpiar_texto(product_data.get("category"))
        fila["categoria_nombre"] = limpiar_texto(product_data.get("category_name"))

        fila["unidad"] = limpiar_texto(product_data.get("unity"))
        fila["available_date"] = limpiar_texto(product_data.get("available_date"))
        fila["availability_message"] = limpiar_texto(product_data.get("availability_message"))

        fila["tax_name"] = limpiar_texto(product_data.get("tax_name"))
        fila["tax_rate"] = product_data.get("rate")

        fila["price_tax_exc"] = product_data.get("price_tax_exc")
        fila["price_without_reduction"] = product_data.get("price_without_reduction")

        fila["date_add"] = limpiar_texto(product_data.get("date_add"))
        fila["date_upd"] = limpiar_texto(product_data.get("date_upd"))

        fila["link_canonico"] = limpiar_texto(product_data.get("link"))

        # EAN desde link del JSON
        ean_link = product_data.get("link")
        ean = extraer_ean_desde_link(ean_link)
        if ean:
            fila["ean"] = ean

        # Imagen principal
        img_url = None
        cover = product_data.get("cover") or {}
        bysize = cover.get("bySize") or {}
        large_default = bysize.get("large_default") or bysize.get("medium_default") or {}
        img_url = large_default.get("url")

        if not img_url:
            images = product_data.get("images") or []
            if images:
                bysize2 = images[0].get("bySize") or {}
                large_default2 = bysize2.get("large_default") or bysize2.get("medium_default") or {}
                img_url = large_default2.get("url")

        fila["imagen_url"] = limpiar_texto(img_url)

        # Si precio_numero no está, usamos price_amount del JSON
        if fila["precio_numero"] is None:
            price_amount = product_data.get("price_amount")
            if price_amount is not None:
                try:
                    fila["precio_numero"] = float(price_amount)
                except ValueError:
                    fila["precio_numero"] = None

        # Si stock_cantidad no está, usamos quantity del JSON
        if fila["stock_cantidad"] is None:
            qty = product_data.get("quantity")
            try:
                fila["stock_cantidad"] = int(qty) if qty is not None else None
            except (ValueError, TypeError):
                pass

        if product_data.get("availability_message") and not fila["stock_texto"]:
            fila["stock_texto"] = limpiar_texto(product_data.get("availability_message"))

        if not fila["ean"]:
            fila["ean"] = extraer_ean_desde_link(fila["link_canonico"])

    # --------------------------
    # Si aún no hay EAN, intentar desde la URL original
    # --------------------------
    if not fila["ean"]:
        fila["ean"] = extraer_ean_desde_link(url)

    return fila


# ==========================
# Helpers MySQL (TU PATRÓN)
# ==========================

def clean(val):
    """Normaliza texto: trim, colapsa espacios, filtra null-likes simples."""
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    if s.lower() in {"", "null", "none", "nan", "na"}:
        return None
    return s

def parse_price(val) -> float:
    """Parsea número o string de precio a float; np.nan si no se puede."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return np.nan
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

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
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))""",
        (
            p.get("ean") or "", nombre, marca,
            p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
        )
    )
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
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
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))""",
        (tienda_id, producto_id, url, nombre_tienda)
    )
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    def to_txt_or_none(x):
        if x is None:
            return None
        v = parse_price(x)
        if isinstance(v, float) and np.isnan(v):
            return None
        return f"{round(float(v), 2)}"  # guardamos como VARCHAR

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


# ==========================
# Main
# ==========================

def main():
    # Leer Excel
    df_in = pd.read_excel(INPUT_XLSX)

    if "URLs" not in df_in.columns:
        raise ValueError("El archivo de entrada debe tener una columna llamada 'URLs'.")

    urls = df_in["URLs"].dropna().astype(str).unique().tolist()

    resultados: List[Dict[str, Any]] = []

    print(f"Total de URLs a procesar: {len(urls)}")

    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] Scrapeando: {url}")
        fila = scrape_producto(url)
        resultados.append(fila)
        time.sleep(SLEEP_BETWEEN)

    # Guardar a Excel para depuración
    df_out = pd.DataFrame(resultados)
    df_out.to_excel(OUTPUT_XLSX, index=False)
    print(f"✅ Listo. Datos guardados en: {OUTPUT_XLSX}")

    # ==============================
    # Inserción en MySQL (TU PATRÓN)
    # ==============================
    if not resultados:
        print("⚠️ No hay resultados para insertar en MySQL.")
        return

    capturado_en = datetime.now()
    conn = None

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0

        for r in resultados:
            # --- lógica de precios lista/oferta ---
            precio_num = r.get("precio_numero")
            price_tax_exc = r.get("price_tax_exc")
            price_without_reduction = r.get("price_without_reduction")

            # Precio base: lo que se cobraría hoy (precio_num o price_tax_exc)
            precio_base = None
            if precio_num is not None:
                precio_base = precio_num
            elif price_tax_exc is not None:
                precio_base = price_tax_exc

            precio_lista = None
            precio_oferta = None

            if price_without_reduction is not None and precio_base is not None:
                try:
                    pwr = float(price_without_reduction)
                    pbase = float(precio_base)
                    if abs(pwr - pbase) > 0.001:
                        precio_lista = pwr
                        precio_oferta = pbase
                    else:
                        precio_lista = pbase
                        precio_oferta = None
                except Exception:
                    precio_lista = precio_base
                    precio_oferta = None
            else:
                precio_lista = precio_base
                precio_oferta = None

            p = {
                "sku":        r.get("referencia"),           # referencia como SKU
                "record_id":  r.get("id_producto"),          # id de Prestashop
                "ean":        r.get("ean"),
                "nombre":     r.get("nombre"),
                "marca":      r.get("marca"),
                "fabricante": None,                          # Prestashop no trae fabricante claro
                "precio_lista":   precio_lista,
                "precio_oferta":  precio_oferta,
                "tipo_oferta":    None,
                "promo_tipo":     None,
                "precio_regular_promo": None,
                "precio_descuento":     None,
                "comentarios_promo":    None,
                "categoria":    r.get("categoria_nombre") or r.get("categoria_slug"),
                "subcategoria": None,
                "url":          r.get("link_canonico") or r.get("url_origen"),
            }

            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

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


if __name__ == "__main__":
    main()
