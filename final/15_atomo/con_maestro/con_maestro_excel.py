#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lee un Excel con una columna 'URLs', visita cada producto de Átomo (Prestashop)
y extrae información de la página de detalle, incluyendo el JSON del atributo
data-product en #product-details.

Salida: atomo_detalle.xlsx
"""

import time
import re
import json
from html import unescape

import requests
import pandas as pd
from bs4 import BeautifulSoup


# ==========================
# Configuración básica
# ==========================

INPUT_XLSX  = "atomo.xlsx"          # nombre del archivo de entrada
OUTPUT_XLSX = "atomo_detalle.xlsx"  # nombre del archivo de salida

TIMEOUT = 20        # segundos de timeout por request
SLEEP_BETWEEN = 0.5 # pausa entre requests para no pegarle muy fuerte al sitio

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


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
    Devuelve un dict con todos los datos.
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

        # Intentar parsear número (formato Argentina: 2.399,00 $)
        # Lo transformamos a 2399.00
        if precio_texto:
            # Quitar moneda
            # p.ej. "2.399,00 $" -> "2.399,00"
            tmp = precio_texto
            tmp = tmp.replace("$", "").strip()
            tmp = tmp.replace(".", "").replace(",", ".")
            # Ahora deberíamos tener "2399.00"
            try:
                fila["precio_numero"] = float(tmp)
            except ValueError:
                fila["precio_numero"] = None

        # Moneda (muy simple: mira último carácter si es $)
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
    # Cantidad dentro de .product-quantities
    qty_span = soup.select_one(".product-quantities span[data-stock]")
    if qty_span and qty_span.has_attr("data-stock"):
        try:
            fila["stock_cantidad"] = int(qty_span["data-stock"])
        except ValueError:
            fila["stock_cantidad"] = None

    # Mensaje de disponibilidad (ej: "Últimas unidades en stock")
    avail_span = soup.select_one("#product-availability span, #product-details span[aria-label='En stock']")

    # O directamente desde data-product (availability_message)
    # pero eso lo hacemos con el JSON de abajo; aquí solo DOM.
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
        # Nombre desde JSON (por si no se pudo desde h1)
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

        # Imagen principal (cover / images[0])
        img_url = None
        cover = product_data.get("cover") or {}
        bysize = cover.get("bySize") or {}
        large_default = bysize.get("large_default") or bysize.get("medium_default") or {}
        img_url = large_default.get("url")

        # Fallback a images[0]
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

        # Si availability_message del JSON está y no llenamos stock_texto antes
        if product_data.get("availability_message") and not fila["stock_texto"]:
            fila["stock_texto"] = limpiar_texto(product_data.get("availability_message"))

        # Si no hay EAN pero aparece en otra parte del link (ya se intentó arriba)
        if not fila["ean"]:
            fila["ean"] = extraer_ean_desde_link(fila["link_canonico"])

    # --------------------------
    # Si aún no hay EAN, intentar desde la URL original
    # --------------------------
    if not fila["ean"]:
        fila["ean"] = extraer_ean_desde_link(url)

    return fila


# ==========================
# Main
# ==========================

def main():
    # Leer Excel
    df_in = pd.read_excel(INPUT_XLSX)

    if "URLs" not in df_in.columns:
        raise ValueError("El archivo de entrada debe tener una columna llamada 'URLs'.")

    urls = df_in["URLs"].dropna().astype(str).unique().tolist()

    resultados = []

    print(f"Total de URLs a procesar: {len(urls)}")

    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] Scrapeando: {url}")
        fila = scrape_producto(url)
        resultados.append(fila)
        time.sleep(SLEEP_BETWEEN)

    df_out = pd.DataFrame(resultados)

    # Guardar a Excel
    df_out.to_excel(OUTPUT_XLSX, index=False)
    print(f"✅ Listo. Datos guardados en: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
