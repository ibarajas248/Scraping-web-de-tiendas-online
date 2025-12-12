#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dia_aux_viernes.py

Lee un Excel con columna 'URLs', visita cada URL y extrae:
- PRECIO_LISTA  (precio base)
- PRECIO_OFERTA (precio con oferta, si existe)
- TIPO_OFERTA   (ej: '25%', si existe)

Casos:
1) Solo precio_base (sin oferta):
   <span class="diaio-store-5-x-sellingPriceValue">$ 3.159</span>

2) Con oferta:
   Precio base (tachado):
   <span class="diaio-store-5-x-listPriceValue strike">$ 8.690</span>

   Precio oferta:
   <span class="diaio-store-5-x-sellingPriceValue">$ 6.517,50</span>

   Tipo oferta:
   <span class="vtex-product-price-1-x-savingsPercentage ...">25%</span>
"""

import time
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- Configura aquí tus archivos ----------
INPUT_FILE = "dia_aux_viernes.xlsx"
OUTPUT_FILE = "dia_precios_viernes.xlsx"


# ---------- Helpers ----------

def limpiar_precio_ar(texto: str):
    """
    Convierte un precio en formato AR a float.

    Ejemplos:
    - '$ 6.517,50' -> 6517.50
    - '$ 1.460'    -> 1460.0
    - '$ 890'      -> 890.0
    """
    if not texto:
        return None

    txt = texto.strip()
    txt = txt.replace("\xa0", " ")  # &nbsp;
    txt = txt.replace("$", "").strip()

    # Dejamos solo dígitos, puntos, comas
    txt = re.sub(r"[^0-9\.,]", "", txt)

    if not txt:
        return None

    if "," in txt:
        # Caso típico AR: miles con '.' y decimales con ','
        # '6.517,50' -> '6517,50' -> '6517.50'
        txt = txt.replace(".", "").replace(",", ".")
    else:
        # No hay coma: asumimos que los '.' son separadores de miles, no decimales
        # '1.460' -> '1460'
        txt = txt.replace(".", "")

    if not txt:
        return None

    try:
        return float(txt)
    except Exception:
        return None



def extraer_precios_desde_html(html: str):
    """
    Devuelve (precio_base, precio_oferta, tipo_oferta):

    - precio_base: float o None
    - precio_oferta: float o None
    - tipo_oferta: str o None (ej: '25%')
    """
    soup = BeautifulSoup(html, "lxml")

    # --- Intentamos primero el caso con oferta ---
    span_list_price = soup.select_one("span.diaio-store-5-x-listPriceValue")
    span_selling_price = soup.select_one("span.diaio-store-5-x-sellingPriceValue")
    span_tipo_oferta = soup.select_one(
        "span.vtex-product-price-1-x-savingsPercentage"
    )

    if span_list_price and span_selling_price:
        # Hay oferta (precio tachado + precio oferta)
        txt_base = span_list_price.get_text(strip=True)
        txt_oferta = span_selling_price.get_text(strip=True)
        txt_tipo = span_tipo_oferta.get_text(strip=True) if span_tipo_oferta else None

        precio_base = limpiar_precio_ar(txt_base)
        precio_oferta = limpiar_precio_ar(txt_oferta)
        tipo_oferta = txt_tipo
    else:
        # No hay oferta: solo precio base en sellingPriceValue
        span_solo_base = soup.select_one("span.diaio-store-5-x-sellingPriceValue")
        if span_solo_base:
            txt_base = span_solo_base.get_text(strip=True)
            precio_base = limpiar_precio_ar(txt_base)
        else:
            precio_base = None

        precio_oferta = None
        tipo_oferta = None

    return precio_base, precio_oferta, tipo_oferta


def extraer_precios_desde_url(url: str, timeout: int = 20):
    """
    Abre la URL y devuelve (precio_base, precio_oferta, tipo_oferta).
    Si algo falla, devuelve (None, None, None).
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return extraer_precios_desde_html(resp.text)
    except Exception as e:
        print(f"[ERROR] URL: {url} -> {e}")
        return None, None, None


# ---------- Main ----------

def main():
    # Leemos el Excel
    df = pd.read_excel(INPUT_FILE)

    # Nos aseguramos de usar la columna 'URLs'
    col_url = "URLs"
    if col_url not in df.columns:
        raise ValueError(
            f"No se encontró la columna '{col_url}' en el Excel. Columnas: {df.columns.tolist()}"
        )

    # Listas para ir guardando los resultados
    precios_base = []
    precios_oferta = []
    tipos_oferta = []

    total = len(df)

    for i, url in enumerate(df[col_url], start=1):
        url = str(url).strip()
        if not url or url.lower() == "nan":
            precios_base.append(None)
            precios_oferta.append(None)
            tipos_oferta.append(None)
            print(f"[{i}/{total}] URL vacía -> se salta")
            continue

        print(f"[{i}/{total}] Scrapeando: {url}")
        precio_base, precio_oferta, tipo_oferta = extraer_precios_desde_url(url)

        print(
            f"   → PRECIO_LISTA (base): {precio_base} | "
            f"PRECIO_OFERTA: {precio_oferta} | "
            f"TIPO_OFERTA: {tipo_oferta}"
        )

        precios_base.append(precio_base)
        precios_oferta.append(precio_oferta)
        tipos_oferta.append(tipo_oferta)

        # Pequeña pausa para no abusar del servidor
        time.sleep(1.0)

    # Sobrescribimos / creamos columnas en el DataFrame
    df["PRECIO_LISTA"] = precios_base
    df["PRECIO_OFERTA"] = precios_oferta
    df["TIPO_OFERTA"] = tipos_oferta

    # Guardamos el resultado
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n✅ Listo. Archivo guardado en: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
