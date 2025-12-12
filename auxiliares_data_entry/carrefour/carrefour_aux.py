#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para extraer precio_base de productos Carrefour (VTEX)
y agregarlo a un Excel existente.

- Lee un archivo de entrada (Excel) con una columna de URLs.
- Para cada URL, descarga el HTML y busca el bloque de precio_base:
    <span class="valtech-carrefourar-product-price-0-x-sellingPriceValue">
        <span class="valtech-carrefourar-product-price-0-x-currencyContainer">
            ...
        </span>
    </span>

- Reconstruye el precio desde los spans:
    Integer + Group + Decimal + Fraction
    Ej: 2 . 079 , 00  -> "2.079,00" -> 2079.00 (float)

- Escribe un archivo de salida con una nueva columna: precio_base
"""

import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ========= CONFIGURACIÓN =========
INPUT_FILE = "carrefour_aux_miercoles.xlsx"      # archivo de entrada
OUTPUT_FILE = "carrefour_precios_base.xlsx"      # archivo de salida
COLUMNA_URL = None  # se detecta entre 'url' o 'URL'

# User-Agent para evitar bloqueos tontos
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def limpiar_precio_vtex(spans):
    """
    Recibe una lista de spans (Integer, Group, Decimal, Fraction)
    y devuelve un float normalizado.

    Ejemplo:
        spans -> "2.079,00" -> 2079.00
    """
    if not spans:
        return None

    # Unir textos de todos los spans
    texto = "".join(span.get_text(strip=True) for span in spans)

    # Normalizar formato argentino: miles con punto, decimales con coma
    #  "2.079,00" -> "2079,00" -> "2079.00"
    texto = texto.replace(".", "")   # elimina separador de miles
    texto = texto.replace(",", ".")  # cambia coma decimal a punto

    try:
        return float(texto)
    except Exception:
        return None


def extraer_precio_base(url, intentos=3, espera=3):
    """
    Dada la URL de un producto, devuelve el precio_base (float) o None.
    Reintenta algunas veces en caso de error de red.
    """
    if not url:
        return None

    url = url.strip()
    if not url:
        return None

    for intento in range(1, intentos + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                print(f"  [HTTP {resp.status_code}] {url}")
                time.sleep(espera)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Bloque específico del precio_base (sellingPrice)
            contenedor = soup.select_one(
                "span.valtech-carrefourar-product-price-0-x-sellingPriceValue"
            )
            if not contenedor:
                # Fallback: intentar desde el contenedor general de precios
                contenedor = soup.select_one(
                    "div.vtex-flex-layout-0-x-flexColChild--product-view-prices-container"
                )

            if not contenedor:
                return None

            spans_precio = contenedor.select(
                "span.valtech-carrefourar-product-price-0-x-currencyInteger, "
                "span.valtech-carrefourar-product-price-0-x-currencyGroup, "
                "span.valtech-carrefourar-product-price-0-x-currencyDecimal, "
                "span.valtech-carrefourar-product-price-0-x-currencyFraction"
            )

            if not spans_precio:
                return None

            return limpiar_precio_vtex(spans_precio)

        except requests.RequestException as e:
            print(f"  [ERROR red intento {intento}/{intentos}] {url} -> {e}")
            time.sleep(espera)

        except Exception as e:
            print(f"  [ERROR parseo] {url} -> {e}")
            return None

    return None


def main():
    global COLUMNA_URL

    print(f"📥 Leyendo archivo de entrada: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)

    # Detectar la columna de URL
    columnas = [c.lower() for c in df.columns]

    if "URLs" in columnas:
        COLUMNA_URL = df.columns[columnas.index("URLs")]
    elif "URL" in df.columns:
        COLUMNA_URL = "URL"
    else:
        # Si se llama distinto, cambia aquí manualmente:
        # COLUMNA_URL = "mi_columna_url"
        raise ValueError(
            "No se encontró columna 'url' ni 'URL' en el Excel. "
            "Renombra la columna o edita el script."
        )

    print(f"✅ Usando columna de URL: {COLUMNA_URL}")

    # Crear columna de salida
    if "precio_base" not in df.columns:
        df["precio_base"] = None

    total = len(df)
    print(f"🔎 Procesando {total} filas...\n")

    for i, row in df.iterrows():
        url = row[COLUMNA_URL]
        print(f"[{i+1}/{total}] URL: {url}")

        precio = extraer_precio_base(url)

        df.at[i, "precio_base"] = precio

        print(f"    → precio_base = {precio}\n")
        # Pequeña pausa para no bombardear al server
        time.sleep(1.0)

    print(f"💾 Guardando resultados en: {OUTPUT_FILE}")
    df.to_excel(OUTPUT_FILE, index=False)
    print("✅ Listo.")


if __name__ == "__main__":
    main()
