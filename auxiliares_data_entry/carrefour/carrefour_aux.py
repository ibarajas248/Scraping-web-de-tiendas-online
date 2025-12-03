#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lee carrefour_aux_miercoles.xlsx, toma la columna 'URLs',
entra a cada página (VTEX Carrefour) y extrae el precio de:

<span class="...currencyContainer">
   $ 2.750,00
</span>

Guarda el resultado en la columna 'PRECIO_LISTA'
(como texto, para evitar problemas de tipos).
"""

import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

INPUT_FILE = "carrefour_aux_miercoles.xlsx"
OUTPUT_FILE = "carrefour_precios.xlsx"


def parse_vtex_price(soup):
    """
    Busca el contenedor de moneda y arma el número:

    <span class="...currencyContainer">
        $ 2.750,00
    </span>

    Devuelve float (2750.00) o 'N/A' si no lo encuentra.
    """
    # cualquier clase que contenga 'currencyContainer'
    container = soup.find(
        "span",
        class_=lambda c: c and "currencyContainer" in c
    )
    if not container:
        return "N/A"

    # ejemplo de texto: "$ 2.750,00"
    raw_text = container.get_text(strip=True)

    # nos quedamos con la primera parte que tenga dígitos / puntos / comas
    matches = re.findall(r"[\d\.,]+", raw_text)
    if not matches:
        return "N/A"

    num_text = matches[0]  # p.ej. "2.750,00"

    # VTEX usa '.' como miles y ',' como decimales
    num_text = num_text.replace(".", "")   # "2750,00"
    num_text = num_text.replace(",", ".")  # "2750.00"

    try:
        return float(num_text)
    except Exception:
        return "N/A"


def extraer_precio(url: str):
    """Hace el GET a la URL y devuelve float o 'N/A'."""
    if not isinstance(url, str) or not url.strip():
        return "N/A"

    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }
        resp = requests.get(url.strip(), headers=headers, timeout=25)
        if resp.status_code != 200:
            print(f"[WARN] {url} → status {resp.status_code}")
            return "N/A"

        soup = BeautifulSoup(resp.text, "html.parser")
        return parse_vtex_price(soup)

    except Exception as e:
        print(f"[ERROR] {url} → {e}")
        return "N/A"


def main():
    # Lee el Excel
    df = pd.read_excel(INPUT_FILE)

    # Si no existe la columna, la creamos vacía
    if "PRECIO_LISTA" not in df.columns:
        df["PRECIO_LISTA"] = ""

    # Forzamos la columna a tipo object para poder guardar strings
    df["PRECIO_LISTA"] = df["PRECIO_LISTA"].astype("object")

    # Recorremos URLs
    for idx, url in df["URLs"].items():
        print(f"Fila {idx} → {url}")
        precio = extraer_precio(url)

        # Guardamos SIEMPRE texto, así evitamos el FutureWarning
        if precio == "N/A":
            df.at[idx, "PRECIO_LISTA"] = "N/A"
        else:
            # lo dejamos con 2 decimales como string
            df.at[idx, "PRECIO_LISTA"] = f"{precio:.2f}"

        time.sleep(1)  # pequeña pausa para no matar el sitio

    # Guardar resultado
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"✔ Listo. Archivo guardado como {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
