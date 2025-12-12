#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

INPUT_FILE = "supermami_aux_miercoles.xlsx"      # archivo de entrada
OUTPUT_FILE = "supermami_precios_miercoles.xlsx" # archivo de salida


def limpiar_precio(texto: str):
    """
    Recibe algo como '$2,200.00' y devuelve 2200.00 (float)
    Si algo sale mal, devuelve 'N/A'.
    """
    if not texto:
        return "N/A"

    try:
        txt = texto.strip()
        # quita símbolo $ y espacios
        txt = txt.replace("$", "").replace(" ", "")
        # para este formato: miles con coma y decimales con punto
        # '$2,200.00' -> '2200.00'
        txt = txt.replace(",", "")
        return float(txt)
    except Exception:
        return "N/A"


def extraer_precio(url: str):
    """
    Abre la URL, busca el bloque:
      <div class="precio-unidad"> ... <span> $2,200.00 </span> ...
    y devuelve el precio como número (float) o 'N/A'.
    """
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
        resp = requests.get(url.strip(), headers=headers, timeout=20)
        if resp.status_code != 200:
            print(f"[WARN] {url} → status {resp.status_code}")
            return "N/A"

        soup = BeautifulSoup(resp.text, "html.parser")

        # Busca el div de precio y el span interno
        contenedor = soup.find("div", class_="precio-unidad")
        if not contenedor:
            print(f"[WARN] No se encontró <div class='precio-unidad'> en {url}")
            # Opcional: guardar HTML para debug la primera vez
            # with open("debug_supermami.html", "w", encoding="utf-8") as f:
            #     f.write(resp.text)
            return "N/A"

        span = contenedor.find("span")
        if not span:
            print(f"[WARN] No se encontró <span> dentro de precio-unidad en {url}")
            return "N/A"

        texto_precio = span.get_text(strip=True)
        return limpiar_precio(texto_precio)

    except Exception as e:
        print(f"[ERROR] {url} → {e}")
        return "N/A"


def main():
    # Lee el Excel
    df = pd.read_excel(INPUT_FILE)

    # Crea la columna si no existe
    if "PRECIO_LISTA" not in df.columns:
        df["PRECIO_LISTA"] = None

    # Recorre las filas
    for idx, url in df["URLs"].items():
        print(f"Procesando fila {idx} → {url}")
        precio = extraer_precio(url)
        df.at[idx, "PRECIO_LISTA"] = precio
        # pequeña pausa para no bombardear al servidor
        time.sleep(1)

    # Guarda resultado
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Listo. Archivo guardado como {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
