#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lee un Excel con URLs y completa las columnas:
- PRECIO_LISTA
- PRECIO_OFERTA
- TIPO_OFERTA

A partir de estructuras como:

<div class="mt-2 small ng-star-inserted">
  <b>Precio regular :</b>  $1.739,00
</div>

<div class="mb-1">
  <var class="price h3"> $869,50 </var>
</div>

<div class="mb-2 ng-star-inserted">
  <b class="text-success">2x1</b>
</div>
"""

import re
import time
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ==========================
#  UTILIDADES
# ==========================

def build_session() -> requests.Session:
    """Crea una sesión con reintentos básicos."""
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        )
    })
    return s


PRICE_RE = re.compile(r"\$?\s*([\d\.\,]+)")


def clean_price_str(text: str | None) -> str | None:
    """
    Devuelve el precio como string “limpio” en formato argentino.
    Ej: " $1.739,00 " → "1.739,00"
    """
    if not text:
        return None
    text = text.strip()
    m = PRICE_RE.search(text)
    if not m:
        return None
    return m.group(1)


# ==========================
#  PARSEO DEL HTML
# ==========================

def parse_promo(html: str) -> tuple[str | None, str | None, str | None]:
    """
    Extrae (precio_lista, precio_oferta, tipo_oferta) del HTML.
    Devuelve strings (formato AR) o None si no se encuentran.
    """
    soup = BeautifulSoup(html, "html.parser")

    # ---- PRECIO LISTA (Precio regular :) ----
    precio_lista = None
    b_regular = soup.find("b", string=lambda t: t and "Precio regular" in t)
    if b_regular and b_regular.parent:
        texto_div = b_regular.parent.get_text(" ", strip=True)
        precio_lista = clean_price_str(texto_div)

    # ---- PRECIO OFERTA (<var class="price h3">) ----
    precio_oferta = None
    var_oferta = soup.find("var", class_=lambda c: c and "price" in c.split())
    if var_oferta:
        precio_oferta = clean_price_str(var_oferta.get_text(" ", strip=True))

    # ---- TIPO OFERTA (<b class="text-success">2x1</b>) ----
    tipo_oferta = None
    b_tipo = soup.find("b", class_=lambda c: c and "text-success" in c.split())
    if b_tipo:
        tipo_oferta = b_tipo.get_text(strip=True) or None

    return precio_lista, precio_oferta, tipo_oferta


# ==========================
#  PROCESO PRINCIPAL
# ==========================

def procesar_excel(
    input_path: str = "coto_aux_miercoles.xlsx",
    output_path: str = "coto_aux_miercoles_precios.xlsx",
    col_urls: str = "URLs",
):
    # Leer Excel
    print(f"Leyendo archivo: {input_path}")
    df = pd.read_excel(input_path)

    if col_urls not in df.columns:
        raise ValueError(f"No se encontró la columna '{col_urls}' en el Excel.")

    # Crear columnas (como texto, para evitar problemas de tipos)
    for col in ["PRECIO_LISTA", "PRECIO_OFERTA", "TIPO_OFERTA"]:
        if col not in df.columns:
            df[col] = None

    session = build_session()

    for idx, row in df.iterrows():
        url = str(row[col_urls]).strip()
        if not url or url.lower() == "nan":
            continue

        print(f"[{idx+1}/{len(df)}] Procesando: {url}")
        try:
            resp = session.get(url, timeout=25)
            if resp.status_code != 200:
                print(f"  -> Status {resp.status_code}, no se pudo obtener la página")
                continue

            precio_lista, precio_oferta, tipo_oferta = parse_promo(resp.text)

            df.at[idx, "PRECIO_LISTA"] = precio_lista
            df.at[idx, "PRECIO_OFERTA"] = precio_oferta
            df.at[idx, "TIPO_OFERTA"] = tipo_oferta

            # Opcional: pequeño sleep para no pegarle tan fuerte al sitio
            time.sleep(0.5)

        except Exception as e:
            print(f"  -> ERROR en {url}: {e}")

    # Guardar resultado
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_path, index=False)
    print(f"Archivo guardado con precios en: {out_path.resolve()}")


if __name__ == "__main__":
    # Ajusta los nombres si tu archivo/columnas se llaman distinto
    procesar_excel(
        input_path="coto_aux_miercoles.xlsx",
        output_path="coto_aux_miercoles_precios.xlsx",
        col_urls="URLs",
    )
