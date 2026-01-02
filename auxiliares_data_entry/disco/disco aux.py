#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Disco (PDP) → completa en el Excel:

- PRECIO_LISTA
- PRECIO_OFERTA
- TIPO_OFERTA (por ahora siempre NULL)

REGLAS:

1) Producto SIN oferta:
   Solo hay uno de estos (normalmente #priceContainer):

   <div id="priceContainer" class="discoargentina-store-theme-1dCOMij_MzTzZOCohX1K7w">$11.888</div>
   o
   <div class="discoargentina-store-theme-2t-mVsKNpKjmCAEM_AMCQH">$1.300</div>

   → PRECIO_LISTA = ese valor
   → PRECIO_OFERTA = NULL
   → TIPO_OFERTA = NULL

2) Producto CON oferta:
   Aparecen AMBOS:

   <div class="discoargentina-store-theme-2t-mVsKNpKjmCAEM_AMCQH">$1.300</div>   → PRECIO_LISTA
   <div id="priceContainer" class="discoargentina-store-theme-1dCOMij_MzTzZOCohX1K7w">$799</div> → PRECIO_OFERTA
"""

import re
import time
import random
from pathlib import Path

import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# ---------- Helpers ----------

def parse_price(text: str):
    """
    Convierte '$11.888' o '$1.300,50' en float.
    Devuelve None si no se puede parsear.
    """
    if not text:
        return None

    # Dejar solo dígitos, puntos y comas
    limpio = re.sub(r"[^\d.,]", "", text)
    if not limpio:
        return None

    # Quitar puntos de miles y usar coma como decimal
    limpio = limpio.replace(".", "").replace(",", ".")
    try:
        return float(limpio)
    except ValueError:
        return None


def scrape_pagina(driver, url: str, idx: int = 0):
    """
    Devuelve (precio_lista, precio_oferta, tipo_oferta) para una PDP de Disco.
    Aplica las reglas descritas en el docstring del archivo.
    """
    print(f"    GET -> {url}")
    try:
        driver.get(url)
    except Exception as e:
        print(f"[ERROR] cargando {url}: {e}")
        return None, None, None

    # Esperar a que aparezca al menos algún contenedor de precio
    try:
        WebDriverWait(driver, 15).until(
            EC.any_of(
                EC.presence_of_element_located((By.ID, "priceContainer")),
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.discoargentina-store-theme-2t-mVsKNpKjmCAEM_AMCQH")
                ),
            )
        )
    except TimeoutException:
        print(f"[WARN] No encontré precios en {url}")
        # Guardar HTML para debug
        debug_path = Path(f"debug_disco_timeout_{idx}.html")
        debug_path.write_text(driver.page_source, encoding="utf-8")
        print(f"    [DEBUG] HTML guardado en {debug_path}")
        return None, None, None

    # Guardar HTML de la primera fila para inspección inicial
    if idx == 0:
        Path("debug_disco_0.html").write_text(driver.page_source, encoding="utf-8")
        print("    [DEBUG] HTML inicial guardado en debug_disco_0.html")

    # ---- Leer #priceContainer (posible oferta o base) ----
    price_container_val = None
    try:
        el_main = driver.find_element(By.ID, "priceContainer")
        price_container_val = parse_price(el_main.text.strip())
    except NoSuchElementException:
        price_container_val = None

    # ---- Leer div de clase '2t-mVs...' (precio base si existe) ----
    base_div_val = None
    try:
        base_el = driver.find_element(
            By.CSS_SELECTOR,
            "div.discoargentina-store-theme-2t-mVsKNpKjmCAEM_AMCQH"
        )
        base_div_val = parse_price(base_el.text.strip())
    except NoSuchElementException:
        base_div_val = None

    # ---- Aplicar reglas de negocio ----
    precio_lista = None
    precio_oferta = None
    tipo_oferta = None  # Disco por ahora sin % explícito estable

    if base_div_val is not None:
        # Si existe el div 2t-mVs..., ESO es PRECIO_LISTA
        precio_lista = base_div_val
        precio_oferta = price_container_val  # y #priceContainer es la oferta
    else:
        # Si NO existe el div 2t-mVs..., lo que haya es solo PRECIO_LISTA
        # (normalmente #priceContainer)
        precio_lista = price_container_val
        precio_oferta = None

    return precio_lista, precio_oferta, tipo_oferta


# ---------- Main ----------

def main(
    input_path: str = "disco_aux_viernescaba.xlsx",
    output_path: str = "disco_aux_vieres_actualizado.xlsx",
    col_url: str = "URLs",
):
    """
    Lee el Excel de entrada, procesa cada URL y completa:
      PRECIO_LISTA, PRECIO_OFERTA, TIPO_OFERTA
    Luego guarda un nuevo Excel.
    """
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {input_file}")

    df = pd.read_excel(input_file)

    if col_url not in df.columns:
        raise ValueError(f"No existe la columna '{col_url}' en el Excel")

    # Asegurar columnas de salida
    for col in ["PRECIO_LISTA", "PRECIO_OFERTA", "TIPO_OFERTA"]:
        if col not in df.columns:
            df[col] = None

    # Configurar Chrome
    options = webdriver.ChromeOptions()
    # Te recomiendo probar primero VIENDO el navegador;
    # cuando todo funcione bien, puedes descomentar headless:
    # options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,800")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )

    try:
        for idx, row in df[df[col_url].notna()].iterrows():
            url = str(row[col_url]).strip()
            if not url:
                continue

            print(f"[{idx}] Procesando: {url}")

            try:
                precio_lista, precio_oferta, tipo_oferta = scrape_pagina(
                    driver, url, idx=idx
                )
            except Exception as e:
                print(f"[ERROR] en idx={idx}, url={url}: {e}")
                precio_lista, precio_oferta, tipo_oferta = None, None, None

            df.at[idx, "PRECIO_LISTA"] = precio_lista
            df.at[idx, "PRECIO_OFERTA"] = precio_oferta
            df.at[idx, "TIPO_OFERTA"] = tipo_oferta

            print(
                f"    -> lista={precio_lista} oferta={precio_oferta} tipo={tipo_oferta}"
            )

            # Sleep aleatorio para bajar cadencia y evitar bloqueos
            time.sleep(random.uniform(2, 5))

            # (Opcional) Reiniciar driver cada N filas para “refrescar”
            # if idx > 0 and idx % 80 == 0:
            #     driver.quit()
            #     driver = webdriver.Chrome(
            #         service=Service(ChromeDriverManager().install()),
            #         options=options,
            #     )

    finally:
        driver.quit()

    df.to_excel(output_path, index=False)
    print(f"\n✅ Archivo guardado en: {output_path}")


if __name__ == "__main__":
    main()
