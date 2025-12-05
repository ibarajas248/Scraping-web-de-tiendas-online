#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lee un Excel con URLs de Comodín y completa:
    PRECIO_LISTA, PRECIO_OFERTA, TIPO_OFERTA

Estructura en la página (después de ejecutar JS):
  <div class="product-tag"><img src="/arquivos/20descuento.png"></div>
  <p class="offer-price mb-1">$ 1.639,99</p>
  <span class="regular-price">$ 2.049,99</span>

Reglas:
- Si hay regular-price -> PRECIO_LISTA = regular, PRECIO_OFERTA = offer.
- Si NO hay regular-price -> PRECIO_LISTA = PRECIO_OFERTA = offer.
- TIPO_OFERTA se arma a partir del src de la imagen (ej: "20% descuento").
"""

import re
import time
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
    '$ 2.049,99' -> 2049.99 (float) o None si no se puede parsear.
    """
    if not text:
        return None

    limpio = re.sub(r"[^\d.,]", "", text)  # quita $, nbsp, etc
    if not limpio:
        return None

    # quita puntos de miles y deja coma como decimal
    limpio = limpio.replace(".", "").replace(",", ".")
    try:
        return float(limpio)
    except ValueError:
        return None


def scrape_pagina(driver, url: str):
    """
    Devuelve (precio_lista, precio_oferta, tipo_oferta) para una URL de Comodín.
    """
    try:
        driver.get(url)
    except Exception as e:
        print(f"[ERROR] al cargar {url}: {e}")
        return None, None, None

    # pequeño wait para que VTEX pinte el PDP
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".shop-detail-right")
            )
        )
    except TimeoutException:
        print(f"[WARN] timeout esperando detalles en {url}")
        # aun así intentamos buscar precios por si acaso
        pass

    # --- Tipo de oferta ---
    tipo_oferta = None
    try:
        img = driver.find_element(By.CSS_SELECTOR, "div.product-tag img")
        src = img.get_attribute("src") or ""
        # nos quedamos con el archivo: /arquivos/20descuento.png -> "20descuento.png"
        nombre = src.split("/")[-1]
        m = re.search(r"(\d+)", nombre)
        if m:
            tipo_oferta = f"{m.group(1)}% descuento"
        else:
            tipo_oferta = nombre or None
    except NoSuchElementException:
        tipo_oferta = None

    # --- Precios ---
    regular_el = None
    offer_el = None

    try:
        offer_el = driver.find_element(By.CSS_SELECTOR, "p.offer-price")
    except NoSuchElementException:
        pass

    # regular-price puede estar dentro de un <p><span class="regular-price">
    try:
        regular_el = driver.find_element(By.CSS_SELECTOR, "span.regular-price")
    except NoSuchElementException:
        regular_el = None

    precio_lista = None
    precio_oferta = None

    if regular_el:
        precio_lista = parse_price(regular_el.text)
        if offer_el:
            precio_oferta = parse_price(offer_el.text)
        else:
            precio_oferta = precio_lista
    elif offer_el:
        precio_oferta = parse_price(offer_el.text)
        precio_lista = precio_oferta
    else:
        precio_lista = None
        precio_oferta = None

    return precio_lista, precio_oferta, tipo_oferta


# ---------- Main ----------

def main(
    input_path="comodin_aux_viernes.xlsx",
    output_path="comodin_aux_viernes_actualizado.xlsx",
    url_column="URLs",  # cambia este nombre si tu columna se llama distinto
):
    input_path = Path(input_path)
    df = pd.read_excel(input_path)

    if url_column not in df.columns:
        raise ValueError(f"No se encontró la columna '{url_column}' en el Excel.")

    # Crear columnas si no existen
    for col in ["PRECIO_LISTA", "PRECIO_OFERTA", "TIPO_OFERTA"]:
        if col not in df.columns:
            df[col] = None

    # Configurar Chrome headless
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )

    try:
        for idx, row in df[df[url_column].notna()].iterrows():
            url = str(row[url_column]).strip()
            if not url:
                continue

            print(f"[{idx}] Procesando: {url}")
            precio_lista, precio_oferta, tipo_oferta = scrape_pagina(driver, url)

            df.at[idx, "PRECIO_LISTA"] = precio_lista
            df.at[idx, "PRECIO_OFERTA"] = precio_oferta
            df.at[idx, "TIPO_OFERTA"] = tipo_oferta

            print(
                f"    -> lista={precio_lista} oferta={precio_oferta} tipo={tipo_oferta}"
            )

            time.sleep(1)  # para no bombardear el sitio
    finally:
        driver.quit()

    df.to_excel(output_path, index=False)
    print(f"\nArchivo guardado en: {output_path}")


if __name__ == "__main__":
    main()
