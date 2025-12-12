#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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


def parse_price(text: str):
    """'$4.199,00' -> 4199.00 (float)"""
    if not text:
        return None
    limpio = re.sub(r"[^\d.,]", "", text)
    if not limpio:
        return None
    limpio = limpio.replace(".", "").replace(",", ".")
    try:
        return float(limpio)
    except ValueError:
        return None


def scrape_pagina(driver, url: str, idx: int = 0):
    # Limpia URLs tipo ...%3F...
    if "%3F" in url:
        url = url.split("%3F", 1)[0]

    print(f"    GET -> {url}")
    try:
        driver.get(url)
    except Exception as e:
        print(f"[ERROR] cargando {url}: {e}")
        return None, None, None

    # Esperar a que Angular pinte algo de precios:
    try:
        WebDriverWait(driver, 15).until(
            EC.any_of(
                EC.presence_of_element_located(
                    (By.XPATH, "//var[contains(@class,'price')]")
                ),
                EC.presence_of_element_located(
                    (By.XPATH, "//*[contains(.,'Precio regular')]")
                )
            )
        )
    except TimeoutException:
        print(f"[WARN] No cargó contenido dinámico en {url}")

    # DEBUG: guarda el HTML de la primera fila para inspeccionar
    if idx == 0:
        Path("debug_coto_0.html").write_text(driver.page_source, encoding="utf-8")
        print("    [DEBUG] HTML guardado en debug_coto_0.html")

    precio_lista = None
    precio_oferta = None
    tipo_oferta = None

    # -------- PRECIO OFERTA --------
    try:
        oferta_el = driver.find_element(
            By.XPATH,
            "//var[contains(@class,'price')]"
        )
        precio_oferta = parse_price(oferta_el.text)
    except NoSuchElementException:
        pass

    # -------- PRECIO LISTA ("Precio regular :") --------
    try:
        base_el = driver.find_element(
            By.XPATH,
            "//div[contains(@class,'small') and contains(.,'Precio regular')]"
        )
        precio_lista = parse_price(base_el.text)
    except NoSuchElementException:
        precio_lista = None

    # -------- TIPO OFERTA (25%Dto) --------
    try:
        tipo_el = driver.find_element(
            By.XPATH,
            "//b[contains(@class,'text-success')]"
        )
        tipo_oferta = tipo_el.text.strip()
    except NoSuchElementException:
        tipo_oferta = None

    # -------- Regla: si lista == oferta, solo lista --------
    if precio_lista is not None and precio_oferta is not None:
        if abs(precio_lista - precio_oferta) < 0.01:
            precio_oferta = None
            tipo_oferta = None

    return precio_lista, precio_oferta, tipo_oferta


def main(
    input_path="coto_maestro_viernes.xlsx",
    output_path="coto_maestro_viernes_actualizado.xlsx",
    col_url="URLs",
):
    df = pd.read_excel(input_path)

    if col_url not in df.columns:
        raise ValueError(f"No existe la columna '{col_url}' en el Excel")

    for col in ["PRECIO_LISTA", "PRECIO_OFERTA", "TIPO_OFERTA"]:
        if col not in df.columns:
            df[col] = None

    options = webdriver.ChromeOptions()
    # PRUEBA PRIMERO SIN HEADLESS PARA VER LA PÁGINA
    # options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

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
            lista, oferta, tipo = scrape_pagina(driver, url, idx=idx)

            df.at[idx, "PRECIO_LISTA"] = lista
            df.at[idx, "PRECIO_OFERTA"] = oferta
            df.at[idx, "TIPO_OFERTA"] = tipo

            print(f"    -> base={lista} oferta={oferta} tipo={tipo}")
            time.sleep(1)
    finally:
        driver.quit()

    df.to_excel(output_path, index=False)
    print(f"\n✅ Archivo guardado en: {output_path}")


if __name__ == "__main__":
    main()
