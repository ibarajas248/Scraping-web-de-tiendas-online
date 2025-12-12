#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import re
import shutil
import tempfile
from urllib.parse import urlparse, parse_qs
from typing import Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# CONFIG DEL SITIO / RUTAS
# =========================
BASE_URL   = "https://elabastecedor.com.ar/"
LOGIN_URL  = "https://elabastecedor.com.ar/login"

ARCHIVO_IN  = "abastecedor_aux_viernes.xlsx"
ARCHIVO_OUT = "abastecedor_con_precios.xlsx"
HOJA        = 0  # o "Hoja1"

# Credenciales (ya no son necesarias si login será manual)
EMAIL    = os.getenv("ELABASTECEDOR_EMAIL", "mauro@factory-blue.com")
PASSWORD = os.getenv("ELABASTECEDOR_PASSWORD", "Compras2025")

# CapSolver (ya no es necesario si login será manual)
CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "")

# Timeouts
PAGE_WAIT = 60

# =========================
# Selenium Driver
# =========================
def make_driver(headless: bool = False) -> (webdriver.Chrome, str):
    user_data_dir = tempfile.mkdtemp(prefix="chrome_profile_")
    opts = Options()

    # ✅ MOSTRAR NAVEGADOR
    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument(f"--user-data-dir={user_data_dir}")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--window-size=1920,1080")

    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    chromedriver_path = os.getenv("CHROMEDRIVER_PATH")
    if chromedriver_path and os.path.exists(chromedriver_path):
        service = Service(chromedriver_path)
    else:
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
    })
    return driver, user_data_dir

# =========================
# Limpieza de precio
# =========================
def clean_price(text: str) -> Optional[float]:
    if not text:
        return None

    s = text.replace("\xa0", " ")
    m = re.search(r"\$\s*([0-9\.\,\s]+)", s)
    if m:
        s = m.group(1)
    else:
        s = text

    s = s.strip()
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return None

    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        frac = s.split(",")[-1]
        if len(frac) in (2, 3):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "." in s:
        parts = s.split(".")
        if len(parts) > 2 or len(parts[-1]) not in (2, 3):
            s = s.replace(".", "")

    try:
        return round(float(s), 2)
    except Exception:
        return None

# =========================
# Extraer precio de una página
# =========================
def extract_precio_lista_from_page(html: str) -> Optional[float]:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        ".pricing-meta .current-price",
        ".product-item-details .price",
        ".product-info-main .price-box .price",
        ".product-info-price .price-final_price .price",
        ".precio",
        ".price",
        "[class*='price']"
    ]

    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if txt:
                val = clean_price(txt)
                if val is not None:
                    return val

    text = soup.get_text(" ", strip=True)
    m = re.search(r"\$\s*[0-9\.\,\s]+", text)
    if m:
        return clean_price(m.group(0))

    return None

# =========================
# Login MANUAL
# =========================
def do_login_manual(driver: webdriver.Chrome, wait: WebDriverWait):
    driver.get(LOGIN_URL)

    # Espera algo de la pantalla de login
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))

    print("\n==============================")
    print("🔐 INICIO DE SESIÓN MANUAL")
    print("1) Se abrió el navegador en /login")
    print("2) Inicia sesión tú (y resuelve captcha si aparece)")
    print("3) Cuando termines y ya estés logueado, vuelve a esta consola")
    input("✅ Presiona ENTER para comenzar el scraping...\n")

# =========================
# MAIN
# =========================
def main():
    df = pd.read_excel(ARCHIVO_IN, sheet_name=HOJA)

    if "URLs" not in df.columns:
        raise Exception("La columna 'URLs' no existe en el Excel.")

    if "PRECIO_LISTA" not in df.columns:
        df["PRECIO_LISTA"] = None

    # ✅ Navegador visible
    driver, profile_dir = make_driver(headless=False)
    wait = WebDriverWait(driver, PAGE_WAIT)

    try:
        # ✅ Login manual
        do_login_manual(driver, wait)

        total = len(df)
        for i, url in enumerate(df["URLs"]):
            print(f"[{i+1}/{total}] URL: {url}")

            if not isinstance(url, str) or not url.strip():
                print("   → URL vacía, PRECIO_LISTA = None")
                df.at[i, "PRECIO_LISTA"] = None
                continue

            try:
                driver.get(url)
                wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
                time.sleep(1.0)

                html = driver.page_source
                precio_lista = extract_precio_lista_from_page(html)
                print(f"   → PRECIO_LISTA: {precio_lista}")
                df.at[i, "PRECIO_LISTA"] = precio_lista

            except Exception as e:
                print(f"   [ERROR] {e}")
                df.at[i, "PRECIO_LISTA"] = None

            time.sleep(0.5)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception:
            pass

    df.to_excel(ARCHIVO_OUT, index=False)
    print(f"\n✔ Listo. Archivo guardado como: {ARCHIVO_OUT}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado por el usuario.")
        sys.exit(1)
