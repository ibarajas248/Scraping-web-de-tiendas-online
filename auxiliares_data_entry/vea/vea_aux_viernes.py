#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import argparse
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import requests
from lxml import html

# Selenium fallback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# =========================
# Utils
# =========================
PRICE_RE = re.compile(r"\$\s*([0-9\.\,]+)")

def normalize_price_to_varchar(raw: Optional[str]) -> Optional[str]:
    """
    Convierte "$1.837,5" -> "1837.50"
    Convierte "$2.450"   -> "2450.00"
    Devuelve string (VARCHAR) o None
    """
    if not raw:
        return None
    s = raw.strip()

    m = PRICE_RE.search(s)
    if not m:
        # quizás ya viene sin $
        s2 = s
    else:
        s2 = m.group(1)

    # s2 puede ser "1.837,5" o "2.450" o "2300"
    # regla: '.' miles, ',' decimal
    s2 = s2.replace(".", "")
    if "," in s2:
        whole, dec = s2.split(",", 1)
        dec = re.sub(r"\D", "", dec)[:2]
        if len(dec) == 0:
            dec = "00"
        elif len(dec) == 1:
            dec = dec + "0"
        else:
            dec = dec[:2]
        return f"{whole}.{dec}"
    else:
        whole = re.sub(r"\D", "", s2)
        if not whole:
            return None
        return f"{whole}.00"


def fetch_html_requests(url: str, timeout: int = 25) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.7",
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code >= 400:
            return None
        return r.text
    except Exception:
        return None


def parse_prices_from_html(page_html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    tree = html.fromstring(page_html)

    print("   🔎 Parseando HTML (requests)")

    # Precio actual
    current_nodes = tree.xpath("//*[@id='priceContainer']//text()")
    current_text = " ".join([t.strip() for t in current_nodes if t and t.strip()])
    current_price = normalize_price_to_varchar(current_text)

    print(f"      Precio actual detectado: {current_text} -> {current_price}")

    candidates = []

    for el in tree.xpath("//div|//span"):
        if el.get("id") == "priceContainer":
            continue
        txt = " ".join([t.strip() for t in el.xpath('.//text()') if t and t.strip()])
        if "$" in txt:
            m = PRICE_RE.search(txt)
            if m:
                val = normalize_price_to_varchar(m.group(0))
                if val and val != current_price:
                    candidates.append(val)

    print(f"      Precios candidatos (posible lista): {candidates}")

    strike_nodes = tree.xpath(
        "//*[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'list') "
        "or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'strike') "
        "or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'selling') "
        "or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'was')]//text()"
    )
    strike_text = " ".join([t.strip() for t in strike_nodes if t and t.strip()])
    strike_price = normalize_price_to_varchar(strike_text)

    if strike_price:
        print(f"      Precio lista por clase: {strike_text} -> {strike_price}")

    list_price = strike_price if strike_price else (candidates[0] if candidates else None)

    if current_price and list_price:
        print("      🟢 OFERTA detectada")
        return list_price, current_price, "OFERTA"
    else:
        print("      ⚪ SIN oferta")
        return current_price, None, None



# =========================
# Selenium fallback
# =========================
def build_driver() -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1365,900")

    # acelerar: bloquear imágenes
    prefs = {"profile.managed_default_content_settings.images": 2}
    opts.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)


def parse_prices_with_selenium(url: str, wait_sec: int = 20) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    print("   🧠 Usando Selenium")
    driver = build_driver()
    try:
        driver.get(url)
        WebDriverWait(driver, wait_sec).until(
            EC.presence_of_element_located((By.ID, "priceContainer"))
        )

        current_el = driver.find_element(By.ID, "priceContainer")
        current_price = normalize_price_to_varchar(current_el.text)
        print(f"      Precio actual (selenium): {current_el.text} -> {current_price}")

        all_price_like = driver.find_elements(By.XPATH, "//*[contains(text(),'$')]")
        candidates = []
        for el in all_price_like:
            val = normalize_price_to_varchar(el.text)
            if val and val != current_price:
                candidates.append(val)

        print(f"      Precios candidatos (selenium): {candidates}")

        list_price = candidates[0] if candidates else None

        if current_price and list_price:
            print("      🟢 OFERTA detectada (selenium)")
            return list_price, current_price, "OFERTA"
        else:
            print("      ⚪ SIN oferta (selenium)")
            return current_price, None, None

    finally:
        driver.quit()



# =========================
# Main
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="input_xlsx", default="vea_aux_viernes.xlsx",
                    help="Excel de entrada (debe tener columna 'URLs').")
    ap.add_argument("--out", dest="output_xlsx", default=None,
                    help="Excel de salida.")
    ap.add_argument("--sleep", dest="sleep_sec", type=float, default=0.4,
                    help="Delay entre requests.")
    ap.add_argument("--selenium", action="store_true",
                    help="Forzar Selenium en todos (más lento, más robusto).")
    args = ap.parse_args()

    out = args.output_xlsx
    if not out:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = f"vea_con_precios_{ts}.xlsx"

    df = pd.read_excel(args.input_xlsx)

    if "URLs" not in df.columns:
        raise Exception("No existe la columna 'URLs' en el Excel de entrada.")

    for col in ["precio_lista", "precio_oferta", "tipo_oferta"]:
        if col not in df.columns:
            df[col] = None

    for i, row in df.iterrows():
        url = str(row["URLs"]).strip() if pd.notna(row["URLs"]) else ""
        if not url or url.lower() == "nan":
            continue

        print("\n" + "=" * 80)
        print(f"[{i + 1}/{len(df)}] Procesando URL:")
        print(url)


        precio_lista = precio_oferta = tipo_oferta = None

        try:
            if args.selenium:
                precio_lista, precio_oferta, tipo_oferta = parse_prices_with_selenium(url)
            else:
                page_html = fetch_html_requests(url)
                if page_html:
                    precio_lista, precio_oferta, tipo_oferta = parse_prices_from_html(page_html)
                else:
                    # fallback selenium si requests falló
                    precio_lista, precio_oferta, tipo_oferta = parse_prices_with_selenium(url)

                # si requests no encontró precio, fallback selenium
                if not precio_lista:
                    precio_lista, precio_oferta, tipo_oferta = parse_prices_with_selenium(url)

        except Exception as e:
            print(f"  !! ERROR: {e}")

        df.at[i, "precio_lista"] = precio_lista
        df.at[i, "precio_oferta"] = precio_oferta
        df.at[i, "tipo_oferta"] = tipo_oferta

        time.sleep(args.sleep_sec)

    df.to_excel(out, index=False)
    print(f"\nOK -> {out}")


if __name__ == "__main__":
    main()
