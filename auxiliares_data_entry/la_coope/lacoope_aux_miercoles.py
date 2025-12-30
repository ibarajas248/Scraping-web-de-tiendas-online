#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import re
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ========= CONFIG =========
ARCHIVO_IN = "la_coope_aux_miercoles.xlsx"
ARCHIVO_OUT = "coope_con_precios_miercoles.xlsx"
HOJA = 0  # o "Hoja1"

# ========= CARGAR EXCEL =========
df = pd.read_excel(ARCHIVO_IN, sheet_name=HOJA)

if "URLs" not in df.columns:
    raise Exception("La columna 'URLs' no existe en el Excel.")

if "PRECIO_LISTA" not in df.columns:
    df["PRECIO_LISTA"] = None

if "PRECIO_OFERTA" not in df.columns:
    df["PRECIO_OFERTA"] = None

# ✅ NUEVA COLUMNA
if "nombre_en_tienda" not in df.columns:
    df["nombre_en_tienda"] = None

# ✅ NUEVA COLUMNA (SKU)
if "sku" not in df.columns:
    df["sku"] = None

# ========= CONFIGURAR SELENIUM (CHROME HEADLESS) =========
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

wait = WebDriverWait(driver, 10)


def limpiar_precio(texto_raw: str):
    """
    Recibe '$2.162,00' y devuelve 2162.0 (float)
    """
    if not texto_raw:
        return None

    # quitar símbolo $, NBSP y espacios
    texto = texto_raw.replace("$", "").replace("\xa0", " ").strip()
    # dejar solo dígitos, puntos, comas y espacios intermedios
    texto = re.sub(r"[^0-9\.,\s]", "", texto)
    texto = texto.replace(" ", "")

    if not texto:
        return None

    # Caso con coma decimal: '2.162,00'
    if "," in texto:
        partes = texto.split(",")
        entero = partes[0].replace(".", "")  # '2.162' -> '2162'
        decimales = partes[1]
        numero_str = f"{entero}.{decimales}"
    else:
        # Sin coma decimal: tratamos puntos como miles
        numero_str = texto.replace(".", "")

    try:
        return float(numero_str)
    except Exception:
        return None


# ========= RECORRER TODAS LAS URLs =========
total = len(df)
for i, url in enumerate(df["URLs"]):
    print(f"[{i+1}/{total}] URL: {url}")

    if not isinstance(url, str) or not url.strip():
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None
        df.at[i, "nombre_en_tienda"] = None
        df.at[i, "sku"] = None
        continue

    try:
        driver.get(url)

        # ✅ Buscar NOMBRE EN TIENDA (h1.articulo-detalle-titulo)
        nombre_en_tienda = None
        try:
            h1_el = wait.until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    "h1.articulo-detalle-titulo"
                ))
            )
            nombre_en_tienda = h1_el.text.strip()
        except TimeoutException:
            nombre_en_tienda = None
        except NoSuchElementException:
            nombre_en_tienda = None

        # ✅ Buscar SKU (Código del producto) -> div.articulo-codigo span
        sku = None
        try:
            sku_el = wait.until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    "div.articulo-codigo span"
                ))
            )
            sku = sku_el.text.strip()
        except TimeoutException:
            sku = None
        except NoSuchElementException:
            sku = None

        # -------- Buscar precio TACHADO (base) --------
        precio_lista_raw = None
        try:
            tachado_el = driver.find_element(
                By.CSS_SELECTOR,
                "span.valor.precio-tachado"
            )
            precio_lista_raw = tachado_el.text.strip()
        except NoSuchElementException:
            precio_lista_raw = None

        # -------- Buscar precio ACTUAL (div.precio.precio-detalle) --------
        elem = wait.until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "div.precio.precio-detalle"
            ))
        )
        precio_actual_raw = elem.text.strip()   # "$2.162,00"

        # -------- Aplicar regla --------
        if precio_lista_raw:
            # Hay precio tachado -> ese es LISTA, el actual es OFERTA
            precio_lista = limpiar_precio(precio_lista_raw)
            precio_oferta = limpiar_precio(precio_actual_raw)
        else:
            # No hay tachado -> solo precio normal
            precio_lista = limpiar_precio(precio_actual_raw)
            precio_oferta = None

        print(f"   → nombre_en_tienda: {nombre_en_tienda}")
        print(f"   → sku: {sku}")
        print(f"   → bruto_lista: {precio_lista_raw}  bruto_oferta: {precio_actual_raw}")
        print(f"   → PRECIO_LISTA: {precio_lista}  PRECIO_OFERTA: {precio_oferta}")

        df.at[i, "nombre_en_tienda"] = nombre_en_tienda
        df.at[i, "sku"] = sku
        df.at[i, "PRECIO_LISTA"] = precio_lista
        df.at[i, "PRECIO_OFERTA"] = precio_oferta

    except TimeoutException:
        print("   [WARN] No se encontró el precio base.")
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None
        # nombre_en_tienda podría haber quedado, pero por consistencia:
        if pd.isna(df.at[i, "nombre_en_tienda"]):
            df.at[i, "nombre_en_tienda"] = None
        if pd.isna(df.at[i, "sku"]):
            df.at[i, "sku"] = None
    except NoSuchElementException:
        print("   [WARN] No existe el nodo del precio.")
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None
        if pd.isna(df.at[i, "nombre_en_tienda"]):
            df.at[i, "nombre_en_tienda"] = None
        if pd.isna(df.at[i, "sku"]):
            df.at[i, "sku"] = None
    except Exception as e:
        print(f"   [ERROR] {e}")
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None
        if pd.isna(df.at[i, "nombre_en_tienda"]):
            df.at[i, "nombre_en_tienda"] = None
        if pd.isna(df.at[i, "sku"]):
            df.at[i, "sku"] = None

    time.sleep(0.5)

# ========= OPCIONAL: FORZAR TIPO NUMÉRICO =========
df["PRECIO_LISTA"] = pd.to_numeric(df["PRECIO_LISTA"], errors="coerce")
df["PRECIO_OFERTA"] = pd.to_numeric(df["PRECIO_OFERTA"], errors="coerce")

# ========= CERRAR NAVEGADOR Y GUARDAR =========
driver.quit()
df.to_excel(ARCHIVO_OUT, index=False)
print(f"\n✔ Listo. Archivo guardado como: {ARCHIVO_OUT}")
