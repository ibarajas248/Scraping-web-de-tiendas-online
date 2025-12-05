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
ARCHIVO_IN = "anonima_aux_viernes.xlsx"
ARCHIVO_OUT = "anonima_con_precios.xlsx"
HOJA = 0  # o "Hoja1"

# ========= CARGAR EXCEL =========
df = pd.read_excel(ARCHIVO_IN, sheet_name=HOJA)

if "URLs" not in df.columns:
    raise Exception("La columna 'URLs' no existe en el Excel.")

if "PRECIO_LISTA" not in df.columns:
    df["PRECIO_LISTA"] = None

if "PRECIO_OFERTA" not in df.columns:
    df["PRECIO_OFERTA"] = None

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
    Recibe cosas tipo:
      '$ 4.850,00' o '$ 6.800'
    y devuelve 4850.0 / 6800.0 (float).
    """
    if not texto_raw:
        return None

    # eliminar símbolo $, espacios y NBSP
    texto = texto_raw.replace("$", "").replace("\xa0", " ").strip()
    # dejar solo dígitos, puntos, comas y espacios
    texto = re.sub(r"[^0-9\.,\s]", "", texto)
    texto = texto.replace(" ", "")

    if not texto:
        return None

    # Formato latino: coma decimal
    if "," in texto:
        partes = texto.split(",")
        entero = partes[0].replace(".", "")  # quita puntos de miles
        decimales = partes[1]
        numero_str = f"{entero}.{decimales}"
    else:
        # sin coma, asumimos que el punto es miles
        numero_str = texto.replace(".", "")

    try:
        return float(numero_str)
    except ValueError:
        return None


# ========= RECORRER TODAS LAS URLs =========
total = len(df)
for i, url in enumerate(df["URLs"]):
    print(f"[{i+1}/{total}] URL: {url}")

    if not isinstance(url, str) or not url.strip():
        print("   → URL vacía, se dejan precios en None")
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None
        continue

    try:
        driver.get(url)

        # Esperar que al menos haya algún bloque de precio visible
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.precio, div.precio.plus")
            )
        )

        precio_lista = None
        precio_oferta = None

        # 1) ¿Hay precio PLUS (descuento)?
        try:
            elem_plus = driver.find_element(By.CSS_SELECTOR, "div.precio.plus > span")
            texto_plus = elem_plus.text.strip()  # ej: "$ 6.450,00"
            precio_plus = limpiar_precio(texto_plus)
            print(f"   → precio PLUS: {texto_plus} -> {precio_plus}")
        except NoSuchElementException:
            precio_plus = None

        # 2) ¿Hay precio normal (sin plus)?
        try:
            # Ojo: esto captura solo el caso sin "plus"
            elem_normal = driver.find_element(By.CSS_SELECTOR, "div.precio:not(.plus) > span")
            texto_normal = elem_normal.text.strip()  # "$ 4.850,00"
            precio_normal = limpiar_precio(texto_normal)
            print(f"   → precio NORMAL: {texto_normal} -> {precio_normal}")
        except NoSuchElementException:
            precio_normal = None

        # 3) ¿Hay precio anterior tachado?
        try:
            elem_ant = driver.find_element(By.CSS_SELECTOR, "div.precio-anterior span.tachado")
            texto_ant = elem_ant.text.strip()  # "$ 6.800"
            precio_anterior = limpiar_precio(texto_ant)
            print(f"   → precio ANTERIOR (tachado): {texto_ant} -> {precio_anterior}")
        except NoSuchElementException:
            precio_anterior = None

        # ===== LÓGICA SEGÚN TUS REGLAS =====
        if precio_anterior is not None:
            # Hay precio base tachado -> este es PRECIO_LISTA
            # y el visible (plus o normal) es PRECIO_OFERTA
            precio_lista = precio_anterior
            precio_oferta = precio_plus if precio_plus is not None else precio_normal
            print("   → Caso: CON precio-anterior (lista) y oferta visible")
        else:
            # No hay precio anterior
            if precio_plus is not None:
                # Precio PLUS, pero sin "anterior" -> lo tomamos como lista
                precio_lista = precio_plus
                precio_oferta = None
                print("   → Caso: SOLO PLUS, sin precio-anterior")
            else:
                # Caso simple: solo precio normal
                precio_lista = precio_normal
                precio_oferta = None
                print("   → Caso: SOLO precio normal")

        df.at[i, "PRECIO_LISTA"] = precio_lista
        df.at[i, "PRECIO_OFERTA"] = precio_oferta

    except TimeoutException:
        print("   [WARN] No se encontró ningún bloque de precio a tiempo.")
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None
    except Exception as e:
        print(f"   [ERROR] {e}")
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None

    time.sleep(0.5)

# ========= CERRAR NAVEGADOR Y GUARDAR =========
driver.quit()
df.to_excel(ARCHIVO_OUT, index=False)
print(f"\n✔ Listo. Archivo guardado como: {ARCHIVO_OUT}")
