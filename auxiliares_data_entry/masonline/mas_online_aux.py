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
ARCHIVO_IN = "mas_online_aux_viernes.xlsx"
ARCHIVO_OUT = "mas_online_con_precios.xlsx"
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
chrome_options.add_argument("--headless=new")  # sin ventana
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
      '$ 1.499,00' o '$ 1.121,25'
    y devuelve 1499.0 / 1121.25 (float).
    """
    if not texto_raw:
        return None

    # quitar símbolo $, espacios y NBSP
    texto = texto_raw.replace("$", "").replace("\xa0", " ").strip()
    # dejar solo dígitos, puntos, comas y espacios
    texto = re.sub(r"[^0-9\.,\s]", "", texto)
    texto = texto.replace(" ", "")

    if not texto:
        return None

    if "," in texto:
        partes = texto.split(",")
        entero = partes[0].replace(".", "")  # '1.499' -> '1499'
        decimales = partes[1]
        numero_str = f"{entero}.{decimales}"  # '1499.00' / '1121.25'
    else:
        # sin coma: punto como miles
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

        # Esperar a que al menos el precio dinámico principal esté presente
        wait.until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "div.valtech-gdn-dynamic-product-1-x-dynamicProductPrice span.valtech-gdn-dynamic-product-1-x-currencyContainer"
            ))
        )

        precio_visible = None
        precio_lista = None
        precio_oferta = None

        # 1) Precio visible (siempre debería existir)
        try:
            elem_visible = driver.find_element(
                By.CSS_SELECTOR,
                "div.valtech-gdn-dynamic-product-1-x-dynamicProductPrice span.valtech-gdn-dynamic-product-1-x-currencyContainer"
            )
            texto_visible = elem_visible.text.strip()  # '$ 1.499,00' o '$ 1.121,25'
            precio_visible = limpiar_precio(texto_visible)
            print(f"   → precio visible: {texto_visible} -> {precio_visible}")
        except NoSuchElementException:
            print("   [INFO] No se encontró precio visible principal.")
            precio_visible = None

        # 2) Precio lista/base, si existe (weighableListPrice)
        try:
            elem_lista = driver.find_element(
                By.CSS_SELECTOR,
                "div.valtech-gdn-dynamic-product-1-x-weighableSavings "
                "span.valtech-gdn-dynamic-product-1-x-weighableListPrice "
                "span.valtech-gdn-dynamic-product-1-x-currencyContainer"
            )
            texto_lista = elem_lista.text.strip()  # '$ 1.725,00'
            precio_lista = limpiar_precio(texto_lista)
            print(f"   → precio LISTA (weighableListPrice): {texto_lista} -> {precio_lista}")
        except NoSuchElementException:
            precio_lista = None
            print("   [INFO] No hay weihableListPrice (sin list price explícito).")

        # ===== LÓGICA =====
        if precio_lista is not None:
            # Hay list price: ese es el PRECIO_LISTA
            # El visible es la oferta/promo
            df.at[i, "PRECIO_LISTA"] = precio_lista
            df.at[i, "PRECIO_OFERTA"] = precio_visible
            print("   → Caso: CON list price. Lista=weighableListPrice, Oferta=visible.")
        else:
            # No hay list price: el visible es el precio base
            df.at[i, "PRECIO_LISTA"] = precio_visible
            df.at[i, "PRECIO_OFERTA"] = None
            print("   → Caso: SIN list price. Lista=visible, sin oferta.")

    except TimeoutException:
        print("   [WARN] No se encontró el bloque de precio a tiempo.")
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
