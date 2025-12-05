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
ARCHIVO_IN = "gallega_aux_viernes.xlsx"
ARCHIVO_OUT = "gallega_con_precios.xlsx"
HOJA = 0  # o "Hoja1"

# ========= CARGAR EXCEL =========
df = pd.read_excel(ARCHIVO_IN, sheet_name=HOJA)

if "URLs" not in df.columns:
    raise Exception("La columna 'URLs' no existe en el Excel.")

if "PRECIO_LISTA" not in df.columns:
    df["PRECIO_LISTA"] = None

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
    Recibe '$1.785,00' y devuelve 1785.0 (float)
    """
    if not texto_raw:
        return None

    texto = texto_raw.replace("$", "").replace("\xa0", " ").strip()
    texto = re.sub(r"[^0-9\.,\s]", "", texto)
    texto = texto.replace(" ", "")

    if not texto:
        return None

    if "," in texto:
        partes = texto.split(",")
        entero = partes[0].replace(".", "")  # '1.785' -> '1785'
        decimales = partes[1]
        numero_str = f"{entero}.{decimales}"
    else:
        numero_str = texto.replace(".", "")

    try:
        return float(numero_str)
    except:
        return None

# ========= RECORRER TODAS LAS URLs =========
total = len(df)
for i, url in enumerate(df["URLs"]):
    print(f"[{i+1}/{total}] URL: {url}")

    if not isinstance(url, str) or not url.strip():
        df.at[i, "PRECIO_LISTA"] = None
        continue

    try:
        driver.get(url)

        elem = wait.until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "div.DetallPrec div.izq"
            ))
        )

        texto_precio = elem.text.strip()   # "$1.785,00"
        precio_limpio = limpiar_precio(texto_precio)

        print(f"   → texto bruto: {texto_precio}")
        print(f"   → PRECIO_LISTA: {precio_limpio}")

        df.at[i, "PRECIO_LISTA"] = precio_limpio

    except TimeoutException:
        print("   [WARN] No se encontró el precio base.")
        df.at[i, "PRECIO_LISTA"] = None
    except NoSuchElementException:
        print("   [WARN] No existe el nodo del precio.")
        df.at[i, "PRECIO_LISTA"] = None
    except Exception as e:
        print(f"   [ERROR] {e}")
        df.at[i, "PRECIO_LISTA"] = None

    time.sleep(0.5)

# ========= CERRAR NAVEGADOR Y GUARDAR =========
driver.quit()
df.to_excel(ARCHIVO_OUT, index=False)
print(f"\n✔ Listo. Archivo guardado como: {ARCHIVO_OUT}")
