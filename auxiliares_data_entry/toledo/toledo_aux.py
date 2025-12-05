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
ARCHIVO_IN = "maestro_toledo_aux.xlsx"
ARCHIVO_OUT = "maestro_toledo_con_precios.xlsx"
HOJA = 0  # o "Hoja1"

# ========= CARGAR EXCEL =========
df = pd.read_excel(ARCHIVO_IN, sheet_name=HOJA)

if "URLs" not in df.columns:
    raise Exception("La columna 'URLs' no existe en el Excel.")

if "PRECIO_LISTA" not in df.columns:
    df["PRECIO_LISTA"] = None

# ========= CONFIGURAR SELENIUM (CHROME HEADLESS) =========
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless=new")  # correr sin abrir ventana
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

wait = WebDriverWait(driver, 10)  # 10s de espera máx por precio


def limpiar_precio(texto_raw: str):
    """
    Recibe algo tipo '$ 2.400,00' y devuelve 2400.00 (float).
    """
    if not texto_raw:
        return None

    # quitar símbolo de $ y espacios
    texto = texto_raw.replace("$", "").strip()

    # dejar solo dígitos, puntos y comas (quita espacios, nbsp, etc.)
    texto = re.sub(r"[^0-9,\.]", "", texto)  # p.ej: '2.400,00'

    if not texto:
        return None

    # Caso con coma decimal (formato latino)
    if "," in texto:
        partes = texto.split(",")
        entero = partes[0].replace(".", "")  # quitar puntos de miles: '2.400' -> '2400'
        decimales = partes[1]
        numero_str = f"{entero}.{decimales}"  # '2400.00'
    else:
        # Sin coma decimal: tratamos punto como separador de miles y lo quitamos
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
        print("   → URL vacía, se deja PRECIO_LISTA = None")
        df.at[i, "PRECIO_LISTA"] = None
        continue

    try:
        driver.get(url)

        # Esperar al contenedor del precio base
        elem = wait.until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "span.vtex-store-components-3-x-currencyContainer--product-price"
            ))
        )

        # Esto ya devuelve todo junto: "$ 2.400,00"
        texto_precio = elem.text.strip()
        precio_limpio = limpiar_precio(texto_precio)

        print(f"   → texto bruto: {texto_precio}")
        print(f"   → PRECIO_LISTA (float): {precio_limpio}")

        df.at[i, "PRECIO_LISTA"] = precio_limpio

    except TimeoutException:
        print("   [WARN] No se encontró el contenedor de precio base a tiempo.")
        df.at[i, "PRECIO_LISTA"] = None
    except NoSuchElementException:
        print("   [WARN] No existe el contenedor de precio base en el DOM.")
        df.at[i, "PRECIO_LISTA"] = None
    except Exception as e:
        print(f"   [ERROR] {e}")
        df.at[i, "PRECIO_LISTA"] = None

    # pequeña pausa para no saturar el sitio
    time.sleep(0.5)

# ========= CERRAR NAVEGADOR Y GUARDAR =========
driver.quit()
df.to_excel(ARCHIVO_OUT, index=False)
print(f"\n✔ Listo. Archivo guardado como: {ARCHIVO_OUT}")
