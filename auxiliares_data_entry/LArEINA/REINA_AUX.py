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
ARCHIVO_IN = "REINA_MAESTRO_AUX.xlsx"       # <-- tu archivo nuevo
ARCHIVO_OUT = "REINA_MAESTRO_CON_PRECIOS.xlsx"
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
    Recibe algo tipo '$3.669,00' o '$ 3.669,00' y devuelve '3669,00' (string).
    """
    if not texto_raw:
        return None

    # quitar símbolo de $ y espacios
    texto = texto_raw.replace("$", "").strip()

    # dejar solo dígitos, puntos y comas
    texto = re.sub(r"[^0-9,\.]", "", texto)  # p.ej: '3.669,00'

    if not texto:
        return None

    # normalizar miles: "3.669,00" → "3669,00"
    if "," in texto:
        partes = texto.split(",")
        entero = partes[0].replace(".", "")  # quitar puntos de miles
        decimales = partes[1]
        return f"{entero},{decimales}"
    else:
        # sin coma decimal, lo devolvemos tal cual
        return texto


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

        # Esperar el <b> dentro de .DetallPrec .izq
        elem = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.DetallPrec div.izq b")
            )
        )

        texto_precio = elem.text.strip()
        precio_limpio = limpiar_precio(texto_precio)

        print(f"   → texto bruto: {texto_precio}")
        print(f"   → PRECIO_LISTA: {precio_limpio}")

        df.at[i, "PRECIO_LISTA"] = precio_limpio

    except TimeoutException:
        print("   [WARN] No se encontró 'div.DetallPrec div.izq b' a tiempo.")
        df.at[i, "PRECIO_LISTA"] = None
    except NoSuchElementException:
        print("   [WARN] No existe 'div.DetallPrec div.izq b' en el DOM.")
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
