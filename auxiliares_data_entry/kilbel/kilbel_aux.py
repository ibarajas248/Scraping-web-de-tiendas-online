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
ARCHIVO_IN = "kilbel_aux_viernes.xlsx"
ARCHIVO_OUT = "kilbel_con_precios.xlsx"
HOJA = 0  # o "Hoja1"

# ========= CARGAR EXCEL =========
df = pd.read_excel(ARCHIVO_IN, sheet_name=HOJA)

if "URLs" not in df.columns:
    raise Exception("La columna 'URLs' no existe en el Excel.")

if "PRECIO_LISTA" not in df.columns:
    df["PRECIO_LISTA"] = None

if "PRECIO_OFERTA" not in df.columns:
    df["PRECIO_OFERTA"] = None

# NUEVO: columna nombre_en_tienda
if "nombre_en_tienda" not in df.columns:
    df["nombre_en_tienda"] = None

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
    Recibe algo tipo '$ 2.660,00' o '$ 1.020,00'
    y devuelve 2660.0 / 1020.0 (float).
    """
    if not texto_raw:
        return None

    texto = texto_raw.replace("$", "").strip()
    # permitir dígitos, puntos, comas, espacio y nbsp
    texto = re.sub(r"[^0-9\.,\s\xa0]", "", texto)
    texto = texto.replace("\xa0", "").replace(" ", "")

    if not texto:
        return None

    # Formato latino con coma decimal
    if "," in texto:
        partes = texto.split(",")
        entero = partes[0].replace(".", "")  # '2.660' -> '2660'
        decimales = partes[1]
        numero_str = f"{entero}.{decimales}"  # '2660.00'
    else:
        # Sin coma decimal: asumimos punto como miles
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
        df.at[i, "nombre_en_tienda"] = None
        continue

    try:
        driver.get(url)

        # NUEVO: esperar el H1 del nombre (si está)
        try:
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "h1.titulo_producto.principal")
                )
            )
        except TimeoutException:
            pass

        # NUEVO: extraer nombre_en_tienda
        try:
            h1 = driver.find_element(By.CSS_SELECTOR, "h1.titulo_producto.principal")
            nombre_en_tienda = h1.text.strip()
            df.at[i, "nombre_en_tienda"] = nombre_en_tienda
            print(f"   → nombre_en_tienda: {nombre_en_tienda}")
        except NoSuchElementException:
            df.at[i, "nombre_en_tienda"] = None
            print("   [INFO] No se encontró h1.titulo_producto.principal")
        
        # Esperar al menos el precio principal (aux1)
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "span.precio.aux1")
            )
        )

        # ---- PRECIO OFERTA (span.precio.aux1) ----
        precio_oferta = None
        try:
            elem_oferta = driver.find_element(By.CSS_SELECTOR, "span.precio.aux1")
            texto_oferta = elem_oferta.text.strip()  # '$ 1.020,00'
            precio_oferta = limpiar_precio(texto_oferta)
            print(f"   → texto oferta: {texto_oferta} -> {precio_oferta}")
        except NoSuchElementException:
            print("   [INFO] No se encontró span.precio.aux1")

        # ---- PRECIO ANTERIOR / BASE (div.precio.anterior.codigo) ----
        precio_lista = None
        try:
            elem_lista = driver.find_element(By.CSS_SELECTOR, "div.precio.anterior.codigo")
            texto_lista = elem_lista.text.strip()  # '$ 2.660,00'
            precio_lista = limpiar_precio(texto_lista)
            print(f"   → texto lista (anterior): {texto_lista} -> {precio_lista}")

            # Regla: si existe este DIV, es el precio base
            # y el aux1 es el precio oferta
            df.at[i, "PRECIO_LISTA"] = precio_lista
            df.at[i, "PRECIO_OFERTA"] = precio_oferta

        except NoSuchElementException:
            # Regla: si NO existe precio anterior, el aux1 es el precio base
            print("   [INFO] No hay precio anterior, aux1 se toma como lista.")
            df.at[i, "PRECIO_LISTA"] = precio_oferta
            df.at[i, "PRECIO_OFERTA"] = None

    except TimeoutException:
        print("   [WARN] No se cargó el precio principal a tiempo.")
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None
        # nombre ya intentado arriba; si quieres forzar None:
        # df.at[i, "nombre_en_tienda"] = None
    except Exception as e:
        print(f"   [ERROR] {e}")
        df.at[i, "PRECIO_LISTA"] = None
        df.at[i, "PRECIO_OFERTA"] = None
        # nombre ya intentado arriba; si quieres forzar None:
        # df.at[i, "nombre_en_tienda"] = None

    time.sleep(0.5)

# ========= CERRAR NAVEGADOR Y GUARDAR =========
driver.quit()
df.to_excel(ARCHIVO_OUT, index=False)
print(f"\n✔ Listo. Archivo guardado como: {ARCHIVO_OUT}")
