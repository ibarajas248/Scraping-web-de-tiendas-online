from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# Configurar navegador
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 15)

# Ir al sitio
url = "https://www.cotodigital.com.ar/sitios/cdigi/categoria?_dyncharset=utf-8&Dy=1&Ntt="
driver.get(url)

productos_data = []
pagina = 1

while True:

    if pagina > 15:
        print("🔚 Límite de 15 páginas alcanzado.")
        break
    print(f"📄 Procesando página {pagina}...")
    time.sleep(2)

    # Esperar productos
    wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "producto-card")))
    productos = driver.find_elements(By.CLASS_NAME, "producto-card")

    for idx, producto in enumerate(productos):
        try:
            a_tag = producto.find_element(By.TAG_NAME, "a")
            link = a_tag.get_attribute("href")

            if not link:
                continue

            driver.execute_script("window.open(arguments[0]);", link)
            driver.switch_to.window(driver.window_handles[1])

            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h2.title.text-dark")))

            # Extraer título
            titulo = driver.find_element(By.CSS_SELECTOR, "h2.title.text-dark").text.strip()

            # Extraer precio
            precio = driver.find_element(By.CSS_SELECTOR, "var.price.h3").text.strip()

            # Extraer EAN
            ean = ""
            try:
                ean_text = driver.find_element(By.CLASS_NAME, "rating-wrap").text
                if "EAN:" in ean_text:
                    ean = ean_text.split("EAN:")[1].strip()
            except:
                pass

            # Extraer características
            caracteristicas = driver.find_elements(By.CSS_SELECTOR, "ul.list-check li")
            caracteristicas_texto = [c.text for c in caracteristicas]
            caracteristicas_joined = " | ".join(caracteristicas_texto)

            # Extraer Precio regular
            precio_regular = ""
            try:
                bloques = driver.find_elements(By.CSS_SELECTOR, "div.mt-2.small.ng-star-inserted")
                for bloque in bloques:
                    if "Precio regular" in bloque.text:
                        precio_regular = bloque.text.split(":")[-1].strip()
                        break
            except:
                pass

            productos_data.append({
                "Título": titulo,
                "Precio": precio,
                "Precio regular": precio_regular,
                "EAN": ean,
                "Características": caracteristicas_joined,
                "URL": link
            })

            print(f"✅ Producto extraído: {titulo}")

            driver.close()
            driver.switch_to.window(driver.window_handles[0])

        except Exception as e:
            print(f"❌ Error en producto: {e}")
            if len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])

    # Intentar pasar a la siguiente página
    try:
        siguiente = driver.find_element(By.XPATH, "//a[contains(@class, 'page-link') and contains(text(), 'Siguiente')]")
        driver.execute_script("arguments[0].scrollIntoView();", siguiente)
        time.sleep(1)
        siguiente.click()
        pagina += 1
    except:
        print("🚫 No hay más páginas.")
        break

# Guardar resultados
df = pd.DataFrame(productos_data)
df.to_excel("productos_cotodigital.xlsx", index=False)
print("📁 Archivo Excel generado con éxito: productos_cotodigital.xlsx")

# Cerrar navegador
driver.quit()
