from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# ✅ Configurar navegador headless (sin ventana visible)
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 15)

# ✅ URL de inicio
url = "https://www.cotodigital.com.ar/sitios/cdigi/categoria?_dyncharset=utf-8&Dy=1&Ntt="
driver.get(url)

productos_data = []
pagina = 1
MAX_PAGINAS = 15  # ✅ límite real

while True:
    if pagina > MAX_PAGINAS:
        print("🔚 Límite de páginas alcanzado.")
        break

    print(f"📄 Procesando página {pagina}...")

    # ✅ Esperar que los productos estén presentes
    try:
        wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "producto-card")))
    except:
        print("🚫 No se encontraron productos en la página.")
        break

    # ✅ Obtener lista de productos
    productos = driver.find_elements(By.CLASS_NAME, "producto-card")

    for idx in range(len(productos)):
        try:
            # ✅ Re-localizar producto para evitar stale element
            productos = driver.find_elements(By.CLASS_NAME, "producto-card")
            producto = productos[idx]

            # ✅ Obtener link del producto
            a_tag = producto.find_element(By.TAG_NAME, "a")
            link = a_tag.get_attribute("href")
            if not link:
                continue

            # ✅ Abrir producto en nueva pestaña
            driver.execute_script("window.open(arguments[0]);", link)
            driver.switch_to.window(driver.window_handles[1])

            # ✅ Esperar que cargue el título
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h2.title.text-dark")))

            # ✅ Extraer datos
            titulo = driver.find_element(By.CSS_SELECTOR, "h2.title.text-dark").text.strip()
            precio = driver.find_element(By.CSS_SELECTOR, "var.price.h3").text.strip()

            # ✅ EAN
            ean = ""
            try:
                ean_text = driver.find_element(By.CLASS_NAME, "rating-wrap").text
                if "EAN:" in ean_text:
                    ean = ean_text.split("EAN:")[1].strip()
            except:
                pass

            # ✅ Características
            caracteristicas = driver.find_elements(By.CSS_SELECTOR, "ul.list-check li")
            caracteristicas_texto = [c.text for c in caracteristicas]
            caracteristicas_joined = " | ".join(caracteristicas_texto)

            # ✅ Precio regular
            precio_regular = ""
            try:
                bloques = driver.find_elements(By.CSS_SELECTOR, "div.mt-2.small.ng-star-inserted")
                for bloque in bloques:
                    if "Precio regular" in bloque.text:
                        precio_regular = bloque.text.split(":")[-1].strip()
                        break
            except:
                pass

            # ✅ Guardar en lista
            productos_data.append({
                "Título": titulo,
                "Precio": precio,
                "Precio regular": precio_regular,
                "EAN": ean,
                "Características": caracteristicas_joined,
                "URL": link
            })

            print(f"✅ Producto extraído: {titulo}")

        except Exception as e:
            print(f"❌ Error en producto: {e}")

        finally:
            # ✅ Cerrar pestaña de producto y volver a principal
            if len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])

    # ✅ Ir a la siguiente página
    try:
        siguiente = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//a[contains(@class, 'page-link') and contains(text(), 'Siguiente')]")))
        driver.execute_script("arguments[0].scrollIntoView();", siguiente)
        time.sleep(1)
        siguiente.click()
        pagina += 1
    except:
        print("🚫 No hay más páginas.")
        break

# ✅ Guardar resultados en Excel
df = pd.DataFrame(productos_data)
df.to_excel("productos_cotodigital.xlsx", index=False)
print("📁 Archivo Excel generado con éxito: productos_cotodigital.xlsx")

driver.quit()
