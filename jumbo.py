from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd

# Lista para almacenar los datos (por ahora vacía)
productos = []

# Configurar navegador en modo headless
options = Options()
#options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--window-size=1920x1080")

# Iniciar driver y wait
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 10)

base_url = "https://www.jumbo.com.ar/electro?page={}"

for page in range(1, 2):  # Cambia el rango si quieres más páginas
    print(f"📄 Página {page}")
    driver.get(base_url.format(page))

    # Esperar a que aparezcan los productos
    try:
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'div.jumboargentinaio-cmedia-integration-cencosud-1-x-galleryItem')))
    except:
        print("🚫 No se encontraron productos.")
        break

    # Parsear con BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")
    items = soup.select('div.jumboargentinaio-cmedia-integration-cencosud-1-x-galleryItem')

    for item in items:
        a_tag = item.find('a', class_='vtex-product-summary-2-x-clearLink')
        if not a_tag or not a_tag.get("href"):
            continue

        link = f"https://www.jumbo.com.ar{a_tag['href']}"
        print(f"🔗 Visitando: {link}")
        driver.get(link)

        # Esperar a que cargue el producto
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'h1.vtex-store-components-3-x-productNameContainer')))
        except:
            print("⚠️ Producto no cargó bien.")
            continue


driver.quit()