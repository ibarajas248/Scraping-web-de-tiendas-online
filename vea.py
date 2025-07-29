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
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--window-size=1920x1080")

# Iniciar driver y wait
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 10)

base_url = "https://www.vea.com.ar/bebidas?page={}"

for page in range(1, 2):
    print(f"📄 Página {page}")
    driver.get(base_url.format(page))

    # Esperar a que cargue al menos un producto
    try:
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'div.veaargentina-cmedia-integration-cencosud-1-x-galleryItem')))
    except:
        print("🚫 No se encontraron productos en esta página.")
        break

    # Parsear con BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")
    items = soup.select('div.veaargentina-cmedia-integration-cencosud-1-x-galleryItem')

    for item in items:
        a_tag = item.find('a', class_='vtex-product-summary-2-x-clearLink')
        if not a_tag or not a_tag.get("href"):
            continue

        link = f"https://www.vea.com.ar{a_tag['href']}"
        print(f"🔗 Visitando: {link}")
        driver.get(link)

        # Esperar a que cargue algo típico del producto, como el nombre
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h1')))
        except:
            print("⚠️ Producto no cargó bien.")
            continue
        soupPaginaDos = BeautifulSoup(driver.page_source, "html.parser")
        titulo_element = soupPaginaDos.select_one(
            'h1.vtex-store-components-3-x-productNameContainer span.vtex-store-components-3-x-productBrand')
        titulo = titulo_element.text.strip() if titulo_element else "Título no encontrado"

        print("🛒 Título del producto:", titulo)

        skuElement =soupPaginaDos.select_one('span.vtex-product-identifier-0-x-product-identifier__value')
        sku =skuElement.text.strip() if skuElement else "SKU no encontrado"
        print ("SKU= "+sku)
        precio_element = soup.select_one('div#priceContainer')
        precio = precio_element.text.strip() if precio_element else "Precio no encontrado"

        print("💰 Precio:", precio)

        precioRegularElement=soupPaginaDos.select_one('div.veaargentina-store-theme-2t-mVsKNpKjmCAEM_AMCQH')
        precioRegular =precioRegularElement.text.strip() if precioRegularElement else "precio regular no encontrado "

        print ("precio regular: "+precioRegular)
        imagen_element = soupPaginaDos.select_one('img.vtex-store-components-3-x-productImageTag--main')
        imagen_url = imagen_element['src'] if imagen_element else "Imagen no encontrada"

        print("🖼️ Imagen:", imagen_url)
        urlproducto = driver.current_url
        print ("URL producto: "+urlproducto)

        productos.append({
            "titulo":titulo,
            "SKU": sku,
            "precio": precio,
            "precio_Regular":precioRegular,
            "Url Imagen ":imagen_url,
            "url": urlproducto

        })


        # Aquí puedes extraer los datos del producto (en el futuro)
        # Por ahora solo navegamos

driver.quit()

# Guardar archivo vacío por ahora
df = pd.DataFrame(productos)
df.to_excel("vea.xlsx", index=False)
print("✅ Archivo generado vea.xlsx")
