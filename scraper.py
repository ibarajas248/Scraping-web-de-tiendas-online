# scraper.py
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

def scrap_coto(keyword, productos_data):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 15)

    url = f"https://www.cotodigital.com.ar/sitios/cdigi/categoria?_dyncharset=utf-8&Dy=1&Ntt={keyword}"
    driver.get(url)

    pagina = 1

    while True:
        if pagina > 5:
            break
        try:
            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "producto-card")))
        except:
            break

        productos = driver.find_elements(By.CLASS_NAME, "producto-card")
        for producto in productos:
            try:
                a_tag = producto.find_element(By.TAG_NAME, "a")
                link = a_tag.get_attribute("href")
                if not link:
                    continue

                driver.execute_script("window.open(arguments[0]);", link)
                driver.switch_to.window(driver.window_handles[1])

                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h2.title.text-dark")))

                titulo = driver.find_element(By.CSS_SELECTOR, "h2.title.text-dark").text.strip()
                precio = driver.find_element(By.CSS_SELECTOR, "var.price.h3").text.strip()

                ean = ""
                try:
                    ean_text = driver.find_element(By.CLASS_NAME, "rating-wrap").text
                    if "EAN:" in ean_text:
                        ean = ean_text.split("EAN:")[1].strip()
                except:
                    pass

                caracteristicas = driver.find_elements(By.CSS_SELECTOR, "ul.list-check li")
                caracteristicas_texto = [c.text for c in caracteristicas]
                caracteristicas_joined = " | ".join(caracteristicas_texto)

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
                    "URL": link,
                    "tienda":"Coto"
                })

                driver.close()
                driver.switch_to.window(driver.window_handles[0])
            except:
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                continue

        try:
            siguiente = driver.find_element(By.XPATH, "//a[contains(@class, 'page-link') and contains(text(), 'Siguiente')]")
            driver.execute_script("arguments[0].scrollIntoView();", siguiente)
            time.sleep(1)
            siguiente.click()
            pagina += 1
        except:
            break

    driver.quit()


def scrap_dia(keyword, productos_data):
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    import time


    headers = {"User-Agent": "Mozilla/5.0"}

    base_url = f"https://diaonline.supermercadosdia.com.ar/{keyword}?_q={keyword}&map=ft&page={{}}"

    for page in range(1, 10):  # Ajusta el rango según cantidad de páginas deseadas
        print(f"📄 Página {page}")
        res = requests.get(base_url.format(page), headers=headers)
        soup = BeautifulSoup(res.text, "html.parser")
        items = soup.select('a.vtex-product-summary-2-x-clearLink')

        if not items:
            break

        for item in items:
            link_rel = item.get("href")
            if not link_rel:
                continue

            link = f"https://diaonline.supermercadosdia.com.ar{link_rel}"
            print(f"🔗 Visitando: {link}")
            prod_res = requests.get(link, headers=headers)
            prod_soup = BeautifulSoup(prod_res.text, "html.parser")

            # Título
            titulo = prod_soup.select_one('h1.vtex-store-components-3-x-productNameContainer')
            titulo = titulo.get_text(strip=True) if titulo else "N/A"

            # Imagen
            imagen = prod_soup.select_one('img.vtex-store-components-3-x-productImageTag')
            imagen = imagen.get("src") if imagen else "N/A"

            # SKU
            sku = prod_soup.select_one('.vtex-product-identifier-0-x-product-identifier__value')
            sku = sku.get_text(strip=True) if sku else "N/A"

            # Precio
            precio = prod_soup.select_one('.diaio-store-5-x-sellingPriceValue')
            precio = precio.get_text(strip=True) if precio else "N/A"

            # Las columnas son las mismas que las de scrap_coto
            productos_data.append({
                "Título": titulo,
                "Precio": precio,
                "Precio regular": "",  # No disponible en este sitio
                "EAN": sku,
                "Características": "",  # No disponible
                "URL": link,
                "tienda": "DIA"
            })

            time.sleep(1)  # Para evitar bloqueo
