import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Base URL de listado
base_url = "https://diaonline.supermercadosdia.com.ar/almacen?page={}"
#"https://diaonline.supermercadosdia.com.ar/gel?_q=gel&map=ft"
headers = {"User-Agent": "Mozilla/5.0"}

productos = []

for page in range(1, 10):  # Aumenta si hay más páginas
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

        # Título del producto
        titulo = prod_soup.select_one('h1.vtex-store-components-3-x-productNameContainer')
        titulo = titulo.get_text(strip=True) if titulo else "N/A"

        # Imagen principal
        imagen = prod_soup.select_one('img.vtex-store-components-3-x-productImageTag')
        imagen = imagen.get("src") if imagen else "N/A"

        # SKU (referencia)
        sku = prod_soup.select_one(
            '.vtex-product-identifier-0-x-product-identifier__value'
        )
        sku = sku.get_text(strip=True) if sku else "N/A"

        # Precio actual
        precio = prod_soup.select_one(
            '.diaio-store-5-x-sellingPriceValue'
        )
        precio = precio.get_text(strip=True) if precio else "N/A"

        productos.append({
            "titulo": titulo,
            "sku": sku,
            "precio": precio,
            "imagen": imagen,
            "url": link
        })

        time.sleep(1)  # Pausa para evitar bloqueo

# Guardar en Excel
df = pd.DataFrame(productos)
df.to_excel("productos_dia_detalle.xlsx", index=False)
print("✅ Datos guardados en productos_dia_detalle.xlsx")
