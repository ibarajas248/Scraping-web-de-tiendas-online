import requests
import pandas as pd

url_base = "https://www.cotodigital.com.ar/sitios/cdigi/categoria"
nrpp = 50
offset = 0
productos = []

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}

ultimo_total = 0
max_paginas = 5 # seguridad para evitar loops infinitos
pagina = 0

while pagina < max_paginas:
    params = {"Dy": "1", "No": str(offset), "Nrpp": str(nrpp), "format": "json"}
    r = requests.get(url_base, params=params, headers=headers)

    print(f"[{offset}] Status: {r.status_code}")
    if r.status_code != 200:
        print("Error HTTP, deteniendo scraping.")
        break

    try:
        data = r.json()
    except:
        print("Respuesta no es JSON, deteniendo scraping.")
        break

    items = data.get("contents", [])
    if not isinstance(items, list) or not items:
        print("No hay más productos.")
        break

    productos.extend(items)
    if len(productos) == ultimo_total:
        print("No se encontraron nuevos productos, deteniendo scraping.")
        break
    ultimo_total = len(productos)

    offset += nrpp
    pagina += 1

if productos:
    df = pd.json_normalize(productos)
    df.to_excel("productos_coto.xlsx", index=False)
    print(f"Se descargaron {len(df)} productos y se guardaron en productos_coto.xlsx")
else:
    print("No se descargaron productos.")
