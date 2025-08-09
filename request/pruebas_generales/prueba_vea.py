import requests
import pandas as pd
import time

# Lista de categorías a recorrer
CATEGORIAS = [
    "electro",
    "tiempo-libre",
    "bebidas",
    "carnes",
    "almacen",
    "frutas-y-verduras",
    "lacteos",
    "perfumeria",
    "bebes-y-ninos",
    "limpieza",
    "quesos-y-fiambres",
    "congelados",
    "panaderia-y-pasteleria",
    "comidas-preparadas",
    "mascotas",
    "hogar-y-textil",
]

BASE_URL = "https://www.vea.com.ar/api/catalog_system/pub/products/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}
STEP = 50  # productos por página

def fetch_categoria(cat):
    productos = []
    offset = 0
    while True:
        url = f"{BASE_URL}/{cat}?_from={offset}&_to={offset + STEP - 1}"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code not in [200, 206]:
            break
        data = resp.json()
        if not data:
            break
        for p in data:
            try:
                item = p["items"][0]
                seller = item["sellers"][0]
                offer = seller["commertialOffer"]
                productos.append({
                    "categoria": cat,
                    "productId": p.get("productId"),
                    "productName": p.get("productName"),
                    "brand": p.get("brand"),
                    "price": offer.get("Price"),
                    "listPrice": offer.get("ListPrice"),
                    "availableQuantity": offer.get("AvailableQuantity"),
                    "isAvailable": offer.get("IsAvailable"),
                    "ean": item.get("ean"),
                })
            except Exception:
                continue
        offset += STEP
        time.sleep(0.3)  # para no saturar el servidor
    return productos

if __name__ == "__main__":
    all_products = []
    for cat in CATEGORIAS:
        print(f"Descargando {cat} …")
        all_products += fetch_categoria(cat)
    df = pd.DataFrame(all_products).drop_duplicates(subset=["productId"])
    df.to_excel("vea_productos.xlsx", index=False)
    print(f"Productos descargados: {len(df)}. Guardado en vea_productos.xlsx")
