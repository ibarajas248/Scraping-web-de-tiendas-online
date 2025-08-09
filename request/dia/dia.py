import requests
import pandas as pd
import time

categorias = [
    "almacen",
    "bebidas",
    "frescos",
    "desayuno",
    "limpieza",
    "perfumeria",
    "congelados",
    "bebes-y-ninos",
    "hogar-y-deco",
    "mascotas",
    "almacen/golosinas-y-alfajores",
    "frescos/frutas-y-verduras",
    "electro-hogar"
]

headers = {
    "User-Agent": "Mozilla/5.0"
}

productos_totales = []

inicio = time.time()  # ⏱️ Inicia cronómetro

for categoria in categorias:
    print(f"\n🔎 Explorando categoría: {categoria}")
    productos = []
    offset = 0
    step = 50

    while True:
        url = f"https://diaonline.supermercadosdia.com.ar/api/catalog_system/pub/products/search/{categoria}?_from={offset}&_to={offset + step - 1}"
        response = requests.get(url, headers=headers)

        if response.status_code not in [200, 206]:
            print(f"⚠️ Error {response.status_code} en la categoría {categoria}")
            break

        try:
            data = response.json()
        except Exception as e:
            print(f"❌ Error al parsear JSON en categoría {categoria}: {e}")
            break

        if not data:
            print("✔️ No hay más productos en esta categoría.")
            break

        for producto in data:
            try:
                item = producto["items"][0]
                seller = item["sellers"][0]
                offer = seller["commertialOffer"]

                # Convertir precios a float y redondear a 2 decimales
                precio = round(float(offer.get("Price", 0)), 2)
                precio_sin_desc = round(float(offer.get("PriceWithoutDiscount", 0)), 2)
                precio_list = round(float(offer.get("ListPrice", 0)), 2)

                productos.append({
                    "categoria": categoria,
                    "id": producto.get("productId"),
                    "ean": item.get("ean"),
                    "nombre": producto.get("productName"),
                    "marca": producto.get("brand"),
                    "slug": producto.get("linkText"),
                    "precio": precio,
                    "precio_sin_descuento": precio_sin_desc,
                    "precioList": precio_list,
                    "stock": offer.get("AvailableQuantity"),
                    "disponible": offer.get("IsAvailable")
                })
            except (IndexError, KeyError, TypeError, ValueError):
                continue

        print(f"➡️ Productos {offset} al {offset + step - 1} ({len(data)} nuevos)")
        offset += step
        time.sleep(0.5)  # Respeto al servidor

    productos_totales.extend(productos)
    print(f"✅ Total en '{categoria}': {len(productos)} productos")

# Guardar resultados finales
df = pd.DataFrame(productos_totales)
df.drop_duplicates(keep="last", inplace=True)

# Asegurar formato numérico de 2 decimales en Excel
for col in ["precio", "precio_sin_descuento", "precioList"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

df.to_excel("dia.xlsx", index=False)
print(f"\n📦 Total productos guardados: {len(df)}")

# Mostrar tiempo de ejecución
fin = time.time()
duracion = fin - inicio
print(f"⏱️ Tiempo total de ejecución: {duracion:.2f} segundos")
