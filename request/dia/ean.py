import requests
import pandas as pd

headers = {
    "User-Agent": "Mozilla/5.0"
}

# Pedir EAN al usuario
ean = input("Ingrese código EAN: ").strip()

# Hacer la consulta directa
url = f"https://diaonline.supermercadosdia.com.ar/api/catalog_system/pub/products/search/?fq=alternateIds_Ean:{ean}"
response = requests.get(url, headers=headers)

if response.status_code not in [200, 206]:
    print(f"⚠️ Error {response.status_code}")
else:
    try:
        data = response.json()
    except Exception as e:
        print(f"❌ Error parseando JSON: {e}")
        data = []

    if not data:
        print("❌ No se encontró producto con ese EAN.")
    else:
        productos = []
        for producto in data:
            try:
                item = producto["items"][0]
                seller = item["sellers"][0]
                offer = seller["commertialOffer"]

                productos.append({
                    "id": producto.get("productId"),
                    "ean": item.get("ean"),
                    "nombre": producto.get("productName"),
                    "marca": producto.get("brand"),
                    "precio": offer.get("Price"),
                    "stock": offer.get("AvailableQuantity"),
                    "disponible": offer.get("IsAvailable")
                })
            except (IndexError, KeyError, TypeError):
                continue

        # Mostrar y guardar
        df = pd.DataFrame(productos)
        print(df)
        df.to_excel("resultado_ean.xlsx", index=False)
        print("💾 Resultado guardado en 'resultado_ean.xlsx'")
