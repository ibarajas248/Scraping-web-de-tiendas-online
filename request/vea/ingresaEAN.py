import requests
import pandas as pd

headers = {
    "User-Agent": "Mozilla/5.0"
}

# Pedir EAN al usuario
ean = input("Ingrese código EAN: ").strip()

# Consulta directa por EAN en VEA
url = f"https://www.vea.com.ar/api/catalog_system/pub/products/search/?fq=alternateIds_Ean:{ean}"
response = requests.get(url, headers=headers, timeout=30)

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
                    "precio_lista": offer.get("ListPrice"),
                    "precio_sin_descuento": offer.get("PriceWithoutDiscount"),
                    "stock": offer.get("AvailableQuantity"),
                    "disponible": offer.get("IsAvailable"),
                    "link": producto.get("linkText"),
                })
            except (IndexError, KeyError, TypeError):
                continue

        # Mostrar y guardar
        df = pd.DataFrame(productos)
        print(df)
        df.to_excel("resultado_ean_vea.xlsx", index=False)
        print("💾 Resultado guardado en 'resultado_ean_vea.xlsx'")
