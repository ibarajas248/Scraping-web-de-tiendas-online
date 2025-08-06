import json
import pandas as pd

# Cargar el JSON descargado
with open("coto_raw.json", "r", encoding="utf-8") as f:
    data = json.load(f)

productos = []

def get_attr(attrs, key):
    """Obtiene el primer valor de un atributo si existe"""
    return attrs.get(key, [""])[0] if key in attrs else ""

def buscar_productos(obj):
    """Recorre recursivamente el JSON buscando nodos con 'attributes'"""
    if isinstance(obj, dict):
        if "attributes" in obj:
            attrs = obj["attributes"]
            productos.append({
                "sku": get_attr(attrs, "sku.repositoryId"),              # SKU
                "ean": get_attr(attrs, "product.eanPrincipal"),          # Código EAN (extraído del JSON)
                "nombre": get_attr(attrs, "product.displayName"),
                "marca": get_attr(attrs, "product.brand"),
                "precio": get_attr(attrs, "sku.referencePrice") or get_attr(attrs, "sku.activePrice"),
                "imagen": get_attr(attrs, "product.mediumImage.url") or get_attr(attrs, "product.largeImage.url"),
                "descripcion": get_attr(attrs, "product.description"),
                "url": "https://www.cotodigital.com.ar" + obj.get("detailsAction", {}).get("recordState", "")
            })
        for v in obj.values():
            buscar_productos(v)
    elif isinstance(obj, list):
        for item in obj:
            buscar_productos(item)

# Ejecutar búsqueda
buscar_productos(data)

# Exportar a Excel
df = pd.DataFrame(productos)
df.to_excel("productos_coto.xlsx", index=False)

print(f"✅ Se extrajeron {len(productos)} productos con SKU y EAN. Guardado en productos_coto.xlsx")