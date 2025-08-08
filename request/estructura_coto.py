import requests
import pandas as pd
import json
import time  # ⏱️ Para medir tiempo de ejecución

url_base = "https://www.cotodigital.com.ar/sitios/cdigi/categoria"
nrpp = 50
offset = 0
productos = []

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}

def get_attr(attrs, key):
    return attrs.get(key, [""])[0] if key in attrs else ""

def parse_json_field(value):
    try:
        return json.loads(value)
    except:
        return value

def extraer_productos_coto(data):
    productos_encontrados = []

    def recorrer(records):
        for rec in records:
            if "attributes" in rec:
                attrs = rec["attributes"]

                dto_price = parse_json_field(get_attr(attrs, "sku.dtoPrice"))
                dto_caract = parse_json_field(get_attr(attrs, "product.dtoCaracteristicas"))
                dto_desc = parse_json_field(get_attr(attrs, "product.dtoDescuentos"))

                precio_lista = dto_price.get("precioLista") if isinstance(dto_price, dict) else ""
                precio_final = dto_price.get("precio") if isinstance(dto_price, dict) else ""

                caracteristicas = []
                if isinstance(dto_caract, list):
                    caracteristicas = [f"{c['nombre']}: {c['descripcion']}" for c in dto_caract]

                promo_tipo = ""
                precio_regular_promo = ""
                precio_descuento = ""
                comentarios_promo = ""

                if isinstance(dto_desc, list) and len(dto_desc) > 0:
                    promos = [d.get("textoDescuento", "") for d in dto_desc]
                    promo_tipo = "; ".join(promos)
                    d0 = dto_desc[0]
                    precio_regular_promo = d0.get("textoPrecioRegular", "").replace("Precio Contado:", "").strip()
                    precio_descuento = d0.get("precioDescuento", "")
                    comentarios_promo = d0.get("comentarios", "").strip()

                producto = {
                    "sku": get_attr(attrs, "sku.repositoryId"),
                    "ean": get_attr(attrs, "product.eanPrincipal"),
                    "nombre": get_attr(attrs, "product.displayName"),
                    "marca": get_attr(attrs, "product.brand") or get_attr(attrs, "product.MARCA"),
                    "precio_referencia": get_attr(attrs, "sku.referencePrice"),
                    "precio_lista": precio_lista,
                    "precio_final": precio_final,
                    "tipo_oferta": get_attr(attrs, "product.tipoOferta"),
                    "promo_tipo": promo_tipo,
                    "precio_regular_promo": precio_regular_promo,
                    "precio_descuento": precio_descuento,
                    "comentarios_promo": comentarios_promo,
                    "sin_tacc": get_attr(attrs, "product.SIN TACC"),
                    "categoria": get_attr(attrs, "product.category"),
                    "familia": get_attr(attrs, "product.FAMILIA"),
                    "unidad": get_attr(attrs, "product.unidades.descUnidad"),
                    "gramaje": get_attr(attrs, "sku.quantity"),
                    "descripcion": get_attr(attrs, "product.description"),
                    "descripcion_larga": get_attr(attrs, "product.longDescription"),
                    "palabras_clave": get_attr(attrs, "product.keywords"),
                    "caracteristicas": "; ".join(caracteristicas),
                    "imagen": get_attr(attrs, "product.mediumImage.url") or get_attr(attrs, "product.largeImage.url"),
                    "url": "https://www.cotodigital.com.ar" + rec.get("detailsAction", {}).get("recordState", "")
                }

                if producto["sku"] or producto["precio_final"] or producto["precio_referencia"]:
                    productos_encontrados.append(producto)

            if "records" in rec and isinstance(rec["records"], list):
                recorrer(rec["records"])

    try:
        records = data["contents"][0]["Main"][2]["contents"][0]["records"]
        recorrer(records)
    except KeyError:
        print("⚠️ No se encontraron productos en la estructura esperada")

    return productos_encontrados

# --- Scraping con medición de tiempo ---
start_time = time.time()

ultimo_total = 0
max_paginas = 5000
pagina = 0

while pagina < max_paginas:
    params = {"Dy": "1", "No": str(offset), "Nrpp": str(nrpp), "format": "json"}
    r = requests.get(url_base, params=params, headers=headers)

    print(f"[{offset}] Status: {r.status_code}")
    if r.status_code != 200:
        break

    try:
        data = r.json()
    except:
        print("⚠️ Respuesta no es JSON, deteniendo scraping.")
        break

    nuevos = extraer_productos_coto(data)
    productos.extend(nuevos)

    if len(productos) == ultimo_total:
        print("⚠️ No se encontraron nuevos productos, deteniendo scraping.")
        break
    ultimo_total = len(productos)

    offset += nrpp
    pagina += 1

# --- Guardar archivos y mostrar resumen ---
if productos:
    df = pd.DataFrame(productos).drop_duplicates(subset=["sku", "nombre"])
    df.to_excel("coto.xlsx", index=False)
    with open("coto.json", "w", encoding="utf-8") as f:
        json.dump(productos, f, indent=2, ensure_ascii=False)

    tiempo_total = round(time.time() - start_time, 2)
    print(f"✅ Se descargaron {len(df)} productos únicos en {pagina} páginas.")
    print(f"⏱️ Tiempo total de ejecución: {tiempo_total} segundos.")
else:
    print("⚠️ No se descargaron productos.")
