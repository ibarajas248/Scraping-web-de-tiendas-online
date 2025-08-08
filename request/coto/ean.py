import requests
import pandas as pd
import json

URL_BASE = "https://www.cotodigital.com.ar/sitios/cdigi/categoria"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}

# --- Helpers (mismos que usas) ---
def get_attr(attrs, key):
    return attrs.get(key, [""])[0] if key in attrs else ""

def parse_json_field(value):
    try:
        return json.loads(value)
    except Exception:
        return value

def extraer_productos_coto(data):
    """Recorre el árbol de 'records' y arma una lista simplificada de productos."""
    productos = []

    def recorrer(records):
        for rec in records:
            if "attributes" in rec:
                attrs = rec["attributes"]

                # Campos compuestos
                dto_price = parse_json_field(get_attr(attrs, "sku.dtoPrice"))
                dto_desc = parse_json_field(get_attr(attrs, "product.dtoDescuentos"))

                # Precios
                precio_lista = dto_price.get("precioLista") if isinstance(dto_price, dict) else ""
                precio_final = dto_price.get("precio") if isinstance(dto_price, dict) else ""

                # Promos (texto breve)
                promo_tipo = ""
                if isinstance(dto_desc, list) and len(dto_desc) > 0:
                    promos = [d.get("textoDescuento", "") for d in dto_desc if isinstance(d, dict)]
                    promo_tipo = "; ".join([p for p in promos if p])

                # Producto plano
                producto = {
                    "sku": get_attr(attrs, "sku.repositoryId"),
                    "ean": get_attr(attrs, "product.eanPrincipal"),
                    "nombre": get_attr(attrs, "product.displayName"),
                    "marca": get_attr(attrs, "product.brand") or get_attr(attrs, "product.MARCA"),
                    "precio_lista": precio_lista,
                    "precio_final": precio_final,
                    "precio_referencia": get_attr(attrs, "sku.referencePrice"),
                    "tipo_oferta": get_attr(attrs, "product.tipoOferta"),
                    "promo": promo_tipo,
                    "categoria": get_attr(attrs, "product.category"),
                    "familia": get_attr(attrs, "product.FAMILIA"),
                    "unidad": get_attr(attrs, "product.unidades.descUnidad"),
                    "gramaje": get_attr(attrs, "sku.quantity"),
                    "imagen": get_attr(attrs, "product.mediumImage.url") or get_attr(attrs, "product.largeImage.url"),
                    "url": "https://www.cotodigital.com.ar" + rec.get("detailsAction", {}).get("recordState", "")
                }

                # Guardar si al menos tenemos sku o algún precio
                if producto["sku"] or producto["precio_final"] or producto["precio_referencia"]:
                    productos.append(producto)

            # Bajar recursivamente
            if "records" in rec and isinstance(rec["records"], list):
                recorrer(rec["records"])

    try:
        records = data["contents"][0]["Main"][2]["contents"][0]["records"]
        recorrer(records)
    except KeyError:
        # Estructura puede variar según la página; intenta buscar otros slots
        for slot in ("contents",):
            try:
                stacks = data["contents"][0]
                # fallback: busca cualquier lista 'records' profunda
                def find_records(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k == "records" and isinstance(v, list):
                                recorrer(v)
                            else:
                                find_records(v)
                    elif isinstance(obj, list):
                        for it in obj:
                            find_records(it)
                find_records(stacks)
            except Exception:
                pass

    return productos

# ------------- BÚSQUEDA POR EAN -------------
headers = HEADERS
ean = input("Ingrese código EAN: ").strip()

# Buscar usando el buscador de Coto (Endeca): Ntt=<término>
params = {
    "Dy": "1",
    "Ntt": ean,     # término de búsqueda (usamos el EAN)
    "No": "0",      # offset
    "Nrpp": "50",   # resultados por página
    "format": "json"
}

r = requests.get(URL_BASE, params=params, headers=headers, timeout=30)

if r.status_code != 200:
    print(f"⚠️ Error {r.status_code}")
else:
    try:
        data = r.json()
    except Exception as e:
        print(f"❌ Respuesta no es JSON: {e}")
        data = {}

    productos = extraer_productos_coto(data)

    # Filtra estrictamente por coincidencia exacta de EAN (por si el buscador devuelve ruidos)
    if ean:
        productos = [p for p in productos if p.get("ean") == ean]

    if not productos:
        print("❌ No se encontró producto con ese EAN.")
    else:
        df = pd.DataFrame(productos).drop_duplicates(subset=["sku", "ean", "nombre"])
        print(df)
        df.to_excel("resultado_ean_coto.xlsx", index=False)
        print("💾 Resultado guardado en 'resultado_ean_coto.xlsx'")
