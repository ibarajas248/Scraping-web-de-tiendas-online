import requests, time, sys, re
import pandas as pd
from html import unescape
from bs4 import BeautifulSoup

# ----------------- Config -----------------
TIENDAS_VTEX = [
    ("Carrefour", "www.carrefour.com.ar"),
    ("Vea", "www.vea.com.ar"),
    ("DIA", "diaonline.supermercadosdia.com.ar"),
    ("Jumbo", "www.jumbo.com.ar"),
    ("Disco", "www.disco_falta.com.ar"),
]

# Lista fija de EANs
EANS = [
    "7622201705169","7622201705077","7792129000766","7792129003644","7792129000759","7792129000742",
    "7796373002330","7796373000206","7796373114903","7796373002156","7796373002736","7796373002453",
    "7798224212585","7796373113401","7796373002248","7796373002163","7796373002828","7796373002132",
    "7796373112701","7790580122300","7790580122287","7790411000012","7790411000814","7790411001378",
    "7790411001521","7790411000470","7790411000807","7790411000548","7791675000572"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}
SLEEP = 0.35
TIMEOUT = 25

# ----------------- Utils -----------------
def clean_html(text: str) -> str:
    if not text:
        return ""
    try:
        return BeautifulSoup(unescape(text), "html.parser").get_text(" ", strip=True)
    except Exception:
        return text or ""

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def categories_flat(p: dict):
    cats = p.get("categories") or []
    if not cats:
        return "", ""
    path = cats[-1].strip("/").split("/")
    cat = path[0] if path else ""
    subcat = path[1] if len(path) > 1 else ""
    return cat, subcat

def precios_y_promos(item: dict):
    try:
        seller = first(item.get("sellers") or [])
        offer = (seller or {}).get("commertialOffer") or {}
        list_price = offer.get("ListPrice")
        price = offer.get("Price")
        dh = offer.get("DiscountHighLight") or offer.get("DiscountHighlights") or []
        if isinstance(dh, dict):
            dh = [dh]
        promo_txt = ", ".join([d.get("name") or d.get("Description") or "" for d in dh if isinstance(d, dict)]) or ""
        return list_price, price, promo_txt
    except Exception:
        return None, None, ""

def match_ean_in_items(items, target_ean: str):
    out = []
    for it in items or []:
        ean = it.get("ean") or it.get("Ean") or ""
        out.append((it, (ean == target_ean)))
    return out

def fila_producto(tienda: str, p: dict, it: dict, ean: str):
    cod_interno = p.get("productReference") or it.get("itemId") or ""
    nombre = p.get("productName") or it.get("name") or ""
    marca = p.get("brand") or ""
    fabricante = p.get("Manufacturer") or p.get("manufacturer") or ""
    cat, subcat = categories_flat(p)
    list_price, price, promo = precios_y_promos(it)
    url = f"https://{tienda_domain_map[tienda]}/{p.get('linkText') or ''}/p" if p.get("linkText") else ""
    return {
        "Tienda": tienda,
        "EAN": ean,
        "Código Interno": cod_interno,
        "Nombre Producto": clean_html(nombre),
        "Categoría": cat,
        "Subcategoría": subcat,
        "Marca": marca,
        "Fabricante": fabricante,
        "Precio de Lista": list_price,
        "Precio de Oferta": price,
        "Tipo de Oferta": promo,
        "URL": url
    }

# ----------------- Core: VTEX por EAN -----------------
def buscar_ean_vtex(tienda: str, domain: str, ean: str):
    base = f"https://{domain}/api/catalog_system/pub/products/search"
    queries = [
        {"fq": f"alternateIds_Ean:{ean}"},
        {"fq": f"alternateIds_Ean%3A{ean}"},
        {"fq": f"productReference:{ean}"},
    ]
    resultados = []
    for q in queries:
        try:
            r = requests.get(base, params=q, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code not in (200, 206):
                continue
            data = r.json()
            if not isinstance(data, list) or not data:
                continue
            for p in data:
                items = p.get("items") or []
                matches = [it for it, ok in match_ean_in_items(items, ean) if ok]
                if not matches:
                    matches = items[:1] if items else [{}]
                for it in matches:
                    fila = fila_producto(tienda, p, it, ean)
                    resultados.append(fila)
                    # Mostrar en pantalla lo encontrado
                    print(f"✅ {tienda} | {ean} | {fila['Nombre Producto']} | ${fila['Precio de Oferta']}")
            if resultados:
                break
        except Exception:
            continue
    return resultados

tienda_domain_map = {name: dom for name, dom in TIENDAS_VTEX}

def buscar_en_tiendas(eans):
    rows = []
    for ean in eans:
        ean = re.sub(r"\D", "", str(ean))
        if not ean:
            continue
        print(f"\n🔎 Buscando EAN {ean}...")
        for tienda, dom in TIENDAS_VTEX:
            res = buscar_ean_vtex(tienda, dom, ean)
            rows.extend(res)
            time.sleep(SLEEP)
    return rows

# ----------------- Main -----------------
def main():
    eans = EANS
    print(f"📦 Iniciando búsqueda de {len(eans)} EAN(es) en {len(TIENDAS_VTEX)} tiendas...")
    rows = buscar_en_tiendas(eans)
    if not rows:
        print("⚠️ No se encontraron coincidencias.")
        sys.exit(0)
    df = pd.DataFrame(rows).drop_duplicates()
    cols = ["Tienda","EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Marca","Fabricante",
            "Precio de Lista","Precio de Oferta","Tipo de Oferta","URL"]
    df = df.reindex(columns=cols)
    df.to_excel("busqueda_ean.xlsx", index=False)
    print(f"\n✅ Listo: busqueda_ean.xlsx ({len(df)} fila/s)")

if __name__ == "__main__":
    main()
