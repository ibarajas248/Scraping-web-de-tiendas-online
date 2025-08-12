#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= Config =================
CATEGORIAS = [
    "almacen","bebidas","frescos","desayuno","limpieza","perfumeria",
    "congelados","bebes-y-ninos","hogar-y-deco","mascotas",
    "almacen/golosinas-y-alfajores","frescos/frutas-y-verduras","electro-hogar",
]

BASE_API = "https://diaonline.supermercadosdia.com.ar/api/catalog_system/pub/products/search"
BASE_WEB = "https://diaonline.supermercadosdia.com.ar"
STEP = 50
SLEEP_OK = 0.4
TIMEOUT = 25
MAX_EMPTY = 2
HEADERS = {"User-Agent": "Mozilla/5.0"}

COLS = [
    "EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Marca",
    "Fabricante","Precio de Lista","Precio de Oferta","Tipo de Oferta","URL"
]

# -------------- sesión con retries --------------
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4, backoff_factor=0.6,
        status_forcelist=(429,500,502,503,504), allowed_methods=("GET",),
        raise_on_status=False,
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update(HEADERS)
    return s

def split_cat(path_or_slug: str) -> tuple[str,str]:
    """Extrae categoría y subcategoría del primer path disponible."""
    if not path_or_slug:
        return "", ""
    parts = [p for p in path_or_slug.strip("/").split("/") if p]
    if not parts: return "", ""
    # Normaliza a 'Título'
    fix = lambda s: s.replace("-", " ").strip().title()
    cat = fix(parts[0])
    sub = fix(parts[1]) if len(parts) > 1 else ""
    return cat, sub

def first_category_parts(prod: dict, fallback_slug: str) -> tuple[str,str]:
    cats = prod.get("categories") or []
    if cats and isinstance(cats, list) and isinstance(cats[0], str):
        return split_cat(cats[0])
    return split_cat(fallback_slug)

def tipo_de_oferta(offer: dict, list_price: float, price: float) -> str:
    # Si VTEX trae destacados de descuento, úsalos
    try:
        dh = offer.get("DiscountHighLight") or []
        if dh and isinstance(dh, list):
            name = (dh[0].get("Name") or "").strip()
            if name:
                return name
    except Exception:
        pass
    # Fallback simple
    return "Descuento" if price < list_price else "Precio regular"

# -------------- scraping --------------
def scrape_categoria(session: requests.Session, slug_categoria: str) -> list[dict]:
    print(f"\n🔎 Explorando categoría: {slug_categoria}")
    out = []; offset = 0; empty_streak = 0

    while True:
        url = f"{BASE_API}/{slug_categoria}?_from={offset}&_to={offset+STEP-1}"
        try:
            r = session.get(url, timeout=TIMEOUT)
        except Exception as e:
            print(f"⚠️ Red: {e}"); break

        if r.status_code not in (200,206):
            print(f"⚠️ HTTP {r.status_code} — corto '{slug_categoria}'"); break

        try:
            data = r.json()
        except Exception as e:
            print(f"❌ JSON err en '{slug_categoria}': {e}"); break

        if not data:
            empty_streak += 1
            print(f"✔️ página vacía {empty_streak}/{MAX_EMPTY} en {offset}-{offset+STEP-1}")
            if empty_streak >= MAX_EMPTY: break
            offset += STEP; time.sleep(SLEEP_OK); continue

        empty_streak = 0
        nuevos = 0

        for prod in data:
            try:
                item = prod["items"][0]
                seller = item["sellers"][0]
                offer  = seller["commertialOffer"]

                list_price = round(float(offer.get("ListPrice") or 0), 2)
                price      = round(float(offer.get("Price") or 0), 2)

                # campos
                ean = item.get("ean")
                codigo_interno = item.get("itemId") or prod.get("productId")
                nombre = prod.get("productName")
                marca  = prod.get("brand")
                fabricante = prod.get("manufacturer") or ""  # puede venir vacío
                cat, sub = first_category_parts(prod, slug_categoria)
                slug = prod.get("linkText") or ""
                url_prod = f"{BASE_WEB}/{slug}/p" if slug else ""

                oferta_tipo = tipo_de_oferta(offer, list_price, price)

                out.append({
                    "EAN": ean,
                    "Código Interno": codigo_interno,
                    "Nombre Producto": nombre,
                    "Categoría": cat,
                    "Subcategoría": sub,
                    "Marca": marca,
                    "Fabricante": fabricante,
                    "Precio de Lista": list_price,
                    "Precio de Oferta": price,
                    "Tipo de Oferta": oferta_tipo,
                    "URL": url_prod,
                })
                nuevos += 1
            except (IndexError, KeyError, TypeError, ValueError):
                continue

        print(f"➡️ {offset}-{offset+STEP-1}: +{nuevos} productos")
        offset += STEP
        time.sleep(SLEEP_OK)

    print(f"✅ Total en '{slug_categoria}': {len(out)}")
    return out

def main():
    inicio = time.time()
    s = build_session()
    all_rows: list[dict] = []
    for cat in CATEGORIAS:
        all_rows.extend(scrape_categoria(s, cat))

    df = pd.DataFrame(all_rows).drop_duplicates(keep="last")

    # asegurar columnas y tipos
    for c in COLS:
        if c not in df.columns:
            df[c] = pd.NA

    df["EAN"] = df["EAN"].astype("string")  # evita perder ceros
    for c in ["Precio de Lista","Precio de Oferta"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    # ordenar columnas
    df = df[COLS]

    # exportar (con formato básico)
    out_xlsx = "dia_formato.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="productos")
        wb = writer.book; ws = writer.sheets["productos"]
        money = wb.add_format({"num_format": "0.00"})
        text  = wb.add_format({"num_format": "@"})
        col = {n:i for i,n in enumerate(COLS)}
        ws.set_column(col["EAN"], col["EAN"], 18, text)
        ws.set_column(col["Nombre Producto"], col["Nombre Producto"], 52)
        ws.set_column(col["Categoría"], col["Categoría"], 20)
        ws.set_column(col["Subcategoría"], col["Subcategoría"], 24)
        ws.set_column(col["Marca"], col["Marca"], 18)
        ws.set_column(col["Fabricante"], col["Fabricante"], 18)
        ws.set_column(col["Precio de Lista"], col["Precio de Lista"], 14, money)
        ws.set_column(col["Precio de Oferta"], col["Precio de Oferta"], 14, money)
        ws.set_column(col["URL"], col["URL"], 42)

    print(f"\n📦 Guardado {len(df)} filas en {out_xlsx}")
    print(f"⏱️ Duración: {time.time() - inicio:.2f}s")

if __name__ == "__main__":
    main()
