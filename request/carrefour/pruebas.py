import requests
import pandas as pd
import time
import json
import os
from typing import List, Dict, Any, Optional

# =========================
# Config & helpers generales
# =========================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}
TIMEOUT = 30
PAUSA = 0.2  # para no spamear

def norm(e):
    return (e or "").strip()

def unique_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def parse_input_to_eans(texto: str) -> List[str]:
    """
    Acepta:
      - EANs separados por coma/espacio/nueva línea
      - una ruta a archivo CSV/XLSX con columna 'ean'
    """
    t = norm(texto)
    if not t:
        return []

    # Si parece ruta a archivo -> leemos
    if os.path.exists(t) and os.path.isfile(t):
        ext = os.path.splitext(t)[1].lower()
        if ext in [".csv"]:
            df = pd.read_csv(t)
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(t)
        else:
            raise ValueError("Formato de archivo no soportado. Usa .csv o .xlsx")
        if "ean" not in df.columns:
            raise ValueError("El archivo debe tener una columna llamada 'ean'")
        eans = [str(x).strip() for x in df["ean"].astype(str).tolist() if str(x).strip()]
        return unique_keep_order(eans)

    # Si no es archivo, parseamos como texto
    seps = [",", ";", "\n", "\t", " "]
    for sep in seps:
        texto = texto.replace(sep, ",")
    eans = [x.strip() for x in texto.split(",") if x.strip()]
    return unique_keep_order(eans)

# ==============
# VTEX (VEA/DIA)
# ==============
def vtex_buscar_por_ean(base: str, ean: str) -> List[Dict[str, Any]]:
    """
    Busca por EAN exacto en tiendas VTEX usando fq=alternateIds_Ean:{ean}
    base: e.g. "https://www.vea.com.ar" o "https://diaonline.supermercadosdia.com.ar"
    """
    url = f"{base}/api/catalog_system/pub/products/search/?fq=alternateIds_Ean:{ean}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code not in [200, 206]:
            return []
        data = r.json()
    except Exception:
        return []

    out = []
    for p in data or []:
        try:
            items = p.get("items") or []
            item0 = items[0] if items else {}
            sellers = item0.get("sellers") or []
            seller0 = sellers[0] if sellers else {}
            offer = seller0.get("commertialOffer", {}) or {}
            out.append({
                "tienda": base.replace("https://", "").replace("www.", ""),
                "productId": p.get("productId"),
                "ean": item0.get("ean"),
                "nombre": p.get("productName"),
                "marca": p.get("brand"),
                "precio": offer.get("Price"),
                "precio_lista": offer.get("ListPrice"),
                "precio_sin_descuento": offer.get("PriceWithoutDiscount"),
                "stock": offer.get("AvailableQuantity"),
                "disponible": offer.get("IsAvailable"),
                "url": f"{base}/{p.get('linkText')}/p" if p.get("linkText") else None,
            })
        except Exception:
            continue
    # Filtro por EAN exacto por si acaso
    out = [r for r in out if str(r.get("ean") or "") == str(ean)]
    return out

def buscar_en_vea(eans: List[str]) -> pd.DataFrame:
    rows = []
    for e in eans:
        rows.extend(vtex_buscar_por_ean("https://www.vea.com.ar", e))
        time.sleep(PAUSA)
    return pd.DataFrame(rows)

def buscar_en_dia(eans: List[str]) -> pd.DataFrame:
    rows = []
    for e in eans:
        rows.extend(vtex_buscar_por_ean("https://diaonline.supermercadosdia.com.ar", e))
        time.sleep(PAUSA)
    return pd.DataFrame(rows)

# ==========
# COTO Endeca
# ==========
COTO_BASE = "https://www.cotodigital.com.ar/sitios/cdigi/categoria"

def _get_attr(attrs: dict, key: str) -> str:
    try:
        v = attrs.get(key)
        if isinstance(v, list) and v:
            return str(v[0])
    except Exception:
        pass
    return ""

def _parse_json_field(value: str):
    try:
        return json.loads(value)
    except Exception:
        return None

def _extraer_productos_coto(data: dict) -> List[Dict[str, Any]]:
    productos: List[Dict[str, Any]] = []

    def recorrer(records):
        for rec in records:
            try:
                attrs = rec.get("attributes", {})
                dto_price = _parse_json_field(_get_attr(attrs, "sku.dtoPrice")) or {}
                dto_desc = _parse_json_field(_get_attr(attrs, "product.dtoDescuentos")) or []

                precio_lista = dto_price.get("precioLista")
                precio_final = dto_price.get("precio")

                promos = []
                if isinstance(dto_desc, list):
                    for d in dto_desc:
                        if isinstance(d, dict):
                            txt = d.get("textoDescuento", "")
                            if txt:
                                promos.append(txt)

                url_rel = None
                try:
                    url_rel = rec.get("detailsAction", {}).get("recordState")
                except Exception:
                    pass

                producto = {
                    "sku": _get_attr(attrs, "sku.repositoryId"),
                    "ean": _get_attr(attrs, "product.eanPrincipal"),
                    "nombre": _get_attr(attrs, "product.displayName"),
                    "marca": _get_attr(attrs, "product.brand") or _get_attr(attrs, "product.MARCA"),
                    "precio_lista": precio_lista,
                    "precio_final": precio_final,
                    "precio_referencia": _get_attr(attrs, "sku.referencePrice"),
                    "tipo_oferta": _get_attr(attrs, "product.tipoOferta"),
                    "promo": "; ".join([p for p in promos if p]),
                    "categoria": _get_attr(attrs, "product.category"),
                    "familia": _get_attr(attrs, "product.FAMILIA"),
                    "unidad": _get_attr(attrs, "product.unidades.descUnidad"),
                    "gramaje": _get_attr(attrs, "sku.quantity"),
                    "imagen": _get_attr(attrs, "product.mediumImage.url") or _get_attr(attrs, "product.largeImage.url"),
                    "url": ("https://www.cotodigital.com.ar" + url_rel) if url_rel else None
                }
                if producto["sku"] or producto["precio_final"] or producto["precio_referencia"]:
                    productos.append(producto)
            except Exception:
                pass

            # recursividad
            try:
                if "records" in rec and isinstance(rec["records"], list):
                    recorrer(rec["records"])
            except Exception:
                pass

    # Buscar listas 'records' en todo el JSON (estructura variable)
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

    find_records(data)
    return productos

def coto_buscar_por_ean(ean: str) -> List[Dict[str, Any]]:
    """
    Coto no expone un fq= EAN; usamos su buscador Endeca (Ntt=<ean>) y filtramos por coincidencia exacta.
    """
    params = {
        "Dy": "1",
        "Ntt": ean,
        "No": "0",
        "Nrpp": "100",
        "format": "json"
    }
    try:
        r = requests.get(COTO_BASE, params=params, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception:
        return []

    productos = _extraer_productos_coto(data)
    # filtro estricto por EAN exacto
    productos = [p for p in productos if str(p.get("ean") or "") == str(ean)]
    # homogenizar campos con VTEX
    out = []
    for p in productos:
        out.append({
            "tienda": "cotodigital.com.ar",
            "productId": p.get("sku"),
            "ean": p.get("ean"),
            "nombre": p.get("nombre"),
            "marca": p.get("marca"),
            "precio": p.get("precio_final"),
            "precio_lista": p.get("precio_lista"),
            "precio_sin_descuento": None,
            "stock": None,
            "disponible": None,
            "url": p.get("url")
        })
    return out

def buscar_en_coto(eans: List[str]) -> pd.DataFrame:
    rows = []
    for e in eans:
        rows.extend(coto_buscar_por_ean(e))
        time.sleep(PAUSA)
    return pd.DataFrame(rows)

# ===========
# Orquestador
# ===========
def run_busqueda_cruzada(eans: List[str]) -> Dict[str, pd.DataFrame]:
    print(f"🔎 Buscando {len(eans)} EAN(s) -> {', '.join(eans[:5])}{'…' if len(eans)>5 else ''}\n")

    df_vea = buscar_en_vea(eans)
    print(f"VEA: {len(df_vea)} coincidencia(s)")

    df_dia = buscar_en_dia(eans)
    print(f"DIA: {len(df_dia)} coincidencia(s)")

    df_coto = buscar_en_coto(eans)
    print(f"COTO: {len(df_coto)} coincidencia(s)")

    # Consolidado
    dfs = [df for df in [df_vea, df_dia, df_coto] if not df.empty]
    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        # ordenar por EAN y tienda para lectura
        cols_ord = ["ean", "tienda", "nombre", "marca", "precio", "precio_lista",
                    "precio_sin_descuento", "stock", "disponible", "productId", "url"]
        df_all = df_all[[c for c in cols_ord if c in df_all.columns]]
    else:
        df_all = pd.DataFrame()

    return {
        "VEA": df_vea,
        "DIA": df_dia,
        "COTO": df_coto,
        "ALL": df_all
    }

def guardar_excel(resultados: Dict[str, pd.DataFrame], path: str = "resultado_ean_multi.xlsx"):
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        for nombre, df in resultados.items():
            if df is not None and not df.empty:
                df.to_excel(writer, sheet_name=nombre[:31], index=False)
    print(f"💾 Archivo guardado: {path}")

# =====
# Main
# =====
if __name__ == "__main__":
    entrada = input("👉 Ingresa EAN(s) separados por coma/espacio o la RUTA a un CSV/XLSX con columna 'ean':\n> ")
    eans = parse_input_to_eans(entrada)
    if not eans:
        print("No hay EANs para buscar.")
        raise SystemExit(0)

    resultados = run_busqueda_cruzada(eans)
    total = sum(len(df) for df in resultados.values() if isinstance(df, pd.DataFrame))
    if total == 0:
        print("❌ No hubo coincidencias en ninguna tienda.")
    else:
        guardar_excel(resultados)
        print("\nResumen:")
        for k in ["VEA", "DIA", "COTO"]:
            df = resultados[k]
            print(f"  {k:>4}: {0 if df is None else len(df)} filas")
        if not resultados["ALL"].empty:
            # Vista rápida por consola
            print("\nTop filas consolidadas:")
            print(resultados["ALL"].head(10).to_string(index=False))
