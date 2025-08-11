import re
import json
import time
import unicodedata
import requests
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional

# ------------ Config ------------
SITE_BASE = "https://www.cotodigital.com.ar"
URL_BASE = f"{SITE_BASE}/sitios/cdigi/categoria"
NRPP = 50
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}
OUT_XLSX = "coto.xlsx"
OUT_JSON = "coto.json"
SLEEP = 0.35
TIMEOUT = 25
MAX_PAGINAS = 5
RETRIES = 3

# Params “seguros” (derivados del navigationState que compartiste)
BASE_PARAMS = {
    "Dy": "1",
    # "Nf": "product.endDate|GTEQ 1.7546976E12||product.startDate|LTEQ 1.7546976E12",
    # "Nr": "AND(product.sDisp_200:1004,product.language:español,OR(product.siteId:CotoDigital))",
    "Nrpp": str(NRPP),
    "format": "json"
}

# ------------ Helpers ------------
def get_attr(attrs: dict, key: str, default: str = "") -> str:
    """Devuelve el primer valor de un atributo Endeca o default."""
    if not isinstance(attrs, dict):
        return default
    v = attrs.get(key, [""])
    if isinstance(v, list) and v:
        return v[0]
    return default

def parse_json_field(value):
    """Convierte strings JSON a objeto; si no se puede, devuelve el valor original."""
    if value is None:
        return value
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value

_price_clean_re = re.compile(r"[^\d,.\-]")

def parse_price(val) -> float:
    """Normaliza y convierte diferentes formatos de precio a float (NaN si falla)."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return np.nan
    s = _price_clean_re.sub("", s)
    if "," in s and "." in s:
        # miles con punto, decimales con coma -> quita miles y cambia coma por punto
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

def extract_records_tree(root) -> List[dict]:
    """Recorre recursivamente y extrae todos los arrays 'records'."""
    found = []
    def walk(node):
        if isinstance(node, dict):
            if "records" in node and isinstance(node["records"], list):
                found.extend(node["records"])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(root)
    return found

def extract_fabricante(dto_caract: Any) -> str:
    """Busca fabricante en dtoCaracteristicas (lista de {nombre, descripcion})."""
    if not isinstance(dto_caract, list):
        return ""
    keys = {"FABRICANTE", "ELABORADO POR", "FABRICADO POR", "PROVEEDOR"}
    for c in dto_caract:
        n = str(c.get("nombre", "")).strip().upper()
        d = str(c.get("descripcion", "")).strip()
        if n in keys and d:
            return d
    return ""

def best_category(attrs: Dict[str, Any]) -> Tuple[str, str]:
    """Devuelve (categoria, subcategoria) usando jerarquía si existe."""
    anc = attrs.get("allAncestors.displayName", [])
    if isinstance(anc, list) and anc:
        clean = [str(x).strip() for x in anc if str(x).strip().lower() != "cotodigital"]
        if len(clean) >= 2:
            return clean[0], clean[-1]
        if len(clean) == 1:
            return clean[0], ""
    cat = get_attr(attrs, "product.LDEPAR") or get_attr(attrs, "product.category")
    sub = get_attr(attrs, "product.FAMILIA") or get_attr(attrs, "parentCategory.displayName") or ""
    return cat, sub

_slug_nonword = re.compile(r"[^a-zA-Z0-9\s-]")
_slug_spaces = re.compile(r"[\s\-]+")

def slugify(text: str) -> str:
    """Convierte nombre a slug URL-safe (sin acentos ni símbolos)."""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = _slug_nonword.sub("", text)
    return _slug_spaces.sub("-", text.strip().lower())

def build_product_url(attrs: Dict[str, Any], rec: Dict[str, Any]) -> str:
    """
    Construye la URL pública del producto en Coto:
    https://www.cotodigital.com.ar/sitios/cdigi/producto/{slug}/_/R-{record_id}
    Fallback: intenta derivar desde detailsAction.recordState si falta algo.
    """
    name = get_attr(attrs, "product.displayName")
    record_id = get_attr(attrs, "record.id")
    if name and record_id:
        return f"{SITE_BASE}/sitios/cdigi/productos/{slugify(name)}/_/R-{record_id}?Dy=1&idSucursal=200"

    # Fallback: usar recordState y anteponer el prefijo de producto
    record_state = ((rec.get("detailsAction", {}) or {}).get("recordState") or "").split("?", 1)[0]
    if record_state:
        # record_state típico: "/suprema-sin-piel-x-kg-congelado/_/R-00042214-00042214-200"
        path = record_state if record_state.startswith("/") else f"/{record_state}"
        return f"{SITE_BASE}/sitios/cdigi/producto{path}"
    return ""

def parse_record(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(rec, dict) or "attributes" not in rec:
        return None
    attrs = rec["attributes"]

    dto_price  = parse_json_field(get_attr(attrs, "sku.dtoPrice"))
    dto_caract = parse_json_field(get_attr(attrs, "product.dtoCaracteristicas"))
    dto_desc   = parse_json_field(get_attr(attrs, "product.dtoDescuentos"))

    precio_lista = dto_price.get("precioLista") if isinstance(dto_price, dict) else ""
    precio_final = dto_price.get("precio") if isinstance(dto_price, dict) else ""

    # Promos
    promo_tipo = ""
    precio_regular_promo = ""
    precio_descuento = ""
    comentarios_promo = ""
    if isinstance(dto_desc, list) and dto_desc:
        promos_txt = [str(d.get("textoDescuento", "")).strip() for d in dto_desc if d]
        promo_tipo = "; ".join([p for p in promos_txt if p])
        d0 = dto_desc[0] or {}
        precio_regular_promo = str(d0.get("textoPrecioRegular", "")).replace("Precio Contado:", "").strip()
        precio_descuento = d0.get("precioDescuento", "")
        comentarios_promo = str(d0.get("comentarios", "")).strip()

    fabricante = extract_fabricante(dto_caract)
    categoria, subcategoria = best_category(attrs)

    producto = {
        "sku": get_attr(attrs, "sku.repositoryId"),
        "record_id": get_attr(attrs, "record.id"),
        "ean": get_attr(attrs, "product.eanPrincipal"),
        "nombre": get_attr(attrs, "product.displayName"),
        "marca": get_attr(attrs, "product.brand") or get_attr(attrs, "product.MARCA"),
        "fabricante": fabricante,
        "precio_lista": precio_lista,
        "precio_oferta": precio_final,  # usamos el precio final como "oferta" efectiva
        "tipo_oferta": get_attr(attrs, "product.tipoOferta"),
        "promo_tipo": promo_tipo,
        "precio_regular_promo": precio_regular_promo,
        "precio_descuento": precio_descuento,
        "comentarios_promo": comentarios_promo,
        "categoria": categoria,
        "subcategoria": subcategoria,
        "url": build_product_url(attrs, rec),
    }

    # Filtro mínimo (que tenga algo relevante)
    if producto["sku"] or producto["precio_lista"] or producto["precio_oferta"]:
        return producto
    return None

def fetch_json(params: Dict[str, str]) -> Dict[str, Any]:
    last_exc = None
    for _ in range(RETRIES):
        try:
            r = requests.get(URL_BASE, params=params, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            last_exc = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_exc = e
        time.sleep(0.4)
    raise last_exc or RuntimeError("Error de red")

# ------------ Main ------------
def main():
    productos: List[Dict[str, Any]] = []
    seen_keys = set()

    offset = 0
    pagina = 0
    t0 = time.time()
    total = None

    while pagina < MAX_PAGINAS:
        params = dict(BASE_PARAMS)
        params["No"] = str(offset)
        params["Nrpp"] = str(NRPP)

        try:
            data = fetch_json(params)
        except Exception as e:
            print(f"[{offset}] ⚠️ {e}")
            break

        # total de resultados si viene
        if total is None:
            total = data.get("totalNumRecs") or data.get("totalNumRecords") or None

        # parse de records
        nuevos = 0
        for rec in extract_records_tree(data):
            p = parse_record(rec)
            if not p:
                continue
            # clave de dedup
            key = p.get("sku") or p.get("record_id") or (p.get("nombre"), p.get("url"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            productos.append(p)
            nuevos += 1

        acum = len(productos)
        print(f"[{offset}] +{nuevos} nuevos (acum: {acum})")

        # Criterios de corte
        if nuevos == 0:
            print("ℹ️ Página sin nuevos; deteniendo.")
            break
        if total and acum >= int(total):
            print(f"ℹ️ Alcanzado total declarado ({int(total)}).")
            break

        offset += NRPP
        pagina += 1
        time.sleep(SLEEP)

    if not productos:
        print("⚠️ No se descargaron productos.")
        return

    df = pd.DataFrame(productos)

    # Tipos y limpieza
    if "ean" in df.columns:
        df["ean"] = (
            df["ean"]
            .astype(str)
            .replace({"None": np.nan, "nan": np.nan, "": np.nan})
        )

    for c in ["precio_lista", "precio_oferta"]:
        if c in df.columns:
            df[c] = df[c].apply(parse_price)

    # Renombrado a columnas requeridas
    rename_map = {
        "ean": "EAN",
        "sku": "Código Interno",
        "nombre": "Nombre Producto",
        "categoria": "Categoría",
        "subcategoria": "Subcategoría",
        "marca": "Marca",
        "fabricante": "Fabricante",
        "precio_lista": "Precio de Lista",
        "precio_oferta": "Precio de Oferta",
        "tipo_oferta": "Tipo de Oferta",
        "url": "URL",
    }
    df_out = df.rename(columns=rename_map)

    final_cols = [
        "EAN", "Código Interno", "Nombre Producto", "Categoría",
        "Subcategoría", "Marca", "Fabricante", "Precio de Lista",
        "Precio de Oferta", "Tipo de Oferta", "URL"
    ]
    # Asegura columnas ausentes como vacías
    for c in final_cols:
        if c not in df_out.columns:
            df_out[c] = np.nan
    df_out = df_out[final_cols]

    # Exporta
    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as writer:
        sh = "productos"
        df_out.to_excel(writer, index=False, sheet_name=sh)
        wb, ws = writer.book, writer.sheets[sh]
        fmt_price = wb.add_format({"num_format": "#,##0.00"})
        ws.set_column(0, 1, 20)   # EAN / Código Interno
        ws.set_column(2, 2, 48)   # Nombre Producto
        ws.set_column(3, 5, 22)   # Categoría / Subcategoría / Marca
        ws.set_column(6, 6, 26)   # Fabricante
        for col_name in ["Precio de Lista", "Precio de Oferta"]:
            j = df_out.columns.get_loc(col_name)
            ws.set_column(j, j, 14, fmt_price)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(df_out.to_dict("records"), f, ensure_ascii=False, indent=2)

    print(f"\n✅ Se descargaron {len(df_out)} productos")
    if total:
        print(f"📦 Total declarado por la API: {int(total)}")
    print(f"📝 Excel: {OUT_XLSX}")
    print(f"🗂️ JSON:  {OUT_JSON}")
    print(f"⏱️ Tiempo: {time.time() - t0:.2f} s")

if __name__ == "__main__":
    main()
