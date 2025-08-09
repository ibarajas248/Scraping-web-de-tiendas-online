#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, datetime as dt, json, re, sys, time
from typing import List, Dict, Any, Tuple
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from html import unescape
from bs4 import BeautifulSoup

BASE = "https://api.lacoopeencasa.coop"
API_SECTOR = f"{BASE}/api/contenido/articulos_sector"
API_ATTR = f"{BASE}/api/articulo/atributos"  # ?cod_interno=XXXX

# ----- Limpieza -----
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
def clean_text(v):
    if v is None: return ""
    if not isinstance(v, str): return v
    try:
        v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
    except Exception:
        pass
    return ILLEGAL_XLSX.sub("", v)

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.lacoopeencasa.coop",
        "Referer": "https://www.lacoopeencasa.coop/",
    })
    retries = Retry(
        total=4, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def parse_extra_kv(extras: List[str]) -> Dict[str, str]:
    params = {}
    for kv in extras or []:
        if "=" in kv:
            k, v = kv.split("=", 1)
            params[k.strip()] = v.strip()
    return params

def fetch_sector(tag: str, template_id: int, extra: Dict[str,str], timeout: int = 25) -> List[Dict[str,Any]]:
    s = make_session()
    params = {"tag": tag, "id_template": str(template_id)}
    params.update(extra)
    r = s.get(API_SECTOR, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    if not isinstance(data, dict) or "datos" not in data:
        raise ValueError(f"Estructura inesperada para tag '{tag}'. Respuesta: {str(data)[:250]}")
    if data.get("estado") not in (1, "1", True):
        msg = data.get("mensaje", "Sin mensaje")
        raise RuntimeError(f"API rechazó tag '{tag}': estado={data.get('estado')} mensaje={msg}")

    items = data.get("datos") or []
    norm = []
    for it in items:
        cleaned = {str(k): (clean_text(v) if isinstance(v, str) else v) for k, v in it.items()}
        cleaned["_tag"] = tag  # traza
        norm.append(cleaned)
    return norm

def enrich_with_ean(items: List[Dict[str,Any]], pause: float = 0.05) -> None:
    """Agrega campo 'ean' si se encuentra en atributos del artículo."""
    s = make_session()
    for it in items:
        cod = it.get("cod_interno")
        if not cod: continue
        try:
            r = s.get(API_ATTR, params={"cod_interno": str(cod)}, timeout=20)
            r.raise_for_status()
            data = r.json()
            # Este endpoint suele devolver lista de 'atributos' por clasificaciones
            # Buscamos una clave/valor que parezca EAN
            ean = None
            if isinstance(data, dict) and "datos" in data:
                bloques = data["datos"]
            else:
                bloques = data
            # Recorremos estructura flexible
            def walk(obj):
                if isinstance(obj, dict):
                    for k,v in obj.items():
                        yield k, v
                        yield from walk(v)
                elif isinstance(obj, list):
                    for x in obj:
                        yield from walk(x)
            for k,v in walk(bloques):
                if isinstance(v, str) and re.fullmatch(r"\d{8,14}", v):
                    # heurística: si clave sugiere EAN o GTIN o COD_BARRAS, mejor
                    if isinstance(k, str) and re.search(r"(ean|gtin|barra)", k, re.I):
                        ean = v; break
                    # si no, nos quedamos con el mejor candidato si aún no hay ean
                    if ean is None: ean = v
            if ean:
                it["ean"] = ean
        except Exception:
            pass
        time.sleep(pause)

def to_dataframe(items: List[Dict[str,Any]]) -> pd.DataFrame:
    if not items: return pd.DataFrame()
    df = pd.DataFrame(items)

    preferred = [
        "cod_interno","ean","descripcion",
        "precio","precio_anterior","precio_promo","tipo_precio",
        "precio_sin_impuestos","precio_no_asociado",
        "precio_unitario","unimed_unitario_desc",
        "gramaje","uxc","unimed_desc",
        "estado","existe_promo","vigencia_promo","vigencia_promo_desde",
        "id_categoria","categoria_desc",
        "id_marca","marca_desc",
        "tipo_articulo","id_promocion","id_sub_promocion",
        "cantidad_promo","tipo_promo","tipo_promo_2",
        "icono_promo_especial_inf_izq","icono_promo_especial_inf_der",
        "tipo_carga_promo_2","tipo_sub_promo",
        "cant_base","cant_variacion",
        "descuento_porcentaje_promo","descuento_precio_promo","descuento_precio_bono",
        "editable","orden","imagen","imagenes","_tag"
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    for c in ["cod_interno","id_categoria","id_marca","id_promocion","id_sub_promocion","estado","ean"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def export_excel(df: pd.DataFrame, out_path: str) -> None:
    if df.empty:
        print("⚠️ Sin datos; no genero Excel.")
        return
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name="datos")
        ws = xw.sheets["datos"]
        for i, col in enumerate(df.columns):
            maxlen = int(df[col].astype(str).str.len().quantile(0.95)) if len(df) else 10
            ws.set_column(i, i, min(max(10, maxlen + 2), 60))

def main():
    ap = argparse.ArgumentParser(description="Extrae artículos por uno o varios 'tag' de /articulos_sector.")
    ap.add_argument("--tags", default="slider-articulos", help="Lista de tags separada por comas.")
    ap.add_argument("--template-id", type=int, default=61, help="id_template (por defecto 61).")
    ap.add_argument("--extra", nargs="*", default=None, help="Parámetros extra k=v (p.ej. cantidad=200 inicio=0).")
    ap.add_argument("--fetch-ean", action="store_true", help="Enriquecer con EAN desde /api/articulo/atributos.")
    ap.add_argument("--out", default=None, help="Ruta Excel salida.")
    args = ap.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out or f"lacoope_sector_{ts}.xlsx"

    extra = parse_extra_kv(args.extra)
    tags = [t.strip() for t in args.tags.split(",") if t.strip()]
    print(f"▶ Tags: {tags} | template={args.template_id} | extra={extra}")

    all_items: List[Dict[str,Any]] = []
    seen = set()
    for tag in tags:
        try:
            items = fetch_sector(tag, args.template_id, extra)
            print(f"  • {tag}: {len(items)} items")
            for it in items:
                key = it.get("cod_interno") or (it.get("descripcion"), it.get("precio"))
                if key in seen: continue
                seen.add(key)
                all_items.append(it)
        except Exception as e:
            print(f"  ! {tag}: {e}")

    print(f"✅ Total acumulado (dedup): {len(all_items)}")
    if args.fetch_ean and all_items:
        print("🔎 Enriqueciendo con EAN...")
        enrich_with_ean(all_items)

    df = to_dataframe(all_items)
    export_excel(df, out_xlsx)
    print(f"💾 Excel: {out_xlsx}")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        body = getattr(e.response, "text", "")
        print(f"❌ HTTPError: {e}\n{body[:500]}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
