#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VTEX — Scraper catálogo completo + Upload MySQL (robusto)

- Descubre categorías con /api/catalog_system/pub/category/tree/{depth}.
- Pagina con _from/_to (STEP=50), corta por MAX_EMPTY_PAGES vacías seguidas.
- Barre canales/sucursales (cookie sc=1..N) para evitar "catálogo vacío".
- Permite filtrar por EAN (archivo XLSX/CSV) o por lista de categorías.
- Normaliza por SKU (variantes). Extrae: EAN (si lo da VTEX), Código Interno,
  Nombre, Categoría, Subcategoría, Marca, Fabricante, Precio Lista, Precio Oferta,
  Tipo de Oferta, URL.
- Graba CSV por tienda y sube a MySQL en tablas:
    - tiendas(codigo) UNIQUE
    - productos(ean) UNIQUE NULLABLE
    - producto_tienda(tienda_id, sku_tienda) UNIQUE
    - historico_precios(tienda_id, producto_tienda_id, capturado_en) [no UNIQUE por defecto]
  Además soporta mapeo de EAN:
    * Tabla ean_map(tienda_id, sku_tienda, ean) UNIQUE
    * O archivo mapeo EAN (CSV/XLSX) con columnas: sku_tienda, ean

Uso:
  python vtex_scraper.py \
      --base https://www.carrefour.com.ar \
      --tienda-codigo carrefour_ar \
      --tienda-nombre "Carrefour AR" \
      --depth 5 --sc-max 8 \
      --out-dir ./out \
      --mysql yes \
      --map-ean-db yes \
      --ean-file ./ean_list.xlsx --solo-ean yes \
      --categorias "Bebidas;Despensa"

Autor: InteligenceBlue – Retail Analytics
"""

from __future__ import annotations
import os, sys, re, time, json, argparse, unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Tu helper de conexión MySQL (ya existente en tu repo) ---
# Debe devolver mysql.connector.connect(...)

import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# =========================
# Parámetros/constantes
# =========================
STEP = 50                 # VTEX: _from=0, _to=49; 50-99; etc.
TIMEOUT = 25
SLEEP_OK = 0.25           # entre páginas
SLEEP_SC = 0.5            # entre cambios de sc
MAX_EMPTY_PAGES = 3       # corta tras N páginas vacías seguidas
MAX_PAGES_API = 2000      # guardrail anti-loop
TREE_DEFAULT_DEPTH = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

# =========================
# Utilidades
# =========================
def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

def norm_text(x: Optional[str]) -> str:
    if x is None:
        return ""
    x = str(x)
    x = ILLEGAL_XLSX.sub("", x).strip()
    return x

def slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()
    return s

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

# =========================
# VTEX helpers
# =========================
def fetch_category_tree(session: requests.Session, base: str, depth: int) -> List[Dict[str, Any]]:
    url = f"{base.rstrip('/')}/api/catalog_system/pub/category/tree/{depth}"
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def iter_leaf_categories(tree: List[Dict[str, Any]]) -> Iterable[Tuple[str, str]]:
    """
    Recorre el árbol y devuelve tuplas (id, name) de hojas.
    """
    stack = list(tree)
    while stack:
        node = stack.pop()
        children = node.get("children") or []
        if children:
            stack.extend(children)
        else:
            yield str(node.get("id", "")), norm_text(node.get("name", ""))

def vtex_search_page(
    session: requests.Session,
    base: str,
    cat_id: str,
    _from: int,
    step: int,
    fq_extra: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Busca una página de productos de la categoría cat_id.
    """
    # VTEX permite map/fq, pero con cat_id suele alcanzar con:
    # .../products/search?fq=C:{cat_id}&_from=...&_to=...
    params = [f"fq=C:{cat_id}", f"_from={_from}", f"_to={_from + step - 1}"]
    if fq_extra:
        params.append(fq_extra)
    url = f"{base.rstrip('/')}/api/catalog_system/pub/products/search?{'&'.join(params)}"
    r = session.get(url, timeout=TIMEOUT)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return []

def extract_offer_info(item: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Devuelve (precio_lista, precio_oferta, tipo_oferta) en string.
    VTEX suele exponer en 'items/sellers/commertialOffer'.
    """
    try:
        it = (item.get("items") or [])[0]
        sellers = it.get("sellers") or []
        offer = (sellers[0].get("commertialOffer") if sellers else {}) or {}
        pl = offer.get("ListPrice")
        po = offer.get("Price")
        # Tipo de oferta es muy dependiente de la tienda; dejamos placeholder:
        tipo = "Oferta" if (po is not None and pl is not None and po < pl) else None
        return (
            (f"{pl:.2f}" if isinstance(pl, (int, float)) else (str(pl) if pl is not None else None)),
            (f"{po:.2f}" if isinstance(po, (int, float)) else (str(po) if po is not None else None)),
            tipo,
        )
    except Exception:
        return None, None, None

def extract_sku_url(item: Dict[str, Any], base: str) -> str:
    # VTEX trae linkText a nivel producto
    link_text = norm_text(item.get("linkText") or item.get("link"))
    if not link_text:
        return ""
    return f"{base.rstrip('/')}/{link_text}/p".replace("//p", "/p")

def flatten_product(
    item: Dict[str, Any],
    base: str,
    cat_name_path: str
) -> Dict[str, Any]:
    """
    Normaliza un producto VTEX a las columnas del reporte.
    - Si hay múltiples SKUs, se devuelve una fila por primer SKU (puedes ampliar a todas las variantes si lo prefieres).
    """
    brand = norm_text(item.get("brand"))
    product_name = norm_text(item.get("productName"))
    manufacturer = norm_text(item.get("Manufacturer") or item.get("manufacturerName") or item.get("manufacturer"))
    ean = None
    sku_internal = None

    items = item.get("items") or []
    if items:
        sku = items[0]
        sku_internal = norm_text(sku.get("itemId") or sku.get("referenceId", [{}])[0].get("Value"))
        # EAN suele venir en referenceId con Key 'EAN' o en "ean"
        # probar múltiples ubicaciones:
        # 1) sku["ean"]
        ean = norm_text(sku.get("ean"))
        # 2) sku["referenceId"][{"Key":"EAN","Value":...}]
        if not ean:
            for ref in sku.get("referenceId") or []:
                if str(ref.get("Key", "")).upper() in ("EAN", "GTIN", "BARCODE"):
                    ean = norm_text(ref.get("Value"))
                    break

    price_list, price_offer, tipo_offer = extract_offer_info(item)
    url = extract_sku_url(item, base)

    return dict(
        EAN=ean or "",
        CodigoInterno=sku_internal or "",
        NombreProducto=product_name,
        Categoria=cat_name_path,
        Subcategoria="",  # si necesitas dividir path, puedes generar jerarquía
        Marca=brand,
        Fabricante=manufacturer,
        PrecioLista=(price_list or ""),
        PrecioOferta=(price_offer or ""),
        TipoOferta=(tipo_offer or ""),
        URL=url,
    )

# =========================
# MySQL
# =========================
def upsert_mysql(
    df: pd.DataFrame,
    tienda_codigo: str,
    tienda_nombre: str,
    base_url: str,
    map_ean_db: bool = False,
    map_ean_file: Optional[str] = None,
) -> None:
    if get_conn is None:
        log("MySQL deshabilitado (no se encontró base_datos.get_conn).")
        return
    if df.empty:
        log("DataFrame vacío: no hay nada que subir a MySQL.")
        return

    # Conexión
    conn = get_conn()
    cur = conn.cursor()

    # 1) tiendas
    cur.execute("""
        INSERT INTO tiendas(codigo, nombre, url_base)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE nombre=VALUES(nombre), url_base=VALUES(url_base)
    """, (tienda_codigo, tienda_nombre, base_url))
    conn.commit()

    cur.execute("SELECT id FROM tiendas WHERE codigo=%s", (tienda_codigo,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("No se pudo recuperar tienda_id")
    tienda_id = int(row[0])

    # 2) Mapeo EAN (DB)
    ean_map: Dict[str, str] = {}
    if map_ean_db:
        cur.execute("""
            SELECT sku_tienda, ean
              FROM ean_map
             WHERE tienda_id=%s
               AND ean IS NOT NULL AND ean <> ''
        """, (tienda_id,))
        for sku, ean in cur.fetchall():
            ean_map[str(sku)] = str(ean)

    # 3) Mapeo EAN (archivo)
    if map_ean_file and os.path.exists(map_ean_file):
        log(f"Cargando mapeo EAN de archivo: {map_ean_file}")
        _ext = os.path.splitext(map_ean_file)[1].lower()
        if _ext in (".xlsx", ".xls"):
            mdf = pd.read_excel(map_ean_file)
        else:
            mdf = pd.read_csv(map_ean_file)
        mdf = mdf.rename(columns={c: c.strip() for c in mdf.columns})
        if "sku_tienda" in mdf.columns and "ean" in mdf.columns:
            for _, r in mdf.dropna(subset=["sku_tienda", "ean"]).iterrows():
                ean_map[str(r["sku_tienda"]).strip()] = str(r["ean"]).strip()

    # 4) Inserciones
    #   - productos(ean) UNIQUE -> permitimos NULL
    #   - producto_tienda UNIQUE(tienda_id, sku_tienda)
    #   - historico_precios: insert simple por snapshot
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    inserted_pt = 0
    inserted_hp = 0

    for _, r in df.iterrows():
        ean = str(r["EAN"]).strip() or None
        sku = str(r["CodigoInterno"]).strip()
        if not sku:
            # fallback a hash del URL si no hay sku (no común en VTEX)
            sku = slugify(str(r["URL"]))[:64]

        # Completar EAN por mapeo si falta
        if not ean:
            ean = ean_map.get(sku)

        # 2.1 productos
        prod_id = None
        if ean:
            cur.execute("""
                INSERT INTO productos(ean, nombre, marca, fabricante, categoria, subcategoria)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  nombre=VALUES(nombre), marca=VALUES(marca),
                  fabricante=VALUES(fabricante), categoria=VALUES(categoria), subcategoria=VALUES(subcategoria)
            """, (
                ean, r["NombreProducto"], r["Marca"], r["Fabricante"], r["Categoria"], r["Subcategoria"]
            ))
            conn.commit()
            cur.execute("SELECT id FROM productos WHERE ean=%s", (ean,))
            row = cur.fetchone()
            prod_id = int(row[0]) if row else None

        # 2.2 producto_tienda
        cur.execute("""
            INSERT INTO producto_tienda(tienda_id, producto_id, sku_tienda, url_tienda, nombre, marca, fabricante, categoria, subcategoria)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              url_tienda=VALUES(url_tienda),
              nombre=VALUES(nombre),
              marca=VALUES(marca),
              fabricante=VALUES(fabricante),
              categoria=VALUES(categoria),
              subcategoria=VALUES(subcategoria)
        """, (
            tienda_id, prod_id, sku, r["URL"], r["NombreProducto"], r["Marca"], r["Fabricante"], r["Categoria"], r["Subcategoria"]
        ))
        conn.commit()
        inserted_pt += cur.rowcount > 0

        # recuperar id de producto_tienda
        cur.execute("""
            SELECT id FROM producto_tienda WHERE tienda_id=%s AND sku_tienda=%s
        """, (tienda_id, sku))
        row = cur.fetchone()
        if not row:
            continue
        pt_id = int(row[0])

        # 2.3 historico_precios (precios como VARCHAR en tu esquema)
        cur.execute("""
            INSERT INTO historico_precios(tienda_id, producto_tienda_id, precio_lista, precio_oferta, tipo_oferta, capturado_en)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            tienda_id, pt_id,
            (str(r["PrecioLista"]) if pd.notna(r["PrecioLista"]) else None),
            (str(r["PrecioOferta"]) if pd.notna(r["PrecioOferta"]) else None),
            (str(r["TipoOferta"]) if pd.notna(r["TipoOferta"]) else None),
            now
        ))
        conn.commit()
        inserted_hp += cur.rowcount > 0

    cur.close()
    conn.close()
    log(f"MySQL: producto_tienda upserts={inserted_pt}, historico_precios inserts={inserted_hp}")

# =========================
# Proceso principal
# =========================
def run(
    base: str,
    tienda_codigo: str,
    tienda_nombre: str,
    depth: int,
    sc_max: int,
    out_dir: str,
    mysql_on: bool,
    map_ean_db: bool,
    map_ean_file: Optional[str],
    categorias_filter: Optional[List[str]],
    ean_file: Optional[str],
    solo_ean: bool,
) -> None:
    ensure_dir(out_dir)
    session = make_session()

    log(f"Iniciando discovery de categorías depth={depth} ...")
    tree = fetch_category_tree(session, base, depth=depth)
    leaves = list(iter_leaf_categories(tree))
    log(f"Categorías hoja detectadas: {len(leaves)}")

    # Filtro por nombres de categoría (opcional)
    if categorias_filter:
        _flt = set([x.strip().lower() for x in categorias_filter if x.strip()])
        leaves = [(cid, cname) for (cid, cname) in leaves if cname.lower() in _flt]
        log(f"Categorías tras filtro: {len(leaves)}")

    # EAN parcial (opcional)
    wanted_ean: Optional[set] = None
    if ean_file and os.path.exists(ean_file):
        log(f"Cargando EANs desde: {ean_file}")
        ext = os.path.splitext(ean_file)[1].lower()
        if ext in (".xlsx", ".xls"):
            edf = pd.read_excel(ean_file)
        else:
            edf = pd.read_csv(ean_file)
        # tomar cualquier columna llamada "ean" (case-insensitive)
        col_ean = None
        for c in edf.columns:
            if str(c).strip().lower() == "ean":
                col_ean = c
                break
        if not col_ean:
            raise RuntimeError("El archivo de EANs debe tener una columna llamada 'ean'")
        wanted_ean = set(str(x).strip() for x in edf[col_ean].dropna().unique())

    rows: List[Dict[str, Any]] = []
    total_seen = 0

    # Barrido por sc
    for sc in range(1, sc_max + 1):
        # cookie de canal/sucursal
        session.headers.update({"Cookie": f"sc={sc};"})
        log(f"=== Comenzando barrido sc={sc} ===")

        for cat_id, cat_name in leaves:
            empty_streak = 0
            page = 0
            _from = 0
            cat_seen = 0

            while True:
                if page >= MAX_PAGES_API:
                    log(f"[sc={sc} cat={cat_name}] MAX_PAGES_API alcanzado, corto.")
                    break

                data = vtex_search_page(session, base, cat_id, _from, STEP)
                n = len(data)
                log(f"[sc={sc}] cat='{cat_name}' page={page} from={_from} items={n}")

                if n == 0:
                    empty_streak += 1
                    if empty_streak >= MAX_EMPTY_PAGES:
                        break
                else:
                    empty_streak = 0
                    for item in data:
                        row = flatten_product(item, base, cat_name_path=cat_name)
                        # Si solo queremos EANs específicos
                        if wanted_ean:
                            if row["EAN"]:
                                in_set = row["EAN"] in wanted_ean
                            else:
                                in_set = False  # si EAN vacío aquí, luego se puede completar por mapeo DB; este filtro es estricto
                            if solo_ean and not in_set:
                                continue
                        rows.append(row)
                        cat_seen += 1
                        total_seen += 1

                _from += STEP
                page += 1
                time.sleep(SLEEP_OK)

            log(f"[sc={sc}] cat='{cat_name}' -> {cat_seen} items acumulados")
        time.sleep(SLEEP_SC)

    if not rows:
        log("No se recolectaron productos. Revisa sc, categorías o rate-limits.")
        return

    df = pd.DataFrame(rows)
    # Limpieza básica
    for c in df.columns:
        df[c] = df[c].apply(norm_text)

    # CSV por tienda con fecha
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_file = os.path.join(out_dir, f"Listado_{tienda_codigo}_{stamp}.csv")
    df.to_csv(out_file, index=False, encoding="utf-8")
    log(f"Archivo generado: {out_file} (filas={len(df)})")

    # MySQL
    if mysql_on:
        upsert_mysql(
            df=df,
            tienda_codigo=tienda_codigo,
            tienda_nombre=tienda_nombre,
            base_url=base,
            map_ean_db=map_ean_db,
            map_ean_file=map_ean_file,
        )

# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    env_base = os.getenv("VTEX_BASE", "https://www.disco.com.ar")
    env_code = os.getenv("TIENDA_CODIGO", "disco_caba")
    env_name = os.getenv("TIENDA_NOMBRE", "Disco CABA")

    p = argparse.ArgumentParser(description="VTEX — Scraper completo + MySQL")
    p.add_argument("--base", default=env_base, help="Base VTEX, ej: https://www.disco.com.ar")
    p.add_argument("--tienda-codigo", default=env_code, help="Código interno de la tienda")
    p.add_argument("--tienda-nombre", default=env_name, help="Nombre visible de la tienda")
    p.add_argument("--depth", type=int, default=TREE_DEFAULT_DEPTH)
    p.add_argument("--sc-max", type=int, default=8)
    p.add_argument("--out-dir", default="./out")
    p.add_argument("--mysql", choices=["yes", "no"], default="yes")
    p.add_argument("--map-ean-db", choices=["yes", "no"], default="no")
    p.add_argument("--map-ean-file", default=None)
    p.add_argument("--categorias", default=None)
    p.add_argument("--ean-file", default=None)
    p.add_argument("--solo-ean", choices=["yes", "no"], default="no")

    args = p.parse_args()
    # Validación suave por si quedaron vacíos
    if not args.base or not args.tienda_codigo or not args.tienda_nombre:
        raise SystemExit("Faltan --base/--tienda-codigo/--tienda-nombre (y no hay defaults válidos).")
    return args


def main() -> None:
    args = parse_args()
    categorias_filter = None
    if args.categorias:
        categorias_filter = [x for x in args.categorias.split(";") if x.strip()]
    run(
        base=args.base,
        tienda_codigo=args.tienda_codigo,
        tienda_nombre=args.tienda_nombre,
        depth=int(args.depth),
        sc_max=int(args.sc_max),
        out_dir=args.out_dir,
        mysql_on=(args.mysql == "yes"),
        map_ean_db=(args.map_ean_db == "yes"),
        map_ean_file=args.map_ean_file,
        categorias_filter=categorias_filter,
        ean_file=args.ean_file,
        solo_ean=(args.solo_ean == "yes"),
    )

if __name__ == "__main__":
    main()
