#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Masonline (VTEX) — Ingesta MySQL por productClusterIds con fallback alfabético

- Intenta traer todo el cluster por paginación (_from/_to).
- Si choca el límite (~2.500 resultados), particiona por 'ft' (0–9, a–z).
- Dedup por productId y por SKU.
- Inserta/actualiza en:
    tiendas, productos, producto_tienda, historico_precios

Requisitos:
  pip install requests pandas mysql-connector-python urllib3

Config MySQL:
  from base_datos import get_conn  # Debe devolver mysql.connector.connect(...)
"""

import time
import argparse
import string
from typing import List, Dict, Any, Optional, Tuple, Set

import requests
from requests.adapters import HTTPAdapter
from requests import HTTPError
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np
import datetime as dt
import sys, os
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn  # <- tu helper de conexión mysql.connector

# ---------------- Identidad de tienda ----------------
TIENDA_CODIGO = "masonline"
TIENDA_NOMBRE = "Masonline (VTEX)"

# ---------------- Límites de columnas (ajustá a tu DDL) ----------------
MAXLEN_TIPO_OFERTA = 255            # historico_precios.tipo_oferta (VARCHAR(255) sugerido)
MAXLEN_PROMO_COMENTARIOS = 1000     # historico_precios.promo_comentarios (TEXT o VARCHAR grande)
MAXLEN_NOMBRE_TIENDA = 255          # producto_tienda.nombre_tienda (VARCHAR(255) sugerido)

def _truncate(s: Optional[str], maxlen: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s[:maxlen] if len(s) > maxlen else s

# ---------------- Config VTEX ----------------
BASE = "https://www.masonline.com.ar"
SEARCH_API = f"{BASE}/api/catalog_system/pub/products/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

STEP = 50                  # VTEX: _to - _from <= 49
SLEEP_BETWEEN = 0.30
MAX_WINDOW_RESULTS = 2500  # 50 páginas * 50 ítems
ORDER_BY = "OrderByNameASC"
ALPHA_TERMS = list(string.digits + string.ascii_lowercase)  # 0-9 + a-z

# ---------------- Sesión HTTP ----------------
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=40, pool_maxsize=40))
    s.headers.update(HEADERS)
    return s

# ---------------- Fetchers VTEX ----------------
def fetch_page(session: requests.Session, cluster_id: str, start: int, step: int) -> List[Dict[str, Any]]:
    params = [
        ("fq", f"productClusterIds:{cluster_id}"),
        ("_from", start),
        ("_to", start + step - 1),
        ("O", ORDER_BY),
    ]
    r = session.get(SEARCH_API, params=params, timeout=30)
    if r.status_code == 400:
        raise HTTPError("VTEX 50-page window reached", response=r)
    r.raise_for_status()
    try:
        data = r.json()
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        return data if isinstance(data, list) else []
    except Exception:
        return []

def fetch_page_alpha(session: requests.Session, cluster_id: str, term: str, start: int, step: int) -> List[Dict[str, Any]]:
    url = f"{SEARCH_API}/{cluster_id}/{term}"
    params = {
        "map": "productClusterIds,ft",
        "_from": start,
        "_to": start + step - 1,
        "O": ORDER_BY,
    }
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 400:
        raise HTTPError("Bad Request on alpha slice", response=r)
    r.raise_for_status()
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []

# ---------------- Helpers de parseo ----------------
def split_categories(paths: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not paths:
        return None, None, None
    best = max(paths, key=lambda p: p.count("/"))
    parts = [p for p in best.strip("/").split("/") if p]
    categoria = parts[0] if len(parts) >= 1 else None
    subcategoria = " > ".join(parts[1:]) if len(parts) > 1 else None
    ruta_full = " / ".join(parts) if parts else None
    return categoria, subcategoria, ruta_full

def extract_offer_type(p: Dict[str, Any], item: Dict[str, Any]) -> str:
    names = []
    sellers = item.get("sellers") or []
    for s in sellers:
        co = (s or {}).get("commertialOffer") or {}
        for t in co.get("Teasers") or []:
            n = (t or {}).get("Name") or (t or {}).get("name")
            if n:
                names.append(str(n))
        for t in co.get("PromotionTeasers") or []:
            n = (t or {}).get("Name") or (t or {}).get("name")
            if n:
                names.append(str(n))
    clusters = p.get("productClusters") or {}
    for _, cname in clusters.items():
        if isinstance(cname, str) and cname:
            names.append(cname)
    names = list(dict.fromkeys([n.strip() for n in names if n and n.strip()]))
    return " | ".join(names)

def choose_seller(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sellers = item.get("sellers") or []
    for s in sellers:
        if s.get("sellerDefault"):
            return s
    for s in sellers:
        co = (s or {}).get("commertialOffer") or {}
        if co.get("IsAvailable"):
            return s
    return sellers[0] if sellers else None

def flatten(products: List[Dict[str, Any]], cluster_id: str, verbose: bool = False) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for p in products:
        categoria, subcategoria, _ruta = split_categories(p.get("categories") or [])
        brand = p.get("brand")
        manufacturer = p.get("Manufacturer") or p.get("manufacturer") or None
        url = p.get("link") or f"{BASE}/{p.get('linkText')}/p"
        cluster_name = None
        pcs = p.get("productClusters") or {}
        if cluster_id in pcs:
            cluster_name = pcs.get(cluster_id)

        for it in p.get("items") or []:
            ean = it.get("ean") or None
            ref_val = None
            for ref in it.get("referenceId") or []:
                if (ref or {}).get("Key") == "RefId":
                    ref_val = ref.get("Value")
                    break
            if not ref_val:
                ref_val = p.get("productReference") or it.get("itemId")

            seller = choose_seller(it) or {}
            co = (seller.get("commertialOffer") or {}) if seller else {}
            price = co.get("Price")
            list_price = co.get("ListPrice")
            tipo_oferta = extract_offer_type(p, it)

            row = {
                "EAN": ean,
                "CodigoInterno": ref_val,
                "NombreProducto": p.get("productName") or it.get("name"),
                "Categoria": categoria,
                "Subcategoria": subcategoria,
                "Marca": brand,
                "Fabricante": manufacturer,
                "PrecioLista": list_price,
                "PrecioOferta": price,
                "TipoOferta": tipo_oferta,
                "URL": url,
                "SKU": it.get("itemId"),
                "ProductId": p.get("productId"),
                "ClusterId": cluster_id,
                "ClusterNombre": cluster_name,
            }
            rows.append(row)

            if verbose:
                print(
                    f"➡ {row['EAN'] or '-'} | {row['NombreProducto']} | {row['Marca'] or '-'} | "
                    f"Lista: {row['PrecioLista'] if row['PrecioLista'] is not None else '-'} | "
                    f"Oferta: {row['PrecioOferta'] if row['PrecioOferta'] is not None else '-'} | "
                    f"{row['URL']}",
                    flush=True
                )
    return rows

# ---------------- Scrapers de cluster ----------------
def scrape_cluster_alpha(session: requests.Session, cluster_id: str, seen_products: Set[str]) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    for term in ALPHA_TERMS:
        start = 0
        print(f"\n--- Partición '{term}' ---", flush=True)
        while True:
            try:
                chunk = fetch_page_alpha(session, cluster_id, term, start, STEP)
            except HTTPError as e:
                print(f"  {term}: stop por {e}.", flush=True)
                break

            if not chunk:
                if start == 0:
                    print(f"  {term}: sin resultados.", flush=True)
                break

            fresh = [p for p in chunk if p.get("productId") not in seen_products]
            for p in fresh:
                seen_products.add(p.get("productId"))

            print(f"  {term}: desde {start} -> {len(fresh)} productos nuevos (acum únicos: {len(seen_products)})", flush=True)
            rows = flatten(fresh, cluster_id, verbose=True)
            all_rows.extend(rows)

            start += STEP
            time.sleep(SLEEP_BETWEEN)
            if len(chunk) < STEP:
                break
    return all_rows

def scrape_cluster(cluster_id: str) -> pd.DataFrame:
    session = make_session()
    start = 0
    seen_ids: Set[str] = set()
    all_rows: List[Dict[str, Any]] = []
    hit_window_cap = False

    # Ventana estándar
    while True:
        try:
            if start >= MAX_WINDOW_RESULTS:
                hit_window_cap = True
                print(f"Ventana estándar alcanzó {MAX_WINDOW_RESULTS} ítems; cambiando a particiones…", flush=True)
                break
            chunk = fetch_page(session, cluster_id, start, STEP)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                hit_window_cap = True
                print(f"HTTP 400 en start={start}. Límite de ~2.500; cambiando a particiones…", flush=True)
                break
            else:
                raise

        if not chunk:
            break

        fresh = [p for p in chunk if p.get("productId") not in seen_ids]
        for p in fresh:
            seen_ids.add(p.get("productId"))

        print(f"Página desde {start}: {len(fresh)} productos nuevos (acum únicos: {len(seen_ids)})", flush=True)
        rows = flatten(fresh, cluster_id, verbose=True)
        all_rows.extend(rows)

        start += STEP
        time.sleep(SLEEP_BETWEEN)
        if len(chunk) < STEP:
            break

    # Particiones alfabéticas si topamos el límite
    if hit_window_cap:
        extra_rows = scrape_cluster_alpha(session, cluster_id, seen_ids)
        all_rows.extend(extra_rows)

    df = pd.DataFrame(all_rows)
    cols = [
        "EAN", "CodigoInterno", "NombreProducto", "Categoria", "Subcategoria",
        "Marca", "Fabricante", "PrecioLista", "PrecioOferta", "TipoOferta",
        "URL", "SKU", "ProductId", "ClusterId", "ClusterNombre"
    ]
    if not df.empty:
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df.reindex(columns=cols)
    return df

# ---------------- Helpers SQL ----------------
def _parse_price(val) -> Optional[str]:
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = (r.get("EAN") or None)
    nombre = (r.get("NombreProducto") or "").strip()
    marca = (r.get("Marca") or None)
    fabricante = (r.get("Fabricante") or None)
    categoria = (r.get("Categoria") or None)
    subcategoria = (r.get("Subcategoria") or None)

    # 1) Por EAN
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(%s, marca),
                  fabricante = COALESCE(%s, fabricante),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (nombre, marca, fabricante, categoria, subcategoria, pid))
            return pid

    # 2) Por (nombre, marca)
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca or ""))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(%s, ean),
                  fabricante = COALESCE(%s, fabricante),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (ean, fabricante, categoria, subcategoria, pid))
            return pid

    # 3) Insert nuevo
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    sku = (r.get("SKU") or None)
    url = (r.get("URL") or None)
    nombre_tienda = _truncate((r.get("NombreProducto") or None), MAXLEN_NOMBRE_TIENDA)
    record_id = (r.get("ProductId") or None)

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    # Sin SKU: usar ProductId
    if record_id:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    # Último recurso
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: dt.datetime):
    precio_lista = _parse_price(r.get("PrecioLista"))
    precio_oferta = _parse_price(r.get("PrecioOferta"))
    tipo_oferta = _truncate((r.get("TipoOferta") or None), MAXLEN_TIPO_OFERTA)

    # Metadatos del cluster en comentarios
    promo_comentarios = None
    cid = r.get("ClusterId")
    cname = r.get("ClusterNombre")
    if cid or cname:
        promo_comentarios = f"cluster_id={cid or ''}; cluster_nombre={cname or ''}"
        promo_comentarios = _truncate(promo_comentarios, MAXLEN_PROMO_COMENTARIOS)

    cur.execute("""
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = VALUES(promo_tipo),
          promo_texto_regular = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios = VALUES(promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista, precio_oferta, tipo_oferta,
        tipo_oferta, None, None, promo_comentarios
    ))

# ---------------- Orquestador MySQL ----------------
def run_to_mysql(cluster_ids: List[str], out_prefix: Optional[str] = None):
    frames = []
    for cid in cluster_ids:
        print(f"\n=== Cluster {cid} ===", flush=True)
        df = scrape_cluster(cid)
        print(f"Cluster {cid}: {len(df)} filas totales", flush=True)
        frames.append(df)

    if not frames:
        print("No se obtuvieron datos.", flush=True)
        return

    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    if df.empty:
        print("Sin filas para insertar.", flush=True)
        return

    # Dedupe por SKU (y por ProductId como respaldo)
    if "SKU" in df.columns:
        df.drop_duplicates(subset=["SKU"], inplace=True, keep="first")
    elif "ProductId" in df.columns:
        df.drop_duplicates(subset=["ProductId"], inplace=True, keep="first")

    print(f"💾 Preparando inserción MySQL ({len(df)} filas únicas)…", flush=True)
    capturado_en = dt.datetime.now()

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        inserted_hist = 0
        for _, r in df.iterrows():
            rec = r.to_dict()
            producto_id = find_or_create_producto(cur, rec)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, rec)
            insert_historico(cur, tienda_id, pt_id, rec, capturado_en)
            inserted_hist += 1

        conn.commit()
        print(f"✅ Guardado en MySQL: {inserted_hist} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    # Export opcional local (útil para auditoría)
    if out_prefix:
        csv_path = f"{out_prefix}.csv"
        xlsx_path = f"{out_prefix}.xlsx"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name="productos")
        print(f"\nArchivos guardados:\n- {csv_path}\n- {xlsx_path}\n", flush=True)

# ---------------- CLI ----------------
def main():
    parser = argparse.ArgumentParser(description="Masonline (VTEX) — Ingesta a MySQL por cluster IDs")
    parser.add_argument("--clusters", type=str, default="3454",
                        help="IDs de cluster separados por coma (ej: 3454,3627)")
    parser.add_argument("--out", type=str, default="masonline_cluster",
                        help="Prefijo de archivo de salida (CSV/XLSX opcional)")
    args = parser.parse_args()

    cluster_ids = [c.strip() for c in args.clusters.split(",") if c.strip()]
    run_to_mysql(cluster_ids, out_prefix=args.out)

if __name__ == "__main__":
    main()
