#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time, re, threading, requests, pandas as pd
from bs4 import BeautifulSoup
from html import unescape
from urllib.parse import unquote, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ===================== Config =====================
BASE = "https://www.disco.com.ar"
SEARCH = f"{BASE}/api/catalog_system/pub/products/search"
FACETS = f"{BASE}/api/catalog_system/pub/facets/search/*?map=c"

STEP = 50
SLEEP_BASE = 0.1
TIMEOUT = 25
MAX_EMPTY_PAGES = 2
RETRIES = 3
MAX_WORKERS = 6           # <- puedes subir/bajar según tu ancho de banda/CPU
MAX_DEPTH = None          # ej. 2 para cortar en departamento/subcat

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# Export opcional (puedes desactivar poniéndolo en None)
OUT_XLSX = "disco_formato.xlsx"
OUT_CSV  = None  # p.ej. "disco_formato.csv"

# Identidad tienda (para DB)
TIENDA_CODIGO = "disco"
TIENDA_NOMBRE = "Disco Argentina"

# Inserción histórico por lotes
HP_BATCH_SIZE = 1000

COLS_FINAL = [
    "EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Marca",
    "Fabricante","Precio de Lista","Precio de Oferta","Tipo de Oferta","URL"
]

ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

# ===================== Utils =====================
def clean_text_fast(v):
    if v is None: return ""
    if not isinstance(v, str): return v
    if "<" in v and ">" in v:
        try:
            v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
        except Exception:
            pass
    return ILLEGAL_XLSX.sub("", v)

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def split_cat(path: str):
    if not path: return "", ""
    parts = [p for p in path.strip("/").split("/") if p]
    fix = lambda s: s.replace("-", " ").strip().title()
    cat = fix(parts[0]) if parts else ""
    sub = fix(parts[1]) if len(parts) > 1 else ""
    return cat, sub

def tipo_de_oferta(offer: dict, list_price: float, price: float) -> str:
    try:
        dh = offer.get("DiscountHighLight") or []
        if dh and isinstance(dh, list):
            name = (dh[0].get("Name") or "").strip()
            if name: return name
    except Exception:
        pass
    return "Descuento" if (price or 0) < (list_price or 0) else "Precio regular"

def normalize_ean(e: Any) -> Optional[str]:
    """Deja sólo dígitos; si queda vacío, None."""
    if e is None: return None
    s = str(e)
    digits = re.sub(r"\D+", "", s)
    return digits if digits else None

def normalize_item_id(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    return s or None

def normalize_url(u: Any) -> Optional[str]:
    """Baja a minúsculas, quita query/fragment, deja esquema+host+path."""
    if u is None: return None
    s = str(u).strip()
    if not s: return None
    try:
        p = urlparse(s)
        # Asegura host correcto; si viene ruta relativa, preserva tal cual
        if not p.scheme or not p.netloc:
            # si es relativa, sólo normaliza path
            path = p.path.rstrip("/")
            return path.lower() or None
        path = p.path.rstrip("/")
        base = f"{p.scheme}://{p.netloc}{path}".lower()
        return base or None
    except Exception:
        return s.strip().lower() or None

def build_norm_key(ean: Optional[str], item_id: Optional[str], url: Optional[str]) -> str:
    """Clave estable: E:ean / I:item_id / U:url_normalizada (sin query)."""
    e = normalize_ean(ean)
    if e: return f"E:{e}"
    i = normalize_item_id(item_id)
    if i: return f"I:{i}"
    u = normalize_url(url)
    return f"U:{u or 'sin-url'}"

def _norm_str(v: Any) -> str:
    """Normaliza cadenas para guardar (trim y None->'')."""
    if v is None: return ""
    try:
        s = str(v).strip()
    except Exception:
        s = ""
    return s

# ===================== HTTP session =====================
def make_session():
    s = requests.Session()
    retry = Retry(
        total=RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],  # si tu urllib3 es viejo, usar method_whitelist
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s

SESSION = make_session()

# ===================== Categorías =====================
def _link_to_segments(link: str):
    if not link: return []
    link = unquote(link)
    path = link.split("?", 1)[0].strip("/")
    if not path: return []
    return [s.strip().lower() for s in path.split("/") if s.strip()]

def _walk_categories(node, results):
    link = (node.get("Link") or node.get("link") or "").strip()
    segs = _link_to_segments(link)
    if segs: results.add(tuple(segs))
    for ch in (node.get("Children") or node.get("children") or []):
        _walk_categories(ch, results)

def get_category_paths(max_depth=None):
    r = SESSION.get(FACETS, timeout=TIMEOUT); r.raise_for_status()
    data = r.json()
    results = set()
    for n1 in (data.get("CategoriesTrees") or []):
        _walk_categories(n1, results)
    if not results:
        for dep in data.get("Departments", []):
            segs = _link_to_segments(dep.get("Link") or dep.get("link") or "")
            if segs: results.add(tuple(segs))
    paths = sorted(results, key=lambda t: (len(t), t))
    if max_depth: paths = [p for p in paths if len(p) <= max_depth]
    return paths

# ===================== Fetch =====================
def fetch_page_by_path(path_segments, offset, sleep_holder):
    path = "/".join(path_segments)
    map_str = ",".join(["c"] * len(path_segments))
    url = f"{SEARCH}/{path}?map={map_str}&_from={offset}&_to={offset + STEP - 1}"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
    except Exception:
        time.sleep(sleep_holder[0]); return []

    if r.status_code in (200, 206):
        try:
            return r.json()
        except Exception:
            time.sleep(sleep_holder[0]); return []
    if r.status_code == 429:
        # backoff adaptativo
        sleep_holder[0] = min(1.0, sleep_holder[0] + 0.2)
        time.sleep(sleep_holder[0]); return []
    if r.status_code in (500, 503):
        time.sleep(sleep_holder[0]); return []
    return []

# ===================== Parse de producto → filas =====================
def rows_from_product(p: dict):
    """Devuelve una lista de filas (una por SKU) mapeadas al formato final."""
    rows = []
    categories = p.get("categories") or []
    cat, sub = ("","")
    if categories and isinstance(categories, list) and isinstance(categories[0], str):
        cat, sub = split_cat(categories[0])

    slug = p.get("linkText")
    base_url = f"{BASE}/{slug}/p" if slug else (p.get("link") or "")

    product_name = clean_text_fast(p.get("productName"))

    brand = clean_text_fast(p.get("brand"))
    manufacturer = _norm_str(p.get("manufacturer"))

    for it in (p.get("items") or []):
        sellers = it.get("sellers") or []
        s0 = sellers[0] if sellers else {}
        offer = s0.get("commertialOffer") or {}


        try:
            list_price = float(offer.get("PriceWithoutDiscount") or 0)
        except Exception:
            list_price = 0.0
        try:
            price      = float(offer.get("Price") or 0)
        except Exception:
            price = 0.0

        ean_raw = it.get("ean") or first(p.get("EAN"))
        item_id = it.get("itemId") or p.get("productId")

        row = {
            "EAN": normalize_ean(ean_raw),
            "Código Interno": normalize_item_id(item_id),
            "Nombre Producto": product_name,
            "Categoría": cat,
            "Subcategoría": sub,
            "Marca": brand,
            "Fabricante": manufacturer,
            "Precio de Lista": round(list_price, 2),
            "Precio de Oferta": round(price, 2),
            "Tipo de Oferta": tipo_de_oferta(offer, list_price, price),
            "URL": normalize_url(base_url),
        }
        rows.append(row)
    return rows

# ===================== Scraping por categoría (dedupe thread-safe) =====================
SEEN_KEYS: set = set()
SEEN_LOCK = threading.Lock()

def scrape_category(segs):
    etiqueta = "/".join(segs)
    out = []
    offset = 0
    empty_streak = 0
    sleep_holder = [SLEEP_BASE]  # mutable para backoff adaptativo

    while True:
        data = fetch_page_by_path(segs, offset, sleep_holder)
        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES: break
            offset += STEP; continue

        empty_streak = 0
        for p in data:
            try:
                for row in rows_from_product(p):
                    key = build_norm_key(row["EAN"], row["Código Interno"], row["URL"])
                    with SEEN_LOCK:
                        if key in SEEN_KEYS:
                            continue
                        SEEN_KEYS.add(key)
                    out.append(row)
            except Exception:
                continue

        if len(data) < STEP: break
        offset += STEP
        time.sleep(sleep_holder[0])

    print(f"🗂️ {etiqueta}: +{len(out)} filas únicas")
    return etiqueta, out

# ===================== Orquestación scraping =====================
def scrape_all(max_workers=MAX_WORKERS, max_depth=MAX_DEPTH):
    paths = get_category_paths(max_depth=max_depth)
    print(f"🔎 {len(paths)} rutas a scrapear (workers={max_workers})")
    all_rows = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(scrape_category, segs): segs for segs in paths}
        for fut in as_completed(futures):
            etiqueta, rows = fut.result()
            all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    # Garantizar columnas/orden
    for c in COLS_FINAL:
        if c not in df.columns: df[c] = pd.NA

    # Tipos
    df["EAN"] = df["EAN"].astype("string")
    for c in ["Precio de Lista","Precio de Oferta"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    # Orden final
    df = df[COLS_FINAL]
    return df

# ===================== Dedupe DataFrame (fuerte, clave canónica) =====================
def dedupe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    1) Elimina duplicados exactos.
    2) Construye clave canónica jerárquica (EAN→Código Interno→URL) con normalización fuerte.
    3) Selecciona la 'mejor' fila por clave (prioriza: tiene EAN, luego Código Interno, luego Precio Oferta > 0).
    """
    if df is None or df.empty:
        return df

    # 1) Duplicados exactos
    before = len(df)
    df = df.drop_duplicates(keep="first")
    removed_exact = before - len(df)

    # 2) Clave jerárquica normalizada
    ean_norm = df["EAN"].map(normalize_ean)
    item_norm = df["Código Interno"].map(normalize_item_id)
    url_norm = df["URL"].map(normalize_url)

    key = ean_norm.fillna("")
    m = key.eq("") | key.isna()
    key = key.astype(str)
    key[m] = item_norm[m].fillna("").astype(str)
    m = key.eq("") | key.isna()
    key[m] = url_norm[m].fillna("").astype(str)

    df["_k"] = key

    # 3) Scoring para elegir la mejor fila por clave
    has_ean  = ean_norm.notna() & (ean_norm.astype(str).str.len() > 0)
    has_item = item_norm.notna() & (item_norm.astype(str).str.len() > 0)
    precio_o = pd.to_numeric(df["Precio de Oferta"], errors="coerce").fillna(0)

    df["_score"] = (
        has_ean.astype(int) * 4
        + has_item.astype(int) * 2
        + (precio_o > 0).astype(int) * 1
    )

    # Ordenamos por clave y score desc + precio desc, y nos quedamos con la primera por clave
    df = df.sort_values(by=["_k", "_score", "Precio de Oferta"], ascending=[True, False, False])
    before2 = len(df)
    df = df.drop_duplicates(subset=["_k"], keep="first")
    removed_key = before2 - len(df)

    # Limpieza columnas auxiliares
    df = df.drop(columns=["_k", "_score"], errors="ignore")

    print(f"🧹 Dedupe DataFrame: -{removed_exact} exactos, -{removed_key} por clave → {len(df)} filas")
    return df.reset_index(drop=True)

def postprocess_and_save(df: pd.DataFrame):
    if df.empty:
        print("⚠️ No se obtuvieron productos.")
        return df

    # 🔹 Dedupe robusto ANTES de exportar/inserción
    df = dedupe_dataframe(df)

    # Guardar en Excel/CSV si corresponde
    if OUT_XLSX:
        try:
            with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as w:
                df.to_excel(w, index=False, sheet_name="productos")
                wb=w.book; ws=w.sheets["productos"]
                money=wb.add_format({"num_format":"0.00"})
                text=wb.add_format({"num_format":"@"})
                col={n:i for i,n in enumerate(COLS_FINAL)}
                ws.set_column(col["EAN"], col["EAN"], 18, text)
                ws.set_column(col["Nombre Producto"], col["Nombre Producto"], 52)
                for c in ["Categoría","Subcategoría","Marca","Fabricante"]:
                    ws.set_column(col[c], col[c], 20)
                ws.set_column(col["Precio de Lista"], col["Precio de Lista"], 14, money)
                ws.set_column(col["Precio de Oferta"], col["Precio de Oferta"], 14, money)
                ws.set_column(col["URL"], col["URL"], 46)
            print(f"💾 XLSX: {OUT_XLSX} ({len(df)} filas)")
        except Exception as e:
            df.to_excel(OUT_XLSX, index=False)
            print(f"💾 XLSX (fallback): {OUT_XLSX} ({len(df)} filas) — aviso: {e}")

    if OUT_CSV:
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"💾 CSV: {OUT_CSV} ({len(df)} filas)")

    return df

# ===================== Helpers DB =====================
def _s(v):
    # str limpio o None
    if pd.isna(v): return None
    v = str(v).strip()
    return v if v else None

def _f(v):
    # float o None
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)): return None
        return float(v)
    except Exception:
        return None

def ensure_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute("""
        INSERT INTO tiendas (codigo, nombre)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)
    """, (codigo, nombre))
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s", (codigo,))
    return cur.fetchone()[0]

def get_or_create_producto(cur, cache_ean: dict, cache_nom_marca: dict, row: dict) -> int:
    """
    Busca por EAN si existe; si existe, NO inserta (devuelve su id).
    Si no hay EAN: intenta por (nombre, marca). Inserta sólo si no existe.
    """
    ean  = _s(row.get("EAN"))
    nom  = _s(row.get("Nombre Producto"))
    marca= _s(row.get("Marca"))
    fabr = _s(row.get("Fabricante"))
    cat  = _s(row.get("Categoría"))
    sub  = _s(row.get("Subcategoría"))

    # --- Caso con EAN ---
    if ean:
        if ean in cache_ean:
            return cache_ean[ean]
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        r = cur.fetchone()
        if r:
            pid = r[0]
            cache_ean[ean] = pid
            return pid
        cur.execute("""
            INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (ean, nom, marca, fabr, cat, sub))
        pid = cur.lastrowid
        cache_ean[ean] = pid
        return pid

    # --- Sin EAN: fallback exacto por (nombre, marca) ---
    key = (nom or "", marca or "")
    if key in cache_nom_marca:
        return cache_nom_marca[key]
    cur.execute("""
        SELECT id FROM productos
        WHERE nombre=%s AND IFNULL(marca,'')=IFNULL(%s,'')
        LIMIT 1
    """, (nom, marca))
    r = cur.fetchone()
    if r:
        pid = r[0]; cache_nom_marca[key] = pid
        return pid
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (None, nom, marca, fabr, cat, sub))
    pid = cur.lastrowid
    cache_nom_marca[key] = pid
    return pid

def _build_sku(row: dict) -> str:
    """
    Siempre devolver un SKU no vacío para respetar UNIQUE (tienda_id, sku_tienda).
    Prioriza itemId; si no, usa EAN o URL con prefijo.
    """
    sku = _s(row.get("Código Interno"))
    if sku: return sku
    ean = _s(row.get("EAN"))
    if ean: return f"E:{ean}"
    url = _s(row.get("URL"))
    return f"U:{url or 'sin-url'}"

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, row: dict) -> int:
    """
    UPSERT usando UNIQUE (tienda_id, sku_tienda).
    Si ya existe el SKU, NO actualiza producto_id. Retorna id vía LAST_INSERT_ID.
    """
    sku  = _build_sku(row)
    url  = _s(row.get("URL"))
    name = _s(row.get("Nombre Producto"))

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            -- NO tocar producto_id cuando ya existe el SKU
            url_tienda   = COALESCE(NULLIF(VALUES(url_tienda), ''), url_tienda),
            nombre_tienda= COALESCE(NULLIF(VALUES(nombre_tienda), ''), nombre_tienda),
            id = LAST_INSERT_ID(id)
    """, (tienda_id, producto_id, sku, None, url, name))

    return cur.lastrowid


def insert_historico_batch(cur, batch):
    cur.executemany("""
        INSERT INTO historico_precios
        (tienda_id, producto_tienda_id, capturado_en, precio_lista, precio_oferta, tipo_oferta,
         promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, batch)

def insertar_df_en_mysql(df: pd.DataFrame,
                         tienda_codigo=TIENDA_CODIGO,
                         tienda_nombre=TIENDA_NOMBRE):
    if df is None or df.empty:
        print("⚠️ DataFrame vacío: nada para insertar.")
        return

    # Dedupe final: garantiza sin repetidos
    df = dedupe_dataframe(df)

    # Congelar un timestamp común para el snapshot
    snapshot_dt = datetime.now().replace(microsecond=0)

    # Normaliza columnas por si vienen con NaN
    for c in COLS_FINAL:
        if c not in df.columns:
            df[c] = pd.NA

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        tienda_id = ensure_tienda(cur, tienda_codigo, tienda_nombre)
        print(f"🏬 tienda_id={tienda_id} ({tienda_codigo})")

        cache_ean = {}          # ean -> producto_id
        cache_nom_marca = {}    # (nombre, marca) -> producto_id

        hp_batch = []
        n_prod_new = 0
        n_hp = 0

        for i, row in enumerate(df.to_dict(orient="records"), start=1):
            # 1) productos
            before_last_id = cur.lastrowid
            pid = get_or_create_producto(cur, cache_ean, cache_nom_marca, row)
            if cur.lastrowid and cur.lastrowid != before_last_id:
                n_prod_new += 1

            # 2) producto_tienda (UPSERT con retorno id)
            ptid = upsert_producto_tienda(cur, tienda_id, pid, row)

            # 3) armar histórico
            hp_batch.append((
                tienda_id,
                ptid,
                snapshot_dt,
                _f(row.get("Precio de Lista")),
                _f(row.get("Precio de Oferta")),
                _s(row.get("Tipo de Oferta")),
                None, None, None, None  # promo_* no disponibles en este endpoint
            ))

            # Insertar por lotes
            if len(hp_batch) >= HP_BATCH_SIZE:
                insert_historico_batch(cur, hp_batch)
                n_hp += len(hp_batch)
                hp_batch.clear()
                conn.commit()

            if i % 1000 == 0:
                print(f"… procesadas {i} filas")

        # Lote final
        if hp_batch:
            insert_historico_batch(cur, hp_batch)
            n_hp += len(hp_batch)
            hp_batch.clear()

        conn.commit()
        print(f"✅ Insert terminado: +{n_prod_new} productos nuevos, histórico {n_hp} filas.")
    except MySQLError as e:
        conn.rollback()
        print(f"❌ Error MySQL: {e}")
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# ===================== Entry =====================
if __name__ == "__main__":
    t0 = time.time()
    print("🚀 Iniciando scraping Disco (VTEX)…")
    df = scrape_all(max_workers=MAX_WORKERS, max_depth=MAX_DEPTH)
    df = postprocess_and_save(df)  # <-- incluye dedupe fuerte
    print("🗄️ Insertando en MySQL…")
    insertar_df_en_mysql(df)
    print(f"⏱️ Tiempo total: {time.time() - t0:.1f}s")
