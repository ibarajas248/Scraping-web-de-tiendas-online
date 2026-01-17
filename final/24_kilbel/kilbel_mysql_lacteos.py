#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import random
import argparse
from urllib.parse import urljoin
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path (igual que tu patrón)
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL


BASE = "https://www.kilbelonline.com"
START = "https://www.kilbelonline.com/lacteos/n1_994/pag/{page}/"

TIENDA_CODIGO = "kilbel"
TIENDA_NOMBRE = "kilbel"

_NULLLIKE = {"", "null", "none", "nan", "na"}


# -------------------------
# HTTP session robusta
# -------------------------
def build_session():
    s = requests.Session()
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=0.9,
        status_forcelist=[403, 408, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def clean(val):
    """Normaliza texto: trim, colapsa espacios, filtra null-likes."""
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s


def clean_text(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def parse_price_to_str(raw: str) -> str:
    """
    "$ 1.690,00" -> "1690.00"
    "$ 1690"     -> "1690"
    """
    raw = clean_text(raw)
    if not raw:
        return ""
    raw = raw.replace("$", "").strip()
    raw = raw.replace(".", "")      # miles
    raw = raw.replace(",", ".")     # decimales
    raw = re.sub(r"[^0-9.]", "", raw)
    if not raw:
        return ""
    if raw.count(".") > 1:
        parts = raw.split(".")
        raw = parts[0] + "." + "".join(parts[1:])
    return raw


def parse_price_float(x) -> Optional[float]:
    """Convierte string numérico ('1690.00') a float, si no puede devuelve None."""
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def to_varchar_2dec(x) -> Optional[str]:
    """Normaliza precio a VARCHAR con 2 decimales, o None."""
    if x is None:
        return None
    v = parse_price_float(x)
    if v is None:
        return None
    return f"{float(v):.2f}"


def compute_tipo_oferta(precio_base: str, precio_oferta: str) -> str:
    """
    Si hay oferta, calcula % descuento como string tipo '10.00%' (o '' si no se puede).
    """
    if not precio_oferta:
        return ""
    b = parse_price_float(precio_base)
    o = parse_price_float(precio_oferta)
    if b is None or o is None or b <= 0:
        return "OFERTA"
    if o >= b:
        return "OFERTA"
    pct = (1.0 - (o / b)) * 100.0
    return f"{pct:.2f}%"


def extract_price_blocks(item) -> Tuple[str, str]:
    """
    REGLA:
    - Si hay oferta:
        precio_base  = div.precio.anterior.codigo
        precio_oferta= (span o div) .precio.aux1
    - Si NO hay oferta:
        precio_base  = (span o div) .precio.aux1
        precio_oferta= ""
    """
    cont = item.select_one("div.precio_complemento")
    if not cont:
        return "", ""

    anterior = cont.select_one("div.precio.anterior.codigo") or cont.select_one("div.precio.anterior")
    anterior_txt = clean_text(anterior.get_text(" ", strip=True)) if anterior else ""

    actual = cont.select_one("span.precio.aux1") or cont.select_one("div.precio.aux1")
    actual_txt = clean_text(actual.get_text(" ", strip=True)) if actual else ""

    if anterior_txt:
        return anterior_txt, actual_txt
    return actual_txt, ""


def guess_record_id_from_url(url: str) -> str:
    """
    Intenta sacar algo estable tipo 'art_19869' o id numérico del final.
    """
    if not url:
        return ""
    m = re.search(r"(art_\d+)", url)
    if m:
        return m.group(1)
    m2 = re.search(r"/(\d+)/?$", url)
    if m2:
        return m2.group(1)
    return ""


def parse_item(item) -> Dict[str, Any]:
    # link + imagen
    a_img = item.select_one("div.ant_imagen a[href]")
    url_producto_rel = a_img.get("href") if a_img else ""
    url_producto = urljoin(BASE, url_producto_rel) if url_producto_rel else ""

    img = a_img.select_one("img") if a_img else None
    url_imagen = ""
    if img:
        url_imagen = img.get("data-src") or img.get("src") or ""
        url_imagen = clean_text(url_imagen)

    # nombre
    a_nombre = item.select_one("div.col1_listado a[id^='btn_nombre_imetrics_']")
    nombre = clean_text(a_nombre.get_text(" ", strip=True)) if a_nombre else ""

    # sku (id_item_XXXX value) -> este es TU SKU de tienda
    sku_input = item.select_one("input[type='hidden'][id^='id_item_']")
    sku = clean_text(sku_input.get("value")) if sku_input else ""

    # id prod_XXXXX por si sirve
    prod_id = clean_text(item.get("id"))  # ej "prod_15592"
    prod_num = ""
    m = re.search(r"prod_(\d+)", prod_id)
    if m:
        prod_num = m.group(1)

    # precios raw según tu regla
    precio_base_raw, precio_oferta_raw = extract_price_blocks(item)
    precio_base = parse_price_to_str(precio_base_raw)
    precio_oferta = parse_price_to_str(precio_oferta_raw)

    # record_id_tienda: prioriza art_XXXXX del URL, si no prod_num
    record_id_tienda = guess_record_id_from_url(url_producto) or prod_num

    # tipo_oferta derivado
    tipo_oferta = compute_tipo_oferta(precio_base, precio_oferta)

    return {
        "sku": clean(sku),
        "record_id": clean(record_id_tienda),
        "ean": None,  # Kilbel PLP no trae EAN
        "nombre": clean(nombre),
        "marca": None,       # si luego extraes marca, aquí la pones
        "fabricante": None,  # idem
        "categoria": "Lácteos",
        "subcategoria": None,
        "url": clean(url_producto),
        "url_imagen": clean(url_imagen),

        # para histórico
        "precio_lista": clean(precio_base),     # tu precio_base
        "precio_oferta": clean(precio_oferta),  # vacío si no hay
        "tipo_oferta": clean(tipo_oferta),

        # promo_* (por ahora vacíos)
        "promo_tipo": None,
        "promo_texto_regular": None,
        "promo_texto_descuento": None,
        "promo_comentarios": None,
    }


def fetch_page_html(session: requests.Session, url: str, proxies=None, timeout: int = 35) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123 Safari/537.36"
        ),
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = session.get(url, headers=headers, timeout=timeout, proxies=proxies)
    r.raise_for_status()
    return r.text


def scrape_category(start_url_pattern: str, max_pages: int, sleep_min: float, sleep_max: float, proxies=None, debug_html: bool = False) -> pd.DataFrame:
    session = build_session()

    rows: List[Dict[str, Any]] = []
    seen_keys = set()
    empty_pages_in_row = 0

    for page in range(1, max_pages + 1):
        url = start_url_pattern.format(page=page)
        print(f"[+] Pag {page}: {url}")

        html = ""
        for attempt in range(1, 3):
            try:
                html = fetch_page_html(session, url, proxies=proxies)
                break
            except Exception as e:
                print(f"    [!] Error fetch (intento {attempt}/2): {e}")
                time.sleep(2.0 + attempt)

        if not html:
            print("    -> Sin HTML, corto.")
            break

        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("div.producto.item")

        if not items:
            empty_pages_in_row += 1
            print(f"    -> 0 items (vacía #{empty_pages_in_row})")
            if debug_html:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                fname = f"debug_kilbel_pag_{page}_{ts}.html"
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"    -> Guardé HTML debug: {fname}")

            if empty_pages_in_row >= 2:
                print("    -> Corto: 2 páginas seguidas sin items.")
                break
            continue

        empty_pages_in_row = 0

        new_count = 0
        missing_price = 0

        for it in items:
            p = parse_item(it)

            # Dedupe (como tu patrón): sku o record_id o (nombre,url)
            key = p.get("sku") or p.get("record_id") or (p.get("nombre"), p.get("url"))
            if key in seen_keys:
                continue
            seen_keys.add(key)

            if not p.get("precio_lista"):
                missing_price += 1

            rows.append(p)
            new_count += 1

        print(f"    -> items: {len(items)} | nuevos: {new_count} | sin precio_base: {missing_price} | total: {len(rows)}")

        if new_count == 0:
            print("    -> Corto: página sin nuevos (todo repetido).")
            break

        time.sleep(random.uniform(sleep_min, sleep_max))

    return pd.DataFrame(rows)


def build_proxies(args):
    if not args.proxy_host:
        return None
    auth = ""
    if args.proxy_user and args.proxy_pass:
        auth = f"{args.proxy_user}:{args.proxy_pass}@"
    proxy_url = f"http://{auth}{args.proxy_host}:{args.proxy_port}"
    return {"http": proxy_url, "https": proxy_url}


# =========================
# MySQL helpers (MISMA LÓGICA estilo cotoMysql2)
# =========================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]


def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    """
    Mantiene tu lógica:
    - prioriza EAN (aquí suele venir vacío)
    - fallback por (nombre, marca) SOLO si marca no está vacía
    - EXTRA fallback práctico para Kilbel: si NO hay marca, intenta por nombre con marca vacía
      (evita crear miles de productos duplicados cuando solo tienes nombre)
    """
    ean = clean(p.get("ean"))
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("nombre") or "", p.get("marca") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    nombre = clean(p.get("nombre")) or ""
    marca  = clean(p.get("marca")) or ""

    # Regla original: (nombre, marca) solo si marca no vacía
    if nombre and marca:
        cur.execute(
            "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
            (nombre, marca)
        )
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("ean") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    # EXTRA fallback (solo cuando marca está vacía): dedupe suave por nombre + marca vacía
    if nombre and not marca:
        cur.execute(
            "SELECT id FROM productos WHERE nombre=%s AND (marca IS NULL OR marca='') LIMIT 1",
            (nombre,)
        )
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("fabricante") or "",
                p.get("categoria") or "",
                p.get("subcategoria") or "",
                pid
            ))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", nombre, marca,
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid


def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    """
    Upsert que devuelve ID con LAST_INSERT_ID.
    Preferimos clave por sku_tienda si existe; si no, record_id_tienda.
    """
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre") or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda
              (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec or "", url, nombre_tienda))
        return cur.lastrowid

    if rec:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid


def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    """
    Guarda histórico: precio_lista=precio_base, precio_oferta si aplica.
    Normaliza a VARCHAR 2 decimales.
    """
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
        to_varchar_2dec(p.get("precio_lista")),
        to_varchar_2dec(p.get("precio_oferta")),
        p.get("tipo_oferta") or None,
        p.get("promo_tipo") or None,
        p.get("promo_texto_regular") or None,
        p.get("promo_texto_descuento") or None,
        p.get("promo_comentarios") or None,
    ))


# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--sleep-min", type=float, default=0.9)
    ap.add_argument("--sleep-max", type=float, default=1.8)
    ap.add_argument("--debug-html", action="store_true", help="Guarda HTML cuando detecta problemas")

    # proxy opcional
    ap.add_argument("--proxy-host", type=str, default="")
    ap.add_argument("--proxy-port", type=int, default=823)
    ap.add_argument("--proxy-user", type=str, default="")
    ap.add_argument("--proxy-pass", type=str, default="")

    # outputs
    ap.add_argument("--out", type=str, default="", help="Si lo pasas, guarda Excel también (opcional)")

    args = ap.parse_args()
    proxies = build_proxies(args)

    # 1) Scrape
    df = scrape_category(
        start_url_pattern=START,
        max_pages=args.max_pages,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
        proxies=proxies,
        debug_html=args.debug_html,
    )

    if df.empty:
        print("⚠️ No se descargaron productos.")
        return

    # (opcional) Excel
    if args.out:
        df.to_excel(args.out, index=False)
        print(f"[OK] Excel guardado en: {args.out}")

    # 2) Persistencia MySQL (capturado_en común)
    capturado_en = datetime.now()

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        productos: List[Dict[str, Any]] = df.to_dict(orient="records")

        insertados = 0
        for p in productos:
            # df trae precio_lista/oferta ya en columnas; aseguramos nombres esperados
            # (si vienes del DF original, re-mapeamos mínimo)
            if "precio_lista" not in p:
                p["precio_lista"] = p.get("precio_base") or ""
            if "precio_oferta" not in p:
                p["precio_oferta"] = p.get("precio_oferta") or ""

            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn:
            conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
