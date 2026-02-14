#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ktronix (Colombia) - Celulares/Smartphones
- Abre PLP
- Click en "Mostrar más productos" hasta que no haya más
- Junta todos los links de producto (sin bajar imágenes)
- Para cada PDP: EAN, nombre, url, url_imagen, precio_oferta, precio_base, tipo_oferta
- Exporta CSV y XLSX
- ✅ Inserta/actualiza en MySQL con lógica estándar:
  upsert_tienda -> find_or_create_producto -> upsert_producto_tienda -> insert_historico

Optimizado para VPS:
- Playwright bloquea: image / media / font (y opcionalmente css)
- PDP se scrapea con requests (más liviano que Playwright)
"""

import re
import time
import random
import argparse
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Any, Optional

import numpy as np
import requests
import pandas as pd
from bs4 import BeautifulSoup
from mysql.connector import Error as MySQLError

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos_local import get_conn  # <- tu conexión MySQL


BASE = "https://www.ktronix.com"
START_URL = "https://www.ktronix.com/electrodomesticos/pequenos-electrodomesticos/cuidado-del-hogar/c/BI_0560_KTRON"

TIENDA_CODIGO = "ktronix"
TIENDA_NOMBRE = "Ktronix"


# ---------------------------
# Utils
# ---------------------------
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean_text(s: str):
    if not s:
        return None
    s = s.replace("\u00a0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    if s.lower() in _NULLLIKE:
        return None
    return s or None

def parse_money_cop(text: str):
    """Convierte '$2.199.010' -> 2199010 (int). Devuelve None si no hay dígitos."""
    if not text:
        return None
    t = re.sub(r"[^\d]", "", text)
    if not t:
        return None
    try:
        return int(t)
    except:
        return None

def parse_price(x) -> float:
    """Acepta int/float/str como '2.199.010' o '2199010' y devuelve float o NaN."""
    if x is None:
        return np.nan
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return np.nan
    s = re.sub(r"[^\d,.\-]", "", s)
    # Para COP normalmente viene con puntos como miles
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    else:
        # si solo hay puntos, asumimos miles
        if s.count(".") >= 1 and s.count(",") == 0:
            s = s.replace(".", "")
    try:
        return float(s)
    except:
        return np.nan

def to_varchar_2dec(x) -> Optional[str]:
    """Normaliza a '1234.00' o None."""
    v = parse_price(x)
    if isinstance(v, float) and np.isnan(v):
        return None
    return f"{float(v):.2f}"

def abs_url(href: str):
    if not href:
        return None
    return urljoin(BASE, href)

def now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def record_id_from_url(url: str) -> str:
    """
    Genera un record_id_tienda estable desde la URL del producto.
    Ej: https://www.ktronix.com/.../p/12345 -> 'p/12345'
    """
    if not url:
        return ""
    u = url.split("?", 1)[0].rstrip("/")
    path = urlparse(u).path.strip("/")
    return path or u


# ---------------------------
# 1) Playwright: cargar listado + obtener URLs
# ---------------------------
def collect_product_urls_playwright(
    start_url: str,
    max_clicks: int = 500,
    wait_ms: int = 1200,
    headless: bool = True,
    block_css: bool = True,
    stable_rounds_stop: int = 3,
    verbose: bool = True,
):
    urls = set()

    def route_block(route):
        rtype = route.request.resource_type
        if rtype in ("image", "media", "font"):
            return route.abort()
        if block_css and rtype == "stylesheet":
            return route.abort()
        return route.continue_()

    btn_sel = "button.ais-InfiniteHits-loadMore"
    item_sel = "li.ais-InfiniteHits-item a[href]"

    if verbose:
        print(f"[{now_ts()}] [PLP] Abriendo: {start_url}")
        print(f"[{now_ts()}] [PLP] headless={headless} block_css={block_css} max_clicks={max_clicks} wait_ms={wait_ms}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
            viewport={"width": 1366, "height": 768},
        )
        page = context.new_page()
        page.route("**/*", route_block)

        page.goto(start_url, wait_until="domcontentloaded", timeout=60000)

        try:
            page.wait_for_selector(item_sel, timeout=30000)
        except PWTimeout:
            print(f"[{now_ts()}] [PLP] ERROR: No aparecieron productos en el listado (timeout).")
            context.close()
            browser.close()
            return []

        def grab_links():
            anchors = page.locator(item_sel)
            n = anchors.count()
            before = len(urls)
            for i in range(n):
                href = anchors.nth(i).get_attribute("href")
                if href and "/p/" in href:
                    urls.add(abs_url(href.split("?")[0]))
            added = len(urls) - before
            return n, added

        n, added = grab_links()
        if verbose:
            print(f"[{now_ts()}] [PLP] Items visibles: {n} | URLs únicas: {len(urls)} (+{added})")

        last_unique = len(urls)
        stable_rounds = 0
        clicks_done = 0

        for _ in range(max_clicks):
            btn = page.locator(btn_sel).first

            if btn.count() == 0:
                if verbose:
                    print(f"[{now_ts()}] [PLP] No hay botón 'Mostrar más' -> fin.")
                break

            try:
                if not btn.is_visible():
                    if verbose:
                        print(f"[{now_ts()}] [PLP] Botón no visible -> fin.")
                    break
                if btn.is_disabled():
                    if verbose:
                        print(f"[{now_ts()}] [PLP] Botón deshabilitado -> fin.")
                    break

                btn.click(timeout=8000)
                clicks_done += 1
                page.wait_for_timeout(wait_ms)

            except PWTimeout:
                if verbose:
                    print(f"[{now_ts()}] [PLP] Timeout al clickear botón -> fin.")
                break
            except Exception as e:
                if verbose:
                    print(f"[{now_ts()}] [PLP] Exception al clickear botón -> fin. ({type(e).__name__}: {e})")
                break

            n, added = grab_links()
            unique_now = len(urls)

            if verbose:
                print(f"[{now_ts()}] [PLP] click={clicks_done} | visibles={n} | URLs únicas={unique_now} (+{added})")

            if unique_now == last_unique:
                stable_rounds += 1
            else:
                stable_rounds = 0
                last_unique = unique_now

            if stable_rounds >= stable_rounds_stop:
                if verbose:
                    print(f"[{now_ts()}] [PLP] Se estancó {stable_rounds_stop} rondas seguidas (sin URLs nuevas) -> fin.")
                break

            time.sleep(random.uniform(0.05, 0.20))

        n, added = grab_links()
        if verbose:
            print(f"[{now_ts()}] [PLP] Final: URLs únicas={len(urls)} | clicks_done={clicks_done}")

        context.close()
        browser.close()

    return sorted(urls)


# ---------------------------
# 2) Requests: scrape PDP liviano
# ---------------------------
def build_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.7",
    })
    return s

def fetch_html(session: requests.Session, url: str, timeout=25, retries=3):
    last_err = None
    for i in range(retries):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}")
            return r.text
        except Exception as e:
            last_err = e
            if i == retries - 1:
                break
            time.sleep(0.8 * (i + 1) + random.uniform(0.1, 0.4))
    raise RuntimeError(f"No se pudo traer HTML ({type(last_err).__name__}: {last_err})")

def parse_pdp(html: str, url: str):
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.select_one("h1.js-main-title")
    name = clean_text(h1.get_text(" ", strip=True)) if h1 else None

    img = soup.select_one('img[fetchpriority="high"]') or soup.select_one("div.item img")
    img_url = abs_url(img.get("src")) if img and img.get("src") else None

    ean_el = soup.select_one("span.js-ean-pdp")
    ean = clean_text(ean_el.get_text(" ", strip=True)) if ean_el else None
    if ean:
        ean = re.sub(r"[^\d]", "", ean) or None

    offer_el = soup.select_one("span#js-original_price")
    precio_oferta = parse_money_cop(offer_el.get_text(" ", strip=True)) if offer_el else None

    base_el = soup.select_one("span.before-price__basePrice")
    precio_base = parse_money_cop(base_el.get_text(" ", strip=True)) if base_el else None

    off_el = soup.select_one("span.label-offer")
    tipo_oferta = clean_text(off_el.get_text(" ", strip=True)) if off_el else None

    if precio_base is None and precio_oferta is not None:
        precio_base = precio_oferta

    return {
        "ean": ean,
        "nombre_producto": name,
        "url": url,
        "url_imagen": img_url,
        "precio_oferta": precio_oferta,
        "precio_base": precio_base,
        "tipo_oferta": tipo_oferta,
        # para DB:
        "record_id": record_id_from_url(url),
        "sku": None,            # Ktronix no expone sku fácil (si lo encontrás, lo conectamos)
        "marca": None,
        "fabricante": None,
        "categoria": "Celulares",
        "subcategoria": "Smartphones",
    }


# ---------------------------
# 3) MySQL helpers (lógica estándar)
# ---------------------------
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean_text(p.get("ean"))
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
                p.get("nombre_producto") or "", p.get("marca") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    nombre = clean_text(p.get("nombre_producto")) or ""
    marca  = clean_text(p.get("marca")) or ""
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca))
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

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", nombre, marca,
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean_text(p.get("sku"))
    rec = clean_text(p.get("record_id"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre_producto") or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid

    # preferimos record_id_tienda si existe
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

    # fallback: sin llaves (mal), igual insert
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    """
    historico_precios:
      - precio_lista: en Ktronix = precio_base
      - precio_oferta: en Ktronix = precio_oferta (si difiere)
      - tipo_oferta: texto tipo "-47%"
      - promo_*: no aplica acá => NULL
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
        to_varchar_2dec(p.get("precio_base")),               # lista/base
        to_varchar_2dec(p.get("precio_oferta")),             # oferta (puede ser None)
        p.get("tipo_oferta") or None,
        None, None, None, None
    ))


# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=START_URL)
    ap.add_argument("--out", default="ktronix_smartphones")
    ap.add_argument("--max_clicks", type=int, default=500)
    ap.add_argument("--wait_ms", type=int, default=1200)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--headless", action="store_true", default=True)
    ap.add_argument("--no_headless", action="store_true", default=False)
    ap.add_argument("--block_css", action="store_true", default=True)
    ap.add_argument("--no_block_css", action="store_true", default=False)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=25)

    # ✅ Control DB
    ap.add_argument("--no_db", action="store_true", default=False, help="No insertar en MySQL")
    ap.add_argument("--commit_every", type=int, default=200, help="Mini-commit cada N filas (0=solo al final)")
    args = ap.parse_args()

    headless = not args.no_headless
    block_css = not args.no_block_css

    print("=" * 80)
    print(f"[{now_ts()}] KTRONIX SCRAPER - START")
    print(f"[{now_ts()}] URL: {args.url}")
    print(f"[{now_ts()}] out: {args.out}")
    print(f"[{now_ts()}] headless={headless} block_css={block_css} workers={args.workers}")
    print(f"[{now_ts()}] db={'OFF' if args.no_db else 'ON'} commit_every={args.commit_every}")
    print("=" * 80)

    # 1) PLP -> URLs
    print(f"[{now_ts()}] [1/4] Recolectando URLs desde PLP...")
    t0 = time.time()
    product_urls = collect_product_urls_playwright(
        args.url,
        max_clicks=args.max_clicks,
        wait_ms=args.wait_ms,
        headless=headless,
        block_css=block_css,
        verbose=True,
    )
    t1 = time.time()
    print(f"[{now_ts()}] [1/4] URLs encontradas: {len(product_urls)} | tiempo={t1 - t0:.1f}s")

    if not product_urls:
        print(f"[{now_ts()}] ERROR: No se encontraron URLs. (bloqueo / selector cambió / no cargó DOM)")
        return

    # 2) PDP -> requests
    print(f"[{now_ts()}] [2/4] Scrapeando PDP (requests, sin bajar imágenes)...")
    session = build_session()

    rows = []
    ok = 0
    fail = 0

    def job(u):
        html = fetch_html(session, u, timeout=args.timeout, retries=args.retries)
        return parse_pdp(html, u)

    t2 = time.time()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(job, u): u for u in product_urls}
        total = len(futs)

        for idx, fut in enumerate(as_completed(futs), start=1):
            u = futs[fut]
            try:
                row = fut.result()
                rows.append(row)
                ok += 1
            except Exception as e:
                rows.append({
                    "ean": None,
                    "nombre_producto": None,
                    "url": u,
                    "url_imagen": None,
                    "precio_oferta": None,
                    "precio_base": None,
                    "tipo_oferta": None,
                    "record_id": record_id_from_url(u),
                    "error": f"{type(e).__name__}: {e}",
                })
                fail += 1

            if idx % 25 == 0 or idx == total:
                print(f"[{now_ts()}] [PDP] progreso {idx}/{total} | ok={ok} fail={fail}")

    t3 = time.time()
    print(f"[{now_ts()}] [2/4] PDP listo | ok={ok} fail={fail} | tiempo={t3 - t2:.1f}s")

    # 3) Export
    print(f"[{now_ts()}] [3/4] Exportando CSV/XLSX...")
    df = pd.DataFrame(rows)

    # Si precio_base == precio_oferta, ocultar precio_oferta (ponerla en None)
    if "precio_base" in df.columns and "precio_oferta" in df.columns:
        df["precio_oferta"] = df["precio_oferta"].where(df["precio_oferta"] != df["precio_base"], None)

    cols = ["ean", "nombre_producto", "precio_oferta", "precio_base", "tipo_oferta", "url", "url_imagen"]
    df = df[[c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]]

    ean_ok = int(df["ean"].notna().sum()) if "ean" in df.columns else 0
    precio_ok = int(df["precio_base"].notna().sum()) if "precio_base" in df.columns else 0
    print(f"[{now_ts()}] [RESUMEN] filas={len(df)} | con_ean={ean_ok} | con_precio_base={precio_ok} | fallas={fail}")

    csv_path = f"{args.out}.csv"
    xlsx_path = f"{args.out}.xlsx"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    print(f"[{now_ts()}] CSV : {csv_path}")
    print(f"[{now_ts()}] XLSX: {xlsx_path}")

    # 4) MySQL insert
    if args.no_db:
        print(f"[{now_ts()}] [4/4] MySQL desactivado (--no_db). Fin.")
        return

    print(f"[{now_ts()}] [4/4] Guardando en MySQL...")
    capturado_en = datetime.now()

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        inserted = 0
        for i, p in enumerate(rows, start=1):
            # saltar filas rotas (sin nombre ni precio ni url)
            if not p.get("url"):
                continue

            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            inserted += 1

            if args.commit_every and (i % int(args.commit_every) == 0):
                conn.commit()
                print(f"[{now_ts()}] [DB] mini-commit {i} | inserted={inserted}")

        conn.commit()
        print(f"[{now_ts()}] 💾 Guardado en MySQL: {inserted} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn:
            conn.rollback()
        print(f"[{now_ts()}] ❌ Error MySQL: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    print("=" * 80)
    print(f"[{now_ts()}] KTRONIX SCRAPER - END")
    print("=" * 80)


if __name__ == "__main__":
    main()
