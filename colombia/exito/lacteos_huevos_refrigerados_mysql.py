#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Éxito (tecnología) → Playwright → MySQL + Excel

Inserta con tu lógica estándar:
1) upsert_tienda(codigo,nombre) -> tienda_id
2) find_or_create_producto (prioriza EAN; fallback (nombre,marca) solo si marca no está vacía; updates parciales)
3) upsert_producto_tienda (prioriza (tienda_id, sku_tienda=PLU si existe); si no, fallback (tienda_id, record_id_tienda=url))
   - Recupera id vía LAST_INSERT_ID
   - Completa url/nombre con COALESCE
   - Si hay SKU, NO pisa producto_id en ON DUPLICATE (regla tipo Kilbel)
4) insert_historico (precios como VARCHAR 2 decimales; capturado_en común por corrida)
"""

import re
import os
import sys
import time
import random
import argparse
from datetime import datetime
from urllib.parse import urljoin
from typing import Dict, Any, Optional, List

import numpy as np
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from mysql.connector import Error as MySQLError

BASE = "https://www.exito.com"

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos_local import get_conn  # <- tu conexión MySQL

TIENDA_CODIGO = "exito"
TIENDA_NOMBRE = "Exito"

# -------------------------
# Utils
# -------------------------
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean_text(s: str):
    if not s:
        return None
    s = s.replace("\u00a0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

def parse_money_co(text: str) -> Optional[int]:
    # "$ 2.299.900" -> 2299900
    if not text:
        return None
    t = re.sub(r"[^\d]", "", str(text))
    if not t:
        return None
    try:
        return int(t)
    except:
        return None

def safe_inner_text(locator, timeout=800):
    try:
        return clean_text(locator.inner_text(timeout=timeout))
    except:
        return None

def safe_attr(locator, name: str, timeout=800):
    try:
        return locator.get_attribute(name, timeout=timeout)
    except:
        return None

def get_ean_fast(page, retries=2, wait_ms=200):
    """
    1) script[data-ean] / script[data-flix-ean]
    2) JSON-LD gtin13
    """
    def try_data_attrs():
        try:
            loc = page.locator("script[data-ean]").first
            val = clean_text(loc.get_attribute("data-ean", timeout=500))
            if val:
                return val
        except:
            pass
        try:
            loc = page.locator("script[data-flix-ean]").first
            val = clean_text(loc.get_attribute("data-flix-ean", timeout=500))
            if val:
                return val
        except:
            pass
        return None

    def try_jsonld():
        try:
            blocks = page.locator("script[type='application/ld+json']").all_inner_texts()
            for b in blocks:
                m = re.search(r'"gtin13"\s*:\s*"(\d{13})"', b)
                if m:
                    return m.group(1)
        except:
            pass
        return None

    e = try_data_attrs()
    if e:
        return e

    for _ in range(retries):
        try:
            page.wait_for_selector(
                "script[data-ean], script[data-flix-ean], script[type='application/ld+json']",
                timeout=1500
            )
        except:
            pass
        page.wait_for_timeout(wait_ms)
        e = try_data_attrs()
        if e:
            return e

    return try_jsonld()

# -------------------------
# Listing
# -------------------------
def collect_product_links(page, listing_url: str):
    page.goto(listing_url, wait_until="domcontentloaded", timeout=45000)

    try:
        page.wait_for_selector("article[class*='productCard_productCard']", timeout=12000)
    except PWTimeout:
        return []

    # mini scroll para lazy load
    for _ in range(2):
        try:
            page.mouse.wheel(0, 1200)
        except:
            pass
        page.wait_for_timeout(250)

    cards = page.locator("article[class*='productCard_productCard']")
    n = cards.count()
    if n == 0:
        return []

    links = set()
    for i in range(n):
        a = cards.nth(i).locator("a[data-testid='product-link']").first
        href = safe_attr(a, "href")
        if href:
            full = urljoin(BASE, href)
            if "/p" in full:
                links.add(full)

    return sorted(links)

# -------------------------
# MySQL helpers (upserts)
# -------------------------
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
    Schema esperado (como tus scrapers): productos(ean,nombre,marca,fabricante,categoria,subcategoria,...)
    - Éxito no siempre da fabricante/categoría/subcategoría en PDP, se guardan si llegan.
    """
    def c(x): return clean_text(x) or ""

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
                c(p.get("nombre")), c(p.get("marca")), c(p.get("fabricante")),
                c(p.get("categoria")), c(p.get("subcategoria")), pid
            ))
            return pid

    nombre = clean_text(p.get("nombre")) or ""
    marca  = clean_text(p.get("marca")) or ""

    # Fallback por (nombre, marca) SOLO si marca no está vacía
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
                c(p.get("ean")), c(p.get("fabricante")),
                c(p.get("categoria")), c(p.get("subcategoria")), pid
            ))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        c(p.get("ean")), nombre, marca,
        c(p.get("fabricante")), c(p.get("categoria")), c(p.get("subcategoria"))
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    """
    Regla:
    - sku_tienda = PLU si existe (mejor llave estable)
    - si no hay PLU, fallback: record_id_tienda = URL
    - Recupera id con LAST_INSERT_ID
    - Si hay SKU, NO pisa producto_id (mantener vínculo existente)
    """
    sku = clean_text(p.get("sku_tienda"))  # PLU
    rec = clean_text(p.get("record_id_tienda"))  # URL
    url = clean_text(p.get("url")) or ""
    nombre_tienda = clean_text(p.get("nombre")) or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              -- NO actualizar producto_id si ya existe por SKU (regla tipo Kilbel)
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
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

    # último recurso
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    def to_varchar_2dec_from_int(n: Optional[int]) -> Optional[str]:
        if n is None:
            return None
        try:
            return f"{float(n):.2f}"
        except:
            return None

    # Mapeo:
    # - precio_lista: precio_tachado (si existe)
    # - precio_oferta: precio actual
    # - tipo_oferta: descuento_pct como texto (o None)
    # - promo_texto_regular / promo_texto_descuento: guardamos textos si existen
    precio_lista = to_varchar_2dec_from_int(p.get("precio_tachado"))
    precio_oferta = to_varchar_2dec_from_int(p.get("precio"))

    tipo_oferta = None
    if p.get("descuento_pct") is not None:
        tipo_oferta = f"{p.get('descuento_pct')}%"

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
        precio_lista,
        precio_oferta,
        tipo_oferta,
        None,  # promo_tipo (no lo tenemos estructurado aquí)
        p.get("precio_tachado_texto") or None,  # texto regular
        p.get("precio_texto") or None,          # texto descuento/actual
        p.get("vendedor") or None,              # comentarios: vendedor
    ))

# -------------------------
# Main scraping + MySQL
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=30)
    ap.add_argument("--sleep-min", type=float, default=0.1)
    ap.add_argument("--sleep-max", type=float, default=0.25)
    ap.add_argument("--concurrency", type=int, default=6, help="Pestañas paralelas.")
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--include-images", action="store_true", help="Más lento. Si no, se bloquean imágenes.")
    ap.add_argument("--no-mysql", action="store_true", help="Solo Excel, no inserta en DB.")
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out.strip() or f"exito_tecnologia_{ts}.xlsx"

    all_rows: List[Dict[str, Any]] = []
    seen_urls = set()

    # capturado_en común para TODA la corrida (como tus scrapers)
    capturado_en_dt = datetime.now()
    capturado_en_str = capturado_en_dt.strftime("%Y-%m-%d %H:%M:%S")

    # ----------------- Scrape -----------------
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="es-CO",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        def route_handler(route):
            r = route.request
            rt = r.resource_type
            url = r.url.lower()

            if not args.include_images:
                if rt in ("image", "media", "font"):
                    return route.abort()

            if rt == "stylesheet":
                return route.abort()

            if any(x in url for x in ("googletagmanager", "google-analytics", "doubleclick", "facebook", "hotjar")):
                return route.abort()

            return route.continue_()

        context.route("**/*", route_handler)

        listing_page = context.new_page()

        for page_num in range(1, args.max_pages + 1):
            listing_url = (
                f"{BASE}/lacteos-huevos-y-refrigerados?"
                f"category-1=mercado&category-2=lacteos-huevos-y-refrigerados&facets=category-1%2Ccategory-2&sort=score_desc&page={page_num}"
            )

            print(f"\n🧾 LISTADO página {page_num}: {listing_url}")
            links = collect_product_links(listing_page, listing_url)
            links = [u for u in links if u not in seen_urls]
            for u in links:
                seen_urls.add(u)

            print(f"   🔎 Productos nuevos: {len(links)}")
            if not links:
                print("   ✅ No hay productos (o no cargó). Corto.")
                break

            # batches
            for i in range(0, len(links), args.concurrency):
                batch = links[i:i + args.concurrency]

                tab_objs = []
                for url in batch:
                    tab = context.new_page()
                    tab_objs.append((url, tab))
                    try:
                        tab.goto(url, wait_until="domcontentloaded", timeout=45000)
                    except:
                        pass

                for url, tab in tab_objs:
                    try:
                        try:
                            tab.wait_for_selector("section[data-fs-product-container='true']", timeout=3500)
                        except:
                            pass

                        title = safe_inner_text(tab.locator("h1.product-title_product-title__heading___mpLA").first)
                        specification = safe_inner_text(tab.locator("h2.product-title_product-title__specification__UTjNc").first)

                        brand = None
                        if specification and "-" in specification:
                            brand = clean_text(specification.split("-")[0])

                        plu = None
                        if specification:
                            m = re.search(r"PLU:\s*([0-9]+)", specification)
                            if m:
                                plu = m.group(1)

                        price_now_txt = safe_inner_text(tab.locator("p.ProductPrice_container__price__XmMWA").first)
                        price_old_txt = safe_inner_text(tab.locator("p.priceSection_container-promotion_price-dashed__FJ7nI").first)
                        discount_pct_txt = safe_inner_text(tab.locator("span[data-percentage='true']").first)

                        price_now = parse_money_co(price_now_txt)
                        price_old = parse_money_co(price_old_txt)

                        discount_pct = None
                        if discount_pct_txt and discount_pct_txt.isdigit():
                            discount_pct = int(discount_pct_txt)

                        seller = safe_inner_text(tab.locator("section.seller-information_fs-seller-information__3otO1 h3").first)
                        if seller:
                            seller = seller.replace("Vendido por:", "").strip()

                        ean = get_ean_fast(tab, retries=2, wait_ms=200)

                        img_urls = None
                        if args.include_images:
                            urls = []
                            thumbs = tab.locator("div[data-fs-thumb-container='true'] img")
                            for k in range(min(thumbs.count(), 12)):
                                src = safe_attr(thumbs.nth(k), "src")
                                if src and src not in urls:
                                    urls.append(src)
                            img_urls = " | ".join(urls) if urls else None

                        row = {
                            "capturado_en": capturado_en_str,
                            "marca": brand,
                            "titulo": title,
                            "plu": plu,
                            "ean": ean,
                            "precio": price_now,
                            "precio_tachado": price_old,
                            "descuento_pct": discount_pct,
                            "vendedor": seller,
                            "url": url,
                            "imagenes": img_urls,
                            "precio_texto": price_now_txt,
                            "precio_tachado_texto": price_old_txt,
                        }

                        all_rows.append(row)
                        print(f"      ✅ OK | {title!r} | ${price_now} | ean={ean} | plu={plu}")
                    except Exception as e:
                        print(f"      ❌ ERROR en {url}: {e}")
                    finally:
                        try:
                            tab.close()
                        except:
                            pass

                time.sleep(random.uniform(args.sleep_min, args.sleep_max))

            time.sleep(random.uniform(0.4, 0.9))

        try:
            listing_page.close()
        except:
            pass

        browser.close()

    # ----------------- Excel -----------------
    df = pd.DataFrame(all_rows)
    cols = [
        "capturado_en", "marca", "titulo", "plu", "ean",
        "precio", "precio_tachado", "descuento_pct",
        "vendedor", "url", "imagenes",
        "precio_texto", "precio_tachado_texto",
    ]
    if not df.empty:
        df = df[[c for c in cols if c in df.columns]]
    df.to_excel(out_xlsx, index=False)
    print(f"\n📦 Listo. Productos: {len(df)}")
    print(f"📄 Excel guardado en: {out_xlsx}")

    # ----------------- MySQL insert -----------------
    if args.no_mysql:
        print("🧠 --no-mysql activado: no inserto en DB.")
        return

    if df.empty:
        print("⚠️ No hay filas para insertar.")
        return

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for row in all_rows:
            # Map al esquema estándar que esperan tus helpers
            p_prod = {
                "ean": row.get("ean"),
                "nombre": row.get("titulo"),
                "marca": row.get("marca"),
                "fabricante": None,
                "categoria": "tecnologia",
                "subcategoria": None,
            }
            producto_id = find_or_create_producto(cur, p_prod)

            # producto_tienda: sku_tienda=PLU si existe; fallback record_id_tienda=url
            p_pt = {
                "sku_tienda": row.get("plu"),
                "record_id_tienda": row.get("url"),
                "url": row.get("url"),
                "nombre": row.get("titulo"),
            }
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p_pt)

            # historico
            insert_historico(cur, tienda_id, pt_id, row, capturado_en_dt)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} filas histórico para {TIENDA_NOMBRE} ({capturado_en_str})")

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
