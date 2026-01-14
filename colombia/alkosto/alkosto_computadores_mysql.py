#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Alkosto (computadores-tablet) → Playwright (async) → MySQL + Excel

Inserta con tu lógica estándar:
1) upsert_tienda(codigo,nombre) -> tienda_id
2) find_or_create_producto (prioriza EAN; fallback (nombre,marca) solo si marca no está vacía; updates parciales)
3) upsert_producto_tienda (prioriza (tienda_id, sku_tienda si existe); si no, fallback (tienda_id, record_id_tienda=url))
   - Recupera id vía LAST_INSERT_ID
   - Completa url/nombre con COALESCE
   - Si hay SKU, NO pisa producto_id en ON DUPLICATE (regla tipo Kilbel)
4) insert_historico (precios como VARCHAR 2 decimales; capturado_en común por corrida)
"""

import re
import os
import sys
import time
import asyncio
import random
import argparse
from datetime import datetime
from urllib.parse import urljoin
from typing import Dict, Any, Optional, List

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from mysql.connector import Error as MySQLError

START_URL = "https://www.alkosto.com/computadores-tablet/c/BI_COMP_ALKOS"
BASE = "https://www.alkosto.com"

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos_local import get_conn  # <- tu conexión MySQL

TIENDA_CODIGO = "alkosto"
TIENDA_NOMBRE = "Alkosto"

# =========================
# Utils
# =========================
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean_text(s: str):
    if not s:
        return None
    s = s.replace("\u00a0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

def parse_money_co(text: str) -> Optional[int]:
    if not text:
        return None
    t = re.sub(r"[^\d]", "", str(text))
    if not t:
        return None
    try:
        return int(t)
    except:
        return None

async def safe_text(page, selector: str, timeout=5000):
    try:
        el = page.locator(selector).first
        await el.wait_for(state="attached", timeout=timeout)
        return clean_text(await el.inner_text(timeout=timeout))
    except:
        return None

async def safe_attr(page, selector: str, attr: str, timeout=5000):
    try:
        el = page.locator(selector).first
        await el.wait_for(state="attached", timeout=timeout)
        return await el.get_attribute(attr, timeout=timeout)
    except:
        return None

async def get_sku_fast(page) -> Optional[str]:
    """
    Intenta extraer un SKU/código estable (si existe).
    Si no aparece, devolvemos None y usaremos record_id_tienda=url.
    """
    # 1) Selectores típicos (pueden variar)
    selectors = [
        "span.js-sku-pdp",
        "span.js-product-sku",
        "[data-testid='sku']",
        "meta[itemprop='sku']",
    ]

    for sel in selectors:
        try:
            if sel.startswith("meta"):
                v = await page.locator(sel).first.get_attribute("content", timeout=800)
                v = clean_text(v)
            else:
                v = await safe_text(page, sel, timeout=1200)
            if v:
                # normaliza: deja solo letras/números/guiones
                vv = re.sub(r"[^\w\-]", "", v)
                return vv or v
        except:
            pass

    # 2) JSON-LD: "sku":"..."
    try:
        blocks = await page.locator("script[type='application/ld+json']").all_inner_texts()
        for b in blocks:
            m = re.search(r'"sku"\s*:\s*"([^"]+)"', b)
            if m:
                v = clean_text(m.group(1))
                if v:
                    vv = re.sub(r"[^\w\-]", "", v)
                    return vv or v
    except:
        pass

    return None

# =========================
# Listado: cargar links
# =========================
async def collect_all_listing_links(page, max_clicks=200):
    product_link_sel = "li.ais-InfiniteHits-item a.product__item__top__link"
    load_more_sel = "button.ais-InfiniteHits-loadMore.js-load-more"

    seen = set()
    stagnant_rounds = 0

    print("🧭 Esperando productos del listado...")
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(800)

    try:
        await page.locator(product_link_sel).first.wait_for(timeout=15000)
        print("✅ Primer producto detectado")
    except:
        print("⚠️ No se detectaron productos inicialmente")

    for round_i in range(max_clicks):
        links = await page.locator(product_link_sel).all()
        before = len(seen)

        print(f"\n📄 Ronda {round_i+1}")
        print(f"   🔎 Productos visibles: {len(links)}")

        for a in links:
            href = await a.get_attribute("href")
            if not href:
                continue
            full = urljoin(BASE, href)
            seen.add(full)

        after = len(seen)
        print(f"   ➕ Nuevos links: {after - before}")
        print(f"   📦 Total acumulado: {after}")

        if after == before:
            stagnant_rounds += 1
            print(f"   💤 Sin nuevos productos ({stagnant_rounds}/3)")
        else:
            stagnant_rounds = 0

        btn = page.locator(load_more_sel).first
        if await btn.count() == 0:
            print("⛔ Botón 'Mostrar más productos' NO existe")
            break

        try:
            if not await btn.is_visible():
                print("⛔ Botón 'Mostrar más' no visible")
                break
        except:
            print("⛔ Error verificando botón")
            break

        if stagnant_rounds >= 3:
            print("⛔ No aparecen más productos, deteniendo carga")
            break

        print("➡️ Click en 'Mostrar más productos'")
        try:
            await btn.click(timeout=7000)
            await page.wait_for_timeout(1200)
            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(800)
        except Exception as e:
            print(f"❌ Error clickeando botón: {e}")
            break

    return sorted(seen)

# =========================
# PDP: scrape paralelo
# =========================
async def scrape_product(context, url: str, worker_id: int, sem: asyncio.Semaphore, polite_delay=0.0):
    async with sem:
        page = await context.new_page()
        tag = f"P{worker_id:04d}"
        print(f"\n[{tag}] 🔗 Abriendo PDP: {url}")

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(600)

            nombre = await safe_text(page, "h1.js-main-title", timeout=15000)
            ean = await safe_text(page, "span.js-ean-pdp", timeout=8000)

            # SKU/código (si existe)
            sku = await get_sku_fast(page)

            # -------------------------
            # PRECIOS / OFERTAS
            # -------------------------
            precio_raw = await safe_text(page, "span#js-original_price", timeout=8000)  # actual visible
            precio_actual = parse_money_co(precio_raw)

            base_price_raw = await safe_text(page, "span.before-price__basePrice", timeout=8000)
            tipo_oferta = await safe_text(page, "span.label-offer", timeout=8000)

            hay_oferta = bool(base_price_raw or tipo_oferta)

            if hay_oferta:
                precio_base = parse_money_co(base_price_raw) or precio_actual
                precio_oferta = precio_actual
            else:
                precio_base = precio_actual
                precio_oferta = precio_base
                tipo_oferta = None

            # =========================
            # IMAGEN (robusto)
            # =========================
            img_src = None
            candidates = [
                'img.owl-lazy[fetchpriority="high"]',
                'img[fetchpriority="high"]',
                'img.owl-lazy',
            ]

            for sel in candidates:
                loc = page.locator(sel)
                try:
                    cnt = await loc.count()
                    if cnt == 0:
                        continue

                    for i_img in range(min(cnt, 8)):
                        el = loc.nth(i_img)
                        try:
                            await el.wait_for(state="attached", timeout=3000)
                        except:
                            pass

                        s = await el.get_attribute("src")
                        if not s:
                            s = await el.get_attribute("data-src")
                        if not s:
                            s = await el.get_attribute("data-original")

                        if s and not s.strip().startswith("data:"):
                            img_src = s.strip()
                            break

                    if img_src:
                        break
                except:
                    continue

            imagen_url = urljoin(BASE, img_src) if img_src else None

            if polite_delay:
                await asyncio.sleep(polite_delay)

            return {
                "capturado_en": None,  # se llena al final con el capturado_en común
                "nombre_producto_co": nombre,
                "ean": ean,
                "sku_tienda": sku,
                "precio_base": precio_base,
                "precio_raw": precio_raw,
                "precio_oferta": precio_oferta,
                "tipo_oferta": tipo_oferta,
                "imagen_url": imagen_url,
                "item_url": url,
            }

        except PWTimeout:
            print(f"[{tag}] ⏱️ Timeout en PDP")
            return {"item_url": url, "error": "timeout"}

        except Exception as e:
            print(f"[{tag}] ❌ Error PDP: {e}")
            return {"item_url": url, "error": str(e)[:250]}

        finally:
            await page.close()

# =========================
# MySQL helpers (upserts)
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
    productos(ean,nombre,marca,fabricante,categoria,subcategoria,...)
    - Prioriza EAN
    - Fallback por (nombre, marca) SOLO si marca no está vacía
    - Updates parciales con COALESCE(NULLIF(...), campo)
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
    marca = clean_text(p.get("marca")) or ""

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
    - sku_tienda si existe (ideal llave estable)
    - si no hay SKU: record_id_tienda = URL
    - Recupera id con LAST_INSERT_ID
    - Completa url/nombre con COALESCE
    - Si hay SKU, NO pisa producto_id (regla tipo Kilbel)
    """
    sku = clean_text(p.get("sku_tienda"))
    rec = clean_text(p.get("record_id_tienda"))
    url = clean_text(p.get("url")) or ""
    nombre_tienda = clean_text(p.get("nombre")) or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              -- NO actualizar producto_id si ya existe por SKU
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

    precio_lista = to_varchar_2dec_from_int(p.get("precio_base"))
    precio_oferta = to_varchar_2dec_from_int(p.get("precio_oferta"))

    tipo_oferta = clean_text(p.get("tipo_oferta"))

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
        None,
        p.get("precio_raw") or None,   # texto "regular" (lo que viste en página)
        None,                          # texto descuento (si lo sacas luego)
        None,
    ))

# =========================
# Main
# =========================
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-clicks", type=int, default=200, help="Rondas de 'Mostrar más'.")
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--polite-delay", type=float, default=0.0)
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--no-mysql", action="store_true", help="Solo Excel, no inserta en DB.")
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out.strip() or f"alkosto_computadores_{ts}.xlsx"

    # capturado_en común para TODA la corrida
    capturado_en_dt = datetime.now()
    capturado_en_str = capturado_en_dt.strftime("%Y-%m-%d %H:%M:%S")

    sem = asyncio.Semaphore(args.concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            locale="es-CO",
        )

        page = await context.new_page()
        print(f"🚀 Abriendo listado principal:\n{START_URL}")
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1000)

        links = await collect_all_listing_links(page, max_clicks=args.max_clicks)
        print(f"\n✅ TOTAL LINKS FINALES: {len(links)}")

        tasks = []
        for i, link in enumerate(links, start=1):
            tasks.append(scrape_product(context, link, i, sem, polite_delay=args.polite_delay))

        print(f"\n⚡ Scrape paralelo iniciado (concurrency={args.concurrency}) ...")
        results = await asyncio.gather(*tasks)

        # Dedupe por EAN si existe, si no por URL
        rows = []
        seen_key = set()
        for r in results:
            if not isinstance(r, dict):
                continue
            if r.get("error"):
                continue
            key = r.get("ean") or r.get("item_url")
            if not key or key in seen_key:
                continue
            seen_key.add(key)
            r["capturado_en"] = capturado_en_str
            rows.append(r)

        df = pd.DataFrame(rows)

        cols = [
            "capturado_en",
            "ean",
            "sku_tienda",
            "nombre_producto_co",
            "precio_base",
            "precio_oferta",
            "tipo_oferta",
            "precio_raw",
            "imagen_url",
            "item_url",
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]

        df.to_excel(out_xlsx, index=False)
        print(f"\n📊 Excel creado: {out_xlsx}")
        print(f"📌 Registros guardados: {len(df)}")

        await context.close()
        await browser.close()

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
        for _, row in df.iterrows():
            # Producto (tu esquema estándar)
            p_prod = {
                "ean": row.get("ean"),
                "nombre": row.get("nombre_producto_co"),
                "marca": None,                 # Alkosto en esta versión no lo extraemos (para no forzar fallback)
                "fabricante": None,
                "categoria": "computadores-tablet",
                "subcategoria": None,
            }
            producto_id = find_or_create_producto(cur, p_prod)

            # producto_tienda: sku_tienda si existe; si no fallback record_id_tienda=url
            p_pt = {
                "sku_tienda": row.get("sku_tienda"),
                "record_id_tienda": row.get("item_url"),
                "url": row.get("item_url"),
                "nombre": row.get("nombre_producto_co"),
            }
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p_pt)

            # histórico
            insert_historico(cur, tienda_id, pt_id, {
                "precio_base": row.get("precio_base"),
                "precio_oferta": row.get("precio_oferta"),
                "tipo_oferta": row.get("tipo_oferta"),
                "precio_raw": row.get("precio_raw"),
            }, capturado_en_dt)

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
        except:
            pass


if __name__ == "__main__":
    asyncio.run(main())
