#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import asyncio
import time
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL = "https://www.alkosto.com/computadores-tablet/c/BI_COMP_ALKOS"
BASE = "https://www.alkosto.com"

# =========================
# Utils
# =========================
def clean_text(s: str):
    if not s:
        return None
    s = s.replace("\u00a0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

def parse_money_co(text: str):
    if not text:
        return None
    t = re.sub(r"[^\d]", "", text)
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

# =========================
# Listado: cargar links
# =========================
async def collect_all_listing_links(page, max_clicks=200):
    product_link_sel = 'li.ais-InfiniteHits-item a.product__item__top__link'
    load_more_sel = 'button.ais-InfiniteHits-loadMore.js-load-more'

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
        tag = f"P{worker_id:02d}"
        print(f"\n[{tag}] 🔗 Abriendo PDP: {url}")

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(600)

            nombre = await safe_text(page, "h1.js-main-title", timeout=15000)
            print(f"[{tag}]   📝 Nombre: {nombre}")

            ean = await safe_text(page, "span.js-ean-pdp", timeout=8000)
            print(f"[{tag}]   🧾 EAN: {ean}")

            precio_raw = await safe_text(page, "span#js-original_price", timeout=8000)
            precio_base = parse_money_co(precio_raw)
            print(f"[{tag}]   💰 Precio raw: {precio_raw}")
            print(f"[{tag}]   💵 Precio parseado: {precio_base}")

            img_src = await safe_attr(page, "img.owl-lazy", "src", timeout=8000)
            if img_src:
                print(f"[{tag}]   🖼️ Imagen encontrada (owl-lazy)")
            else:
                img_src = await safe_attr(page, 'img[fetchpriority="high"]', "src", timeout=5000)
                print(f"[{tag}]   🖼️ Imagen fallback usada" if img_src else f"[{tag}]   ❌ Imagen NO encontrada")

            imagen_url = urljoin(BASE, img_src) if img_src else None
            item_url = page.url

            if polite_delay:
                await asyncio.sleep(polite_delay)

            return {
                "nombre_producto_co": nombre,
                "ean": ean,
                "precio_base": precio_base,
                "precio_raw": precio_raw,
                "imagen_url": imagen_url,
                "item_url": item_url,
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
# Main
# =========================
async def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = f"alkosto_computadores_{ts}.xlsx"

    # 🔧 Ajusta esto:
    CONCURRENCY = 8          # 4-12 recomendado
    POLITE_DELAY = 0.0       # si te bloquean, pon 0.2–0.5

    sem = asyncio.Semaphore(CONCURRENCY)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36")
        )

        page = await context.new_page()

        print(f"🚀 Abriendo listado principal:\n{START_URL}")
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1000)

        links = await collect_all_listing_links(page)
        print(f"\n✅ TOTAL LINKS FINALES: {len(links)}")

        # ---- Paralelo: crea tasks
        tasks = []
        for i, link in enumerate(links, start=1):
            # worker_id solo para prints (rota 1..CONCURRENCY)
            worker_id = (i % CONCURRENCY) or CONCURRENCY
            tasks.append(scrape_product(context, link, worker_id, sem, polite_delay=POLITE_DELAY))

        print(f"\n⚡ Scrape paralelo iniciado (concurrency={CONCURRENCY}) ...")
        results = await asyncio.gather(*tasks)

        # ---- Dedupe por EAN si existe, si no por URL
        rows = []
        seen_key = set()
        for r in results:
            key = r.get("ean") or r.get("item_url")
            if not key or key in seen_key:
                continue
            seen_key.add(key)
            rows.append(r)

        df = pd.DataFrame(rows)
        cols = ["ean", "nombre_producto_co", "precio_base", "precio_raw", "imagen_url", "item_url"]
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]

        df.to_excel(out_xlsx, index=False)
        print(f"\n📊 Excel creado: {out_xlsx}")
        print(f"📌 Registros guardados: {len(df)}")

        await context.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
