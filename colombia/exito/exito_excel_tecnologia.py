#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import random
import argparse
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE = "https://www.exito.com"

# -------------------------
# Utils
# -------------------------
def clean_text(s: str):
    if not s:
        return None
    s = s.replace("\u00a0", " ").strip()
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
    Versión rápida:
    1) script[data-ean] / script[data-flix-ean]
    2) JSON-LD gtin13
    Reintentos pocos y sin scroll fuerte.
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
            page.wait_for_selector("script[data-ean], script[data-flix-ean], script[type='application/ld+json']",
                                   timeout=1500)
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

    # mini scroll para lazy load (poco)
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
# PDP (detail)
# -------------------------
def scrape_product_detail_fast(context, url: str, include_images: bool = False):
    page = context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)

        # intenta esperar poco el container; si no, seguimos igual
        try:
            page.wait_for_selector("section[data-fs-product-container='true']", timeout=6000)
        except:
            pass

        title = safe_inner_text(page.locator("h1.product-title_product-title__heading___mpLA").first)
        specification = safe_inner_text(page.locator("h2.product-title_product-title__specification__UTjNc").first)

        brand = None
        if specification and "-" in specification:
            brand = clean_text(specification.split("-")[0])

        plu = None
        if specification:
            m = re.search(r"PLU:\s*([0-9]+)", specification)
            if m:
                plu = m.group(1)

        price_now_txt = safe_inner_text(page.locator("p.ProductPrice_container__price__XmMWA").first)
        price_old_txt = safe_inner_text(page.locator("p.priceSection_container-promotion_price-dashed__FJ7nI").first)
        discount_pct_txt = safe_inner_text(page.locator("span[data-percentage='true']").first)

        price_now = parse_money_co(price_now_txt)
        price_old = parse_money_co(price_old_txt)

        discount_pct = None
        if discount_pct_txt and discount_pct_txt.isdigit():
            discount_pct = int(discount_pct_txt)

        seller = safe_inner_text(page.locator("section.seller-information_fs-seller-information__3otO1 h3").first)
        if seller:
            seller = seller.replace("Vendido por:", "").strip()

        ean = get_ean_fast(page, retries=2, wait_ms=200)

        img_urls = None
        if include_images:
            urls = []
            thumbs = page.locator("div[data-fs-thumb-container='true'] img")
            for i in range(min(thumbs.count(), 12)):
                src = safe_attr(thumbs.nth(i), "src")
                if src and src not in urls:
                    urls.append(src)
            if not urls:
                bigs = page.locator("section[data-fs-image-gallery] img")
                for i in range(min(bigs.count(), 12)):
                    src = safe_attr(bigs.nth(i), "src")
                    if src and src not in urls:
                        urls.append(src)
            img_urls = " | ".join(urls) if urls else None

        scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return {
            "url": url,
            "titulo": title,
            "marca": brand,
            "plu": plu,
            "precio": price_now,
            "precio_texto": price_now_txt,
            "precio_tachado": price_old,
            "precio_tachado_texto": price_old_txt,
            "descuento_pct": discount_pct,
            "vendedor": seller,
            "ean": ean,
            "imagenes": img_urls,
            "capturado_en": scraped_at,
        }
    finally:
        try:
            page.close()
        except:
            pass

# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=30)
    ap.add_argument("--sleep-min", type=float, default=0.1)
    ap.add_argument("--sleep-max", type=float, default=0.25)
    ap.add_argument("--concurrency", type=int, default=6, help="Pestañas paralelas.")
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--include-images", action="store_true", help="Más lento. Si no, se bloquean imágenes.")
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = args.out.strip() or f"exito_tecnologia_{ts}.xlsx"

    all_rows = []
    seen_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="es-CO",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        # 🚀 Bloqueo de recursos pesados (acelera MUCHO)
        def route_handler(route):
            r = route.request
            rt = r.resource_type
            url = r.url.lower()

            if not args.include_images:
                if rt in ("image", "media", "font"):
                    return route.abort()

            # css a veces no es necesario; abortarlo puede acelerar más
            # pero si el sitio depende del css para render, puedes comentar esto
            if rt == "stylesheet":
                return route.abort()

            # analytics / trackers típicos
            if any(x in url for x in ("googletagmanager", "google-analytics", "doubleclick", "facebook", "hotjar")):
                return route.abort()

            return route.continue_()

        context.route("**/*", route_handler)

        listing_page = context.new_page()

        for page_num in range(1, args.max_pages + 1):
            listing_url = (
                f"{BASE}/tecnologia?"
                f"category-1=tecnologia&facets=category-1&sort=score_desc&page={page_num}"
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

            # Procesa en lotes con concurrencia
            for i in range(0, len(links), args.concurrency):
                batch = links[i:i + args.concurrency]
                pages = []
                for url in batch:
                    pages.append((url,))

                # Abrimos cada PDP en su propia pestaña, pero secuencialmente dentro del batch
                # (Playwright sync no tiene await; esto igual acelera porque carga en paralelo a nivel red/renderer)
                # Truco: lanzamos go-to en cada tab y luego extraemos.
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
                        # Extrae desde la tab ya cargada (rápido)
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

                        scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        row = {
                            "url": url,
                            "titulo": title,
                            "marca": brand,
                            "plu": plu,
                            "precio": price_now,
                            "precio_texto": price_now_txt,
                            "precio_tachado": price_old,
                            "precio_tachado_texto": price_old_txt,
                            "descuento_pct": discount_pct,
                            "vendedor": seller,
                            "ean": ean,
                            "imagenes": img_urls,
                            "capturado_en": scraped_at,
                        }

                        all_rows.append(row)
                        print(f"      ✅ OK | {title!r} | ${price_now} | ean={ean}")
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

    df = pd.DataFrame(all_rows)
    cols = [
        "capturado_en", "marca", "titulo", "plu", "ean",
        "precio", "precio_tachado", "descuento_pct",
        "vendedor", "url", "imagenes",
        "precio_texto", "precio_tachado_texto",
    ]
    df = df[[c for c in cols if c in df.columns]]

    df.to_excel(out_xlsx, index=False)
    print(f"\n📦 Listo. Productos: {len(df)}")
    print(f"📄 Excel guardado en: {out_xlsx}")

if __name__ == "__main__":
    main()
