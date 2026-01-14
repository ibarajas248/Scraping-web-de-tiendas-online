#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import json
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


CATEGORY_URL_TEMPLATE = "https://www.falabella.com.co/falabella-co/category/cat171006/Computadores?page={page}"


# -------------------------
# Utils
# -------------------------
def clean_text(s: str):
    if not s:
        return None
    s = s.replace("\u00a0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s or None


def strip_query(url: str) -> str:
    """Quita querystring para dedupe (sponsoredClickData, etc.)."""
    try:
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))  # sin query, sin fragment
    except Exception:
        return url


def parse_price_co(text: str):
    """Convierte '$ 1.234.567' -> 1234567 (int)."""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def first_locator_text(page, selectors, timeout=2000):
    """Devuelve inner_text del primer selector que exista (best-effort)."""
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue
            # no siempre está "visible", así que solo intenta leer
            txt = loc.inner_text(timeout=timeout)
            txt = clean_text(txt)
            if txt:
                return txt
        except Exception:
            continue
    return None


def first_locator_attr(page, selectors, attr, timeout=2000):
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue
            val = loc.get_attribute(attr, timeout=timeout)
            val = clean_text(val)
            if val:
                return val
        except Exception:
            continue
    return None


def extract_jsonld_fields(html: str):
    """
    Intenta extraer {name, brand, image, price} desde JSON-LD (si existe).
    Devuelve dict con claves opcionales.
    """
    out = {}
    # captura scripts JSON-LD
    for m in re.finditer(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE):
        raw = m.group(1).strip()
        if not raw:
            continue
        # a veces viene con múltiples objetos o listas
        try:
            data = json.loads(raw)
        except Exception:
            continue

        candidates = data if isinstance(data, list) else [data]

        for obj in candidates:
            if not isinstance(obj, dict):
                continue

            # name
            if not out.get("name"):
                n = obj.get("name")
                if isinstance(n, str) and n.strip():
                    out["name"] = n.strip()

            # brand
            if not out.get("brand"):
                b = obj.get("brand")
                if isinstance(b, dict):
                    bn = b.get("name")
                    if isinstance(bn, str) and bn.strip():
                        out["brand"] = bn.strip()
                elif isinstance(b, str) and b.strip():
                    out["brand"] = b.strip()

            # image
            if not out.get("image"):
                img = obj.get("image")
                if isinstance(img, str) and img.strip():
                    out["image"] = img.strip()
                elif isinstance(img, list) and img:
                    for it in img:
                        if isinstance(it, str) and it.strip():
                            out["image"] = it.strip()
                            break

            # offers price
            if out.get("price") is None:
                offers = obj.get("offers")
                # offers puede ser dict o lista
                if isinstance(offers, dict):
                    p = offers.get("price")
                    if isinstance(p, (int, float)):
                        out["price"] = int(p)
                    elif isinstance(p, str):
                        pp = parse_price_co(p)
                        if pp is not None:
                            out["price"] = pp
                elif isinstance(offers, list):
                    for off in offers:
                        if not isinstance(off, dict):
                            continue
                        p = off.get("price")
                        if isinstance(p, (int, float)):
                            out["price"] = int(p)
                            break
                        elif isinstance(p, str):
                            pp = parse_price_co(p)
                            if pp is not None:
                                out["price"] = pp
                                break

        # si ya conseguimos lo principal, corta
        if out.get("name") and out.get("image") and (out.get("price") is not None):
            break

    return out


# -------------------------
# Scrape
# -------------------------
def block_heavy_resources(route, request):
    if request.resource_type in ("image", "media", "font"):
        return route.abort()
    return route.continue_()


def collect_product_urls_from_listing(page) -> list[str]:
    """
    En el listado: toma hrefs dentro de pods y filtra los que contengan '/product/'.
    """
    # Espera a que carguen pods
    page.wait_for_selector('[data-testid="ssr-pod"] a[href]', timeout=30000)

    # scroll suave para que termine de hidratar/cargar
    for _ in range(6):
        page.mouse.wheel(0, 2200)
        time.sleep(0.25)

    hrefs = page.eval_on_selector_all(
        '[data-testid="ssr-pod"] a[href]',
        "els => els.map(e => e.href)"
    ) or []

    out = []
    seen = set()
    for h in hrefs:
        if not h:
            continue
        if "/product/" not in h:
            continue
        h2 = strip_query(h)
        if h2 in seen:
            continue
        seen.add(h2)
        out.append(h2)

    return out


def scrape_product_detail(context, url: str, slow_sleep=0.2) -> dict:
    p = context.new_page()
    try:
        p.goto(url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(slow_sleep)

        # intenta esperar por nombre (pero sin morir si no aparece)
        try:
            p.wait_for_selector("h1", timeout=10000)
        except Exception:
            pass

        # --- DOM selectors (los de tu ejemplo + fallback) ---
        nombre = first_locator_text(p, [
            "h1.pdp-basic-info__product-name",
            "h1[class*='pdp-basic-info__product-name']",
            "h1",
        ])

        vendedor = first_locator_text(p, [
            "span.seller-info__seller-name",
            "span[class*='seller-info__seller-name']",
            "[data-testid*='seller'] span",
            "text=/Vendido por/i",
        ])

        fabricante = first_locator_text(p, [
            "a.pdp-basic-info__brand-link",
            "a[class*='pdp-basic-info__brand-link']",
            "span.pdp-basic-info__brand-and-separator a",
        ])

        # imagen (mejor vía meta og:image o primera img “grande”)
        imagen = first_locator_attr(p, [
            "meta[property='og:image']",
            "meta[name='og:image']",
        ], "content")

        if not imagen:
            imagen = first_locator_attr(p, [
                "img[src*='media.falabella.com'][src*='w=1200']",
                "img[src*='media.falabella.com'][src*='width=1200']",
                "img[src*='media.falabella.com']",
            ], "src")

        # precio base (primero intenta spans, si no JSON-LD)
        precio_txt = first_locator_text(p, [
            "span.copy12.primary.senary",
            "span[class*='copy12'][class*='senary']",
            "[data-testid*='price'] span",
            "li[data-cmr-price] span",
        ])
        precio_base = parse_price_co(precio_txt) if precio_txt else None

        # --- Fallback: JSON-LD ---
        html = p.content()
        j = extract_jsonld_fields(html)

        if not nombre and j.get("name"):
            nombre = j["name"]
        if not fabricante and j.get("brand"):
            fabricante = j["brand"]
        if not imagen and j.get("image"):
            imagen = j["image"]
        if precio_base is None and j.get("price") is not None:
            precio_base = j["price"]

        return {
            "url": p.url,
            "imagen": imagen,
            "nombre_producto": nombre,
            "precio_base": precio_base,   # int (sin puntos), ejemplo 129900
            "vendedor": vendedor,
            "fabricante": fabricante,
        }

    finally:
        try:
            p.close()
        except Exception:
            pass


def main():
    # ====== CONFIG RÁPIDA ======
    START_PAGE = 1
    MAX_PAGES = 3          # súbelo (ej 20) si quieres más
    MAX_PRODUCTS = 80      # límite de productos totales (para pruebas); pon None para ilimitado
    HEADLESS = True

    rows = []
    seen_product_urls = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox"]
        )

        context = browser.new_context(
            locale="es-CO",
            viewport={"width": 1365, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        context.route("**/*", block_heavy_resources)

        page = context.new_page()

        try:
            for n in range(START_PAGE, START_PAGE + MAX_PAGES):
                listing_url = CATEGORY_URL_TEMPLATE.format(page=n)
                print(f"\n📄 LISTADO page={n}: {listing_url}")

                try:
                    page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
                except PWTimeout:
                    print("⚠️ Timeout cargando listado. Reintento 1...")
                    page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)

                # si hay cookies/consent, intenta cerrar (best-effort)
                for sel in [
                    "button:has-text('Aceptar')",
                    "button:has-text('Aceptar todas')",
                    "button:has-text('Entendido')",
                ]:
                    try:
                        btn = page.locator(sel).first
                        if btn.count():
                            btn.click(timeout=1500)
                            break
                    except Exception:
                        pass

                try:
                    product_urls = collect_product_urls_from_listing(page)
                except Exception as e:
                    print(f"❌ No pude leer productos en page={n}: {e}")
                    product_urls = []

                if not product_urls:
                    print("✅ No hay productos (o no cargó). Corto paginación.")
                    break

                # dedupe global
                new_urls = []
                for u in product_urls:
                    if u not in seen_product_urls:
                        seen_product_urls.add(u)
                        new_urls.append(u)

                print(f"   🔎 Productos en esta página: {len(product_urls)} | nuevos: {len(new_urls)}")

                for i, u in enumerate(new_urls, 1):
                    if MAX_PRODUCTS is not None and len(rows) >= MAX_PRODUCTS:
                        print("🛑 Llegué a MAX_PRODUCTS. Corto.")
                        raise SystemExit

                    print(f"   ({i}/{len(new_urls)}) -> {u}")
                    try:
                        row = scrape_product_detail(context, u, slow_sleep=0.15)
                        rows.append(row)
                    except Exception as e:
                        print(f"      ❌ Error en producto: {e}")

                    time.sleep(0.15)  # pausa anti-rate-limit suave

        except SystemExit:
            pass
        finally:
            try:
                page.close()
            except Exception:
                pass
            context.close()
            browser.close()

    # ====== Export Excel ======
    df = pd.DataFrame(rows, columns=[
        "url", "imagen", "nombre_producto", "precio_base", "vendedor", "fabricante"
    ])

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"Falabella_Computadores_{ts}.xlsx"
    df.to_excel(out, index=False)

    print(f"\n✅ Excel generado: {out} | filas: {len(df)}")


if __name__ == "__main__":
    main()
