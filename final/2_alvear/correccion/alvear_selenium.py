#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
from datetime import datetime
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.alvearonline.com.ar/#/"

KEYWORDS = [
    "leche", "salame", "salchicha", "jamon", "mortadela",
    "leber", "fiambre", "medallon", "papa", "hamburguesa"
]

def clean_text(s):
    if not s:
        return None
    s = s.replace("\u00a0", " ").strip()
    return s or None

def parse_price(text):
    """
    "$3.999,00" -> "3999.00"
    "$5999.00"  -> "5999.00"
    """
    if not text:
        return None
    t = text.strip()
    m = re.search(r"([0-9\.\,]+)", t)
    if not m:
        return None
    num = m.group(1)

    if "," in num and "." in num:
        num = num.replace(".", "").replace(",", ".")
    else:
        if "," in num:
            num = num.replace(".", "").replace(",", ".")
    try:
        return f"{float(num):.2f}"
    except:
        return num

def safe_inner_text(locator, timeout=800):
    try:
        return clean_text(locator.inner_text(timeout=timeout))
    except:
        return None

def safe_attr(locator, attr, timeout=800):
    try:
        return locator.get_attribute(attr, timeout=timeout)
    except:
        return None

def close_popups(page):
    candidates = [
        "button:has-text('Aceptar')",
        "button:has-text('ACEPTAR')",
        "button:has-text('Entendido')",
        "button:has-text('Cerrar')",
        "button[aria-label='close']",
        "button[aria-label='Close']",
        "div[role='dialog'] button",
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible():
                btn.click(timeout=1000)
                time.sleep(0.4)
        except:
            pass

def scroll_until_stable(page, cards_sel, max_rounds=50):
    stable_rounds = 0
    last_count = 0

    for _ in range(max_rounds):
        count = page.locator(cards_sel).count()
        if count == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = count

        if stable_rounds >= 4:
            break

        page.mouse.wheel(0, 2600)
        time.sleep(0.8)

def do_search(page, keyword):
    # input buscador
    search_sel = 'input[placeholder="Buscar"]'
    page.wait_for_selector(search_sel, timeout=30000)

    search = page.locator(search_sel).first
    search.click()
    # select-all + escribir
    search.press("Control+A")
    search.fill(keyword)
    search.press("Enter")

    # esperar cards (o al menos una señal de que ya cargó)
    cards_sel = "div.flexCard div.MuiCard-root.card"
    try:
        page.wait_for_selector(cards_sel, timeout=30000)
    except PWTimeout:
        # puede que no haya resultados; devolvemos 0
        return 0

    # scroll para cargar
    scroll_until_stable(page, cards_sel, max_rounds=60)
    return page.locator(cards_sel).count()

def extract_cards(page, keyword):
    cards_sel = "div.flexCard div.MuiCard-root.card"
    cards = page.locator(cards_sel)
    n = cards.count()

    rows = []
    capturado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for i in range(n):
        c = cards.nth(i)

        nombre = safe_inner_text(c.locator("h6"))
        precio_actual_txt = safe_inner_text(c.locator(".visualizadorPrecio h3"))
        precio_actual = parse_price(precio_actual_txt)

        precio_antes_txt = safe_inner_text(c.locator(".visualizadorPrecio .precioTachado"))
        precio_antes = parse_price(precio_antes_txt)

        descuento = safe_inner_text(c.locator(".labelDescuento"))
        precio_sin_imp_txt = safe_inner_text(c.locator(".visualizadorPrecioSinImpuestos p"))
        precio_sin_imp = parse_price(precio_sin_imp_txt)

        img_principal = safe_attr(c.locator(".containerImage img").first, "src")
        img_sello = safe_attr(c.locator(".selloOferta img").first, "src")

        rows.append({
            "capturado_en": capturado_en,
            "query": keyword,
            "nombre": nombre,
            "precio_actual": precio_actual,
            "precio_antes": precio_antes,
            "descuento_label": descuento,
            "precio_sin_impuestos": precio_sin_imp,
            "img_principal": img_principal,
            "img_sello_oferta": img_sello,
        })

    return rows

def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = f"Alvear_multi_keywords_{ts}.xlsx"

    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # False si querés ver
        context = browser.new_context(
            locale="es-AR",
            viewport={"width": 1366, "height": 768},
        )
        page = context.new_page()

        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1500)
        close_popups(page)

        for kw in KEYWORDS:
            print(f"\n🔎 Buscando: {kw}")
            try:
                count = do_search(page, kw)
                if count == 0:
                    print(f"⚠️ Sin resultados (o no cargó cards) para: {kw}")
                    continue

                rows = extract_cards(page, kw)
                print(f"✅ Extraídas: {len(rows)} filas para '{kw}'")
                all_rows.extend(rows)

                # mini pausa para no ir tan agresivo
                time.sleep(1.2)

            except Exception as e:
                print(f"❌ Error en keyword '{kw}': {e}")
                continue

        context.close()
        browser.close()

    df = pd.DataFrame(all_rows)

    # Dedupe razonable (evita repetir lo mismo por renders/scroll)
    if not df.empty:
        df = df.drop_duplicates(subset=["query", "nombre", "img_principal", "precio_actual"], keep="first")

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="alvear")

    print(f"\n📦 Excel generado: {out_xlsx} | filas: {len(df)}")

if __name__ == "__main__":
    main()
