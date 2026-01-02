#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Abastecedor (VTEX)
- Login
- Selección “Retiro en sucursal”
- Scrape Almacén paginado

Flujo:
- Va a /almacen?page=1..N
- Para cada página:
    - toma TODOS los productos (links)
    - entra a cada uno
    - extrae: nombre, sku (Referencia), precio, url
- Imprime progreso
- Guarda XLSX final
"""

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import pandas as pd
from datetime import datetime


# ============================================================
# Configuración
# ============================================================

BASE = "https://www.abastecedor.com.ar"
START_LISTING = f"{BASE}/almacen?page=1"

EMAIL = "mauro@factory-blue.com"
PASSWORD = "Compras2025"

OUT_XLSX = f"Abastecedor_Almacen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


# ============================================================
# Helpers
# ============================================================

def safe_text(loc):
    """Devuelve texto seguro desde un locator de Playwright."""
    try:
        if loc.count() <= 0:
            return None
        txt = loc.first.inner_text().strip()
        return txt if txt else None
    except Exception:
        return None


# ============================================================
# Login + selección de entrega
# ============================================================

def login_and_choose_delivery(page):
    """Login VTEX + selección 'Retiro en sucursal'."""

    # 1) Home
    page.goto(BASE, wait_until="domcontentloaded", timeout=60000)

    # 2) Modal login
    page.wait_for_selector(
        "div.vtex-login-2-x-contentFormVisible",
        timeout=20000
    )

    # 3) Opción email + contraseña
    page.get_by_role(
        "button",
        name="Entrar con e-mail y contraseña"
    ).click(timeout=15000)

    # 4) Email
    email_sel = (
        'div.vtex-login-2-x-inputContainerEmail '
        'input[placeholder="Ej.: ejemplo@mail.com"]'
    )
    email = page.locator(email_sel).first
    email.wait_for(state="visible", timeout=20000)
    email.click()
    page.keyboard.press("Control+A")
    page.keyboard.type(EMAIL, delay=35)

    if (email.input_value() or "").strip() != EMAIL:
        page.evaluate(
            """
            (v) => {
                const el =
                    document.querySelector(
                        'div.vtex-login-2-x-inputContainerEmail input[placeholder="Ej.: ejemplo@mail.com"]'
                    ) ||
                    [...document.querySelectorAll('input[placeholder="Ej.: ejemplo@mail.com"]')]
                        .find(e => e.offsetParent !== null);
                if (!el) return;
                el.focus();
                el.value = v;
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
            }
            """,
            EMAIL
        )

    # 5) Password
    pass_sel = (
        'div.vtex-login-2-x-inputContainerPassword '
        'input[placeholder="Ingrese su contraseña "]'
    )
    pwd = page.locator(pass_sel).first
    pwd.wait_for(state="visible", timeout=20000)
    pwd.click()
    page.keyboard.press("Control+A")
    page.keyboard.type(PASSWORD, delay=35)

    if (pwd.input_value() or "") != PASSWORD:
        page.evaluate(
            """
            (v) => {
                const el =
                    document.querySelector(
                        'div.vtex-login-2-x-inputContainerPassword input[placeholder="Ingrese su contraseña "]'
                    ) ||
                    [...document.querySelectorAll('input[type="password"]')]
                        .find(e => e.offsetParent !== null);
                if (!el) return;
                el.focus();
                el.value = v;
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
            }
            """,
            PASSWORD
        )

    # 6) Entrar
    page.get_by_role("button", name="Entrar").click(timeout=20000)

    # 7) Selector de entrega
    page.wait_for_selector(
        "div.elabastecedorar-redclover-theme-0-x-deliverySelectorOptions",
        timeout=60000
    )

    # 8) Retiro en sucursal (ULTRA robusto)
    retiro_card = page.locator(
        "div.elabastecedorar-redclover-theme-0-x-deliverySelectorOption"
        ":has(h4:has-text('Retiro en sucursal'))"
    ).first

    retiro_card.wait_for(state="visible", timeout=20000)
    retiro_card.scroll_into_view_if_needed()
    page.wait_for_timeout(400)

    try:
        retiro_card.click(timeout=8000)
    except Exception:
        try:
            retiro_card.click(timeout=8000, force=True)
        except Exception:
            page.evaluate(
                """
                () => {
                    const cards = Array.from(
                        document.querySelectorAll(
                            'div.elabastecedorar-redclover-theme-0-x-deliverySelectorOption'
                        )
                    );
                    const card = cards.find(c =>
                        (c.innerText || '').includes('Retiro en sucursal')
                    );
                    if (!card) return false;
                    card.scrollIntoView({block:'center'});
                    card.click();
                    return true;
                }
                """
            )

    page.wait_for_timeout(700)


# ============================================================
# Listado
# ============================================================

def collect_product_links_from_listing(page):
    """Devuelve lista de URLs absolutas de productos desde el listado."""

    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(800)

    cards = page.locator(
        "div.vtex-search-result-3-x-galleryItem "
        "a.vtex-product-summary-2-x-clearLink"
    )

    if cards.count() == 0:
        cards = page.locator('a[href$="/p"], a[href*="/p?"]')

    urls = []
    for i in range(cards.count()):
        href = cards.nth(i).get_attribute("href")
        if not href:
            continue
        urls.append(
            href if href.startswith("http")
            else BASE.rstrip("/") + href
        )

    # dedupe conservando orden
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)

    return uniq


# ============================================================
# Detalle producto
# ============================================================

def scrape_product_detail(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=60000)

    # Nombre
    name = safe_text(
        page.locator(
            "h1.vtex-store-components-3-x-productNameContainer "
            "span.vtex-store-components-3-x-productBrand"
        )
    ) or safe_text(
        page.locator("h1 span.vtex-store-components-3-x-productBrand")
    ) or safe_text(
        page.locator("h1")
    )

    # SKU / Referencia
    sku = safe_text(
        page.locator(
            "span.vtex-product-identifier-0-x-product-identifier__value"
        )
    ) or safe_text(
        page.locator("span:has-text('Referencia')")
            .locator("xpath=following-sibling::span")
            .first
    )

    # Precio
    price = safe_text(
        page.locator("span.vtex-product-price-1-x-sellingPriceValue")
    ) or safe_text(
        page.locator("span.vtex-store-components-3-x-sellingPriceValue")
    ) or safe_text(
        page.locator("span.vtex-product-price-1-x-sellingPrice")
    )

    return {
        "nombre": name,
        "sku": sku,
        "precio": price,
        "url": url
    }


# ============================================================
# Main
# ============================================================

def main():
    rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("🔐 Login + selección de entrega…")
        login_and_choose_delivery(page)
        print("✅ Login OK")

        page_num = 1

        while True:
            listing_url = f"{BASE}/almacen?page={page_num}"
            print(f"\n📄 Listado: {listing_url}")

            page.goto(
                listing_url,
                wait_until="domcontentloaded",
                timeout=60000
            )
            page.wait_for_timeout(1200)

            product_urls = collect_product_links_from_listing(page)
            if not product_urls:
                print("✅ No hay más productos. Fin.")
                break

            print(
                f"🧾 Productos en página {page_num}: "
                f"{len(product_urls)}"
            )

            for idx, u in enumerate(product_urls, start=1):
                try:
                    data = scrape_product_detail(page, u)
                    rows.append(data)
                    print(
                        f" ✅ ({page_num}-{idx}/{len(product_urls)}) "
                        f"{data.get('sku') or '-'} | "
                        f"{data.get('precio') or '-'} | "
                        f"{data.get('nombre') or '-'}"
                    )
                except Exception as e:
                    print(
                        f" ❌ ({page_num}-{idx}/{len(product_urls)}) "
                        f"Error en {u}: {repr(e)}"
                    )
                finally:
                    page.goto(
                        listing_url,
                        wait_until="domcontentloaded",
                        timeout=60000
                    )
                    page.wait_for_timeout(600)

            page_num += 1

        # Guardar XLSX
        df = pd.DataFrame(
            rows,
            columns=["nombre", "sku", "precio", "url"]
        )
        df.to_excel(OUT_XLSX, index=False)

        print(
            f"\n📦 XLSX generado: {OUT_XLSX} | "
            f"filas: {len(df)}"
        )
        print("👀 Navegador queda abierto. Cierra la ventana para terminar.")
        page.wait_for_timeout(9999999)


if __name__ == "__main__":
    main()
