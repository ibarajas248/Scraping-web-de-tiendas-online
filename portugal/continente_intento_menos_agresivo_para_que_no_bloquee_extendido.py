from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import time, random, json, re
import pandas as pd
from datetime import datetime

URL = "https://www.continente.pt/"
FRESCOS_URL = "https://www.continente.pt/frescos/queijos/?start=0&srule=FRESH-Generico&pmin=0.01"
OUT_XLSX = f"continente_frescos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

# =============== Helpers ===============

def jitter(a=700, b=1700):
    time.sleep(random.uniform(a/1000, b/1000))

def is_waf_blocked(page) -> bool:
    try:
        html = page.content().lower()
        return ("link11" in html) or ("request blocked" in html) or ("status code: 474" in html)
    except Exception:
        return False

def safe_click(page, selector, timeout=15000, force_fallback=True, label=""):
    page.wait_for_selector(selector, timeout=timeout)
    loc = page.locator(selector).first
    loc.scroll_into_view_if_needed()
    jitter(250, 650)
    try:
        loc.click(timeout=timeout)
    except Exception:
        if not force_fallback:
            raise
        loc.click(force=True, timeout=timeout)
    if label:
        print(f"✅ Click: {label}")

def safe_type(page, selector, text, timeout=15000):
    page.wait_for_selector(selector, timeout=timeout)
    loc = page.locator(selector).first
    loc.scroll_into_view_if_needed()
    jitter(250, 650)
    loc.click()
    jitter(200, 500)
    loc.fill("")
    jitter(150, 400)
    loc.type(text, delay=80)
    print(f"✅ Escrito: {text!r}")

def wait_results_loaded(page, timeout=30000):
    page.wait_for_selector("#delivery-area", timeout=timeout)
    page.wait_for_selector("#delivery-area .store-details.search-postal-code", timeout=timeout)

def count_products(page) -> int:
    loc = page.locator(".productTile, [data-af-element='search-result'], .product-tile, .product")
    try:
        return loc.count()
    except Exception:
        return 0

def safe_inner_text(loc):
    try:
        if loc.count() == 0:
            return None
        t = loc.first.inner_text().strip()
        return t if t else None
    except Exception:
        return None

def safe_attr(loc, name: str):
    try:
        if loc.count() == 0:
            return None
        v = loc.first.get_attribute(name)
        return v
    except Exception:
        return None

def money_to_float(pt_money: str):
    """'€6,99' -> 6.99 (si no puede, None)"""
    if not pt_money:
        return None
    s = pt_money.strip()
    s = s.replace("€", "").replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".")  # EU -> float
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else None

def try_parse_json(s: str):
    if not s:
        return None
    # viene con &quot; si lo sacas del HTML, pero Playwright suele devolverlo ya decodificado.
    try:
        return json.loads(s)
    except Exception:
        return None

def extract_all_tiles(page):
    """
    Extrae todo lo "rico" posible por tile:
    - pid / data-pid / data-idx
    - nombre, marca, cantidad
    - precio principal + unidad, precio secundario (kg) + unidad
    - url producto, url imagen
    - varios data-* del tile (mensajes, brandId, etc.)
    - JSON data-product-tile-impression (si existe)
    """
    tiles = page.locator(".productTile")
    n = tiles.count()
    print(f"🔎 Tiles detectados: {n}")

    rows = []
    seen = set()

    for i in range(n):
        t = tiles.nth(i)

        # A veces el pid está en .product[data-pid], a veces en data-pid del botón, etc.
        pid = None
        pid = pid or safe_attr(t.locator(".product"), "data-pid")
        pid = pid or safe_attr(t.locator(".product-tile"), "data-pid")
        pid = pid or safe_attr(t.locator("[data-pid]"), "data-pid")

        idx = safe_attr(t, "data-idx")

        # evita duplicados si el DOM repite algo raro
        key = pid or f"idx:{idx}:{i}"
        if key in seen:
            continue
        seen.add(key)

        # Nombre / marca / cantidad
        name = safe_inner_text(t.locator("h2"))
        brand = safe_inner_text(t.locator(".pwc-tile--brand, .col-tile--brand"))
        quantity = safe_inner_text(t.locator(".pwc-tile--quantity, .col-tile--quantity"))

        # URL producto + imagen
        product_url = safe_attr(t.locator(".ct-pdp-link a, a[href*='/produto/']"), "href")
        img_url = safe_attr(t.locator("img.ct-tile-image, img[data-src], img[src]"), "data-src") or safe_attr(
            t.locator("img.ct-tile-image, img[data-src], img[src]"), "src"
        )

        # Precio principal: (texto + value content=)
        price_text = safe_inner_text(t.locator(".pwc-tile--price-primary .ct-price-formatted, .ct-price-formatted"))
        price_content = safe_attr(t.locator(".pwc-tile--price-primary .value, .sales .value"), "content")
        price_unit = safe_inner_text(t.locator(".pwc-tile--price-primary .pwc-m-unit"))

        price_float = None
        if price_content:
            try:
                price_float = float(price_content)
            except Exception:
                price_float = money_to_float(price_text)
        else:
            price_float = money_to_float(price_text)

        # Precio secundario (por kg, etc.)
        price2_text = safe_inner_text(t.locator(".pwc-tile--price-secondary .ct-price-value"))
        price2_unit = safe_inner_text(t.locator(".pwc-tile--price-secondary .pwc-m-unit"))
        price2_float = money_to_float(price2_text)

        # data-product-tile-impression (JSON con name/id/price/brand/category/channel...)
        impression_raw = safe_attr(t.locator(".product-tile"), "data-product-tile-impression")
        impression_json = try_parse_json(impression_raw)

        # Varias cosas útiles de atributos data-* del .product-tile (cuando existen)
        tile_loc = t.locator(".product-tile").first
        tile_data = {}
        for attr in [
            "data-brandid",
            "data-delay-time",
            "data-variants-mapping",
            "data-in-cart-msg",
            "data-one-product-added",
            "data-remove-from-cart-msg",
            "data-stay-open",
        ]:
            v = safe_attr(tile_loc, attr)
            if v is not None:
                tile_data[attr] = v

        rows.append({
            "idx": idx,
            "pid": pid,
            "name": name,
            "brand": brand,
            "quantity_text": quantity,

            "price_text": price_text,
            "price_value": price_float,
            "price_unit": price_unit,

            "price2_text": price2_text,
            "price2_value": price2_float,
            "price2_unit": price2_unit,

            "product_url": product_url,
            "image_url": img_url,

            "tile_impression_raw": impression_raw,
            "tile_impression_json": json.dumps(impression_json, ensure_ascii=False) if impression_json else None,

            "tile_data_json": json.dumps(tile_data, ensure_ascii=False) if tile_data else None,
        })

    return rows

# =============== Main ===============

def main():
    print("🚀 Iniciando Playwright...")
    with sync_playwright() as pw:
        print("🌐 Lanzando Chromium (no headless)...")
        browser = pw.chromium.launch(headless=False)

        context = browser.new_context(
            locale="pt-PT",
            timezone_id="Europe/Lisbon",
            viewport={"width": 1366, "height": 768},
        )
        page = context.new_page()

        print(f"➡️  Abriendo {URL}")
        page.goto(URL, wait_until="domcontentloaded")
        jitter(900, 1600)

        if is_waf_blocked(page):
            print("⛔ WAF bloqueó en homepage. Corto.")
            return

        # 1) Cookies
        print("🍪 Buscando banner de cookies...")
        try:
            safe_click(page, "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
                       timeout=12000, force_fallback=True, label="Aceptar cookies")
        except Exception as e:
            print("ℹ️  No apareció banner de cookies:", str(e)[:120])

        jitter(900, 1700)

        # 2) Selector entrega
        print("📍 Click en selector de entrega...")
        delivery_btn = 'button[data-target="#collapseDelivery"]'
        safe_click(page, delivery_btn, label="Selector entrega")
        jitter(800, 1600)

        # 3) Abrir modal cobertura
        print("🧷 Abriendo coverage-area-modal...")
        details_btn = 'button.options-detail[data-method="home"][data-target="#coverage-area-modal"]'
        safe_click(page, details_btn, label="Abrir modal cobertura")
        jitter(900, 1600)

        # 4) Escribir lisboa
        print("⌨️ Buscando Lisboa...")
        safe_type(page, "#coverage-postal-code", "lisboa")
        jitter(600, 1200)

        # 5) Enviar
        print("🔎 Enviando búsqueda...")
        safe_click(page, 'button[name="submit-postal-code"]', label="Enviar postal")
        wait_results_loaded(page)
        jitter(900, 1600)

        if is_waf_blocked(page):
            print("⛔ WAF bloqueó tras búsqueda postal. Corto.")
            return

        # 6) Seleccionar primer customerAddress
        print("📌 Seleccionando primer customerAddress...")
        addr_radio = "#delivery-area .store-details.search-postal-code input[name='customerAddress']"
        addr_label = "#delivery-area .store-details.search-postal-code label.store-label"
        page.wait_for_selector(addr_label, timeout=20000)
        jitter(500, 1200)
        try:
            page.locator(addr_radio).first.click(timeout=8000)
        except Exception:
            page.locator(addr_label).first.click(timeout=8000)

        print("✅ customerAddress seleccionado")
        jitter(900, 1600)

        # 7) Confirmar
        print("✅ Confirmando área...")
        safe_click(page, 'button[data-target="#confirm-coverage-area-modal"]', label="Confirmar")
        jitter(900, 1600)

        # 8) Continuar (si aparece)
        print("➡️ Continuar (si aparece)...")
        continuar_btn = "button.confirm-coverage-area-select"
        try:
            page.wait_for_selector(continuar_btn, timeout=8000)
            safe_click(page, continuar_btn, label="Continuar")
        except PWTimeout:
            print("ℹ️  No apareció 'Continuar' (ok).")

        jitter(900, 1700)

        if is_waf_blocked(page):
            print("⛔ WAF bloqueó tras confirmar/continuar. Corto.")
            return

        # 9) Ir a Frescos
        print("🥬 Navegando a Frescos...")
        page.goto(FRESCOS_URL, wait_until="domcontentloaded")
        jitter(1200, 2200)

        if is_waf_blocked(page):
            print("⛔ WAF bloqueó al entrar a Frescos. Corto.")
            return

        page.wait_for_selector(".productTile, [data-af-element='search-result'], .search-results",
                               timeout=30000)
        print("✅ Frescos cargado")

        # 10) “Ver mais produtos”
        print("⬇️ Cargando productos con 'Ver mais produtos' (suave)...")
        load_more = "button.js-show-more-products"

        max_clicks = 15
        stagnations = 0
        prev_count = count_products(page)
        print(f"🧺 Productos iniciales: {prev_count}")

        for i in range(max_clicks):
            if is_waf_blocked(page):
                print("⛔ WAF detectado durante paginado. Corto.")
                break

            btn = page.locator(load_more)
            if btn.count() == 0 or not btn.first.is_visible() or not btn.first.is_enabled():
                print("🛑 No hay más botón 'Ver mais produtos'. Fin.")
                break

            page.mouse.wheel(0, random.randint(1200, 2600))
            jitter(600, 1400)
            btn.first.scroll_into_view_if_needed()
            jitter(500, 1200)

            try:
                btn.first.click(timeout=8000)
            except Exception:
                btn.first.click(force=True, timeout=8000)

            print(f"➕ Click {i+1}/{max_clicks} en 'Ver mais produtos'")
            jitter(1400, 2600)

            grew = False
            for _ in range(10):
                cur = count_products(page)
                if cur > prev_count:
                    grew = True
                    prev_count = cur
                    print(f"🧺 Productos ahora: {cur}")
                    break
                jitter(400, 900)

            if not grew:
                stagnations += 1
                print("⚠️ No crecieron los productos en este intento.")
                if stagnations >= 2:
                    print("🛑 Estancado 2 veces. Fin para no insistir.")
                    break
            else:
                stagnations = 0

        print("✅ Fin de carga de productos")

        # =========================
        # ✅ EXTRAER + GUARDAR EXCEL
        # =========================
        if is_waf_blocked(page):
            print("⛔ WAF detectado antes de extraer. No guardo.")
            return

        # Asegura que el DOM esté “quieto” un momento
        jitter(1200, 2200)

        rows = extract_all_tiles(page)
        if not rows:
            print("❌ No se extrajo nada (rows=0).")
            return

        df = pd.DataFrame(rows)

        # Ordena columnas un poco
        preferred = [
            "idx", "pid", "name", "brand", "quantity_text",
            "price_text", "price_value", "price_unit",
            "price2_text", "price2_value", "price2_unit",
            "product_url", "image_url",
            "tile_impression_json", "tile_data_json"
        ]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]

        df.to_excel(OUT_XLSX, index=False)
        print(f"📦 Excel guardado: {OUT_XLSX} (filas={len(df)})")

        # Mantener abierto si quieres inspeccionar
        print("🟢 Listo. Dejo el navegador abierto. Ctrl+C para salir.")
        while True:
            time.sleep(60)

if __name__ == "__main__":
    main()
