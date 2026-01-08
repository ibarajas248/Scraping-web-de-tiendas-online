from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError
import time, random, json, re
import pandas as pd
from datetime import datetime

# =======================
# PROXY (DataImpulse)
# =======================
PROXY_HOST = "gw.dataimpulse.com"
PROXY_PORT = 823
PROXY_USER = "78c35339645165da7ac7__cr.pt"
PROXY_PASS = "94a4b8a28d1505aa"

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
        return loc.first.get_attribute(name)
    except Exception:
        return None

def money_to_float(pt_money: str):
    if not pt_money:
        return None
    s = pt_money.strip().replace("€", "").replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else None

def try_parse_json(s: str):
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

def safe_goto(page, url, label="", attempts=4, timeout=90000):
    last = None
    for i in range(1, attempts + 1):
        try:
            print(f"➡️  GOTO {label} intento {i}/{attempts}: {url}")
            page.goto(url, wait_until="commit", timeout=timeout)
            page.wait_for_load_state("domcontentloaded", timeout=timeout)
            return True
        except PWError as e:
            last = e
            print(f"⚠️  GOTO falló ({i}/{attempts}): {str(e)[:220]}")
            jitter(1200, 2500)
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
    print(f"❌ No se pudo navegar a {label}. Último error: {str(last)[:260]}")
    return False

# =============== Proxy usage meter (estimación) ===============

class ProxyUsageMeter:
    """
    Estima uso de datos:
    - downloaded_bytes: suma Content-Length de responses cuando exista
    - uploaded_bytes: suma tamaño del post_data_buffer en requests
    NOTA: es aproximado (no todas las responses traen content-length).
    """
    def __init__(self):
        self.downloaded_bytes = 0
        self.uploaded_bytes = 0
        self.failed = 0

    def attach(self, page):
        def on_request(req):
            try:
                buf = req.post_data_buffer
                if buf:
                    self.uploaded_bytes += len(buf)
            except Exception:
                pass

        def on_response(resp):
            try:
                h = resp.headers
                cl = h.get("content-length")
                if cl and cl.isdigit():
                    self.downloaded_bytes += int(cl)
            except Exception:
                pass

        def on_request_failed(_):
            self.failed += 1

        page.on("request", on_request)
        page.on("response", on_response)
        page.on("requestfailed", on_request_failed)

    def mb(self, b):
        return b / (1024 * 1024)

    def report(self, prefix="📶 Proxy usage"):
        print(
            f"{prefix}: ↓ {self.mb(self.downloaded_bytes):.2f} MB | "
            f"↑ {self.mb(self.uploaded_bytes):.2f} MB | "
            f"requestfailed={self.failed}"
        )

# =============== Mejor carga total ===============

def wait_count_increase(page, selector: str, prev: int, timeout=45000) -> bool:
    try:
        page.wait_for_function(
            "(sel, prev) => document.querySelectorAll(sel).length > prev",
            arg=(selector, prev),
            timeout=timeout
        )
        return True
    except Exception:
        return False

def load_all_products(page):
    """
    Carga TODO lo posible en la categoría.
    Corta cuando:
    - no existe botón, o
    - botón no usable repetidas veces, o
    - demasiados intentos seguidos sin crecimiento.
    """
    TILE_SEL = ".productTile"
    BTN_SEL = "button.js-show-more-products"

    no_growth = 0
    max_no_growth = 8
    max_total_clicks = 500  # seguridad por si el sitio se queda “en loop”

    prev = page.locator(TILE_SEL).count()
    print(f"🧺 Tiles iniciales: {prev}")

    clicks = 0
    while True:
        if is_waf_blocked(page):
            print("⛔ WAF detectado durante carga. Corto.")
            break

        # scroll suave para asegurar que el botón se “active”
        page.mouse.wheel(0, random.randint(1800, 3200))
        jitter(700, 1300)

        cur = page.locator(TILE_SEL).count()
        if cur > prev:
            prev = cur
            no_growth = 0
            print(f"🧺 Tiles ahora (carga async): {cur}")

        btn_count = page.locator(BTN_SEL).count()
        if btn_count == 0:
            print("🛑 No existe botón 'Ver mais produtos'. Asumo fin.")
            break

        btn = page.locator(BTN_SEL).first

        # intenta asegurar visibilidad/habilitado
        try:
            btn.scroll_into_view_if_needed()
            jitter(500, 1000)
            visible = btn.is_visible()
            enabled = btn.is_enabled()
        except Exception:
            visible, enabled = False, False

        if not visible or not enabled:
            # baja más, a veces “aparece”
            page.mouse.wheel(0, random.randint(2500, 5200))
            jitter(900, 1600)
            try:
                visible = btn.is_visible()
                enabled = btn.is_enabled()
            except Exception:
                visible, enabled = False, False

            if not visible or not enabled:
                print("🛑 Botón no visible/habilitado. Asumo fin.")
                break

        # click
        try:
            btn.click(timeout=15000)
        except Exception:
            btn.click(force=True, timeout=15000)

        clicks += 1
        print(f"➕ Click #{clicks} en 'Ver mais produtos'")
        jitter(900, 1600)

        # apoya con networkidle (sin bloquear demasiado)
        try:
            page.wait_for_load_state("networkidle", timeout=25000)
        except Exception:
            pass

        grew = wait_count_increase(page, TILE_SEL, prev, timeout=45000)
        cur2 = page.locator(TILE_SEL).count()

        if grew and cur2 > prev:
            prev = cur2
            no_growth = 0
            print(f"🧺 Tiles ahora: {cur2}")
        else:
            no_growth += 1
            print(f"⚠️ No creció tras click (no_growth={no_growth}/{max_no_growth}). Tiles={cur2}")

            # plan B: scroll grande + espera + re-medir
            page.mouse.wheel(0, random.randint(3500, 7000))
            jitter(1200, 2400)
            cur3 = page.locator(TILE_SEL).count()
            if cur3 > prev:
                prev = cur3
                no_growth = 0
                print(f"🧺 Tiles ahora (plan B): {cur3}")

        if no_growth >= max_no_growth:
            print("🛑 Demasiados intentos sin crecimiento. Fin para no insistir.")
            break

        if clicks >= max_total_clicks:
            print("🛑 Alcancé max_total_clicks. Fin.")
            break

    print(f"✅ Fin carga. Tiles finales: {prev} | clicks: {clicks}")
    return prev

# =============== Extract ===============

def extract_all_tiles(page):
    tiles = page.locator(".productTile")
    n = tiles.count()
    print(f"🔎 Tiles detectados: {n}")

    rows = []
    seen = set()

    for i in range(n):
        t = tiles.nth(i)

        pid = None
        pid = pid or safe_attr(t.locator(".product"), "data-pid")
        pid = pid or safe_attr(t.locator(".product-tile"), "data-pid")
        pid = pid or safe_attr(t.locator("[data-pid]"), "data-pid")

        idx = safe_attr(t, "data-idx")
        key = pid or f"idx:{idx}:{i}"
        if key in seen:
            continue
        seen.add(key)

        name = safe_inner_text(t.locator("h2"))
        brand = safe_inner_text(t.locator(".pwc-tile--brand, .col-tile--brand"))
        quantity = safe_inner_text(t.locator(".pwc-tile--quantity, .col-tile--quantity"))

        product_url = safe_attr(t.locator(".ct-pdp-link a, a[href*='/produto/']"), "href")
        img_url = safe_attr(t.locator("img.ct-tile-image, img[data-src], img[src]"), "data-src") or safe_attr(
            t.locator("img.ct-tile-image, img[data-src], img[src]"), "src"
        )

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

        price2_text = safe_inner_text(t.locator(".pwc-tile--price-secondary .ct-price-value"))
        price2_unit = safe_inner_text(t.locator(".pwc-tile--price-secondary .pwc-m-unit"))
        price2_float = money_to_float(price2_text)

        impression_raw = safe_attr(t.locator(".product-tile"), "data-product-tile-impression")
        impression_json = try_parse_json(impression_raw)

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
        print("🌐 Lanzando Chromium (no headless) + Proxy PT...")

        browser = pw.chromium.launch(
            headless=False,
            proxy={
                "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
                "username": PROXY_USER,
                "password": PROXY_PASS,
            },
        )

        context = browser.new_context(
            locale="pt-PT",
            timezone_id="Europe/Lisbon",
            viewport={"width": 1366, "height": 768},
            ignore_https_errors=True,
        )

        context.set_default_navigation_timeout(90000)
        context.set_default_timeout(30000)

        page = context.new_page()

        # Medidor proxy (tráfico)
        meter = ProxyUsageMeter()
        meter.attach(page)

        print(f"➡️  Abriendo {URL}")
        ok = safe_goto(page, URL, label="HOME", attempts=4, timeout=90000)
        if not ok:
            meter.report("📶 Proxy usage (hasta fallo)")
            return
        jitter(900, 1600)

        if is_waf_blocked(page):
            print("⛔ WAF bloqueó en homepage. Corto.")
            meter.report("📶 Proxy usage (WAF)")
            return

        # 1) Cookies
        print("🍪 Buscando banner de cookies...")
        try:
            safe_click(page, "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
                       timeout=12000, force_fallback=True, label="Aceptar cookies")
        except Exception as e:
            print("ℹ️  No apareció banner de cookies:", str(e)[:140])

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
            meter.report("📶 Proxy usage (WAF)")
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

        # Espera estabilidad antes de navegar
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        jitter(800, 1400)

        if is_waf_blocked(page):
            print("⛔ WAF bloqueó tras confirmar/continuar. Corto.")
            meter.report("📶 Proxy usage (WAF)")
            return

        # 9) Ir a Frescos (retry)
        print("🥬 Navegando a Frescos...")
        ok = safe_goto(page, FRESCOS_URL, label="FRESCOS", attempts=4, timeout=90000)
        if not ok:
            meter.report("📶 Proxy usage (hasta fallo)")
            return
        jitter(1200, 2200)

        if is_waf_blocked(page):
            print("⛔ WAF bloqueó al entrar a Frescos. Corto.")
            meter.report("📶 Proxy usage (WAF)")
            return

        page.wait_for_selector(".productTile, [data-af-element='search-result'], .search-results", timeout=30000)
        print("✅ Frescos cargado")

        # 10) Cargar todo (robusto)
        print("⬇️ Cargando TODOS los productos (modo robusto)...")
        load_all_products(page)

        # Reporte proxy tras carga
        meter.report("📶 Proxy usage (tras carga)")

        # ✅ EXTRAER + GUARDAR EXCEL
        if is_waf_blocked(page):
            print("⛔ WAF detectado antes de extraer. No guardo.")
            meter.report("📶 Proxy usage (WAF)")
            return

        jitter(1200, 2200)

        rows = extract_all_tiles(page)
        if not rows:
            print("❌ No se extrajo nada (rows=0).")
            meter.report("📶 Proxy usage (sin extracción)")
            return

        df = pd.DataFrame(rows)
        preferred = [
            "idx", "pid", "name", "brand", "quantity_text",
            "price_text", "price_value", "price_unit",
            "price2_text", "price2_value", "price2_unit",
            "product_url", "image_url",
            "tile_impression_json", "tile_data_json",
        ]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]

        df.to_excel(OUT_XLSX, index=False)
        print(f"📦 Excel guardado: {OUT_XLSX} (filas={len(df)})")

        # Reporte final proxy
        meter.report("📶 Proxy usage (final)")

        print("🟢 Listo. Dejo el navegador abierto. Ctrl+C para salir.")
        while True:
            time.sleep(60)

if __name__ == "__main__":
    main()
