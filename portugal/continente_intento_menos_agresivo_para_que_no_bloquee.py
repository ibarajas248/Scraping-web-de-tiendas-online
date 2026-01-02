from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import time, random

URL = "https://www.continente.pt/"
FRESCOS_URL = "https://www.continente.pt/frescos/queijos/?start=0&srule=FRESH-Generico&pmin=0.01"

# =============== Helpers ===============

def jitter(a=700, b=1700):
    """Pequeña pausa variable (ms) para evitar ráfagas."""
    time.sleep(random.uniform(a/1000, b/1000))

def is_waf_blocked(page) -> bool:
    """Detecta página de bloqueo Link11 (sin intentar evadir)."""
    try:
        html = page.content().lower()
        return ("link11" in html) or ("request blocked" in html) or ("status code: 474" in html)
    except Exception:
        return False

def safe_click(page, selector, timeout=15000, force_fallback=True, label=""):
    """Click robusto con scroll + fallback force."""
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
    loc.fill("")               # limpia
    jitter(150, 400)
    loc.type(text, delay=80)   # delay humano
    print(f"✅ Escrito: {text!r}")

def wait_results_loaded(page, timeout=30000):
    # Espera señales de resultados en el modal de cobertura
    page.wait_for_selector("#delivery-area", timeout=timeout)
    page.wait_for_selector("#delivery-area .store-details.search-postal-code", timeout=timeout)

def count_products(page) -> int:
    # Selector flexible (depende del layout A/B)
    loc = page.locator("[data-af-element='search-result'], .product-tile, .product")
    try:
        return loc.count()
    except Exception:
        return 0

# =============== Main ===============

def main():
    print("🚀 Iniciando Playwright...")
    with sync_playwright() as pw:
        print("🌐 Lanzando Chromium (no headless)...")
        browser = pw.chromium.launch(headless=False)

        # Contexto “normal” (más estable que new_page suelto)
        context = browser.new_context(
            locale="pt-PT",
            timezone_id="Europe/Lisbon",
            viewport={"width": 1366, "height": 768},
        )

        page = context.new_page()

        # 🚫 (OPCIONAL) Bloquear imágenes. Déjalo apagado si te sube el riesgo de WAF.
        BLOCK_IMAGES = False
        if BLOCK_IMAGES:
            page.route("**/*", lambda route, request: (
                route.abort() if request.resource_type in ["image", "media", "font"] else route.continue_()
            ))

        print(f"➡️  Abriendo {URL}")
        page.goto(URL, wait_until="domcontentloaded")
        jitter(900, 1600)

        if is_waf_blocked(page):
            print("⛔ WAF bloqueó en homepage. Corto para no insistir.")
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

        # 6) Seleccionar primer customerAddress (radio o label)
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

        page.wait_for_selector("[data-af-element='search-result'], .product-tile, .product, .search-results", timeout=30000)
        print("✅ Frescos cargado")


        # 10) “Ver mais produtos” con límites y espera por crecimiento
        print("⬇️ Cargando productos con 'Ver mais produtos' (suave)...")
        load_more = "button.js-show-more-products"

        max_clicks = 15      # evita ráfagas largas (ajusta)
        stagnations = 0      # cuántas veces no crece el conteo
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

            # Scroll con rueda + al botón
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

            # Espera por crecimiento real de items (hasta 10s)
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

        print("✅ Fin de carga de productos (modo suave)")

        # Mantener abierto
        print("🟢 Script en espera infinita. Ctrl+C para salir.")
        while True:
            time.sleep(60)

if __name__ == "__main__":
    main()
