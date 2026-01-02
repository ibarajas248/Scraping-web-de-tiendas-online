from playwright.sync_api import sync_playwright
import time

URL = "https://www.continente.pt/"

def main():
    print("🚀 Iniciando Playwright...")
    pw = sync_playwright().start()

    print("🌐 Lanzando Chromium (no headless)...")
    browser = pw.chromium.launch(headless=False)
    page = browser.new_page()

    print(f"➡️  Abriendo {URL}")
    page.goto(URL, wait_until="domcontentloaded")
    print("✅ Página cargada")

    # ======================================================
    # 1) Aceptar cookies
    # ======================================================
    print("🍪 Buscando banner de cookies...")
    try:
        page.wait_for_selector("#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll", timeout=12000)
        page.click("#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")
        print("✅ Cookies aceptadas")
    except Exception as e:
        print("ℹ️  No apareció banner de cookies:", str(e)[:120])

    # ======================================================
    # 2) Click en el botón de "Entrega em Casa" (delivery selector)
    # ======================================================
    print("📍 Buscando botón de entrega (Entrega em Casa / código postal)...")

    # Opción A (recomendada): por data-target del collapse
    delivery_btn = 'button[data-target="#collapseDelivery"]'

    # Si esa no existiera en algún A/B test, alternativa por clase:
    # delivery_btn = "button.delivery-methods-button"

    try:
        page.wait_for_selector(delivery_btn, timeout=15000)

        # Asegura que sea visible/clicable
        page.locator(delivery_btn).scroll_into_view_if_needed()
        page.locator(delivery_btn).click(timeout=15000)

        print("✅ Click hecho en selector de entrega")
    except Exception as e:
        print("❌ No pude clickear el selector de entrega:", str(e)[:200])

    # ======================================================
    # 3) Click en "options-details" (abre el modal #coverage-area-modal)
    # ======================================================
    print("🧷 Click en detalle de entrega (abre coverage-area-modal)...")

    details_btn = 'button.options-detail[data-method="home"][data-target="#coverage-area-modal"]'

    try:
        page.wait_for_selector(details_btn, timeout=15000)
        page.locator(details_btn).scroll_into_view_if_needed()
        page.locator(details_btn).click(timeout=15000)
        print("✅ Click hecho en options-detail (coverage-area-modal)")
    except Exception as e:
        print("❌ No pude clickear options-detail:", str(e)[:200])
        # fallback por si hay overlay/animación
        try:
            page.locator(details_btn).click(force=True, timeout=15000)
            print("✅ Click forzado hecho en options-detail")
        except Exception as e2:
            print("❌ Tampoco con force:", str(e2)[:200])

    # ======================================================
    # 4) Escribir "lisboa" en el input de código postal
    # ======================================================
    print("⌨️ Escribiendo 'lisboa' en el campo de búsqueda...")

    postal_input = "#coverage-postal-code"

    try:
        page.wait_for_selector(postal_input, timeout=15000)

        # Asegura foco limpio
        page.locator(postal_input).click()
        page.locator(postal_input).fill("")   # limpia por si acaso
        page.locator(postal_input).type("lisboa", delay=80)

        print("✅ Texto 'lisboa' escrito correctamente")
    except Exception as e:
        print("❌ No pude escribir en el input postal:", str(e)[:200])


    # ======================================================
    # 5) Click en el botón "Enviar" (submit postal code)
    # ======================================================
    print("🔎 Enviando búsqueda de código postal...")

    submit_btn = 'button[name="submit-postal-code"]'

    try:
        page.wait_for_selector(submit_btn, timeout=15000)
        page.locator(submit_btn).scroll_into_view_if_needed()
        page.locator(submit_btn).click(timeout=15000)
        print("✅ Click hecho en submit-postal-code")
    except Exception as e:
        print("❌ No pude clickear submit-postal-code:", str(e)[:200])
        # fallback si hay overlay o estado disabled temporal
        try:
            page.locator(submit_btn).click(force=True, timeout=15000)
            print("✅ Click forzado en submit-postal-code")
        except Exception as e2:
            print("❌ Tampoco con force:", str(e2)[:200])

    # ======================================================
    # 6b) Seleccionar el primer radio de customerAddress
    # ======================================================
    print("📌 Seleccionando el primer customerAddress...")

    first_addr = "#delivery-area .store-details.search-postal-code input[name='customerAddress']"

    try:
        page.wait_for_selector(first_addr, timeout=20000)
        page.locator(first_addr).first.scroll_into_view_if_needed()

        # A veces el input está oculto; el label es más clickeable
        try:
            page.locator(first_addr).first.click(timeout=15000)
        except Exception:
            page.locator("#delivery-area .store-details.search-postal-code label.store-label").first.click(timeout=15000)

        print("✅ Primer customerAddress seleccionado")
    except Exception as e:
        print("❌ No pude seleccionar customerAddress:", str(e)[:200])
        # fallback
        try:
            page.locator("#delivery-area .store-details.search-postal-code label.store-label").first.click(force=True, timeout=15000)
            print("✅ Primer customerAddress seleccionado (force)")
        except Exception as e2:
            print("❌ Tampoco con force:", str(e2)[:200])
    # ======================================================
    # 7) Click en "Confirmar" (confirm-coverage-area-modal)
    # ======================================================
    print("✅ Confirmando área de cobertura...")

    confirm_btn = 'button[data-target="#confirm-coverage-area-modal"]'

    try:
        page.wait_for_selector(confirm_btn, timeout=20000)
        page.locator(confirm_btn).scroll_into_view_if_needed()
        page.locator(confirm_btn).click(timeout=15000)
        print("🎉 Confirmación enviada")
    except Exception as e:
        print("❌ No pude clickear Confirmar:", str(e)[:200])
        # fallback por overlay/animación
        try:
            page.locator(confirm_btn).click(force=True, timeout=15000)
            print("🎉 Confirmación enviada (force)")
        except Exception as e2:
            print("❌ Tampoco con force:", str(e2)[:200])



    # ======================================================
    # 8) Click en "Continuar" (confirmación final)
    # ======================================================
    print("➡️ Click en Continuar...")

    continuar_btn = "button.confirm-coverage-area-select"

    try:
        page.wait_for_selector(continuar_btn, timeout=20000)
        page.locator(continuar_btn).scroll_into_view_if_needed()
        page.locator(continuar_btn).click(timeout=15000)
        print("✅ Click hecho en Continuar")
    except Exception as e:
        print("❌ No pude clickear Continuar:", str(e)[:200])
        # fallback típico si hay overlay
        try:
            page.locator(continuar_btn).click(force=True, timeout=15000)
            print("✅ Click forzado en Continuar")
        except Exception as e2:
            print("❌ Tampoco con force:", str(e2)[:200])

        # ======================================================
        # 9) Navegar a la categoría "Frescos" en la MISMA sesión
        # ======================================================
    frescos_url = "https://www.continente.pt/frescos/?start=0&srule=FRESH-Generico&pmin=0.01"

    print("🥬 Navegando a categoría Frescos...")
    page.goto(frescos_url, wait_until="domcontentloaded")

    # Espera a que carguen los productos (grid/listado)
    page.wait_for_selector("[data-af-element='search-result'], .product-tile, .product", timeout=30000)

    print("✅ Categoría Frescos cargada con contexto activo")

    # ======================================================
    # 10) Scroll y click en "Ver mais produtos" hasta terminar
    # ======================================================
    print("⬇️ Cargando todos los productos (scroll + ver más)...")

    load_more_btn = "button.js-show-more-products"

    while True:
        try:
            # Espera corta por si aparece
            page.wait_for_selector(load_more_btn, timeout=5000)

            btn = page.locator(load_more_btn)

            # Si no es visible o está deshabilitado → fin
            if not btn.is_visible():
                print("🛑 Botón 'Ver mais produtos' no visible. Fin.")
                break

            # Scroll hasta el botón
            btn.scroll_into_view_if_needed()
            page.wait_for_timeout(500)

            # Click
            btn.click(timeout=5000)
            print("➕ Click en 'Ver mais produtos'")

            # Espera a que carguen nuevos productos (XHR)
            page.wait_for_timeout(2000)

        except Exception:
            print("🛑 No hay más productos para cargar.")
            break

    print("✅ Todos los productos cargados")


    # ======================================================
    # ESPERA INFINITA (NO CIERRA NUNCA)
    # ======================================================
    print("🟢 Script en espera infinita.")
    print("🧠 El navegador queda ABIERTO.")
    print("🛑 Ctrl+C en consola para detener manualmente.")

    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
