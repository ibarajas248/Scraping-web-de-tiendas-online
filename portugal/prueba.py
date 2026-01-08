from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from datetime import datetime

# =======================
# PROXY (DataImpulse)
# =======================
PROXY_HOST = "gw.dataimpulse.com"
PROXY_PORT = 823

# ⚠️ Reemplaza por tus credenciales (mejor si las pones en variables de entorno)
PROXY_USER = "a1d102f8514e7ff8eea7"
PROXY_PASS = "ad339fe6c2486f3c"

URL = "https://www.loja-online.intermarche.pt/shop/2595"

def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    screenshot_path = f"intermarche_{ts}.png"

    with sync_playwright() as p:
        browser = p.firefox.launch(
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
        )

        page = context.new_page()

        # Logs básicos (útiles para ver si hay bloqueos/errores)
        page.on("requestfailed", lambda r: print(f"❌ requestfailed: {r.url} -> {r.failure}"))
        page.on("console", lambda m: print(f"🖥️ console[{m.type}]: {m.text}"))

        try:
            print(f"➡️ Abriendo: {URL}")
            resp = page.goto(URL, wait_until="domcontentloaded", timeout=90000)

            status = resp.status if resp else None
            final_url = page.url
            print(f"✅ Status: {status} | Final URL: {final_url}")

            # Espera un poco para que termine de cargar lo visible
            page.wait_for_timeout(5000)

            # Screenshot para diagnosticar (por si aparece página de bloqueo/captcha)
            page.screenshot(path=screenshot_path, full_page=True)
            print(f"📸 Screenshot guardado: {screenshot_path}")

            print("🟢 Listo. Puedes navegar manualmente en la ventana del navegador.")
            input("Pulsa ENTER para cerrar...")

        except PWTimeout:
            print("⏳ Timeout cargando la página (posible bloqueo o conexión lenta).")
            page.screenshot(path=screenshot_path, full_page=True)
            print(f"📸 Screenshot guardado: {screenshot_path}")
        finally:
            context.close()
            browser.close()

if __name__ == "__main__":
    main()
