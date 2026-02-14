from playwright.sync_api import sync_playwright

URL = "https://www.pedidosya.com.ar/restaurantes/buenos-aires/faricci-helados-congreso-menu?category=25&origin=shop_list"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # abre ventana visible
        page = browser.new_page()
        page.goto(URL, wait_until="domcontentloaded")
        page.wait_for_timeout(120_000)  # deja el navegador abierto 2 min
        browser.close()

if __name__ == "__main__":
    main()
