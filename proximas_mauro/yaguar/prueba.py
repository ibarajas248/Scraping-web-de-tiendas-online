from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from urllib.parse import urljoin
import csv
import re
import pandas as pd

URL = "https://yaguar.com.ar/tienda/"
USER = "tienda@store-blue.com"
PASS = "Productos2026$"

KEEP_OPEN_MS = 20000000

# === Elegí destino ===
REGION = "GBA"
SUCURSAL = "Tigre"


def close_elementor_popup_if_present(page) -> bool:
    """Cierra popup Elementor (dialog-lightbox) si aparece."""
    close_selectors = [
        "a.dialog-close-button.dialog-lightbox-close-button",
        "a.dialog-lightbox-close-button",
        "a[aria-label='Close']",
    ]
    for _ in range(3):
        for sel in close_selectors:
            loc = page.locator(sel)
            try:
                if loc.count() > 0 and loc.first.is_visible():
                    loc.first.click(timeout=1500)
                    page.wait_for_timeout(250)
                    return True
            except Exception:
                try:
                    if loc.count() > 0:
                        loc.first.click(timeout=1500, force=True)
                        page.wait_for_timeout(250)
                        return True
                except Exception:
                    pass
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass
        page.wait_for_timeout(600)
    return False


def close_yaguar_popup_if_present(page) -> bool:
    """
    Cierra el popup propio de Yaguar:
      <div class="yaguar-popup-modal"> ... <button class="yaguar-popup-close" aria-label="Cerrar"> ...
    """
    modal = page.locator("div.yaguar-popup-modal")
    btn = page.locator("button.yaguar-popup-close[aria-label='Cerrar']")

    # Intentamos un par de veces (a veces aparece tarde)
    for _ in range(4):
        try:
            if modal.count() > 0 and modal.first.is_visible():
                # Click al botón cerrar
                if btn.count() > 0:
                    btn.first.click(timeout=2000)
                else:
                    # fallback: buscar button dentro del modal
                    modal.first.locator("button.yaguar-popup-close").first.click(timeout=2000)
                page.wait_for_timeout(300)
                return True
        except Exception:
            # fallback: ESC a veces lo cierra si está como modal
            try:
                page.keyboard.press("Escape")
                page.wait_for_timeout(300)
                if modal.count() > 0 and not modal.first.is_visible():
                    return True
            except Exception:
                pass

        page.wait_for_timeout(700)

    return False


def _first_visible(loc):
    try:
        n = loc.count()
    except Exception:
        return None
    for i in range(n):
        try:
            el = loc.nth(i)
            if el.is_visible():
                return el
        except Exception:
            pass
    return None


def _force_show_ul(page, ul_locator):
    ul = _first_visible(ul_locator) or ul_locator.first
    page.evaluate(
        """(ul) => {
            ul.style.display = 'block';
            ul.style.visibility = 'visible';
            ul.style.opacity = '1';
            ul.setAttribute('aria-hidden','false');
            ul.setAttribute('aria-expanded','true');
        }""",
        ul.element_handle(),
    )
    return ul


def _open_submenu_by_li(page, li_locator, ul_child_locator, tries=6):
    li = _first_visible(li_locator) or li_locator.first
    a = li.locator("xpath=.//a[1]").first
    ul_candidates = ul_child_locator

    for t in range(tries):
        try:
            li.scroll_into_view_if_needed()
        except Exception:
            pass

        try:
            li.hover(timeout=4000)
        except Exception:
            pass

        try:
            a.hover(timeout=3000)
        except Exception:
            pass

        page.wait_for_timeout(250)

        ul_vis = _first_visible(ul_candidates)
        if ul_vis:
            return ul_vis

        if t % 2 == 1:
            try:
                a.click(timeout=3000, force=True)
            except Exception:
                pass

        page.wait_for_timeout(300)

    return _force_show_ul(page, ul_candidates)


def select_region_and_sucursal(page, region_text: str, sucursal_text: str):
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(300)

    nav = page.locator("nav.elementor-nav-menu--main:visible").first
    nav.wait_for(state="visible", timeout=30000)

    suc_a = nav.locator("a.elementor-item.has-submenu:has(i.fa-map-marker-alt)").first
    suc_a.wait_for(state="attached", timeout=15000)

    suc_li = suc_a.locator("xpath=ancestor::li[1]")
    suc_ul = suc_li.locator("xpath=./ul[contains(@class,'sub-menu')]")

    submenu1 = _open_submenu_by_li(page, suc_li, suc_ul, tries=6)

    region_a = submenu1.locator("a.elementor-sub-item.has-submenu").filter(has_text=region_text)
    region_a_vis = _first_visible(region_a) or region_a.first
    region_li = region_a_vis.locator("xpath=ancestor::li[1]")
    region_ul = region_li.locator("xpath=./ul[contains(@class,'sub-menu')]")

    submenu2 = _open_submenu_by_li(page, region_li, region_ul, tries=6)

    target = submenu2.locator("a.elementor-sub-item").filter(has_text=sucursal_text)
    target_vis = _first_visible(target) or target.first
    target_vis.click(timeout=8000, force=True)

    try:
        page.wait_for_load_state("networkidle", timeout=12000)
    except PWTimeoutError:
        pass

def _clean_price(text: str) -> str:
    # Ej: "$ 4.298" -> "4298" (o déjalo como "4.298" si prefieres)
    t = (text or "").strip()
    t = t.replace("\xa0", " ")
    # deja solo dígitos y separadores
    m = re.search(r"([\d\.\,]+)", t)
    return m.group(1) if m else ""

def _clean_code(text: str) -> str:
    # "Cod. 9055" -> "9055"
    t = (text or "").strip()
    m = re.search(r"(\d+)", t)
    return m.group(1) if m else ""

def scrape_items_on_page(page):
    # Espera a que carguen los productos
    page.wait_for_selector('div[data-elementor-type="loop-item"].product', timeout=30000)

    items = page.locator('div[data-elementor-type="loop-item"].product')
    n = items.count()

    out = []
    for i in range(n):
        it = items.nth(i)

        # Nombre
        name = it.locator("h3.product_title").first.inner_text(timeout=2000).strip()

        # Precio
        price_txt = it.locator("span.woocommerce-Price-amount").first.inner_text(timeout=2000).strip()
        price = _clean_price(price_txt)

        # Código "Cod. ####"
        code_txt = it.locator('h2.elementor-heading-title:has-text("Cod.")').first.inner_text(timeout=2000).strip()
        code = _clean_code(code_txt)

        # URL producto (link que envuelve imagen)
        href = it.locator('a[href*="/producto/"]').first.get_attribute("href") or ""
        product_url = urljoin(page.url, href)

        # Imagen (src)
        img_src = it.locator("img").first.get_attribute("src") or ""
        img_url = urljoin(page.url, img_src)

        out.append({
            "nombre_producto": name,
            "precio": price,
            "codigo": code,
            "img_url": img_url,
            "product_url": product_url,
        })

    return out
def scrape_all_pages(page, max_pages=500):
    """
    Recorre paginación Jet Filters: click 'Siguiente' hasta que no exista/esté deshabilitado
    o hasta que no se detecten productos nuevos.
    Devuelve lista consolidada de productos.
    """
    all_rows = []
    seen = set()  # para dedupe (product_url, o (codigo, nombre))

    for page_num in range(1, max_pages + 1):
        # Asegura que la grilla exista antes de leer
        page.wait_for_selector('div[data-elementor-type="loop-item"].product', timeout=30000)

        rows = scrape_items_on_page(page)

        # Deduplicar por URL (más confiable)
        new_count = 0
        for r in rows:
            key = (r.get("product_url") or "", r.get("codigo") or "", r.get("nombre_producto") or "")
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(r)
            new_count += 1

        print(f"Página {page_num}: {len(rows)} items, nuevos: {new_count}, total: {len(all_rows)}")

        # Buscar botón "Siguiente"
        next_btn = page.locator('div.jet-filters-pagination__item.prev-next.next:has-text("Siguiente")')

        # Si no existe, terminamos
        if next_btn.count() == 0:
            print("No existe botón 'Siguiente'. Fin.")
            break

        # Si está oculto o no clickeable, terminamos
        try:
            if not next_btn.first.is_visible():
                print("'Siguiente' no visible. Fin.")
                break
        except Exception:
            print("No pude verificar visibilidad de 'Siguiente'. Fin.")
            break

        # Capturar una "huella" del primer producto para confirmar que cambió la página
        first_before = ""
        try:
            first_before = page.locator('div[data-elementor-type="loop-item"].product a[href*="/producto/"]').first.get_attribute("href") or ""
        except Exception:
            pass

        # Click siguiente
        try:
            next_btn.first.click(timeout=8000, force=True)
        except Exception:
            # fallback click en el link interno
            page.locator('div.jet-filters-pagination__item.prev-next.next .jet-filters-pagination__link').first.click(timeout=8000, force=True)

        # Espera a que cambie el listado (AJAX)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeoutError:
            pass

        # Esperar a que el primer producto cambie (si no cambia, probablemente no hay más páginas)
        try:
            page.wait_for_function(
                """(prevHref) => {
                    const a = document.querySelector('div[data-elementor-type="loop-item"].product a[href*="/producto/"]');
                    const href = a ? a.getAttribute('href') : '';
                    return href && href !== prevHref;
                }""",
                arg=first_before,
                timeout=15000
            )
        except Exception:
            # Si no cambió, verificamos si el click no produjo nuevos productos
            # (muchas veces es señal de fin)
            print("No detecté cambio de productos tras 'Siguiente'. Fin.")
            break

        # Por si aparece popup al paginar
        close_yaguar_popup_if_present(page)

    return all_rows
def save_excel(rows, path="yaguar_tigre_todos.xlsx"):
    if not rows:
        print("No hay filas para guardar.")
        return
    df = pd.DataFrame(rows)
    df.to_excel(path, index=False)
    print(f"✅ Guardado: {path} ({len(df)} filas)")

def save_csv(rows, path="yaguar_tigre_items.csv"):
    if not rows:
        print("No hay filas para guardar.")
        return
    cols = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"✅ Guardado: {path} ({len(rows)} filas)")
def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=120)
        context = browser.new_context(viewport={"width": 1280, "height": 800})
        page = context.new_page()

        # 1) ir a la página
        page.goto(URL, wait_until="domcontentloaded")
        page.wait_for_timeout(1600)

        # 2) cerrar popups iniciales si aparecen
        closed1 = close_elementor_popup_if_present(page)
        closed2 = close_yaguar_popup_if_present(page)
        print("Popup Elementor cerrado:", closed1)
        print("Popup Yaguar cerrado:", closed2)

        # 3) login
        page.wait_for_selector("#username", timeout=25000)
        page.fill("#username", USER)

        page.wait_for_selector("#password", timeout=25000)
        page.fill("#password", PASS)

        close_elementor_popup_if_present(page)
        close_yaguar_popup_if_present(page)

        page.locator("#user_registration_ajax_login_submit").click()

        # 4) espera
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except PWTimeoutError:
            pass

        # popups post-login (por si aparecen después)
        close_elementor_popup_if_present(page)
        close_yaguar_popup_if_present(page)

        page.screenshot(path="yaguar_post_login.png", full_page=True)
        print("✅ Login ejecutado. Screenshot: yaguar_post_login.png")
        print("URL actual:", page.url)

        # 5) seleccionar región/sucursal
        close_elementor_popup_if_present(page)
        close_yaguar_popup_if_present(page)
        select_region_and_sucursal(page, REGION, SUCURSAL)

        # popup puede aparecer tras cambiar sucursal
        close_yaguar_popup_if_present(page)

        page.screenshot(path="yaguar_after_select.png", full_page=True)
        print("✅ Selección hecha. Screenshot: yaguar_after_select.png")
        print("URL luego de seleccionar:", page.url)

        # Ir a la tienda de Tigre
        page.goto("https://yaguar.com.ar/tigre/tienda/", wait_until="domcontentloaded")
        page.wait_for_timeout(1500)

        # Por si aparece el popup de Yaguar en esa página también
        rows = scrape_all_pages(page)
        print("Total productos:", len(rows))
        save_csv(rows, "yaguar_tigre_todos.csv")
        save_excel(rows, "yaguar_tigre_todos.xlsx")


if __name__ == "__main__":
    main()
