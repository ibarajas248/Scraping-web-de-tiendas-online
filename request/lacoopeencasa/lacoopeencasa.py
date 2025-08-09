# -*- coding: utf-8 -*-
# Requisitos:
#   pip install selenium webdriver-manager beautifulsoup4 pandas

import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException,
    ElementClickInterceptedException, InvalidSelectorException, WebDriverException
)

# ------------------ Config ------------------
BASE = "https://www.lacoopeencasa.coop"
CATEGORY = "almacen"
CAT_ID = 2

PAGE_START = 1
PAGE_END = 20              # sube este valor para cubrir más páginas
WAIT = 25
HUMAN_SLEEP = 0.2
OUT_XLSX = "la_coope_almacen.xlsx"

CSS_CARD_ANCHOR = "col-listado-articulo a[id^='listadoArt']"
MONEY_RX = re.compile(r"[^\d,.\-]")
ID_RX = re.compile(r"/(\d+)(?:$|[/?#])")  # captura el código al final del href (e.g., .../311787)

# ------------------ Driver ------------------
def make_driver(headless=False):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=es-AR")
    options.add_argument("--window-size=1366,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Evita "Chrome is being controlled by automated test software"
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(45)
    return driver

# ------------------ Helpers DOM ------------------
def wait_cards(driver, timeout=WAIT):
    """Espera presencia y visibilidad de cards; retorna True/False en vez de lanzar siempre."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CSS_CARD_ANCHOR))
        )
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_any_elements_located((By.CSS_SELECTOR, CSS_CARD_ANCHOR))
        )
        return True
    except TimeoutException:
        return False

def get_page_cards(driver):
    """Obtiene (href, title) de cada card; tolerante a StaleElementReference."""
    try:
        anchors = driver.find_elements(By.CSS_SELECTOR, CSS_CARD_ANCHOR)
    except WebDriverException:
        anchors = []
    items = []
    for a in anchors:
        try:
            href = a.get_attribute("href") or a.get_attribute("ng-reflect-router-link")
            if href and href.startswith("/"):
                href = BASE + href
            title = a.get_attribute("title") or a.text.strip()
            if href:
                items.append((href, (title or "").strip()))
        except StaleElementReferenceException:
            continue
        except WebDriverException:
            continue
    return items

def product_code_from_href(href: str) -> str:
    m = ID_RX.search(href or "")
    return m.group(1) if m else ""

def to_relative_path(href: str) -> str:
    p = urlparse(href or "")
    if p.scheme and p.netloc:
        return p.path
    return href or ""

def css_escape_attr_value(v: str) -> str:
    """Escapa para a[href="..."] (maneja comillas y backslashes)."""
    if v is None:
        return ""
    return v.replace("\\", "\\\\").replace('"', r'\"')

def sel_href_eq(v: str) -> str:
    return f'a[href="{css_escape_attr_value(v)}"]'

def lazy_scroll_find(driver, selector: str, max_steps: int = 10, pause: float = 0.18):
    """Hace scroll por tramos y retorna el elemento si aparece; maneja InvalidSelector/Stale."""
    try:
        for i in range(max_steps):
            try:
                els = driver.find_elements(By.CSS_SELECTOR, selector)
            except InvalidSelectorException:
                return None
            except WebDriverException:
                els = []
            if els:
                return els[0]
            try:
                driver.execute_script(
                    "window.scrollTo(0, (document.body.scrollHeight/arguments[0])*arguments[1]);",
                    max_steps, i + 1
                )
            except WebDriverException:
                pass
            time.sleep(pause)
        # último intento: top de nuevo
        try:
            driver.execute_script("window.scrollTo(0,0);")
        except WebDriverException:
            pass
        try:
            els = driver.find_elements(By.CSS_SELECTOR, selector)
        except (InvalidSelectorException, WebDriverException):
            return None
        return els[0] if els else None
    except Exception:
        return None

def smart_open_product(driver, list_url: str, href: str, timeout: int = 25, retries: int = 2) -> bool:
    """
    Abre el producto de la grilla con 3 estrategias y reintentos:
    1) por href (abs/relativo) + lazy scroll
    2) por id #listadoArt{codigo}
    3) fallback: driver.get(href)
    Retorna True si terminó en /producto/.
    """
    rel = to_relative_path(href)
    code = product_code_from_href(href)

    # Asegurar que estamos en la grilla correcta
    try:
        if not driver.current_url.startswith(list_url):
            driver.get(list_url)
            if not wait_cards(driver, timeout):
                return False
    except WebDriverException:
        return False

    for attempt in range(retries + 1):
        mode = None
        # 1) Intento por href (abs o relativo) con lazy-scroll y selector seguro
        sel_href = f"{sel_href_eq(rel)}, {sel_href_eq(href)}"
        el = lazy_scroll_find(driver, sel_href)
        if el:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.06)
                driver.execute_script("arguments[0].click();", el)
                mode = "href"
            except (ElementClickInterceptedException, WebDriverException, StaleElementReferenceException):
                # fallback a navigate
                try:
                    driver.get(href)
                    mode = "get"
                except WebDriverException:
                    mode = "error"

        else:
            # 2) Por id listadoArt{codigo}
            if code:
                sel_id = f"a#listadoArt{code}, a[id='listadoArt{code}']"
                el2 = lazy_scroll_find(driver, sel_id)
                if el2:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el2)
                        time.sleep(0.05)
                        driver.execute_script("arguments[0].click();", el2)
                        mode = "id"
                    except (ElementClickInterceptedException, WebDriverException, StaleElementReferenceException):
                        try:
                            driver.get(href)
                            mode = "get"
                        except WebDriverException:
                            mode = "error"
            # 3) Fallback: navegar directo si todavía no cambiamos de URL
            if not mode:
                try:
                    driver.get(href)
                    mode = "get"
                except WebDriverException:
                    mode = "error"

        # Esperar que realmente estemos en detalle
        try:
            WebDriverWait(driver, timeout).until(EC.url_contains("/producto/"))
            print(f"      ↪️  abierto por: {mode} (intento {attempt+1})")
            return True
        except TimeoutException:
            print(f"      ⚠️  no abrió detalle (mode={mode}, intento {attempt+1})")
            # pequeño backoff y reintento
            time.sleep(0.3 + attempt * 0.3)
            # volver a la grilla si nos quedamos en otro lado
            try:
                driver.get(list_url)
                wait_cards(driver, timeout)
            except WebDriverException:
                pass

    return False

# ------------------ Helpers parsing ------------------
def money_to_float(txt: str):
    """Convierte '$4.099,00' -> 4099.00; devuelve None si no puede."""
    if not txt:
        return None
    t = MONEY_RX.sub("", txt).strip()
    t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None

def safe_select_text(soup, selector, attr=None):
    el = soup.select_one(selector)
    if not el:
        return ""
    if attr:
        return (el.get(attr) or "").strip()
    return el.get_text(" ", strip=True)

def extract_product_fields(driver):
    """Desde la página de detalle ya cargada, extrae los campos solicitados."""
    try:
        WebDriverWait(driver, WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".articulo-detalle-titulo, .precio-detalle"))
        )
    except TimeoutException:
        pass

    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return {}

    titulo = safe_select_text(soup, "h1.articulo-detalle-titulo")

    # Marca y categoría
    marca = safe_select_text(soup, ".articulo-detalle-marca h2")
    categoria = ""
    try:
        spans = soup.select(".articulo-detalle-marca")
        if len(spans) >= 2:
            h2 = spans[1].select_one("h2")
            if h2:
                categoria = h2.get_text(" ", strip=True)
    except Exception:
        pass

    precio_txt = safe_select_text(soup, ".precio.precio-detalle")
    precio = money_to_float(precio_txt)

    precio_unitario_txt = safe_select_text(soup, ".precio-unitario")
    precio_unitario = None
    if precio_unitario_txt:
        m = re.search(r"\$?\s*([\d\.\,]+)", precio_unitario_txt)
        if m:
            precio_unitario = money_to_float(m.group(1))

    precio_sin_imp_txt = safe_select_text(soup, ".precio-sin-impuestos span:nth-of-type(2)")
    precio_sin_impuestos = money_to_float(precio_sin_imp_txt)

    codigo_interno = safe_select_text(soup, ".articulo-codigo span")

    imagen_url = safe_select_text(soup, ".articulo-detalle-imagen-ppal", attr="src")
    if not imagen_url:
        imagen_url = safe_select_text(soup, ".articulo-detalle-imagen-contenedor img", attr="src")

    url_producto = ""
    try:
        url_producto = driver.current_url
    except WebDriverException:
        url_producto = ""

    return {
        "titulo": titulo,
        "marca": marca,
        "categoria": categoria,
        "precio": precio,
        "precio_txt": precio_txt,
        "precio_unitario": precio_unitario,
        "precio_unitario_txt": precio_unitario_txt,
        "precio_sin_impuestos": precio_sin_impuestos,
        "precio_sin_impuestos_txt": precio_sin_imp_txt,
        "codigo_interno": codigo_interno,
        "imagen_url": imagen_url,
        "url_producto": url_producto
    }

# ------------------ Main ------------------
def run():
    driver = make_driver(headless=False)  # True si quieres headless
    data = []
    seen_codes = set()
    try:
        for page in range(PAGE_START, PAGE_END + 1):
            url = f"{BASE}/listado/categoria/{CATEGORY}/{CAT_ID}/pagina--{page}"
            print(f"\n📄 Página {page}: {url}")

            try:
                driver.get(url)
            except WebDriverException as e:
                print(f"⚠️  No se pudo cargar la página {page}: {e}")
                continue

            # Esperar tarjetas
            if not wait_cards(driver):
                print("⚠️  No se detectaron productos en esta página. Deteniendo.")
                break

            # pequeño scroll para disparar lazy
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.25);")
            except WebDriverException:
                pass
            time.sleep(0.3)

            cards = get_page_cards(driver)
            print(f"🧾 Encontrados {len(cards)} productos en la grilla de la página {page}")
            if not cards:
                print("⚠️  Sin tarjetas; deteniendo.")
                break

            for i, (href, title) in enumerate(cards, start=1):
                print(f"   {i:02d}/{len(cards)} -> abrir: {title or '(sin título)'} | {href}")

                ok = smart_open_product(driver, url, href, timeout=WAIT, retries=2)
                if not ok:
                    print("   ⚠️  No navegó al detalle (timeout). Sigo con el siguiente.")
                    continue

                # Extraer campos
                try:
                    row = extract_product_fields(driver)
                    if not row or not (row.get("titulo") or row.get("codigo_interno")):
                        print("      ⚠️  Extracción vacía; skipping.")
                    else:
                        code = (row.get("codigo_interno") or "").strip()
                        if code and code in seen_codes:
                            print(f"      ℹ️  Duplicado cod {code}; skipping.")
                        else:
                            data.append(row)
                            if code:
                                seen_codes.add(code)
                            print(f"      ✅ {row.get('titulo','(sin título)')} | ${row.get('precio')} | cod {code or '-'}")
                except Exception as e:
                    print(f"      ⚠️  Error extrayendo: {e}")

                time.sleep(HUMAN_SLEEP)
                # Volver a la grilla con tolerancia
                try:
                    driver.back()
                    wait_cards(driver)
                except WebDriverException:
                    try:
                        driver.get(url)
                        wait_cards(driver)
                    except WebDriverException:
                        pass
                time.sleep(0.1)

            time.sleep(0.4)

        # Exportar
        if data:
            df = pd.DataFrame(data)
            cols = [
                "titulo","marca","categoria",
                "precio","precio_txt","precio_unitario","precio_unitario_txt",
                "precio_sin_impuestos","precio_sin_impuestos_txt",
                "codigo_interno","imagen_url","url_producto"
            ]
            df = df.reindex(columns=cols)
            df.to_excel(OUT_XLSX, index=False)
            print(f"\n💾 Exportado: {OUT_XLSX} | filas: {len(df)}")
        else:
            print("\nℹ️ No se recolectó ningún dato.")
    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    run()
