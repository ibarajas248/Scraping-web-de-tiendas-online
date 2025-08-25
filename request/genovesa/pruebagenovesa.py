#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Genovesa (ALMACÉN) – Scraper con Selenium + BeautifulSoup

- Abre HOME -> acepta/selecciona sucursal si aparece -> va a la categoría
- Scroll gentil (PAGE_DOWN + pasos chicos + near-bottom + botón "Ver más")
- Recorre cada tarjeta, entra al detalle y extrae:
  EAN, Título, Precio unitario, Precio de referencia, Imagen, URL
- Imprime todo lo que va encontrando
- Exporta a XLSX y deja archivos de diagnóstico si falla la carga
"""

import re
import time
import random
import argparse
from typing import Optional, List, Dict, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, WebDriverException

# ================= Config =================
BASE = "https://www.lagenovesadigital.com.ar"
DEFAULT_URL = (
    "https://www.lagenovesadigital.com.ar/ProdFiltrados_DFS"
    "?id=2&dptoID=2&descripcion=ALMACEN%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20"
)

# Listado
LIST_SELECTOR = "div.columContainerList"
CARD_TITLE_SEL = ".textTituloProductos"
CARD_PRICE_SEL = "span.textPrecio b"
CARD_IMG_SEL   = "img.imgListadoProducto"
CARD_LINK_SEL  = "a.columTextList, a.columImgList"
CARD_STOCK_TEXT = ".textSemaforo"
CARD_STOCK_DOT  = "b.Semaforo"

# Detalle
DETAIL_TITLE_SEL = "p.titulo"
DETAIL_EAN_SEL   = "div.textDescripcionDetalle"
DETAIL_PRICE_SEL = "div.textPrecioUnitario"
DETAIL_REF_SEL   = "div.textMensajeReferenciaEscritorio"
DETAIL_IMG_SEL   = "img.imgPromocionesL, img.imgPromociones"

# ===== Tiempos / Scroll =====
WAIT = 30
SCROLL_MAX_ROUNDS = 120              # tope de rondas
SCROLL_KEYS_STEPS = 3                # PAGE_DOWN por ráfaga
SCROLL_GENTLE_STEPS = 12             # desplazamientos pequeños por ráfaga
SCROLL_STEP_PX = 160                 # "más bajo" = pasos chicos
SCROLL_NEAR_BOTTOM_OFFSET = 220      # quedarse cerca del fondo (no fondo fondo)
SCROLL_MAX_NO_GROWTH_BURSTS = 6      # ráfagas sin crecimiento antes de cortar
AFTER_ACTION_SLEEP = (0.6, 1.0)      # espera tras acciones
POLL_AFTER_SCROLL_SEC = 2.0          # espera (con polling) para que aparezcan nuevos ítems
POLL_INTERVAL_SEC = 0.2

# ================ Utils ================
def log(msg: str) -> None:
    print(msg, flush=True)

def human_sleep(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))

def parse_price(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace("\xa0", " ")
    s = re.sub(r"[^\d,.\-]", "", s)  # quita símbolos
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None

def safe_text(el) -> str:
    return el.get_text(strip=True) if el else ""

def snapshot_debug(driver, prefix="genovesa_debug"):
    try:
        with open(f"{prefix}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        driver.save_screenshot(f"{prefix}.png")
        log(f"[DEBUG] Guardados {prefix}.html / {prefix}.png")
    except Exception:
        pass

# ============== Selenium setup ==============
def make_driver(headless: bool = False, user_data_dir: Optional[str] = None, profile_dir: Optional[str] = None):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    if profile_dir:
        opts.add_argument(f"--profile-directory={profile_dir}")

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=es-AR")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
        )
    except Exception:
        pass

    return driver

# ============== Modal / Sucursal ==============
def click_first_visible(driver, labels: List[str]) -> bool:
    for txt in labels:
        els = driver.find_elements(By.XPATH, f"//*[self::button or self::a][contains(., '{txt}')]")
        for e in els:
            try:
                if e.is_displayed():
                    e.click()
                    human_sleep(*AFTER_ACTION_SLEEP)
                    log(f"[MODAL] Click en: {txt}")
                    return True
            except (ElementClickInterceptedException, WebDriverException):
                continue
    return False

def select_first_valid_option(sel_el) -> bool:
    try:
        s = Select(sel_el)
        for i, op in enumerate(s.options):
            val = (op.get_attribute("value") or "").strip()
            if val and val not in ("0", "-1"):
                s.select_by_index(i)
                human_sleep(0.2, 0.4)
                return True
    except Exception:
        pass
    return False

def try_accept_location_modal(driver) -> bool:
    acted = False
    human_sleep(0.4, 0.8)

    selects = driver.find_elements(By.XPATH, "//select[not(@disabled)]")
    for sel in selects:
        if not sel.is_displayed():
            continue
        if select_first_valid_option(sel):
            acted = True
            log("[MODAL] Select sucursal/zona elegido.")

    if click_first_visible(driver, ["Aceptar", "Confirmar", "Continuar", "Ingresar", "Entrar", "Guardar"]):
        acted = True

    closes = driver.find_elements(By.XPATH, "//button[contains(@class,'close') or @aria-label='Close' or contains(., '×')]")
    for c in closes:
        try:
            if c.is_displayed():
                c.click()
                human_sleep(0.2, 0.4)
                log("[MODAL] Cerrado con botón de cierre")
                acted = True
                break
        except Exception:
            pass

    return acted

def warmup_lazy_load(driver, selector: str, rounds: int = 6) -> bool:
    """Scroll arriba/abajo suave para disparar los primeros items."""
    for _ in range(rounds):
        driver.execute_script("window.scrollBy(0, 600);")
        human_sleep(0.3, 0.6)
        driver.execute_script("window.scrollBy(0, -400);")
        human_sleep(0.25, 0.5)
        if driver.find_elements(By.CSS_SELECTOR, selector):
            return True
    return False

def ensure_listing_ready(driver, url: str):
    for attempt in range(1, 3):
        try:
            WebDriverWait(driver, WAIT).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, LIST_SELECTOR))
            )
            cards = driver.find_elements(By.CSS_SELECTOR, LIST_SELECTOR)
            if cards:
                log(f"[INIT] Listado visible con {len(cards)} tarjetas (intento {attempt}).")
                return
        except TimeoutException:
            log(f"[INIT] Timeout esperando listado (intento {attempt}). Intentando resolver modal…")
            if try_accept_location_modal(driver):
                human_sleep(0.6, 1.0)
                driver.get(url)
                human_sleep(0.6, 1.0)
                continue

            if warmup_lazy_load(driver, LIST_SELECTOR, rounds=10):
                return

            snapshot_debug(driver, "genovesa_debug_init")
            if attempt == 1:
                driver.get(url)
                human_sleep(0.8, 1.2)
                continue
            else:
                raise

# ============== Breadcrumb ==============
def get_breadcrumb(driver) -> Tuple[Optional[str], Optional[str]]:
    categoria = subcategoria = None
    try:
        categoria = driver.find_element(By.XPATH, "/html/body/div[1]/main/div[2]/div/div/nav/ol/li[2]").text.strip()
    except Exception:
        pass
    try:
        subcategoria = driver.find_element(By.XPATH, "/html/body/div[1]/main/div[2]/div/div/nav/ol/li[3]").text.strip()
    except Exception:
        pass
    if not categoria or not subcategoria:
        try:
            items = driver.find_elements(By.CSS_SELECTOR, "nav ol li")
            if not categoria and len(items) >= 3:
                categoria = items[1].text.strip()
            if not subcategoria and len(items) >= 4:
                subcategoria = items[2].text.strip()
        except Exception:
            pass
    log(f"[BC] Categoría='{categoria or '-'}' | Subcategoría='{subcategoria or '-'}'")
    return categoria, subcategoria

# ============== Botón "Ver más" (si existe) ==============
def try_click_load_more(driver) -> bool:
    xpaths = [
        "//button[contains(., 'Ver más') or contains(., 'Mostrar más') or contains(., 'Cargar más')]",
        "//a[contains(., 'Ver más') or contains(., 'Mostrar más') or contains(., 'Cargar más')]",
        "//*[contains(@class,'load') and (self::button or self::a)]",
        "//*[contains(@class,'more') and (self::button or self::a)]",
    ]
    for xp in xpaths:
        els = driver.find_elements(By.XPATH, xp)
        for el in els:
            try:
                if el.is_displayed() and el.is_enabled():
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    human_sleep(0.2, 0.4)
                    el.click()
                    log("[SCROLL] Click en botón de carga incremental.")
                    human_sleep(*AFTER_ACTION_SLEEP)
                    return True
            except Exception:
                continue
    return False

# ============== Scroll gentil ==============
def _scroll_burst_gentle(driver, item_selector: str):
    """Ráfaga 'baja': PAGE_DOWN + pasos chicos + near-bottom + último item a la vista."""
    body = driver.find_element(By.TAG_NAME, "body")

    # 1) PAGE_DOWN un par de veces (como humano)
    for _ in range(SCROLL_KEYS_STEPS):
        try:
            body.send_keys(Keys.PAGE_DOWN)
        except Exception:
            pass
        human_sleep(0.08, 0.15)

    # 2) Pasitos cortos
    for _ in range(SCROLL_GENTLE_STEPS):
        driver.execute_script(
            "window.scrollBy(0, arguments[0]); window.dispatchEvent(new Event('scroll'));",
            SCROLL_STEP_PX
        )
        human_sleep(0.10, 0.18)

    # 3) Near-bottom (no hasta el fondo: deja margen)
    try:
        height = driver.execute_script("return document.scrollingElement.scrollHeight")
        vh = driver.execute_script("return window.innerHeight")
        target = max(0, int(height - vh - SCROLL_NEAR_BOTTOM_OFFSET))
        driver.execute_script("window.scrollTo(0, arguments[0]);", target)
        driver.execute_script("window.dispatchEvent(new Event('scroll'));")
    except Exception:
        pass
    human_sleep(0.15, 0.25)

    # 4) Último item a la vista (por si el trigger es por elemento)
    cards = driver.find_elements(By.CSS_SELECTOR, item_selector)
    if cards:
        last = cards[-1]
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'end'});", last)
        except Exception:
            pass

def scroll_until_no_more(
    driver,
    item_selector: str,
    calm_rounds: int = 3,
    max_rounds: int = SCROLL_MAX_ROUNDS,
    max_no_growth_bursts: int = SCROLL_MAX_NO_GROWTH_BURSTS
):
    """
    Scroll 'bajo' y con polling para detectar carga. También prueba botón 'Ver más'.
    Corta cuando:
      - no crece en 'calm_rounds' chequeos y
      - además 'max_no_growth_bursts' ráfagas extra sin crecimiento,
      - o llega a 'max_rounds'.
    """
    WebDriverWait(driver, WAIT).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, item_selector))
    )
    prev_count = len(driver.find_elements(By.CSS_SELECTOR, item_selector))
    prev_height = driver.execute_script("return document.scrollingElement.scrollHeight")
    log(f"[SCROLL] Inicio con {prev_count} tarjetas visibles.")
    same_rounds = 0
    rounds = 0
    no_growth_bursts = 0

    while rounds < max_rounds:
        rounds += 1

        # Botón "Ver más" si existe
        if try_click_load_more(driver):
            # espera con polling a ver si crece
            grew = _poll_for_growth(driver, item_selector, prev_count, prev_height)
            if grew:
                prev_count, prev_height = grew
                same_rounds = 0
                no_growth_bursts = 0
                continue

        # Ráfaga gentil
        _scroll_burst_gentle(driver, item_selector)

        # Polling después del scroll para dar tiempo a Ajax
        grew = _poll_for_growth(driver, item_selector, prev_count, prev_height)
        if grew:
            prev_count, prev_height = grew
            same_rounds = 0
            no_growth_bursts = 0
            continue

        # Sin crecimiento
        same_rounds += 1
        cur = len(driver.find_elements(By.CSS_SELECTOR, item_selector))
        log(f"[SCROLL] Sin nuevos items (chequeo {same_rounds}/{calm_rounds}) — total {cur}.")

        if same_rounds >= calm_rounds:
            no_growth_bursts += 1
            log(f"[SCROLL] Ráfaga extra sin crecimiento ({no_growth_bursts}/{max_no_growth_bursts}).")
            same_rounds = 0

            # otra ráfaga extra gentil
            _scroll_burst_gentle(driver, item_selector)
            grew2 = _poll_for_growth(driver, item_selector, prev_count, prev_height)
            if grew2:
                prev_count, prev_height = grew2
                no_growth_bursts = 0
                continue

            if no_growth_bursts >= max_no_growth_bursts:
                log("[SCROLL] No se cargan más productos tras varias ráfagas extra. Fin del scroll.")
                break

    if rounds >= max_rounds:
        log("[SCROLL] Alcanzado tope de rondas. Fin del scroll.")

def _poll_for_growth(driver, item_selector: str, prev_count: int, prev_height: int) -> Optional[Tuple[int, int]]:
    """
    Espera hasta POLL_AFTER_SCROLL_SEC, chequeando cada POLL_INTERVAL_SEC
    si aumentó el conteo de items o el alto del documento.
    Devuelve (new_count, new_height) si creció, o None si no.
    """
    deadline = time.time() + POLL_AFTER_SCROLL_SEC
    while time.time() < deadline:
        human_sleep(POLL_INTERVAL_SEC, POLL_INTERVAL_SEC + 0.02)
        new_count = len(driver.find_elements(By.CSS_SELECTOR, item_selector))
        new_height = driver.execute_script("return document.scrollingElement.scrollHeight")
        if new_count > prev_count or new_height > prev_height:
            log(f"[SCROLL] +{new_count - prev_count} (total {new_count}).")
            return new_count, new_height
    return None

# ============== Lectura de Listado ==============
def collect_list_cards(driver) -> List[Dict]:
    html = driver.page_source
    soup = BeautifulSoup(html, "lxml")
    out: List[Dict] = []

    for card in soup.select(LIST_SELECTOR):
        link = card.select_one(CARD_LINK_SEL)
        href = urljoin(BASE, link.get("href")) if link else None
        title = safe_text(card.select_one(CARD_TITLE_SEL))
        price_list = safe_text(card.select_one(CARD_PRICE_SEL))
        price_list_num = parse_price(price_list)
        img = card.select_one(CARD_IMG_SEL)
        img_src = urljoin(BASE, img.get("src")) if img and img.get("src") else None
        stock_txt = safe_text(card.select_one(CARD_STOCK_TEXT))
        stock_dot = card.select_one(CARD_STOCK_DOT)
        stock_color = stock_dot.get("style") if stock_dot else ""

        articulo_id = None
        if href and "ArticuloID=" in href:
            qs = parse_qs(urlparse(href).query)
            articulo_id = (qs.get("ArticuloID") or [None])[0]

        row = {
            "list_title": title,
            "list_price_text": price_list,
            "list_price": price_list_num,
            "list_img": img_src,
            "list_stock_text": stock_txt,
            "list_stock_color": stock_color,
            "detail_url": href,
            "articulo_id": articulo_id,
        }
        out.append(row)
        log(f"[LIST] {title} | {price_list or '-'} | {href or '-'}")

    return out

# ============== Detalle ==============
def parse_detail_page(driver, url: str) -> Dict:
    driver.get(url)
    WebDriverWait(driver, WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, DETAIL_TITLE_SEL)))
    human_sleep(0.2, 0.5)

    html = driver.page_source
    soup = BeautifulSoup(html, "lxml")

    title = safe_text(soup.select_one(DETAIL_TITLE_SEL))

    ean = None
    for div in soup.select(DETAIL_EAN_SEL):
        t = safe_text(div)
        m = re.search(r"\b(\d{8,14})\b", t)
        if m:
            ean = m.group(1)
            break

    price_text = safe_text(soup.select_one(DETAIL_PRICE_SEL))
    price_num  = parse_price(price_text)
    ref_text   = safe_text(soup.select_one(DETAIL_REF_SEL))
    img        = soup.select_one(DETAIL_IMG_SEL)
    img_src    = urljoin(BASE, img.get("src")) if img and img.get("src") else None

    log(f"[DETAIL] EAN={ean or '-'} | {title} | {price_text or '-'} | {url}")

    return {
        "ean": ean,
        "detail_title": title,
        "detail_price_text": price_text,
        "detail_price": price_num,
        "detail_ref": ref_text,
        "detail_img": img_src,
        "detail_url": url,
    }

# ============== Orquestador ==============
def scrape_categoria(
    url: str,
    headless: bool = False,
    user_data_dir: Optional[str] = None,
    profile_dir: Optional[str] = None,
    sleep_between_detail=(0.05, 0.15),
) -> pd.DataFrame:
    driver = make_driver(headless=headless, user_data_dir=user_data_dir, profile_dir=profile_dir)
    try:
        # HOME
        log("[INIT] Entrando al HOME…")
        driver.get(BASE)
        human_sleep(0.8, 1.2)
        try_accept_location_modal(driver)

        # Categoría
        log(f"[INIT] Abriendo categoría: {url}")
        driver.get(url)

        ensure_listing_ready(driver, url)

        # Breadcrumb
        categoria, subcategoria = get_breadcrumb(driver)

        # Scroll gentil
        log("[INIT] Iniciando scroll gentil…")
        scroll_until_no_more(
            driver,
            LIST_SELECTOR,
            calm_rounds=3,
            max_rounds=SCROLL_MAX_ROUNDS,
            max_no_growth_bursts=SCROLL_MAX_NO_GROWTH_BURSTS
        )

        # Listado
        log("[LIST] Leyendo tarjetas del listado…")
        cards = collect_list_cards(driver)
        log(f"[LIST] Total en listado: {len(cards)}")

        # Detalles
        rows: List[Dict] = []
        for i, card in enumerate(cards, 1):
            href = card.get("detail_url")
            if not href:
                log(f"[WARN] Sin URL de detalle para: {card.get('list_title','(sin título)')}")
                continue

            tries = 0
            while tries < 2:
                try:
                    det = parse_detail_page(driver, href)
                    row = {**card, **det}
                    row["categoria"] = categoria
                    row["subcategoria"] = subcategoria
                    rows.append(row)
                    log(f"[OK] ({i}/{len(cards)}) Guardado.")
                    break
                except TimeoutException:
                    tries += 1
                    log(f"[ERROR] Timeout detalle (intento {tries}) -> {href}")
                    if tries >= 2:
                        snapshot_debug(driver, f"genovesa_detail_fail_{i}")
                except Exception as e:
                    tries += 1
                    log(f"[ERROR] {type(e).__name__}: {e}")
                    if tries >= 2:
                        snapshot_debug(driver, f"genovesa_detail_fail_{i}")
                finally:
                    human_sleep(*sleep_between_detail)

        # DataFrame
        df = pd.DataFrame(rows)
        cols = [
            "ean", "articulo_id",
            "detail_title", "detail_price", "detail_price_text", "detail_ref",
            "detail_img", "detail_url",
            "list_title", "list_price", "list_price_text",
            "list_img", "list_stock_text", "list_stock_color",
            "categoria", "subcategoria",
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
        log(f"[DONE] Productos con detalle: {len(df)}")
        return df

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        log("[END] Driver cerrado.")

# ============== CLI ==============
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL, help="URL de la categoría a scrapear")
    parser.add_argument("--headless", action="store_true", help="Ejecutar Chrome en modo headless")
    parser.add_argument("--outfile", default="LaGenovesa_ALMACEN.xlsx", help="Archivo XLSX de salida")
    parser.add_argument("--user-data-dir", default=None, help="Ruta a User Data dir de Chrome (opcional)")
    parser.add_argument("--profile-dir", default=None, help="Nombre de profile de Chrome (opcional, ej. 'Default')")
    args = parser.parse_args()

    df = scrape_categoria(
        url=args.url,
        headless=args.headless,
        user_data_dir=args.user_data_dir,
        profile_dir=args.profile_dir,
    )
    log(f"[SAVE] Exportando a {args.outfile} …")
    df.to_excel(args.outfile, index=False)
    log("[SAVE] Archivo XLSX generado.")
