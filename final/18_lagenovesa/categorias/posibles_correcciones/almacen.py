#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Genovesa (ALMACÉN) – Scraper con Selenium + BeautifulSoup + Persistencia MySQL
Hardening para ejecución concurrente de múltiples scrapers.

- HOME -> aceptar sucursal si aparece -> ir a categoría
- Scroll sobre contenedor real (wheel + END + fondo real)
- Lee tarjetas del listado, entra al detalle y extrae:
  EAN, Título, Precio unitario, Precio de referencia, Imagen, URL
- Exporta a XLSX y persiste en MySQL (tiendas, productos, producto_tienda, historico_precios)
- Concurrency-safe:
    * Perfil y puerto exclusivos por proceso
    * GET_LOCK por tienda durante la persistencia
    * Retries + backoff en escritura

Requisitos en VPS (Ubuntu):
  sudo apt update
  sudo apt install -y google-chrome-stable || sudo snap install chromium
  sudo apt install -y unzip xvfb libnss3 libxss1 libasound2 libx11-xcb1 \
     libxrandr2 libxcomposite1 libxdamage1 libxi6 libgtk-3-0 libgbm1 \
     ca-certificates fonts-liberation xdg-utils
  pip install --upgrade pip
  pip install selenium webdriver-manager beautifulsoup4 lxml pandas openpyxl mysql-connector-python
"""

import os
import re
import sys
import time
import random
import socket
import shutil
import argparse
import tempfile
import platform
from typing import Optional, List, Dict, Tuple, Any
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

from mysql.connector import Error as MySQLError

# ---------- Path robusto a base_datos.py ----------
def _ensure_project_root_for_import():
    env_root = os.environ.get("PROJECT_ROOT")
    if env_root and os.path.isfile(os.path.join(env_root, "base_datos.py")):
        sys.path.append(os.path.abspath(env_root))
        return
    here = os.path.abspath(os.path.dirname(__file__))
    for up in range(5):
        candidate = os.path.abspath(os.path.join(here, *([".."] * up)))
        if os.path.isfile(os.path.join(candidate, "base_datos.py")):
            sys.path.append(candidate)
            return

_ensure_project_root_for_import()
from base_datos import get_conn  # <- tu conexión MySQL

# ================= Config =================
BASE = "https://www.lagenovesadigital.com.ar"
DEFAULT_URL = (
    "https://www.lagenovesadigital.com.ar/ProdFiltrados_DFS"
    "?id=2&dptoID=2&descripcion=ALMACEN%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20"
)

# Listado
LIST_SELECTOR   = "div.columContainerList"
CARD_TITLE_SEL  = ".textTituloProductos"
CARD_PRICE_SEL  = "span.textPrecio b"
CARD_IMG_SEL    = "img.imgListadoProducto"
CARD_LINK_SEL   = "a.columTextList, a.columImgList"
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
SCROLL_MAX_ROUNDS = 120
SCROLL_MAX_NO_GROWTH_BURSTS = 6
AFTER_ACTION_SLEEP = (0.6, 1.0)

# ================ Utils ================
def log(msg: str) -> None:
    print(msg, flush=True)

def human_sleep(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))

def parse_price(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace("\xa0", " ")
    s = re.sub(r"[^\d,.\-]", "", s)
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
        try:
            driver.save_screenshot(f"{prefix}.png")
        except Exception:
            pass
        log(f"[DEBUG] Guardados {prefix}.html / {prefix}.png")
    except Exception:
        pass

# ===== Helpers de perfil / puerto =====
def _get_free_port(start=9222, end=9360) -> Optional[int]:
    for p in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", p))
                return p
            except OSError:
                continue
    return None

def _cleanup_singleton_locks(user_data_dir: str):
    try:
        for root, _, files in os.walk(user_data_dir):
            for name in files:
                if name.startswith("Singleton"):
                    try:
                        os.remove(os.path.join(root, name))
                    except Exception:
                        pass
    except Exception:
        pass
    try:
        for f in os.listdir("/tmp"):
            if f.startswith(".org.chromium.Chromium"):
                try: os.remove(os.path.join("/tmp", f))
                except Exception: pass
    except Exception:
        pass

# ---------- Proxy helper ----------
def _apply_proxy_from_env(opts: Options):
    proxy = os.environ.get("SELENIUM_PROXY") or os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
    if proxy:
        opts.add_argument(f"--proxy-server={proxy}")

# ---------- Chrome binary candidates ----------
def _possible_binaries():
    env_bin = os.environ.get("CHROME_BIN") or os.environ.get("GOOGLE_CHROME_BIN")
    if env_bin:
        yield env_bin
    for p in [
        "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable",
        "/snap/bin/chromium", "/usr/bin/chromium", "/usr/bin/chromium-browser",
        "/opt/google/chrome/google-chrome"
    ]:
        if os.path.exists(p):
            yield p

# ============== Selenium setup (endurecido VPS) ==============
def make_driver(
    headless: bool = True,
    user_data_dir: Optional[str] = None,
    profile_dir: Optional[str] = None,
    keep_profile: bool = False,
    block_images: bool = True,
    job_id: Optional[str] = None
):
    log(f"[ENV] Python={platform.python_version()} | System={platform.platform()}")
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")

    # Preferencias (bloquear imágenes para paralelismo)
    if block_images:
        prefs = {"profile.managed_default_content_settings.images": 2}
        opts.add_experimental_option("prefs", prefs)

    # Perfil único por proceso/job
    temp_dir = None
    if not user_data_dir:
        temp_dir = tempfile.mkdtemp(prefix=f"chrome-prof-{os.getpid()}-{int(time.time())}-{random.randint(1000,9999)}-")
        user_data_dir = temp_dir
    else:
        os.makedirs(user_data_dir, exist_ok=True)
        _cleanup_singleton_locks(user_data_dir)
    opts.add_argument(f"--user-data-dir={user_data_dir}")

    if not profile_dir:
        ts = time.strftime("%Y%m%d-%H%M%S")
        rnd = random.randint(1000, 9999)
        profile_dir = f"Profile-{os.getpid()}-{ts}-{rnd}"
    opts.add_argument(f"--profile-directory={profile_dir}")

    # Puerto debugging exclusivo
    free_port = _get_free_port()
    if free_port:
        opts.add_argument(f"--remote-debugging-port={free_port}")

    # Flags VPS
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("--lang=es-AR")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )
    _apply_proxy_from_env(opts)

    # Intentar fijar binario si existe
    for bin_path in _possible_binaries():
        try:
            if os.path.exists(bin_path):
                opts.binary_location = bin_path
                log(f"[CHROME] Usando binario: {bin_path}")
                break
        except Exception:
            pass

    # Forzar cache local de webdriver_manager para reducir contención
    os.environ.setdefault("WDM_LOCAL", "1")

    # Crear driver (webdriver_manager) con fallback a /usr/bin/chromedriver
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
    except Exception as e:
        log(f"[CHROME] Error inicial creando driver: {e}. Intentando /usr/bin/chromedriver…")
        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=opts)

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
        )
    except Exception:
        pass

    # Limpieza automática si fue un perfil temporal
    if temp_dir and not keep_profile:
        import atexit
        @atexit.register
        def _cleanup():
            try:
                driver.quit()
            except Exception:
                pass
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
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
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", e)
                    human_sleep(0.1, 0.2)
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
    for _ in range(rounds):
        driver.execute_script("window.scrollBy(0, 600);")
        human_sleep(0.3, 0.6)
        driver.execute_script("window.scrollBy(0, -400);")
        human_sleep(0.25, 0.5)
        if driver.find_elements(By.CSS_SELECTOR, selector):
            return True
    return False

def ensure_listing_ready(driver, url: str):
    try:
        log(f"[NAV] UA: {driver.execute_script('return navigator.userAgent')}")
    except Exception:
        pass
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

# ============== detectar contenedor real de scroll ==============
def get_scroll_root(driver, item_selector: str):
    js = r"""
    const sel = arguments[0];
    const list = document.querySelector(sel);
    if (!list) return null;
    function isScrollable(n){
      if (!n) return false;
      const st = getComputedStyle(n);
      const hasSpace = n.scrollHeight > (n.clientHeight + 5);
      const y = st.overflowY;
      return hasSpace && /(auto|scroll|overlay)/.test(y);
    }
    let el = list;
    while (el && el !== document.body) {
      if (isScrollable(el)) { el.setAttribute('data-scrroot','1'); return el; }
      el = el.parentElement;
    }
    const root = document.scrollingElement || document.documentElement || document.body;
    root.setAttribute('data-scrroot','1');
    return root;
    """
    driver.execute_script(js, item_selector)
    try:
        return driver.find_element(By.CSS_SELECTOR, "[data-scrroot='1']")
    except Exception:
        return driver.find_element(By.TAG_NAME, "body")

# ============== Scroll gentil ==============
def _scroll_burst_gentle(driver, item_selector: str, root_el):
    try:
        driver.execute_script("arguments[0].focus();", root_el)
        root_el.click()
    except Exception:
        pass
    for _ in range(6):
        try:
            driver.execute_script(
                "arguments[0].dispatchEvent(new WheelEvent('wheel', {deltaY: 600, bubbles: true}));",
                root_el
            )
        except Exception:
            break
        human_sleep(0.05, 0.12)
    try:
        for _ in range(10):
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 200;", root_el)
            human_sleep(0.08, 0.15)
    except Exception:
        pass
    try:
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", root_el)
    except Exception:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    human_sleep(0.12, 0.22)
    try:
        root_el.send_keys(Keys.END)
    except Exception:
        pass

def _poll_for_growth(driver, item_selector: str, prev_count: int, prev_height: int, root_el) -> Optional[Tuple[int, int]]:
    deadline = time.time() + 3.5
    while time.time() < deadline:
        human_sleep(0.20, 0.25)
        new_count = len(driver.find_elements(By.CSS_SELECTOR, item_selector))
        try:
            new_height = driver.execute_script(
                "return arguments[0].scrollHeight || document.scrollingElement.scrollHeight;", root_el
            )
        except Exception:
            new_height = driver.execute_script("return document.scrollingElement.scrollHeight")
        if new_count > prev_count or new_height > prev_height:
            log(f"[SCROLL] +{new_count - prev_count} (total {new_count}).")
            return new_count, int(new_height)
    return None

def scroll_until_no_more(
    driver,
    item_selector: str,
    calm_rounds: int = 3,
    max_rounds: int = SCROLL_MAX_ROUNDS,
    max_no_growth_bursts: int = SCROLL_MAX_NO_GROWTH_BURSTS
):
    WebDriverWait(driver, WAIT).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, item_selector))
    )
    root_el = get_scroll_root(driver, item_selector)

    prev_count = len(driver.find_elements(By.CSS_SELECTOR, item_selector))
    try:
        prev_height = driver.execute_script(
            "return arguments[0].scrollHeight || document.scrollingElement.scrollHeight;", root_el
        )
    except Exception:
        prev_height = driver.execute_script("return document.scrollingElement.scrollHeight")

    log(f"[SCROLL] Inicio con {prev_count} tarjetas visibles.")
    same_rounds = 0
    rounds = 0
    no_growth_bursts = 0

    while rounds < max_rounds:
        rounds += 1

        if try_click_load_more(driver):
            grew = _poll_for_growth(driver, item_selector, prev_count, prev_height, root_el)
            if grew:
                prev_count, prev_height = grew
                same_rounds = 0
                no_growth_bursts = 0
                continue

        _scroll_burst_gentle(driver, item_selector, root_el)
        grew = _poll_for_growth(driver, item_selector, prev_count, prev_height, root_el)
        if grew:
            prev_count, prev_height = grew
            same_rounds = 0
            no_growth_bursts = 0
            continue

        same_rounds += 1
        cur = len(driver.find_elements(By.CSS_SELECTOR, item_selector))
        log(f"[SCROLL] Sin nuevos items (chequeo {same_rounds}/{calm_rounds}) — total {cur}.")

        if same_rounds >= calm_rounds:
            no_growth_bursts += 1
            log(f"[SCROLL] Ráfaga extra sin crecimiento ({no_growth_bursts}/{max_no_growth_bursts}).")
            same_rounds = 0

            _scroll_burst_gentle(driver, item_selector, root_el)
            grew2 = _poll_for_growth(driver, item_selector, prev_count, prev_height, root_el)
            if grew2:
                prev_count, prev_height = grew2
                no_growth_bursts = 0
                continue

            if no_growth_bursts >= max_no_growth_bursts:
                log("[SCROLL] No se cargan más productos tras varias ráfagas extra. Fin del scroll.")
                break

    if rounds >= max_rounds:
        log("[SCROLL] Alcanzado tope de rondas. Fin del scroll.")

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
    headless: bool = True,
    user_data_dir: Optional[str] = None,
    profile_dir: Optional[str] = None,
    keep_profile: bool = False,
    sleep_between_detail=(0.05, 0.15),
    job_id: Optional[str] = None
) -> pd.DataFrame:
    driver = make_driver(
        headless=headless,
        user_data_dir=user_data_dir,
        profile_dir=profile_dir,
        keep_profile=keep_profile,
        job_id=job_id
    )
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

# =================== DB (contrato estándar) ===================
_NULLLIKE = {"", "null", "none", "nan", "na"}
def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

TIENDA_CODIGO = "lagenovesa"
TIENDA_NOMBRE = "La Genovesa"

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("ean"))
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("nombre") or "", p.get("marca") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    nombre = clean(p.get("nombre")) or ""
    marca  = clean(p.get("marca")) or ""
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("ean") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", nombre, marca, p.get("fabricante") or "",
        p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre_tienda") or p.get("nombre") or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,'')) 
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid

    if rec:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,'')) 
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en):
    def to_txt_or_none(x):
        v = parse_price(x)
        if x is None: return None
        if isinstance(v, float) and (v != v):  # NaN
            return None
        try:
            return f"{round(float(v), 2)}"
        except Exception:
            return None

    cur.execute("""
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = VALUES(promo_tipo),
          promo_texto_regular = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios = VALUES(promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        to_txt_or_none(p.get("precio_lista")),
        to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None,
        p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None,
        p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# ---- mapeo específico La Genovesa -> contrato DB ----
def row_to_db_product(row: Dict[str, Any]) -> Dict[str, Any]:
    sku = (row.get("articulo_id") or
           (parse_qs(urlparse(row.get("detail_url") or "").query).get("ArticuloID") or [None])[0])

    precio_oferta = row.get("detail_price")
    precio_lista  = row.get("list_price")
    if not precio_lista and row.get("detail_ref"):
        m = re.search(r"([\$]?\s*\d[\d\.\,]*)", row["detail_ref"])
        if m:
            precio_lista = m.group(1)

    nombre_det = clean(row.get("detail_title") or row.get("list_title"))

    return {
        "sku":        clean(sku),
        "record_id":  None,
        "ean":        clean(row.get("ean")),
        "nombre":     nombre_det,
        "marca":      None,
        "fabricante": None,
        "categoria":  clean(row.get("categoria")),
        "subcategoria": clean(row.get("subcategoria")),

        "precio_lista":  clean(precio_lista),
        "precio_oferta": clean(precio_oferta),
        "tipo_oferta":   None,
        "promo_tipo":    None,
        "precio_regular_promo": clean(row.get("detail_ref")),
        "precio_descuento":     None,
        "comentarios_promo":    None,

        "url":            clean(row.get("detail_url")),
        "nombre_tienda":  nombre_det,
    }

def persistir_df_en_mysql(
    df: pd.DataFrame,
    tienda_codigo=TIENDA_CODIGO,
    tienda_nombre=TIENDA_NOMBRE,
    chunk_size: int = 200,
    max_retries: int = 3,
    lock_timeout_sec: int = 60
):
    productos = [row_to_db_product(r) for r in df.to_dict(orient="records")]
    if not productos:
        print("⚠️ No hay productos para guardar en DB.")
        return

    from datetime import datetime
    capturado_en = datetime.now()

    conn = None
    lock_name = f"retail:persist:{tienda_codigo}"
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        # Ajustes de sesión para minimizar bloqueos
        try:
            cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception:
            pass
        try:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 25")
        except Exception:
            pass

        # ---- Lock lógico por tienda (concurrency-safe) ----
        cur.execute("SELECT GET_LOCK(%s, %s)", (lock_name, lock_timeout_sec))
        got_lock = (cur.fetchone() or [0])[0] == 1
        if not got_lock:
            raise MySQLError(f"No se obtuvo GET_LOCK({lock_name}) en {lock_timeout_sec}s")

        try:
            tienda_id = upsert_tienda(cur, tienda_codigo, tienda_nombre)
            conn.commit()

            total = len(productos)
            for start in range(0, total, chunk_size):
                chunk = productos[start:start + chunk_size]

                for attempt in range(max_retries):
                    try:
                        cur = conn.cursor()
                        for p in chunk:
                            producto_id = find_or_create_producto(cur, p)
                            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                        conn.commit()
                        print(f"💾 Guardado chunk {start+1}-{start+len(chunk)} de {total}")
                        break
                    except MySQLError as e:
                        code = getattr(e, "errno", None)
                        conn.rollback()
                        if code in (1205, 1213) and attempt < max_retries - 1:
                            delay = 1.5 * (attempt + 1) + random.uniform(0.2, 0.8)
                            print(f"⏳ Lock/Deadlock (errno={code}). Reintentando en {delay:.1f}s…")
                            time.sleep(delay)
                            continue
                        else:
                            raise
        finally:
            # Liberar lock aunque haya fallos
            try:
                cur = conn.cursor()
                cur.execute("DO RELEASE_LOCK(%s)", (lock_name,))
                conn.commit()
            except Exception:
                pass

        print(f"✅ Persistencia completa: {len(productos)} filas de histórico para {tienda_nombre} ({capturado_en})")

    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

# ============== CLI ==============
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL, help="URL de la categoría a scrapear")
    parser.add_argument("--headless", action="store_true", help="Forzar modo headless")
    parser.add_argument("--no-headless", action="store_true", help="Desactivar headless")
    parser.add_argument("--outfile", default="LaGenovesa_ALMACEN.xlsx", help="Archivo XLSX de salida")
    parser.add_argument("--no-xlsx", action="store_true", help="No exportar XLSX")
    parser.add_argument("--user-data-dir", default=None, help="Ruta a User Data dir de Chrome (opcional)")
    parser.add_argument("--profile-dir", default=None, help="Nombre de profile de Chrome (opcional, ej. 'Default')")
    parser.add_argument("--keep-profile", action="store_true", help="No borrar el perfil temporal al salir")
    parser.add_argument("--job-id", default=None, help="Identificador opcional del job (solo trazas)")
    args = parser.parse_args()

    _headless = True
    if args.no_headless:
        _headless = False
    if args.headless:
        _headless = True

    log(f"[RUN] Headless={_headless} | Outfile={args.outfile} | JobID={args.job_id or '-'}")

    df = scrape_categoria(
        url=args.url,
        headless=_headless,
        user_data_dir=args.user_data_dir,
        profile_dir=args.profile_dir,
        keep_profile=args.keep_profile,
        job_id=args.job_id
    )

    # Persistir en MySQL con contrato estándar (en chunks con reintentos para evitar 1205)
    persistir_df_en_mysql(df, tienda_codigo=TIENDA_CODIGO, tienda_nombre=TIENDA_NOMBRE)

    # Exportar a Excel
    if not args.no_xlsx:
        log(f"[SAVE] Exportando a {args.outfile} …")
        try:
            #df.to_excel(args.outfile, index=False)
            log("[SAVE] Archivo XLSX generado.")
        except Exception as e:
            log(f"[SAVE] Error al exportar XLSX: {e}")
