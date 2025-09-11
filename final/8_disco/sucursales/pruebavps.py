#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, json, unicodedata, signal, subprocess, contextlib
import sys
import tempfile, shutil, atexit
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
from mysql.connector import Error as MySQLError

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.common.exceptions import SessionNotCreatedException, WebDriverException, TimeoutException
from selenium.webdriver.chrome.service import Service

# webdriver_manager (fallback opcional)
try:
    from webdriver_manager.chrome import ChromeDriverManager
    _HAS_WDM = True
except Exception:
    _HAS_WDM = False

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

# --- Helper de conexión MySQL (ajusta a tu proyecto)
from base_datos import get_conn

# =========================
# Parámetros (ENV primero, con defaults)
# =========================
BASE          = "https://www.disco.com.ar"
DISCO_USER    = os.getenv("DISCO_USER", "comercial@factory-blue.com")
DISCO_PASS    = os.getenv("DISCO_PASS", "Compras2025")

PROVINCIA     = os.getenv("DISCO_PROVINCIA", "CORDOBA").strip()
TIENDA_NOM    = os.getenv("DISCO_TIENDA", "Disco Alta Córdoba Cabrera 493").strip()

# punto de entrada de scraping (puede ser /almacen, /bebidas, /lacteos, etc.)
CATEGORIA_URL = os.getenv("DISCO_CATEGORIA_URL", "/almacen").strip()

# Control de crawling
MAX_EMPTY     = int(os.getenv("DISCO_MAX_EMPTY", "1"))    # páginas vacías toleradas
SLEEP_PDP     = float(os.getenv("DISCO_SLEEP_PDP", "0.6"))  # espera entre PDPs
SLEEP_PAGE    = float(os.getenv("DISCO_SLEEP_PAGE", "1.0")) # espera al cargar la lista

# Identificadores de tienda para tu esquema
TIENDA_CODIGO = os.getenv("DISCO_TIENDA_CODIGO", "disco_cordoba_alto_cordoba_cabrera_493")
TIENDA_NOMBRE = os.getenv("DISCO_TIENDA_NOMBRE", "Disco_cordoba_alto_cordoba_cabrera_493")
TIENDA_REF    = os.getenv("DISCO_REF_TIENDA", "disco_cordoba_cabrera_493")

# Opcional: matar huérfanos Chrome
KILL_STALE_CHROME = 1

# =========================
# Utilidades
# =========================
def _normalize(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s or '') if not unicodedata.combining(c)).strip().lower()

def _clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def _clip(s: Optional[str], maxlen: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s[:maxlen]

def _parse_price(text: str):
    """'$1.912,5' / '$2.550' -> (float_or_nan, raw)"""
    raw = (text or "").strip()
    if not raw:
        return np.nan, raw
    s = re.sub(r"[^\d,\.]", "", raw)
    if not s:
        return np.nan, raw
    last_comma = s.rfind(",")
    last_dot   = s.rfind(".")
    if last_comma > last_dot:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s), raw
    except Exception:
        return np.nan, raw

def _to_txt(x):
    try:
        f = float(x)
        if np.isnan(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

def _click_with_retry(driver, wait, xpath: str, retries: int = 3) -> None:
    last_exc = None
    for _ in range(retries):
        try:
            el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            try:
                el.click()
            except Exception:
                try:
                    ActionChains(driver).move_to_element(el).pause(0.2).click(el).perform()
                except Exception:
                    driver.execute_script("arguments[0].click();", el)
            return
        except Exception as e:
            last_exc = e
            time.sleep(0.8)
    raise last_exc

def _type_with_retry(driver, wait, xpath: str, text: str, retries: int = 3) -> None:
    last_exc = None
    for _ in range(retries):
        try:
            el = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            with contextlib.suppress(Exception):
                el.clear()
            el.send_keys(text)
            return
        except Exception as e:
            last_exc = e
            time.sleep(0.8)
    raise last_exc

def _select_by_text_case_insensitive(driver, wait, select_xpath: str, target_text: str, retries: int = 3) -> None:
    last_exc = None
    tgt = _normalize(target_text)
    for _ in range(retries):
        try:
            sel = wait.until(EC.presence_of_element_located((By.XPATH, select_xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sel)
            wait.until(lambda d: len(sel.find_elements(By.TAG_NAME, "option")) > 0)
            # 1) Intento directo por visible_text
            with contextlib.suppress(Exception):
                Select(sel).select_by_visible_text(target_text)
                return
            # 2) Igualando por texto normalizado
            value = None
            for o in sel.find_elements(By.TAG_NAME, "option"):
                if o.get_attribute("disabled"):
                    continue
                if _normalize(o.text) == tgt or tgt in _normalize(o.text):
                    value = o.get_attribute("value")
                    break
            if value is None:
                raise RuntimeError(f"Opción no encontrada: {target_text}")
            with contextlib.suppress(Exception):
                Select(sel).select_by_value(value)
                return
            driver.execute_script("""
                const s = arguments[0], val = arguments[1];
                s.value = val; s.dispatchEvent(new Event('change', {bubbles:true}));
            """, sel, value)
            return
        except Exception as e:
            last_exc = e
            time.sleep(0.8)
    raise last_exc

# =========================
# Carga completa de productos en la página (scroll + “ver más” + estabilidad)
# =========================
CARD_ANCHOR_XPATHS = [
    "//a[contains(@class,'vtex-product-summary-2-x-clearLink') and contains(@href,'/p')]",
    "//a[@data-testid='product-summary-link' and contains(@href,'/p')]",
    "//section[contains(@class,'gallery')]//a[contains(@href,'/p')]",
]

def _count_card_links(driver) -> int:
    seen = set()
    for xp in CARD_ANCHOR_XPATHS:
        for a in driver.find_elements(By.XPATH, xp):
            href = a.get_attribute("href") or ""
            if href:
                seen.add(href)
    return len(seen)

def _click_load_more_if_any(driver) -> bool:
    # Botones típicos “Mostrar más”, “Cargar más”, “Ver más”
    candidates = driver.find_elements(
        By.XPATH,
        "//button[not(@disabled) and (contains(.,'Mostrar más') or contains(.,'Cargar más') or contains(.,'Ver más'))]"
    )
    for b in candidates:
        try:
            if b.is_displayed() and b.is_enabled():
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                time.sleep(0.2)
                b.click()
                return True
        except Exception:
            continue
    return False

def _load_all_cards_on_page(driver, settle_checks: int = 3, max_rounds: int = 40) -> None:
    """
    Realiza scrolls y clicks en 'mostrar más' hasta que:
     - no crece el número de cards tras varios intentos ('settle_checks'), o
     - se llega a max_rounds.
    """
    last_n = -1
    stable = 0
    rounds = 0

    # pequeña espera para que VTEX hidrate la grilla
    time.sleep(0.8)

    while rounds < max_rounds:
        rounds += 1

        # Scroll suave + END
        driver.execute_script("window.scrollBy(0, 900);")
        time.sleep(0.15)
        driver.execute_script("window.scrollBy(0, 900);")
        time.sleep(0.15)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.25)

        # Intentar “ver más”
        with contextlib.suppress(Exception):
            if _click_load_more_if_any(driver):
                time.sleep(0.7)

        # Recuento tras acciones
        n = _count_card_links(driver)
        # Pequeña espera de settling del DOM
        time.sleep(0.35)
        n2 = _count_card_links(driver)
        n = max(n, n2)

        if n == last_n:
            stable += 1
        else:
            stable = 0
        last_n = n

        # Si ya no crece, repetir algunos checks más para confirmar estabilidad
        if stable >= settle_checks:
            break

def _collect_product_links_on_page(driver) -> List[str]:
    _load_all_cards_on_page(driver, settle_checks=3, max_rounds=50)
    links = set()
    for xp in CARD_ANCHOR_XPATHS:
        for a in driver.find_elements(By.XPATH, xp):
            href = a.get_attribute("href") or ""
            if not href:
                continue
            # normalizamos a relativo (para concatenar con BASE)
            if href.startswith(BASE):
                href = href[len(BASE):]
            if href.startswith("/"):
                links.add(href)
    return sorted(links)

# =========================
# Extracción PDP
# =========================
def _extract_jsonld_ean(driver) -> Optional[str]:
    """Intenta leer EAN/GTIN desde JSON-LD o identificadores visibles."""
    # JSON-LD
    try:
        scripts = driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
        for s in scripts:
            with contextlib.suppress(Exception):
                data = json.loads(s.get_attribute("innerText") or "{}")
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                for k in ("gtin13", "gtin", "gtin14", "sku"):
                    v = obj.get(k)
                    if v:
                        vs = str(v).strip()
                        if k.startswith("gtin") and re.fullmatch(r"\d{8,14}", vs):
                            return vs
    except Exception:
        pass
    # Tabla o etiqueta visible
    xp = (
        "//span[contains(@class,'product-identifier__label') and "
        "(contains(.,'EAN') or contains(.,'Gtin') or contains(.,'GTIN'))]"
        "/following-sibling::span[contains(@class,'product-identifier__value')]"
    )
    with contextlib.suppress(Exception):
        v = driver.find_element(By.XPATH, xp).get_text().strip()  # a veces innerText
        if re.fullmatch(r"\d{8,14}", v):
            return v
    with contextlib.suppress(Exception):
        v = driver.find_element(By.XPATH, xp).text.strip()
        if re.fullmatch(r"\d{8,14}", v):
            return v
    return None

def _scrape_pdp(driver, wait, pdp_url_rel: str) -> dict:
    url_full = f"{BASE}{pdp_url_rel}"
    driver.get(url_full)

    # Esperar nombre producto
    with contextlib.suppress(Exception):
        wait.until(EC.presence_of_element_located((By.XPATH, "//h1[contains(@class,'productNameContainer')]")))

    # Nombre
    try:
        name = driver.find_element(By.XPATH, "//h1[contains(@class,'productNameContainer')]//span").text.strip()
    except Exception:
        name = ""

    # Marca
    try:
        brand = driver.find_element(By.XPATH, "//*[contains(@class,'productBrandName')]").text.strip()
    except Exception:
        brand = ""

    # SKU
    sku = ""
    with contextlib.suppress(Exception):
        sku = driver.find_element(
            By.XPATH,
            "//span[contains(@class,'product-identifier__label') and normalize-space()='SKU']"
            "/following-sibling::span[contains(@class,'product-identifier__value')]"
        ).text.strip()
    if not sku:
        with contextlib.suppress(Exception):
            sku_txt = driver.find_element(By.XPATH, "//*[contains(@class,'product-identifier')][contains(.,'SKU')]").text
            sku = re.sub(r".*SKU\s*:\s*", "", sku_txt, flags=re.I).strip()

    # EAN
    ean = _extract_jsonld_ean(driver) or ""

    # Precios (robusto)
    def _first_price_text():
        # toma el primer bloque que contenga un $
        xps = [
            "//*[@id='priceContainer']",
            "(//*[contains(@class,'store-theme')][contains(.,'$')])[1]",
            "//section//*[contains(text(),'$')][1]"
        ]
        for xp in xps:
            with contextlib.suppress(Exception):
                t = driver.find_element(By.XPATH, xp).text
                if "$" in t:
                    return t
        return ""
    price_now_text = _first_price_text()
    price_now, price_now_raw = _parse_price(price_now_text)

    def _second_price_text():
        # intenta segundo precio para “regular”/tachado
        xps = [
            "(//div[contains(@class,'store-theme')][contains(text(),'$')])[2]",
            "(//section//*[contains(text(),'$')])[2]"
        ]
        for xp in xps:
            with contextlib.suppress(Exception):
                return driver.find_element(By.XPATH, xp).text
        return ""
    price_reg_text = _second_price_text()
    price_reg, price_reg_raw = _parse_price(price_reg_text)

    with contextlib.suppress(Exception):
        discount_text = driver.find_element(
            By.XPATH, "//span[contains(text(),'%') and contains(@class,'store-theme')]"
        ).text.strip()
    if 'discount_text' not in locals():
        discount_text = ""

    with contextlib.suppress(Exception):
        unit_text = driver.find_element(By.XPATH, "//*[contains(@class,'vtex-custom-unit-price')]").text.strip()
    if 'unit_text' not in locals():
        unit_text = ""
    with contextlib.suppress(Exception):
        iva_text = driver.find_element(By.XPATH, "//p[contains(@class,'iva-pdp')]").text.strip()
    if 'iva_text' not in locals():
        iva_text = ""

    return {
        "url": url_full,
        "provincia": PROVINCIA,
        "tienda": TIENDA_NOM,
        "sku": sku,
        "ean": ean or "",
        "marca": brand,
        "nombre": name,
        "precio_actual": price_now,
        "precio_actual_raw": price_now_raw,
        "precio_regular": price_reg,
        "precio_regular_raw": price_reg_raw,
        "descuento_texto": discount_text,   # lo mapeamos a promo_tipo
        "unitario_texto": unit_text,
        "iva_texto": iva_text,
        "capturado_en": datetime.now(),
    }

# =========================
# MySQL helpers (contrato estándar)
# =========================
def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    return None if s.lower() in {"", "null", "none", "nan", "na"} else s

def upsert_tienda(cur, codigo: str, nombre: str, ref_tienda: str, provincia: str, sucursal: str) -> int:
    nombre = _clip(_clean_text(nombre), 255) or ""
    ref_tienda = _clip(_clean_text(ref_tienda), 80)
    provincia = _clip(_clean_text(provincia), 80)
    sucursal = _clip(_clean_text(sucursal), 160)

    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    with contextlib.suppress(Exception):
        cur.execute("""
            UPDATE tiendas
            SET ref_tienda = COALESCE(%s, ref_tienda),
                provincia  = COALESCE(%s, provincia),
                sucursal   = COALESCE(%s, sucursal)
            WHERE codigo = %s
        """, (ref_tienda, provincia, sucursal, codigo))

    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    nombre_in = _clip(_clean_text(p.get("nombre")), 512) or ""
    marca_in  = _clip(_clean_text(p.get("marca")), 256) or ""
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
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (nombre_in, marca_in, None, None, pid))
            return pid

    if nombre_in and marca_in:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre_in, marca_in))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean)
                WHERE id=%s
            """, (p.get("ean") or "", pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULL, NULL, NULL)
    """, (p.get("ean") or "", nombre_in, marca_in))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    sku = _clip(_clean_text(p.get("sku")), 120)
    url = _clip(p.get("url") or "", 512)
    nombre_tienda = _clip(_clean_text(p.get("nombre") or ""), 512)

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULL, NULLIF(%s,''), NULLIF(%s,'')) 
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, url, nombre_tienda))
        return cur.lastrowid

    # Fallback sin SKU: usa URL como cuasi id (si existe UNIQUE(tienda_id, url_tienda))
    try:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULL, NULLIF(%s,''), NULLIF(%s,'')) 
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, url, nombre_tienda))
        return cur.lastrowid
    except Exception:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))""",
            (tienda_id, producto_id, url, nombre_tienda))
        return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    precio_lista_f   = p.get("precio_regular")
    precio_oferta_f  = p.get("precio_actual")
    tipo_oferta      = _clip(_clean_text(p.get("descuento_texto") or None), 255)

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
        _to_txt(precio_lista_f), _to_txt(precio_oferta_f),
        tipo_oferta,
        tipo_oferta,  # promo_tipo (reuso)
        _clip(p.get("precio_regular_raw") or None, 255),
        _clip(p.get("precio_actual_raw") or None, 255),
        _clip(f"unit:{p.get('unitario_texto') or ''} | iva:{p.get('iva_texto') or ''}", 512)
    ))

# =========================
# Driver para VPS (headless) robusto
# =========================
def _best_effort_kill_stale_chrome():
    if not KILL_STALE_CHROME:
        return
    try:
        out = subprocess.run(["pgrep", "-a", "chrome"], capture_output=True, text=True, timeout=2)
        for line in (out.stdout or "").splitlines():
            if "--user-data-dir=/tmp/chrome-prof-" in line:
                pid = line.strip().split()[0]
                with contextlib.suppress(Exception):
                    os.kill(int(pid), signal.SIGTERM)
    except Exception:
        pass

def _find_chrome_binary_candidates():
    env_bin = os.environ.get("CHROME_BIN") or os.environ.get("GOOGLE_CHROME_BIN")
    if env_bin and os.path.exists(env_bin):
        yield env_bin
    for p in [
        "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable",
        "/snap/bin/chromium", "/usr/bin/chromium", "/usr/bin/chromium-browser",
        "/opt/google/chrome/google-chrome"
    ]:
        if os.path.exists(p):
            yield p

def _build_chrome_options(prof_dir: str) -> webdriver.ChromeOptions:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--window-size=1280,2200")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-features=Translate,BackForwardCache,NetworkService")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--lang=es-AR")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )

    # Perfil temporario aislado
    cache_dir = os.path.join(prof_dir, "cache")
    os.makedirs(cache_dir, exist_ok=True)
    opts.add_argument(f"--user-data-dir={prof_dir}")
    opts.add_argument("--profile-directory=Default")
    opts.add_argument(f"--disk-cache-dir={cache_dir}")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-crash-reporter")

    # Debug port único
    dbg_port = 9222 + (os.getpid() % 1000)
    opts.add_argument(f"--remote-debugging-port={dbg_port}")

    # Proxy opcional
    proxy = os.environ.get("SELENIUM_PROXY") or os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
    if proxy:
        opts.add_argument(f"--proxy-server={proxy}")

    # Fijar binario si existe
    for bin_path in _find_chrome_binary_candidates():
        try:
            opts.binary_location = bin_path
            break
        except Exception:
            pass

    return opts

def _make_driver_once() -> Tuple[webdriver.Chrome, str]:
    prof_dir = tempfile.mkdtemp(prefix="chrome-prof-")
    os.environ.setdefault("XDG_RUNTIME_DIR", prof_dir)

    opts = _build_chrome_options(prof_dir)

    # Estrategia en cascada:
    # 1) webdriver_manager (si disponible)
    # 2) Selenium Manager (driver embebido) -> webdriver.Chrome(options=opts)
    # 3) /usr/bin/chromedriver
    last_err = None
    try:
        if _HAS_WDM:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=opts)
            driver.set_page_load_timeout(60)
            return driver, prof_dir
    except Exception as e:
        last_err = e

    try:
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(60)
        return driver, prof_dir
    except Exception as e:
        last_err = e

    try:
        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(60)
        return driver, prof_dir
    except Exception as e:
        last_err = e

    # Si nada funcionó, propaga el último error
    raise last_err if last_err else RuntimeError("No se pudo inicializar ChromeDriver")

def _make_driver(max_retries: int = 3) -> webdriver.Chrome:
    _best_effort_kill_stale_chrome()
    last_err = None
    for _ in range(max_retries):
        try:
            driver, prof_dir = _make_driver_once()
            def _cleanup():
                with contextlib.suppress(Exception):
                    driver.quit()
                with contextlib.suppress(Exception):
                    shutil.rmtree(prof_dir, ignore_errors=True)
            atexit.register(_cleanup)
            def _sigterm_handler(signum, frame):
                _cleanup()
                os._exit(0)
            signal.signal(signal.SIGTERM, _sigterm_handler)
            with contextlib.suppress(Exception):
                driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
                )
            return driver
        except SessionNotCreatedException as e:
            last_err = e
            msg = str(e)
            if "user data directory is already in use" in msg:
                time.sleep(0.7)
                continue
            else:
                break
        except WebDriverException as e:
            last_err = e
            time.sleep(0.7)
            continue
        except Exception as e:
            last_err = e
            break
    raise last_err if last_err else RuntimeError("No se pudo crear el driver")

# =========================
# Flujo de login + selección de tienda
# =========================
def _dismiss_cookies(driver, wait):
    with contextlib.suppress(Exception):
        btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(.,'Aceptar') or contains(.,'cookies') or contains(.,'Acepto')]")
        ))
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.2)

def login_and_select_store(driver, wait):
    driver.get(BASE)
    _dismiss_cookies(driver, wait)

    # Mi cuenta
    _click_with_retry(driver, wait, "//span[normalize-space()='Mi Cuenta']")
    time.sleep(0.6)

    # Entrar con e-mail y contraseña
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//span[normalize-space()='Entrar con e-mail y contraseña']/ancestor::button[1]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//button[.//span[contains(normalize-space(),'Entrar con e-mail')]]")

    # Email / Password
    with contextlib.suppress(Exception):
        _type_with_retry(driver, wait, "//input[@placeholder='Ej. nombre@mail.com']", DISCO_USER)
    with contextlib.suppress(Exception):
        _type_with_retry(driver, wait, "//input[contains(@placeholder,'mail.com')]", DISCO_USER)

    with contextlib.suppress(Exception):
        _type_with_retry(driver, wait, "//input[@type='password' and contains(@class,'vtex-styleguide-9-x-input')]", DISCO_PASS)
    with contextlib.suppress(Exception):
        _type_with_retry(driver, wait, "//input[@type='password' or contains(@placeholder,'●')]", DISCO_PASS)

    # Entrar
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//span[normalize-space()='Entrar']/ancestor::button[@type='submit'][1]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-login-2-x-sendButton')]//button[@type='submit']")
    time.sleep(1.6)

    # Selector método de entrega
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//span[contains(normalize-space(),'Seleccioná') and contains(.,'método de entrega')]/ancestor::*[@role='button'][1]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'discoargentina-delivery-modal-1-x-containerTrigger')]/ancestor::div[@role='button'][1]")
    time.sleep(0.6)

    # Retirar en una tienda
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'pickUpSelectionContainer')]//button[.//p[contains(normalize-space(),'Retirar en una tienda')]]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//button[.//p[contains(normalize-space(),'Retirar en una tienda')]]")

    # Provincia
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar Provincia')]]//div[contains(@class,'vtex-dropdown__button')]")
    _select_by_text_case_insensitive(driver, wait,
        "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar Provincia')]]//select",
        PROVINCIA
    )
    time.sleep(0.8)

    # Tienda
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//div[contains(@class,'vtex-dropdown__button')]")
    store_select_xpath = "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//select"
    # Esperar que aparezca la opción
    wait.until(EC.presence_of_element_located(
        (By.XPATH, f"{store_select_xpath}/option[contains(., '{TIENDA_NOM}') or contains(., '{TIENDA_NOM.replace('ó','o')}')]")
    ))
    _select_by_text_case_insensitive(driver, wait, store_select_xpath, TIENDA_NOM)

    # Confirmar
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'discoargentina-delivery-modal-1-x-buttonStyle')]//button[.//div[normalize-space()='Confirmar']]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[@role='dialog']//button[.//div[normalize-space()='Confirmar'] or normalize-space()='Confirmar']")
    time.sleep(0.9)

# =========================
# Orquestador
# =========================
def run_scrape_and_persist():
    driver = _make_driver(max_retries=3)
    wait = WebDriverWait(driver, 30)

    try:
        # 1) login + tienda
        login_and_select_store(driver, wait)

        # 2) Crawl por páginas (?page=1,2,3,...) con carga completa de cada página
        data: List[Dict[str, Any]] = []
        page = 1
        empty_pages = 0

        while True:
            list_url = f"{BASE}{CATEGORIA_URL}?page={page}"
            print(f"\n📄 Página: {page} -> {list_url}")
            driver.get(list_url)
            time.sleep(SLEEP_PAGE)

            # Cargar TODO lo que hay en la página (scroll + ver más + estabilidad DOM)
            links = _collect_product_links_on_page(driver)
            total = len(links)
            if total == 0:
                print("⚠️  Sin items en la página.")
                empty_pages += 1
                if empty_pages > MAX_EMPTY:
                    print("⛔ Fin: no hay más productos.")
                    break
                page += 1
                continue

            empty_pages = 0
            print(f"🔗 Productos encontrados en la página {page}: {total}")

            # 3) Visitar cada PDP y recolectar datos
            for i, rel in enumerate(links, 1):
                try:
                    print(f"  → [{i}/{total}] {rel}")
                    item = _scrape_pdp(driver, wait, rel)
                    data.append(item)
                except Exception as e:
                    print(f"    × Error en {rel}: {e}")
                finally:
                    # volver a la lista para mantener el contexto/cookies
                    driver.get(list_url)
                    time.sleep(SLEEP_PDP)

            page += 1

        if not data:
            print("⚠️ No se capturaron productos; no se escribe MySQL.")
            return

        # 4) Persistencia MySQL
        capturado_en = datetime.now()
        conn = None
        try:
            conn = get_conn()
            conn.autocommit = False
            cur = conn.cursor()

            tienda_id = upsert_tienda(cur,
                                      TIENDA_CODIGO,
                                      TIENDA_NOMBRE,
                                      TIENDA_REF,
                                      PROVINCIA,
                                      TIENDA_NOM)

            insertados = 0
            for p in data:
                producto_id = find_or_create_producto(cur, p)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                insertados += 1

            conn.commit()
            print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOM} ({capturado_en})")

        except MySQLError as e:
            if conn: conn.rollback()
            print(f"❌ Error MySQL: {e}")
        finally:
            with contextlib.suppress(Exception):
                if conn: conn.close()

    finally:
        with contextlib.suppress(Exception):
            driver.quit()

# =========================
# Main
# =========================
if __name__ == "__main__":
    run_scrape_and_persist()
