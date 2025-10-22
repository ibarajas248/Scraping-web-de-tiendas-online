#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, json, unicodedata, signal, subprocess, contextlib
import sys
import tempfile, shutil, atexit
import threading
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd
from mysql.connector import Error as MySQLError

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.common.exceptions import SessionNotCreatedException, WebDriverException, TimeoutException

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

# --- Tu helper de conexión ---
from base_datos import get_conn

# =========================
# Ctrl+C / ENTER para corte ordenado
# =========================
STOP_EVENT = threading.Event()

def _enter_listener():
    try:
        input("🔴 Presioná ENTER para terminar y guardar lo recolectado hasta ahora...\n")
    except EOFError:
        pass
    STOP_EVENT.set()

# =========================
# Parámetros de negocio (ENV primero, con default)
# =========================
BASE        = "https://www.disco.com.ar"

# credenciales: export DISCO_USER=... ; export DISCO_PASS=...
DISCO_USER  = os.getenv("DISCO_USER", "comercial@factory-blue.com")
DISCO_PASS  = os.getenv("DISCO_PASS", "Compras2025")

PROVINCIA     = os.getenv("DISCO_PROVINCIA", "CORDOBA").strip()
TIENDA_NOM    = os.getenv("DISCO_TIENDA", "Disco Alta Córdoba Cabrera 493").strip()
CATEGORIA_URL = os.getenv("DISCO_CATEGORIA_URL", "/almacen").strip()  # punto de entrada de scraping
MAX_EMPTY     = int(os.getenv("DISCO_MAX_EMPTY", "1"))  # páginas vacías toleradas
SLEEP_PDP     = float(os.getenv("DISCO_SLEEP_PDP", "0.6"))
SLEEP_PAGE    = float(os.getenv("DISCO_SLEEP_PAGE", "1.0"))

TIENDA_CODIGO = "disco_cordoba_alto_cordoba_cabrera_493"
TIENDA_NOMBRE = "Disco_cordoba_alto_cordoba_cabrera_493"
TIENDA_REF    = os.getenv("DISCO_REF_TIENDA", "disco_cordoba_cabrera_493")

# Opcional: matar huérfanos Chrome lanzados por perfiles temporales previos (0/1)
KILL_STALE_CHROME=1

# =========================
# Utilidades
# =========================
def _safe_get(driver, url, tries=4, base_sleep=1.0):
    """driver.get con reintentos/backoff."""
    for i in range(1, tries+1):
        try:
            driver.get(url)
            return True
        except (TimeoutException, WebDriverException):
            time.sleep(base_sleep * i)  # backoff lineal
            if i == tries:
                raise
    return False

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
            time.sleep(1)
    raise last_exc

def _type_with_retry(driver, wait, xpath: str, text: str, retries: int = 3) -> None:
    last_exc = None
    for _ in range(retries):
        try:
            el = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            try:
                el.clear()
            except Exception:
                pass
            el.send_keys(text)
            return
        except Exception as e:
            last_exc = e
            time.sleep(1)
    raise last_exc

def _select_by_text_case_insensitive(driver, wait, select_xpath: str, target_text: str, retries: int = 3) -> None:
    last_exc = None
    tgt = _normalize(target_text)
    for _ in range(retries):
        try:
            sel = wait.until(EC.presence_of_element_located((By.XPATH, select_xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sel)
            wait.until(lambda d: len(sel.find_elements(By.TAG_NAME, "option")) > 1)
            try:
                Select(sel).select_by_visible_text(target_text)
                return
            except Exception:
                pass
            value = None
            for o in sel.find_elements(By.TAG_NAME, "option"):
                if o.get_attribute("disabled"):
                    continue
                if _normalize(o.text) == tgt or tgt in _normalize(o.text):
                    value = o.get_attribute("value")
                    break
            if value is None:
                raise RuntimeError(f"Opción no encontrada: {target_text}")
            try:
                Select(sel).select_by_value(value)
                return
            except Exception:
                driver.execute_script("""
                    const s = arguments[0], val = arguments[1];
                    s.value = val; s.dispatchEvent(new Event('change', {bubbles:true}));
                """, sel, value)
                return
        except Exception as e:
            last_exc = e
            time.sleep(1)
    raise last_exc

def _collect_product_links_on_page(driver, timeout=12):
    """Devuelve hrefs relativos '/.../p'."""
    t0 = time.time()
    links = set()
    while time.time() - t0 < timeout:
        cards = driver.find_elements(
            By.XPATH,
            "//a[contains(@class,'vtex-product-summary-2-x-clearLink') and contains(@href,'/p')]"
        )
        for a in cards:
            href = a.get_attribute("href") or ""
            if href.startswith(BASE):
                href = href[len(BASE):]
            if href.startswith("/"):
                links.add(href)
        if links:
            break
        time.sleep(0.5)
    return list(links)

def _load_all_products_on_list(driver, wait, max_wait: float = 20, max_clicks: int = 20) -> None:
    """Fuerza a cargar todos los productos de la lista (scroll + 'Mostrar más')."""
    try:
        wait.until(EC.presence_of_element_located((
            By.XPATH,
            "//a[contains(@class,'vtex-product-summary-2-x-clearLink') and contains(@href,'/p')]"
        )))
    except Exception:
        time.sleep(0.8)

    last_count = -1
    stable_since = time.time()
    clicks = 0

    while True:
        cards = driver.find_elements(
            By.XPATH,
            "//a[contains(@class,'vtex-product-summary-2-x-clearLink') and contains(@href,'/p')]"
        )
        count = len(cards)
        if count != last_count:
            last_count = count
            stable_since = time.time()

        # 1) Botón "Mostrar más" / "Ver más"
        show_more_btns = driver.find_elements(
            By.XPATH,
            "//button[contains(@class,'vtex-search-result-3-x-buttonShowMore') or contains(.,'Mostrar más') or contains(.,'Ver más')]"
        )
        if show_more_btns:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", show_more_btns[0])
                time.sleep(0.2)
                try:
                    show_more_btns[0].click()
                except Exception:
                    driver.execute_script("arguments[0].click();", show_more_btns[0])
                clicks += 1
                time.sleep(1.2)
                continue
            except Exception:
                pass

        # 2) Scroll para lazy-load
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.8)
        driver.execute_script("window.scrollBy(0,-150);")
        time.sleep(0.2)

        # salidas
        if (time.time() - stable_since) > 2.5 and not show_more_btns:
            break
        if (time.time() - stable_since) > max_wait:
            break
        if clicks >= max_clicks:
            break
        if STOP_EVENT.is_set():
            break

def _extract_jsonld_ean(driver) -> Optional[str]:
    """Intenta leer EAN/GTIN desde JSON-LD."""
    try:
        scripts = driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
        for s in scripts:
            try:
                data = json.loads(s.get_attribute("innerText") or "{}")
            except Exception:
                continue
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
    # fallback visible
    try:
        ean_el = driver.find_element(
            By.XPATH,
            "//span[contains(@class,'product-identifier__label') and (contains(.,'EAN') or contains(.,'Gtin'))]/following-sibling::span[contains(@class,'product-identifier__value')]"
        )
        v = ean_el.text.strip()
        if re.fullmatch(r"\d{8,14}", v):
            return v
    except Exception:
        pass
    return None

def _scrape_pdp(driver, wait, pdp_url_rel: str) -> dict:
    url_full = f"{BASE}{pdp_url_rel}"
    _safe_get(driver, url_full)
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "//h1[contains(@class,'productNameContainer')]")))
    except Exception:
        time.sleep(0.8)

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
    try:
        sku = driver.find_element(
            By.XPATH,
            "//span[contains(@class,'product-identifier__label') and normalize-space()='SKU']/following-sibling::span[contains(@class,'product-identifier__value')]"
        ).text.strip()
    except Exception:
        try:
            sku = driver.find_element(By.XPATH, "//*[contains(@class,'product-identifier')][contains(.,'SKU')]").text
            sku = re.sub(r".*SKU\s*:\s*", "", sku, flags=re.I).strip()
        except Exception:
            sku = ""

    # EAN
    ean = _extract_jsonld_ean(driver)

    # Precios
    try:
        price_now_text = driver.find_element(By.XPATH, "//*[@id='priceContainer']").text
    except Exception:
        try:
            price_now_text = driver.find_element(By.XPATH, "(//*[contains(@class,'store-theme')][contains(.,'$')])[1]").text
        except Exception:
            price_now_text = ""
    price_now, price_now_raw = _parse_price(price_now_text)

    try:
        price_reg_text = driver.find_element(
            By.XPATH, "(//div[contains(@class,'store-theme')][contains(text(),'$')])[2]"
        ).text
    except Exception:
        price_reg_text = ""
    price_reg, price_reg_raw = _parse_price(price_reg_text)

    try:
        discount_text = driver.find_element(
            By.XPATH, "//span[contains(text(),'%') and contains(@class,'store-theme')]"
        ).text.strip()
    except Exception:
        discount_text = ""

    try:
        unit_text = driver.find_element(By.XPATH, "//*[contains(@class,'vtex-custom-unit-price')]").text.strip()
    except Exception:
        unit_text = ""
    try:
        iva_text = driver.find_element(By.XPATH, "//p[contains(@class,'iva-pdp')]").text.strip()
    except Exception:
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
        "descuento_texto": discount_text,
        "unitario_texto": unit_text,
        "iva_texto": iva_text,
        "capturado_en": datetime.now(),
    }

# =========================
# MySQL helpers
# =========================
def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    return None if s.lower() in {"", "null", "none", "nan", "na"} else s

def parse_price_text(val) -> Optional[str]:
    """Convierte a float y lo devuelve como texto con 2 decimales, o None."""
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

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
    try:
        cur.execute("""
            UPDATE tiendas
            SET ref_tienda = COALESCE(%s, ref_tienda),
                provincia  = COALESCE(%s, provincia),
                sucursal   = COALESCE(%s, sucursal)
            WHERE codigo = %s
        """, (ref_tienda, provincia, sucursal, codigo))
    except Exception:
        pass

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

    # sin SKU: usa URL como quasi id
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
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
        """, (tienda_id, producto_id, url, nombre_tienda))
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
        parse_price_text(precio_lista_f), parse_price_text(precio_oferta_f),
        tipo_oferta,
        tipo_oferta,
        _clip(p.get("precio_regular_raw") or None, 255),
        _clip(p.get("precio_actual_raw") or None, 255),
        _clip(f"unit:{p.get('unitario_texto') or ''} | iva:{p.get('iva_texto') or ''}", 512)
    ))

# =========================
# Driver para VPS (headless) con perfil único + retries + cleanup
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

def _make_driver_once() -> Tuple[webdriver.Chrome, str]:
    prof_dir = tempfile.mkdtemp(prefix="chrome-prof-")
    cache_dir = os.path.join(prof_dir, "cache")
    os.makedirs(cache_dir, exist_ok=True)

    def _cleanup():
        try:
            shutil.rmtree(prof_dir, ignore_errors=True)
        except Exception:
            pass
    atexit.register(_cleanup)

    os.environ.setdefault("XDG_RUNTIME_DIR", prof_dir)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,2000")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    options.add_argument(f"--user-data-dir={prof_dir}")
    options.add_argument("--profile-directory=Default")
    options.add_argument(f"--disk-cache-dir={cache_dir}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-crash-reporter")
    options.add_argument("--disable-features=Translate,BackForwardCache")

    dbg_port = 9222 + (os.getpid() % 1000)
    options.add_argument(f"--remote-debugging-port={dbg_port}")

    options.add_argument(f"--homedir={prof_dir}")
    options.add_argument(f"--data-path={prof_dir}")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver, prof_dir

def _make_driver(max_retries: int = 3) -> webdriver.Chrome:
    _best_effort_kill_stale_chrome()
    last_err = None
    for _ in range(1, max_retries + 1):
        try:
            driver, _ = _make_driver_once()
            def _sigterm_handler(signum, frame):
                with contextlib.suppress(Exception):
                    driver.quit()
                os._exit(0)
            signal.signal(signal.SIGTERM, _sigterm_handler)
            return driver
        except SessionNotCreatedException as e:
            last_err = e
            msg = str(e)
            if "user data directory is already in use" in msg:
                time.sleep(0.8)
                continue
            else:
                break
        except WebDriverException as e:
            last_err = e
            time.sleep(0.8)
            continue
        except Exception as e:
            last_err = e
            break
    raise last_err if last_err else RuntimeError("No se pudo crear el driver")

def _dismiss_cookies(driver, wait):
    try:
        btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(.,'Aceptar') or contains(.,'cookies') or contains(.,'Acepto')]")
        ))
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.3)
    except Exception:
        pass

def login_and_select_store(driver, wait):
    _safe_get(driver, BASE)
    _dismiss_cookies(driver, wait)

    # Mi cuenta
    _click_with_retry(driver, wait, "//span[normalize-space()='Mi Cuenta']")
    time.sleep(1)

    # 2) Entrar con e-mail y contraseña (actualizado)
    try:
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//div[contains(@class,'vtex-login-2-x-button')]")
        ))
    except Exception:
        pass

    # Botón principal + fallbacks
    try:
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-login-2-x-emailPasswordOptionBtn')]//button")
    except Exception:
        try:
            _click_with_retry(driver, wait, "//button[.//span[normalize-space()='Email y contraseña']]")
        except Exception:
            _click_with_retry(driver, wait, "//button[.//span[contains(normalize-space(),'Entrar con e-mail')]]")

    # 3) Email (actualizado)
    try:
        _type_with_retry(driver, wait, "//input[@placeholder='Email' and @type='text']", DISCO_USER)
    except Exception:
        try:
            _type_with_retry(driver, wait, "//input[contains(@class,'vtex-styleguide-9-x-input') and not(@type='password')]", DISCO_USER)
        except Exception:
            _type_with_retry(driver, wait, "(//input[not(@type='password') and not(@type='hidden')])[1]", DISCO_USER)

    # Password
    try:
        _type_with_retry(driver, wait, "//input[@type='password' and contains(@class,'vtex-styleguide-9-x-input')]", DISCO_PASS)
    except Exception:
        _type_with_retry(driver, wait, "//input[@type='password' or contains(@placeholder,'●')]", DISCO_PASS)

    # Entrar
    try:
        _click_with_retry(driver, wait, "//span[normalize-space()='Entrar']/ancestor::button[@type='submit'][1]")
    except Exception:
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-login-2-x-sendButton')]//button[@type='submit']")
    time.sleep(2)

    # Selector método de entrega
    try:
        _click_with_retry(driver, wait, "//span[contains(normalize-space(),'Seleccioná') and contains(.,'método de entrega')]/ancestor::*[@role='button'][1]")
    except Exception:
        _click_with_retry(driver, wait, "//div[contains(@class,'discoargentina-delivery-modal-1-x-containerTrigger')]/ancestor::div[@role='button'][1]")
    time.sleep(1)

    # Retirar en una tienda
    try:
        _click_with_retry(driver, wait, "//div[contains(@class,'pickUpSelectionContainer')]//button[.//p[contains(normalize-space(),'Retirar en una tienda')]]")
    except Exception:
        _click_with_retry(driver, wait, "//button[.//p[contains(normalize-space(),'Retirar en una tienda')]]")

    # Provincia
    try:
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar Provincia')]]//div[contains(@class,'vtex-dropdown__button')]")
    except Exception:
        pass
    _select_by_text_case_insensitive(driver, wait,
        "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar Provincia')]]//select",
        PROVINCIA
    )
    time.sleep(1.2)

    # Tienda
    try:
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//div[contains(@class,'vtex-dropdown__button')]")
    except Exception:
        pass
    store_select_xpath = "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//select"
    wait.until(EC.presence_of_element_located(
        (By.XPATH, f"{store_select_xpath}/option[contains(., '{TIENDA_NOM}') or contains(., '{TIENDA_NOM.replace('ó','o')}')]")
    ))
    _select_by_text_case_insensitive(driver, wait, store_select_xpath, TIENDA_NOM)

    # Confirmar
    try:
        _click_with_retry(driver, wait, "//div[contains(@class,'discoargentina-delivery-modal-1-x-buttonStyle')]//button[.//div[normalize-space()='Confirmar']]")
    except Exception:
        _click_with_retry(driver, wait, "//div[@role='dialog']//button[.//div[normalize-space()='Confirmar'] or normalize-space()='Confirmar']")
    time.sleep(1.2)

def run_scrape_and_persist():
    # Listener de ENTER en paralelo
    t_listener = threading.Thread(target=_enter_listener, daemon=True)
    t_listener.start()

    driver = _make_driver(max_retries=3)
    wait = WebDriverWait(driver, 30)

    data: List[Dict[str, Any]] = []
    try:
        # 1) login + tienda
        login_and_select_store(driver, wait)

        # 2) Crawl por páginas
        page = 1
        empty_pages = 0
        while True:
            if STOP_EVENT.is_set():
                print("🛑 Corte solicitado por usuario (ENTER). Saliendo del bucle de páginas…")
                break

            list_url = f"{BASE}{CATEGORIA_URL}?page={page}"
            print(f"\n📄 Página: {page} -> {list_url}")
            _safe_get(driver, list_url)
            time.sleep(SLEEP_PAGE)

            # cargar todos los productos renderizados de la página
            _load_all_products_on_list(driver, wait, max_wait=20, max_clicks=20)

            links = _collect_product_links_on_page(driver, timeout=14)
            if not links:
                print("⚠️  Sin items en la página.")
                empty_pages += 1
                if empty_pages > MAX_EMPTY:
                    print("⛔ Fin: no hay más productos.")
                    break
                else:
                    page += 1
                    continue

            empty_pages = 0
            print(f"🔗 Productos encontrados: {len(links)}")

            for i, rel in enumerate(links, 1):
                if STOP_EVENT.is_set():
                    print("🛑 Corte solicitado por usuario (ENTER). Deteniendo productos restantes en esta página…")
                    break
                try:
                    print(f"  → [{i}/{len(links)}] {rel}")
                    item = _scrape_pdp(driver, wait, rel)
                    data.append(item)
                except Exception as e:
                    print(f"    × Error en {rel}: {e}")
                finally:
                    # volver a la lista para mantener el contexto/cookies
                    _safe_get(driver, list_url)
                    time.sleep(SLEEP_PDP)

            if STOP_EVENT.is_set():
                break

            page += 1

        if not data:
            print("⚠️ No se capturaron productos; no se escribe MySQL.")
            return

        # 3) Persistencia MySQL (commits por bloques para resiliencia)
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
            for idx, p in enumerate(data, 1):
                producto_id = find_or_create_producto(cur, p)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                insertados += 1

                if insertados % 50 == 0:
                    conn.commit()
                    print(f"💾 Commit intermedio: {insertados} filas…")

            conn.commit()
            print(f"✅ Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOM} ({capturado_en})")

        except MySQLError as e:
            if conn: conn.rollback()
            print(f"❌ Error MySQL: {e}")
        finally:
            with contextlib.suppress(Exception):
                if conn: conn.close()

    finally:
        with contextlib.suppress(Exception):
            driver.quit()

if __name__ == "__main__":
    run_scrape_and_persist()
