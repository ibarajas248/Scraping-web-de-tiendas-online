#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, json, unicodedata, signal, subprocess, contextlib
import sys
import tempfile, shutil, atexit
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd
from mysql.connector import Error as MySQLError

import requests
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.common.exceptions import SessionNotCreatedException, WebDriverException

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

# --- Tu helper de conexión ---
# Asegúrate que base_datos.get_conn() funcione en el VPS (MariaDB local).
from base_datos import get_conn

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

# Ajustes de scraping
MAX_EMPTY   = int(os.getenv("DISCO_MAX_EMPTY", "3"))   # tolera más páginas vacías
SLEEP_PDP   = float(os.getenv("DISCO_SLEEP_PDP", "0.4"))
SLEEP_PAGE  = float(os.getenv("DISCO_SLEEP_PAGE", "0.8"))
STEP        = int(os.getenv("DISCO_STEP", "50"))       # VTEX suele ser 50 por página (_from/_to)

TIENDA_CODIGO = os.getenv("DISCO_TIENDA_CODIGO", "disco_cordoba_alto_cordoba_cabrera_493")
TIENDA_NOMBRE = os.getenv("DISCO_TIENDA_NOMBRE", "Disco_cordoba_alto_cordoba_cabrera_493")

# Etiquetado de corrida a sucursal específica en 'tiendas'
TIENDA_REF   = os.getenv("DISCO_REF_TIENDA", "disco_cordoba_cabrera_493")

# Opcional: matar huérfanos Chrome lanzados por perfiles temporales previos (0/1)
KILL_STALE_CHROME = int(os.getenv("KILL_STALE_CHROME", "1"))

# =========================
# Utilidades
# =========================
def _normalize(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s or '') if not unicodedata.combining(c)).strip().lower()

def _clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    # quita NBSP y espacios raros
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
            with contextlib.suppress(Exception):
                el.clear()
            el.send_keys(text)
            return
        except Exception as e:
            last_exc = e
            time.sleep(1)
    raise last_exc

def _extract_jsonld_ean(driver) -> Optional[str]:
    """Intenta leer EAN/GTIN desde JSON-LD o etiqueta visible."""
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
    # fallback: buscar identificador "EAN" visible
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
    driver.get(url_full)

    with contextlib.suppress(Exception):
        wait.until(EC.presence_of_element_located((By.XPATH, "//h1[contains(@class,'productNameContainer')]")))
    time.sleep(0.2)

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
        with contextlib.suppress(Exception):
            t = driver.find_element(By.XPATH, "//*[contains(@class,'product-identifier')][contains(.,'SKU')]").text
            sku = re.sub(r".*SKU\s*:\s*", "", t, flags=re.I).strip()

    # Intentar EAN
    ean = _extract_jsonld_ean(driver)

    # Precios
    try:
        price_now_text = driver.find_element(By.XPATH, "//*[@id='priceContainer']").text
    except Exception:
        with contextlib.suppress(Exception):
            price_now_text = driver.find_element(By.XPATH, "(//*[contains(@class,'store-theme')][contains(.,'$')])[1]").text
        if 'price_now_text' not in locals():
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
        "descuento_texto": discount_text,   # lo mapeamos a promo_tipo
        "unitario_texto": unit_text,
        "iva_texto": iva_text,
        "capturado_en": datetime.now(),
    }

# =========================
# MySQL helpers (mismo patrón que Coto) + “airbag” long text
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
    # clip por si el schema tiene longitudes más cortas
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
    # aplica limpieza y clip seguro
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

    # sin SKU: usa URL como quasi id (si tienes UNIQUE(tienda_id, url_tienda) súbela)
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
        # último recurso sin llave natural
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
        """, (tienda_id, producto_id, url, nombre_tienda))
        return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    # Mapear a tus columnas estándar
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
        tipo_oferta,           # promo_tipo (reusamos el mismo texto)
        _clip(p.get("precio_regular_raw") or None, 255),
        _clip(p.get("precio_actual_raw") or None, 255),
        _clip(f"unit:{p.get('unitario_texto') or ''} | iva:{p.get('iva_texto') or ''}", 512)
    ))

# =========================
# Driver para VPS (headless) con perfil único + retries + cleanup
# =========================
def _best_effort_kill_stale_chrome():
    """Mata procesos Chrome asociados a perfiles temporales previos (opcional)."""
    if not KILL_STALE_CHROME:
        return
    try:
        # Solo mata los que usen /tmp/chrome-prof-*
        out = subprocess.run(["pgrep", "-a", "chrome"], capture_output=True, text=True, timeout=2)
        for line in (out.stdout or "").splitlines():
            if "--user-data-dir=/tmp/chrome-prof-" in line:
                pid = line.strip().split()[0]
                with contextlib.suppress(Exception):
                    os.kill(int(pid), signal.SIGTERM)
    except Exception:
        pass

def _make_driver_once() -> Tuple[webdriver.Chrome, str]:
    """Crea una instancia de Chrome con perfil temporal; retorna (driver, prof_dir)."""
    prof_dir = tempfile.mkdtemp(prefix="chrome-prof-")
    cache_dir = os.path.join(prof_dir, "cache")
    os.makedirs(cache_dir, exist_ok=True)

    def _cleanup():
        with contextlib.suppress(Exception):
            shutil.rmtree(prof_dir, ignore_errors=True)
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

    # Perfil/Cache/Cookies totalmente aislados
    options.add_argument(f"--user-data-dir={prof_dir}")
    options.add_argument("--profile-directory=Default")
    options.add_argument(f"--disk-cache-dir={cache_dir}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-crash-reporter")
    options.add_argument("--disable-features=Translate,BackForwardCache")

    # Puerto de debug único por proceso para evitar conflictos
    dbg_port = 9222 + (os.getpid() % 1000)
    options.add_argument(f"--remote-debugging-port={dbg_port}")

    # (Opcional) Si /dev/shm es chico en el VPS, forzamos a usar disco:
    options.add_argument(f"--homedir={prof_dir}")
    options.add_argument(f"--data-path={prof_dir}")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver, prof_dir

def _make_driver(max_retries: int = 3) -> webdriver.Chrome:
    """Crea el driver; si falla por 'user-data-dir in use', reintenta con otro perfil."""
    _best_effort_kill_stale_chrome()
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            driver, prof_dir = _make_driver_once()
            # registro de cleanup adicional si el proceso recibe SIGTERM
            def _sigterm_handler(signum, frame):
                with contextlib.suppress(Exception):
                    driver.quit()
                # el atexit ya limpia prof_dir
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
    with contextlib.suppress(Exception):
        btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(.,'Aceptar') or contains(.,'cookies') or contains(.,'Acepto')]")
        ))
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.3)

def login_and_select_store(driver, wait):
    driver.get(BASE)
    _dismiss_cookies(driver, wait)

    # Mi cuenta
    _click_with_retry(driver, wait, "//span[normalize-space()='Mi Cuenta']")
    time.sleep(1)

    # Entrar con e-mail y contraseña
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//span[normalize-space()='Entrar con e-mail y contraseña']/ancestor::button[1]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//button[.//span[contains(normalize-space(),'Entrar con e-mail')]]")

    # Email
    with contextlib.suppress(Exception):
        _type_with_retry(driver, wait, "//input[@placeholder='Ej. nombre@mail.com']", DISCO_USER)
    with contextlib.suppress(Exception):
        _type_with_retry(driver, wait, "//input[contains(@placeholder,'mail.com')]", DISCO_USER)

    # Password
    with contextlib.suppress(Exception):
        _type_with_retry(driver, wait, "//input[@type='password' and contains(@class,'vtex-styleguide-9-x-input')]", DISCO_PASS)
    with contextlib.suppress(Exception):
        _type_with_retry(driver, wait, "//input[@type='password' or contains(@placeholder,'●')]", DISCO_PASS)

    # Entrar
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//span[normalize-space()='Entrar']/ancestor::button[@type='submit'][1]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-login-2-x-sendButton')]//button[@type='submit']")
    time.sleep(2)

    # Selector método de entrega
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//span[contains(normalize-space(),'Seleccioná') and contains(.,'método de entrega')]/ancestor::*[@role='button'][1]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'discoargentina-delivery-modal-1-x-containerTrigger')]/ancestor::div[@role='button'][1]")
    time.sleep(1)

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
    time.sleep(1.2)

    # Tienda
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//div[contains(@class,'vtex-dropdown__button')]")
    store_select_xpath = "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//select"
    WebDriverWait(driver, 30).until(EC.presence_of_element_located(
        (By.XPATH, f"{store_select_xpath}/option[contains(., '{TIENDA_NOM}') or contains(., '{TIENDA_NOM.replace('ó','o')}')]")
    ))
    _select_by_text_case_insensitive(driver, wait, store_select_xpath, TIENDA_NOM)

    # Confirmar
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[contains(@class,'discoargentina-delivery-modal-1-x-buttonStyle')]//button[.//div[normalize-space()='Confirmar']]")
    with contextlib.suppress(Exception):
        _click_with_retry(driver, wait, "//div[@role='dialog']//button[.//div[normalize-space()='Confirmar'] or normalize-space()='Confirmar']")
    time.sleep(1.2)

# =========================
# API VTEX – helpers
# =========================
def _requests_from_selenium_cookies(driver) -> requests.Session:
    """Crea una sesión requests con las cookies actuales del navegador (incluye vtex_segment)."""
    sess = requests.Session()
    for c in driver.get_cookies():
        try:
            sess.cookies.set(c['name'], c['value'], domain=c.get('domain') or None)
        except Exception:
            sess.cookies.set(c['name'], c['value'])
    sess.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": BASE + "/",
    })
    return sess

def _safe_get_json(sess: requests.Session, url: str, params: dict, retries=3, sleep=0.7) -> Optional[list]:
    for i in range(retries):
        try:
            r = sess.get(url, params=params, timeout=25)
            if r.status_code == 200:
                return r.json()
            time.sleep(sleep)
        except Exception:
            time.sleep(sleep)
    return None

def _try_float(x):
    try:
        if x is None: return np.nan
        f = float(x)
        return f if not np.isnan(f) else np.nan
    except Exception:
        return np.nan

def _extract_from_vtex_product(prod: dict) -> List[dict]:
    """
    Normaliza el JSON VTEX de /api/catalog_system/pub/products/search
    a registros por SKU (item).
    """
    out = []
    product_name = (prod.get("productName") or "").strip()
    brand = (prod.get("brand") or "").strip()
    product_link = prod.get("linkText") or prod.get("link") or ""
    items = prod.get("items") or []
    for it in items:
        sku = str(it.get("itemId") or "").strip()
        # EAN: múltiples posibles ubicaciones
        ean = it.get("ean") or it.get("Ean")
        if not ean:
            # referenceId [{Key: 'EAN', Value:'...'}]
            for ref in it.get("referenceId") or []:
                k = (ref.get("Key") or "").lower()
                v = (ref.get("Value") or "").strip()
                if k in ("ean", "gtin", "gtin13", "gtin14") and re.fullmatch(r"\d{8,14}", v):
                    ean = v
                    break
        if not ean:
            for spec in it.get("SkuSpecifications") or it.get("skuSpecifications") or []:
                name = (spec.get("FieldName") or spec.get("name") or "").lower()
                vals = spec.get("FieldValues") or spec.get("values") or []
                if "ean" in name and vals:
                    cand = str(vals[0]).strip()
                    if re.fullmatch(r"\d{8,14}", cand):
                        ean = cand
                        break

        rel = f"/{product_link}/p" if product_link and not product_link.endswith("/p") else ("/" + product_link if product_link else "")
        seller0 = ((it.get("sellers") or [{}])[0].get("commertialOffer") or {})
        out.append({
            "url": urljoin(BASE, rel) if rel else "",
            "sku": sku,
            "ean": ean or "",
            "marca": brand,
            "nombre": product_name,
            "precio_actual": _try_float(seller0.get("Price")),
            "precio_regular": _try_float(seller0.get("ListPrice")),
            "precio_actual_raw": str(seller0.get("Price") or ""),
            "precio_regular_raw": str(seller0.get("ListPrice") or ""),
            "descuento_texto": "",
            "unitario_texto": "",
            "iva_texto": "",
        })
    return out

def harvest_vtex_catalog(sess: requests.Session, categoria_rel: str, max_pages: int = 200) -> List[dict]:
    """
    Pagina la categoría usando la API pública de VTEX.
    Respeta el segmento (sucursal/canal) via cookie de la sesión 'sess'.
    """
    data = []
    base_api = urljoin(BASE, "/api/catalog_system/pub/products/search")
    cat = categoria_rel.split("?")[0].strip()
    page = 0
    while page < max_pages:
        _from = page * STEP
        _to = _from + STEP - 1
        url = urljoin(base_api + "/", cat.lstrip("/"))
        payload = {"_from": _from, "_to": _to}

        js = _safe_get_json(sess, url, payload, retries=3, sleep=0.8)
        if not js:
            # variante sin barra extra
            url_alt = urljoin(base_api, cat)
            js = _safe_get_json(sess, url_alt, payload, retries=2, sleep=1.0)

        if not js or not isinstance(js, list) or not js:
            break

        block = []
        for prod in js:
            block.extend(_extract_from_vtex_product(prod))

        # de-dup por SKU dentro del bloque
        seen = set()
        dedup = []
        for r in block:
            k = r.get("sku") or r.get("url")
            if k and k not in seen:
                seen.add(k)
                dedup.append(r)
        data.extend(dedup)

        if len(js) < STEP:
            break
        page += 1
        time.sleep(0.25)  # respeto
    return data

def _discover_subcategories_api(sess: requests.Session, root_rel: str) -> List[str]:
    """
    Descubre subcategorías usando la API de facets de VTEX (cuando está expuesta).
    Si falla, vuelve con [root_rel].
    """
    try:
        facets_url = urljoin(BASE, "/api/catalog_system/pub/facets/search")
        path = root_rel.strip("/").split("?")[0]
        r = sess.get(f"{facets_url}/{path}", params={"map": "c"}, timeout=20)
        if r.status_code != 200:
            return [root_rel]
        js = r.json()
        subs = set()
        def walk(node):
            if not isinstance(node, dict):
                return
            link = node.get("Link")
            if link and path in link and "/p" not in link:
                rel = "/" + link.strip("/").split("?")[0]
                subs.add(rel)
            for ch in node.get("Children") or []:
                walk(ch)
        walk(js)
        outs = sorted([s for s in subs if s.startswith("/" + path)])
        return outs or [root_rel]
    except Exception:
        return [root_rel]

# =========================
# RUN: híbrido (API + PDP fallback) + persistencia MySQL
# =========================
def run_scrape_and_persist():
    driver = _make_driver(max_retries=3)
    wait = WebDriverWait(driver, 30)

    try:
        # 1) login + tienda (fija sucursal/canal en vtex_segment)
        login_and_select_store(driver, wait)

        # 2) Crear sesión requests con cookies del navegador
        sess = _requests_from_selenium_cookies(driver)

        # 3) Descubrir subcategorías (max cobertura). Si no funciona, usa solo la raíz
        cats = _discover_subcategories_api(sess, CATEGORIA_URL)
        print(f"📂 Categorías a recorrer: {len(cats)}")
        all_rows: List[Dict[str, Any]] = []

        for cat in cats:
            print(f"\n====== {cat} ======")
            rows = harvest_vtex_catalog(sess, cat, max_pages=200)  # 200*STEP (p.ej. 10k SKUs potenciales)
            print(f"🧾 API devolvió {len(rows)} SKUs en {cat}")

            need_ean = [r for r in rows if not r.get("ean")]
            have_ean = [r for r in rows if r.get("ean")]
            print(f"   → con EAN: {len(have_ean)} | sin EAN: {len(need_ean)}")

            # 3.1) Fallback PDP sólo si falta EAN (para no saturar: tandas)
            for i, r in enumerate(need_ean, 1):
                try:
                    if i % 12 == 0:
                        time.sleep(0.9)
                    rel = r["url"][len(BASE):] if r.get("url","").startswith(BASE) else r.get("url","")
                    if not rel:
                        continue
                    item = _scrape_pdp(driver, wait, rel)
                    # fusionar precios si API no los trajo
                    for k in ("precio_actual","precio_regular","precio_actual_raw","precio_regular_raw"):
                        if (r.get(k) in (None, "", np.nan)) and item.get(k) not in (None, "", np.nan):
                            r[k] = item[k]
                    # siempre actualiza ean/marca/nombre por PDP si vino
                    if item.get("ean"):   r["ean"] = item["ean"]
                    if item.get("marca"): r["marca"] = item["marca"]
                    if item.get("nombre"):r["nombre"] = item["nombre"]
                except Exception as e:
                    print(f"    × PDP fallback error en {r.get('url','')}: {e}")

            all_rows.extend(have_ean + need_ean)

        if not all_rows:
            print("⚠️ No se capturaron productos; no se escribe MySQL.")
            return

        # 4) Persistir en MySQL (tu pipeline actual)
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
            for p in all_rows:
                # adapta al formato esperado por tus helpers
                p_rec = {
                    "url": p.get("url"),
                    "provincia": PROVINCIA,
                    "tienda": TIENDA_NOM,
                    "sku": p.get("sku") or "",
                    "ean": p.get("ean") or "",
                    "marca": p.get("marca") or "",
                    "nombre": p.get("nombre") or "",
                    "precio_actual": p.get("precio_actual"),
                    "precio_actual_raw": p.get("precio_actual_raw"),
                    "precio_regular": p.get("precio_regular"),
                    "precio_regular_raw": p.get("precio_regular_raw"),
                    "descuento_texto": p.get("descuento_texto") or "",
                    "unitario_texto": p.get("unitario_texto") or "",
                    "iva_texto": p.get("iva_texto") or "",
                    "capturado_en": capturado_en,
                }

                producto_id = find_or_create_producto(cur, p_rec)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p_rec)
                insert_historico(cur, tienda_id, pt_id, p_rec, capturado_en)
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
# MAIN
# =========================
if __name__ == "__main__":
    run_scrape_and_persist()
