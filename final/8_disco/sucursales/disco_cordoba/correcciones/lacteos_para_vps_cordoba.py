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
import socket
import logging
from logging.handlers import RotatingFileHandler

# ====== (Opcional) webdriver_manager si no hay CHROMEDRIVER_BIN ======
try:
    from webdriver_manager.chrome import ChromeDriverManager  # type: ignore
    HAVE_WDM = True
except Exception:
    HAVE_WDM = False

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.common.exceptions import SessionNotCreatedException, WebDriverException, TimeoutException
from selenium.webdriver.chrome.service import Service

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

from base_datos import get_conn

STOP_EVENT = threading.Event()

# =========================
# Logging robusto
# =========================
def setup_logging():
    base_dir = os.path.abspath(os.path.dirname(__file__))
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, "disco_scrape.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Evitar duplicados si se reinvoca
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    fh = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(sh)

    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    logging.info("📝 Logging inicializado en %s", log_path)

# =========================
# Entorno cron (solo si no hay TTY)
# =========================
def setup_cron_environment():
    is_tty = False
    try:
        is_tty = sys.stdin.isatty()
    except Exception:
        is_tty = False

    if not is_tty:
        os.environ.setdefault("HOME", "/tmp")
        os.environ.setdefault("TMPDIR", "/tmp")
        os.environ.setdefault("XDG_RUNTIME_DIR", "/tmp")
        os.environ.pop("DISPLAY", None)
        os.environ.setdefault("LANG", "C.UTF-8")
        os.environ.setdefault("LC_ALL", "C.UTF-8")

        default_path = "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/snap/bin"
        os.environ["PATH"] = os.environ.get("PATH") or default_path
        if default_path not in os.environ["PATH"]:
            os.environ["PATH"] = os.environ["PATH"] + ":" + default_path

        logging.info("🧩 Entorno cron aplicado: HOME=%s PATH=%s", os.environ["HOME"], os.environ["PATH"])

def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]

def _enter_listener():
    try:
        input("🔴 Presioná ENTER para terminar y guardar lo recolectado hasta ahora...\n")
        STOP_EVENT.set()
    except EOFError:
        return

def start_enter_listener_if_tty():
    try:
        if sys.stdin and sys.stdin.isatty():
            t_listener = threading.Thread(target=_enter_listener, daemon=True)
            t_listener.start()
        else:
            logging.info("ℹ️  stdin no interactivo (cron). No se inicia listener ENTER.")
    except Exception:
        pass

# =========================
# Parámetros (ENV primero)
# =========================
BASE        = "https://www.disco.com.ar"

DISCO_USER  = os.getenv("DISCO_USER", "comercial@factory-blue.com")
DISCO_PASS  = os.getenv("DISCO_PASS", "Compras2025")

PROVINCIA     = os.getenv("DISCO_PROVINCIA", "CORDOBA").strip()
TIENDA_NOM    = os.getenv("DISCO_TIENDA", "Disco Alta Córdoba Cabrera 493").strip()
CATEGORIA_URL = os.getenv("DISCO_CATEGORIA_URL", "/lacteos").strip()
MAX_EMPTY     = int(os.getenv("DISCO_MAX_EMPTY", "1"))
SLEEP_PDP     = float(os.getenv("DISCO_SLEEP_PDP", "0.6"))
SLEEP_PAGE    = float(os.getenv("DISCO_SLEEP_PAGE", "1.0"))

EMBEDDED_CHROME_BIN = os.getenv("EMBEDDED_CHROME_BIN", "/usr/bin/google-chrome-stable")
EMBEDDED_CHROMEDRIVER_BIN = os.getenv("EMBEDDED_CHROMEDRIVER_BIN", "")  # dejalo vacío si no estás seguro

TIENDA_CODIGO = "Disco Cordoba"
TIENDA_NOMBRE = "Disco Cordoba"
TIENDA_REF    = os.getenv("DISCO_REF_TIENDA", "disco_cordoba_cabrera_493")

KILL_STALE_CHROME = int(os.getenv("KILL_STALE_CHROME", "0"))

# =========================
# Utilidades
# =========================
def _safe_get(driver, url, tries=4, base_sleep=1.0):
    for i in range(1, tries+1):
        try:
            driver.get(url)
            return True
        except (TimeoutException, WebDriverException) as e:
            logging.warning("get(%s) fallo intento %d/%d: %s", url, i, tries, e)
            time.sleep(base_sleep * i)
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
    raw = (text or "").strip()
    if not raw:
        return np.nan, raw
    s = re.sub(r"[^\d,\.]", "", raw)
    if not s:
        return np.nan, raw
    has_comma = "," in s
    has_dot   = "." in s
    try:
        if has_comma and has_dot:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif has_comma and not has_dot:
            s = s.replace(",", ".")
        elif has_dot and not has_comma:
            parts = s.split(".")
            if len(parts[-1]) == 3 and all(p.isdigit() for p in parts):
                s = s.replace(".", "")
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
            WebDriverWait(driver, 10).until(lambda d: len(sel.find_elements(By.TAG_NAME, "option")) > 1)
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

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.8)
        driver.execute_script("window.scrollBy(0,-150);")
        time.sleep(0.2)

        if (time.time() - stable_since) > 2.5 and not show_more_btns:
            break
        if (time.time() - stable_since) > max_wait:
            break
        if clicks >= max_clicks:
            break
        if STOP_EVENT.is_set():
            break

def _extract_jsonld_ean(driver) -> Optional[str]:
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
    try:
        ean_el = driver.find_element(
            By.XPATH,
            "//span[contains(@class,'product-identifier__label') and (contains(.,'EAN') or contains(.,'Gtin'))]"
            "/following-sibling::span[contains(@class,'product-identifier__value')]"
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

    try:
        name = driver.find_element(By.XPATH, "//h1[contains(@class,'productNameContainer')]//span").text.strip()
    except Exception:
        name = ""

    try:
        brand = driver.find_element(By.XPATH, "//*[contains(@class,'productBrandName')]").text.strip()
    except Exception:
        brand = ""

    sku = ""
    try:
        sku = driver.find_element(
            By.XPATH,
            "//span[contains(@class,'product-identifier__label') and normalize-space()='SKU']"
            "/following-sibling::span[contains(@class,'product-identifier__value')]"
        ).text.strip()
    except Exception:
        try:
            sku = driver.find_element(By.XPATH, "//*[contains(@class,'product-identifier')][contains(.,'SKU')]").text
            sku = re.sub(r".*SKU\s*:\s*", "", sku, flags=re.I).strip()
        except Exception:
            sku = ""

    ean = _extract_jsonld_ean(driver)

    price_now_text = ""
    for xp in [
        "//*[@id='priceContainer']",
        "//*[contains(@class,'vtex-product-price-1-x-sellingPrice')]",
        "(//*[contains(@class,'store-theme')][contains(.,'$')])[1]"
    ]:
        try:
            price_now_text = driver.find_element(By.XPATH, xp).text
            if price_now_text.strip():
                break
        except Exception:
            continue
    price_now, price_now_raw = _parse_price(price_now_text)

    price_reg_text = ""
    for xp in [
        "//*[contains(@class,'vtex-product-price-1-x-listPrice')]",
        "(//div[contains(@class,'store-theme')][contains(text(),'$')])[2]"
    ]:
        try:
            price_reg_text = driver.find_element(By.XPATH, xp).text
            if price_reg_text.strip():
                break
        except Exception:
            continue
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
# Driver VPS: arregla mismatch Chrome vs Chromedriver
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

def _run_version(cmd: List[str]) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=3)
        s = (out.stdout or out.stderr or "").strip()
        return s
    except Exception:
        return ""

def _major_version_from_text(s: str) -> Optional[int]:
    # Chrome: "Google Chrome 120.0.6099.129"
    # Driver: "ChromeDriver 120.0.6099.109 ..."
    m = re.search(r"(\d+)\.", s or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def _probe_exe(path: str) -> bool:
    return bool(path and os.path.exists(path) and os.access(path, os.X_OK))

def _resolve_browser_and_driver_paths() -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (chrome_binary, chromedriver_path) pero:
    - Si driver existe y su major NO coincide con el major de Chrome => lo ignora (None)
    - Si no hay driver usable => None (Selenium Manager resuelve)
    """
    chrome_bin = os.getenv("CHROME_BIN") or ""
    driver_bin = os.getenv("CHROMEDRIVER_BIN") or ""

    # Chrome candidates
    chrome_candidates = []
    if chrome_bin:
        chrome_candidates.append(chrome_bin)
    if EMBEDDED_CHROME_BIN:
        chrome_candidates.append(EMBEDDED_CHROME_BIN)
    chrome_candidates += [
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
    ]

    chrome_bin_ok = None
    for c in chrome_candidates:
        if _probe_exe(c):
            vtxt = _run_version([c, "--version"])
            if vtxt:
                chrome_bin_ok = c
                break

    # Driver candidates
    driver_candidates = []
    if driver_bin:
        driver_candidates.append(driver_bin)
    if EMBEDDED_CHROMEDRIVER_BIN:
        driver_candidates.append(EMBEDDED_CHROMEDRIVER_BIN)
    driver_candidates += [
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/usr/lib/chromium/chromedriver",
        "/snap/bin/chromium.chromedriver",
    ]

    driver_bin_ok = None
    for d in driver_candidates:
        if _probe_exe(d):
            driver_bin_ok = d
            break

    # Log versions
    chrome_major = None
    if chrome_bin_ok:
        vchrome = _run_version([chrome_bin_ok, "--version"])
        chrome_major = _major_version_from_text(vchrome)
        logging.info("🧭 Navegador detectado: %s (%s)", chrome_bin_ok, vchrome or "sin versión")
    else:
        logging.warning("🧭 No detecté Chrome/Chromium por ruta. Selenium Manager intentará resolver.")

    if driver_bin_ok:
        vdrv = _run_version([driver_bin_ok, "--version"])
        drv_major = _major_version_from_text(vdrv)
        logging.info("🧭 Chromedriver detectado: %s (%s)", driver_bin_ok, vdrv or "sin versión")

        # Si hay Chrome y mismatch -> IGNORAR driver (esto arregla tu crash)
        if chrome_major is not None and drv_major is not None and chrome_major != drv_major:
            logging.warning(
                "⚠️  MISMATCH: Chrome major=%s vs Chromedriver major=%s. "
                "Ignorando %s y usando Selenium Manager.",
                chrome_major, drv_major, driver_bin_ok
            )
            driver_bin_ok = None
    else:
        logging.info("🧭 Sin chromedriver explícito detectado; usando Selenium Manager.")

    # Si no hay driver y está webdriver_manager, dejarlo como plan B (pero SOLO si hay Chrome detectado)
    if not driver_bin_ok and HAVE_WDM and chrome_bin_ok:
        try:
            # webdriver_manager suele descargar “algo” compatible,
            # pero si falla, igual Selenium Manager a veces es mejor.
            dpath = ChromeDriverManager().install()
            if _probe_exe(dpath):
                # validar mismatch también
                vdrv = _run_version([dpath, "--version"])
                drv_major = _major_version_from_text(vdrv)
                if chrome_major is None or drv_major is None or chrome_major == drv_major:
                    driver_bin_ok = dpath
                    logging.info("⬇️ webdriver_manager instaló chromedriver en: %s (%s)", dpath, vdrv or "sin versión")
                else:
                    logging.warning("⚠️ webdriver_manager descargó driver major=%s != Chrome major=%s. Se ignora.", drv_major, chrome_major)
        except Exception as e:
            logging.warning("No se pudo instalar chromedriver con webdriver_manager: %s", e)

    return chrome_bin_ok, driver_bin_ok

def _make_driver_once() -> webdriver.Chrome:
    prof_dir = tempfile.mkdtemp(prefix="chrome-prof-", dir="/tmp")
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

    options.add_argument("--lang=es-AR")
    options.add_argument("--accept-lang=es-AR,es;q=0.9,en;q=0.8")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    options.add_argument(f"--user-data-dir={prof_dir}")
    options.add_argument("--profile-directory=Default")
    options.add_argument(f"--disk-cache-dir={cache_dir}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-extensions")

    dbg_port = _pick_free_port()
    options.add_argument(f"--remote-debugging-port={dbg_port}")

    chrome_bin, driver_bin = _resolve_browser_and_driver_paths()
    if chrome_bin:
        options.binary_location = chrome_bin

    # 🔑 Estrategia:
    # 1) Si hay driver_bin validado => intentar con Service
    # 2) Si crashea => fallback inmediato a Selenium Manager (sin Service)
    if driver_bin:
        try:
            service = Service(executable_path=driver_bin)
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(60)
            return driver
        except WebDriverException as e:
            logging.warning("⚠️ Falló iniciar con chromedriver explícito (%s): %s", driver_bin, e)
            logging.info("↩️ Fallback: creando driver con Selenium Manager (sin Service)...")

    driver = webdriver.Chrome(options=options)  # Selenium Manager
    driver.set_page_load_timeout(60)
    return driver

def _make_driver(max_retries: int = 3) -> webdriver.Chrome:
    _best_effort_kill_stale_chrome()
    last_err = None
    for intento in range(1, max_retries + 1):
        try:
            driver = _make_driver_once()

            def _sigterm_handler(signum, frame):
                logging.info("📴 SIGTERM recibido. Cerrando driver…")
                with contextlib.suppress(Exception):
                    driver.quit()
                os._exit(0)

            signal.signal(signal.SIGTERM, _sigterm_handler)
            return driver

        except SessionNotCreatedException as e:
            last_err = e
            msg = str(e)
            logging.error("SessionNotCreatedException (intento %d/%d): %s", intento, max_retries, msg)
            time.sleep(1.0 * intento)
            continue

        except WebDriverException as e:
            last_err = e
            logging.warning("WebDriverException creando driver (intento %d/%d): %s", intento, max_retries, e)
            time.sleep(1.0 * intento)
            continue

        except Exception as e:
            last_err = e
            logging.error("Error creando driver: %s", e)
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

    _click_with_retry(driver, wait, "//span[normalize-space()='Mi Cuenta']")
    time.sleep(1)

    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'vtex-login-2-x-button')]")))
    except Exception:
        pass

    try:
        _click_with_retry(driver, wait, "//div[contains(@class,'vtex-login-2-x-emailPasswordOptionBtn')]//button")
    except Exception:
        try:
            _click_with_retry(driver, wait, "//button[.//span[normalize-space()='Email y contraseña']]")
        except Exception:
            _click_with_retry(driver, wait, "//button[.//span[contains(normalize-space(),'Entrar con e-mail')]]")

    for xp in [
        "//input[@placeholder='Email' and @type='text']",
        "//input[contains(@class,'vtex-styleguide-9-x-input') and not(@type='password')]",
        "(//input[not(@type='password') and not(@type='hidden')])[1]",
    ]:
        try:
            _type_with_retry(driver, wait, xp, DISCO_USER)
            break
        except Exception:
            continue

    for xp in [
        "//input[@type='password' and contains(@class,'vtex-styleguide-9-x-input')]",
        "//input[@type='password' or contains(@placeholder,'●')]",
    ]:
        try:
            _type_with_retry(driver, wait, xp, DISCO_PASS)
            break
        except Exception:
            continue

    for xp in [
        "//span[normalize-space()='Entrar']/ancestor::button[@type='submit'][1]",
        "//div[contains(@class,'vtex-login-2-x-sendButton')]//button[@type='submit']",
    ]:
        try:
            _click_with_retry(driver, wait, xp)
            break
        except Exception:
            continue
    time.sleep(2)

    for xp in [
        "//span[contains(normalize-space(),'Seleccioná') and contains(.,'método de entrega')]/ancestor::*[@role='button'][1]",
        "//div[contains(@class,'discoargentina-delivery-modal-1-x-containerTrigger')]/ancestor::div[@role='button'][1]",
    ]:
        try:
            _click_with_retry(driver, wait, xp)
            break
        except Exception:
            continue
    time.sleep(1)

    for xp in [
        "//div[contains(@class,'pickUpSelectionContainer')]//button[.//p[contains(normalize-space(),'Retirar en una tienda')]]",
        "//button[.//p[contains(normalize-space(),'Retirar en una tienda')]]",
    ]:
        try:
            _click_with_retry(driver, wait, xp)
            break
        except Exception:
            continue

    try:
        _click_with_retry(driver, wait,
            "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar Provincia')]]"
            "//div[contains(@class,'vtex-dropdown__button')]"
        )
    except Exception:
        pass

    _select_by_text_case_insensitive(driver, wait,
        "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar Provincia')]]//select",
        PROVINCIA
    )
    time.sleep(1.2)

    try:
        _click_with_retry(driver, wait,
            "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]"
            "//div[contains(@class,'vtex-dropdown__button')]"
        )
    except Exception:
        pass

    store_select_xpath = "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//select"
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, f"{store_select_xpath}/option[contains(., '{TIENDA_NOM}')]")))
    except Exception:
        pass

    _select_by_text_case_insensitive(driver, wait, store_select_xpath, TIENDA_NOM)

    for xp in [
        "//div[contains(@class,'discoargentina-delivery-modal-1-x-buttonStyle')]//button[.//div[normalize-space()='Confirmar']]",
        "//div[@role='dialog']//button[.//div[normalize-space()='Confirmar'] or normalize-space()='Confirmar']",
    ]:
        try:
            _click_with_retry(driver, wait, xp)
            break
        except Exception:
            continue
    time.sleep(1.2)

def run_scrape_and_persist():
    start_enter_listener_if_tty()

    driver = _make_driver(max_retries=3)
    wait = WebDriverWait(driver, 30)

    data: List[Dict[str, Any]] = []
    try:
        logging.info("🔐 Iniciando login y selección de tienda…")
        login_and_select_store(driver, wait)
        logging.info("✅ Login/tienda OK")

        page = 1
        empty_pages = 0
        while True:
            if STOP_EVENT.is_set():
                logging.info("🛑 Corte solicitado. Fin de páginas.")
                break

            list_url = f"{BASE}{CATEGORIA_URL}?page={page}"
            logging.info("📄 Página: %d -> %s", page, list_url)
            _safe_get(driver, list_url)
            time.sleep(SLEEP_PAGE)

            _load_all_products_on_list(driver, wait, max_wait=20, max_clicks=20)
            links = _collect_product_links_on_page(driver, timeout=14)

            if not links:
                logging.warning("⚠️  Sin items en la página %d.", page)
                empty_pages += 1
                if empty_pages > MAX_EMPTY:
                    logging.info("⛔ Fin: no hay más productos.")
                    break
                page += 1
                continue

            empty_pages = 0
            logging.info("🔗 Productos encontrados: %d", len(links))

            for i, rel in enumerate(links, 1):
                if STOP_EVENT.is_set():
                    logging.info("🛑 Corte solicitado. Deteniendo restantes en esta página…")
                    break
                try:
                    logging.info("  → [%d/%d] %s", i, len(links), rel)
                    item = _scrape_pdp(driver, wait, rel)
                    data.append(item)
                except Exception as e:
                    logging.error("    × Error en %s: %s", rel, e)
                finally:
                    with contextlib.suppress(Exception):
                        _safe_get(driver, list_url)
                    time.sleep(SLEEP_PDP)

            if STOP_EVENT.is_set():
                break

            page += 1

        if not data:
            logging.warning("⚠️ No se capturaron productos; no se escribe MySQL.")
            return

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

                if insertados % 50 == 0:
                    conn.commit()
                    logging.info("💾 Commit intermedio: %d filas…", insertados)

            conn.commit()
            logging.info("✅ Guardado en MySQL: %d filas de histórico para %s (%s)", insertados, TIENDA_NOM, capturado_en)

        except MySQLError as e:
            if conn:
                conn.rollback()
            logging.error("❌ Error MySQL: %s", e)
        finally:
            with contextlib.suppress(Exception):
                if conn:
                    conn.close()

    finally:
        with contextlib.suppress(Exception):
            driver.quit()
        logging.info("🧹 Driver cerrado.")

def main():
    setup_logging()
    setup_cron_environment()
    run_scrape_and_persist()

if __name__ == "__main__":
    main()
