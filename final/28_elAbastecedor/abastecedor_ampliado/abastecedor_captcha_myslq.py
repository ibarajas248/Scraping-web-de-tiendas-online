#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import shutil
import tempfile
import argparse
from urllib.parse import urlparse, parse_qs, urljoin
from typing import Optional, List, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re
# ======= MySQL helper =======
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # type: ignore

# =========================
# CONFIG DEL SITIO / RUTAS
# =========================
BASE_URL   = "https://elabastecedor.com.ar/"
LOGIN_URL  = "https://elabastecedor.com.ar/login"
TARGET_URL = "https://elabastecedor.com.ar/almacen-aceites"
OUT_XLSX   = "aceites_y_mas.xlsx"
OUT_CSV    = None  # por ej. "aceites_y_mas.csv"

# Credenciales
EMAIL    = os.getenv("ELABASTECEDOR_EMAIL", "mauro@factory-blue.com")
PASSWORD = os.getenv("ELABASTECEDOR_PASSWORD", "Compras2025")

# CapSolver
CAPSOLVER_API_KEY = os.getenv(
    "CAPSOLVER_API_KEY",
    "CAP-D2D4BC1B86FD4F550ED83C329898264E02F0E2A7A81E1B079F64F7F11477C8FD"
)
CAPSOLVER_CREATE_URL = "https://api.capsolver.com/createTask"
CAPSOLVER_RESULT_URL = "https://api.capsolver.com/getTaskResult"

# Timeouts
PAGE_WAIT = 60
CAPSOLVER_POLL_INTERVAL = 2.0
CAPSOLVER_TIMEOUT = 180
SUBMIT_WAIT_AFTER_TOKEN = 0.8

# Identidad de tienda (para la DB)
TIENDA_CODIGO = "elabastecedor"
TIENDA_NOMBRE = "El Abastecedor"

# ====== Límites VARCHAR ======
MAXLEN_NOMBRE = 255
MAXLEN_MARCA = 128
MAXLEN_FABRICANTE = 128
MAXLEN_CATEGORIA = 128
MAXLEN_SUBCATEGORIA = 128
MAXLEN_URL = 512
MAXLEN_NOMBRE_TIENDA = 255
MAXLEN_TIPO_OFERTA = 190
MAXLEN_PROMO_COMENTARIOS = 480

# =========================
# CATEGORÍAS A ITERAR
# (slug, nombre legible)
# =========================
CATEGORIES: List[Tuple[str, str]] = [
    ("almacen-aceites", "ACEITES."),
    ("almacen-aderezos", "ADEREZOS ."),
    ("almacen-apto-celiacos", "APTOS PARA CELIACOS."),
    ("almacen-arroces", "ARROCES ."),
    ("almacen-pascuas", "ART. DE PASCUA."),
    ("almacen-azucar", "AZUCAR ."),
    ("almacen-bizcochuelos-para-preparar", "BIZCOCHUELOS P/PREPARAR ."),
    ("almacen-bizcochuelos-preparados", "BIZCOCHUELOS PREPARADOS ."),
    ("almacen-budines-magdalenas", "BUDINES Y MAGDALENAS."),
    ("almacen-cacaos", "CACAOS."),
    ("almacen-cafes", "CAFES."),
    ("almacen-caldos", "CALDOS ."),
    ("almacen-cereales", "CEREALES ."),
    ("almacen-conservas-carnes", "CONSERVAS DE CARNES."),
    ("almacen-conservas-pescado", "CONSERVAS DE PESCADO."),
    ("almacen-conservas-legumbres", "CONSERVAS LEGUMBRES Y VEGETALES."),
    ("almacen-edulcoran", "EDULCORANTES."),
    ("almacen-encurtido", "ENCURTIDO ."),
    ("almacen-especias-condimentos", "ESPECIAS/CONDIMENTOS."),
    ("almacen-fiestas", "FIESTAS ."),
    ("almacen-fruta-conserva", "FRUTA CONSERVA ."),
    ("almacen-galletas-tostadas-grisines", "GALLETAS / TOSTADAS / GRISINES."),
    ("almacen-galletitas-dulces", "GALLETITAS DULCES."),
    ("almacen-galletitas-saladas", "GALLETITAS SALADAS."),
    ("almacen-gelatina", "GELATINA."),
    ("almacen-grasas", "GRASAS."),
    ("almacen-harinas", "HARINAS."),
    ("almacen-ketchup", "KETCHUP."),
    ("almacen-legumbres-secas", "LEGUMBRES SECAS ."),
    ("almacen-mayonesa", "MAYONESA."),
    ("almacen-mermelada", "MERMELADA ."),
    ("almacen-miel", "MIEL ."),
    ("almacen-mostaza", "MOSTAZA."),
    ("almacen-pan-rallado", "PAN RALLADO ."),
    ("almacen-pastas-secas", "PASTAS SECAS ."),
    ("almacen-postres-polvo", "POSTRES POLVO ."),
    ("almacen-pure-de-tomate", "PURE DE TOMATE."),
    ("almacen-pure-instantaneo", "PURE INSTANTANEO ."),
    ("almacen-reposteria-varios", "REPOSTERIA VARIOS ."),
    ("almacen-sales", "SALES ."),
    ("almacen-salsa-golf", "SALSA GOLF."),
    ("almacen-salsas", "SALSAS ."),
    ("almacen-snacks", "SNACKS ."),
    ("almacen-sopas", "SOPAS ."),
    ("almacen-te-y-mate-cocido", "TE Y MATE COCIDO ."),
    ("almacen-vinagre-aceto-jugo-limon", "VINAGRE /ACETO / JUGO DE LIMON."),
    ("almacen-yerbas", "YERBAS ."),

    #frescos

    ("frescos-comidas-elaboradas-panificados", "COMIDAS ELABORADAS Y PANIFICADOS. "),
    ("frescos-frutas-congeladas", "FRUTAS CONGELADAS. "),
    ("frescos-hamburguesas-medallones", "HAMBURGUESAS Y MEDALLONES. "),
    ("frescos-helados-postres", "HELADOS Y POSTRES. "),
    ("frescos-papas-congeladas", "PAPAS CONGELADAS. "),
    ("frescos-pescados-mariscos", "PESCADOS Y MARISCOS. "),
    ("frescos-rebozados-congelados", "REBOZADOS CONGELADOS. "),
    ("frescos-vegetales-congelados", "VEGETALES CONGELADOS. "),
    ("frescos-fiambres", "FIAMBRES. "),
    ("frescos-crema-de-leche", "CREMA DE LECHE. "),
    ("frescos-dulce-de-leche", "DULCE DE LECHE. "),
    ("frescos-lacteos-bebe", "LACTEOS BEBE. "),
    ("frescos-leches", "LECHES . "),
    ("frescos-mantecas", "MANTECAS . "),
    ("frescos-margarina", "MARGARINA . "),
    ("frescos-postres", "POSTRES . "),
    ("frescos-salchichas", "SALCHICHAS. "),
    ("frescos-yogur", "YOGUR. "),
    ("frescos-empanadas-tapas", "EMPANADAS TAPAS. "),
    ("frescos-levaduras", "LEVADURAS . "),
    ("frescos-pascualinas-tapas", "PASCUALINAS TAPAS . "),
    ("frescos-pastas-frescas", "PASTAS FRESCAS . "),
    ("frescos-dulces-solidos", "DULCES SOLIDOS. "),
    ("frescos-quesos-blandos", "QUESOS BLANDOS. "),
    ("frescos-quesos-duros", "QUESOS DUROS. "),
    ("frescos-quesos-rallados", "QUESOS RALLADOS. "),
    ("frescos-quesos-semiduros", "QUESOS SEMIDUROS. "),
    ("frescos-quesos-untables", "QUESOS UNTABLES . "),

    ("carniceria-achuras", "ACHURAS . "),
    ("carniceria-cerdo", "CERDO . "),
    ("carniceria-cortes-vacunos", "CORTES VACUNOS. "),
    ("carniceria-embutidos", "EMBUTIDOS. "),
    ("carniceria-granja", "GRANJA. "),
    ("carniceria-pollo", "POLLO . "),
    ("carniceria-preparados", "PREPARADOS . "),

    ("panificados-panificados", "PANIFICADOS. "),

    ("bebidas-aperitivo", "APERITIVO C/ALCOHOL. "),
    ("bebidas-bebidas-blancas", "BEBIDAS BLANCAS . "),
    ("bebidas-cervezas", "CERVEZAS . "),
    ("bebidas-espumantes-sidras", "ESPUMANTES / SIDRAS. "),
    ("bebidas-estucheria", "ESTUCHERIA. "),
    ("bebidas-generosos", "GENEROSOS. "),
    ("bebidas-licores", "LICORES . "),
    ("bebidas-vinos-blancos", "VINOS BLANCOS. "),
    ("bebidas-vinos-rosados", "VINOS ROSADOS. "),
    ("bebidas-vinos-tintos", "VINOS TINTOS. "),
    ("bebidas-whiskys", "WHISKYS. "),
    ("bebidas-aguas", "AGUAS. "),
    ("bebidas-aguas-saborizadas", "AGUAS SABORIZADAS. "),
    ("bebidas-aperitivos", "APERITIVO SIN ALCOHOL. "),
    ("bebidas-gaseosas", "GASEOSAS . "),
    ("bebidas-granadina", "GRANADINA. "),
    ("bebidas-energizantes", "ISOTONICAS / ENERGIZANTES. "),
    ("bebidas-jugos", "JUGOS. "),
    ("bebidas-jugos-preparar", "JUGOS PARA PREPARAR. "),

    ("verduleria-frutas", "FRUTAS . "),
    ("verduleria-frutas-perecederas", "FRUTAS PERECEDERAS . "),
    ("verduleria-frutas-verduras-envasadas", "FRUTAS Y VERDURAS ENVASADAS. "),
    ("verduleria-hortalizas-pesadas", "HORTALIZAS PESADAS . "),
    ("verduleria-productos-de-granja", "PROD. DE GRANJA . "),
    ("verduleria-verdura", "VERDURA . "),
    ("verduleria-verdura-en-hoja", "VERDURA EN HOJA . "),

    ("kiosco-alfajores", "ALFAJORES. "),
    ("kiosco-caramelos", "CARAMELOS. "),
    ("kiosco-chicles", "CHICLES. "),
    ("kiosco-chocolates", "CHOCOLATES. "),
    ("kiosco-obleas", "OBLEAS. "),
    ("kiosco-pochoclos", "POCHOLOS. "),
    ("kiosco-turrones", "TURRONES. "),
]

# =========================
# Utilidades
# =========================
def _truncate(val: Optional[Any], maxlen: int) -> Optional[str]:
    if val is None:
        return None
    s = str(val)
    return s if len(s) <= maxlen else s[:maxlen]

# =========================
# Selenium Driver
# =========================
def make_driver(headless: bool = True) -> (webdriver.Chrome, str):
    user_data_dir = tempfile.mkdtemp(prefix="chrome_profile_")
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--window-size=1920,1080")

    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin
    chromedriver_path = os.getenv("CHROMEDRIVER_PATH")

    if chromedriver_path and os.path.exists(chromedriver_path):
        service = Service(chromedriver_path)
    else:
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
    })
    return driver, user_data_dir

# =========================
# CapSolver helpers
# =========================
def caps_create_task(api_key: str, website_url: str, sitekey: str, is_enterprise=False) -> Optional[str]:
    task_type = "ReCaptchaV2EnterpriseTaskProxyLess" if is_enterprise else "ReCaptchaV2TaskProxyLess"
    payload = {"clientKey": api_key, "task": {"type": task_type, "websiteURL": website_url, "websiteKey": sitekey}}
    try:
        r = requests.post(CAPSOLVER_CREATE_URL, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("errorId") == 0:
            return data.get("taskId")
        print(f"[CapSolver] createTask error: {data}")
        return None
    except Exception as e:
        print(f"[CapSolver] createTask exception: {e}")
        return None

def caps_poll_result(api_key: str, task_id: str, timeout_sec: int = CAPSOLVER_TIMEOUT) -> Optional[str]:
    start = time.time()
    payload = {"clientKey": api_key, "taskId": task_id}
    while time.time() - start < timeout_sec:
        try:
            r = requests.post(CAPSOLVER_RESULT_URL, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data.get("errorId") != 0:
                print(f"[CapSolver] getTaskResult error: {data}")
                return None
            if data.get("status") == "ready":
                sol = data.get("solution", {})
                token = sol.get("gRecaptchaResponse") or sol.get("text")
                if token:
                    return token
            time.sleep(CAPSOLVER_POLL_INTERVAL)
        except Exception as e:
            print(f"[CapSolver] getTaskResult exception: {e}")
            time.sleep(CAPSOLVER_POLL_INTERVAL)
    print("[CapSolver] Timeout esperando resultado.")
    return None

# =========================
# reCAPTCHA helpers
# =========================
def find_anchor_iframe(driver):
    frames = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='/recaptcha/api2/anchor']")
    return frames[0] if frames else None

def find_bframe_iframe(driver):
    frames = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='/recaptcha/api2/bframe']")
    for f in frames:
        try:
            if f.is_displayed():
                return f
        except Exception:
            pass
    return frames[0] if frames else None

def extract_sitekey_from_iframe_src(src: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(src).query)
        k = q.get("k", [])
        if k:
            return k[0]
    except Exception:
        pass
    return None

def detect_recaptcha_sitekey(driver) -> Optional[str]:
    try:
        iframe = find_anchor_iframe(driver) or find_bframe_iframe(driver)
        if not iframe:
            return None
        src = iframe.get_attribute("src") or ""
        return extract_sitekey_from_iframe_src(src)
    except Exception:
        return None

def is_enterprise_recaptcha(driver) -> bool:
    try:
        iframe = find_anchor_iframe(driver) or find_bframe_iframe(driver)
        if not iframe:
            return False
        src = iframe.get_attribute("src") or ""
        return ("enterprise" in src.lower())
    except Exception:
        return False

def inject_recaptcha_token_and_trigger(driver, token: str) -> None:
    js = r"""
    (function(token) {
        function setVal(el, val){
            if (!el) return;
            el.value = val;
            el.dispatchEvent(new Event('change', { bubbles: true }));
            el.dispatchEvent(new Event('input',  { bubbles: true }));
        }
        var main = document.getElementById('g-recaptcha-response');
        if (main) setVal(main, token);
        var taList = document.querySelectorAll("textarea[name='g-recaptcha-response'], textarea.g-recaptcha-response");
        for (var i=0; i<taList.length; i++){ setVal(taList[i], token); }
        try {
            var form = (main && main.closest('form')) || document.querySelector('form');
            if (form) {
                var ev = new Event('submit', { bubbles: true, cancelable: true });
                form.dispatchEvent(ev);
            }
        } catch(e){}
    })(arguments[0]);
    """
    driver.execute_script(js, token)

# =========================
# Cookies -> requests
# =========================
def export_cookies_to_requests(driver) -> requests.Session:
    s = requests.Session()
    for c in driver.get_cookies():
        s.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"), path=c.get("path", "/"))
    return s

# =========================
# Utils scraping
# =========================
def clean_price(text: str) -> Optional[float]:
    if not text:
        return None
    s = str(text).replace("\xa0", " ").strip()
    # Quitar símbolos y letras, dejar dígitos y separadores
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return None

    # Si hay coma y punto: el separador decimal suele ser el de más a la derecha
    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            # Caso tipo "$ 8,199.00" → decimal es el punto
            s = s.replace(",", "")
        else:
            # Caso tipo "$ 8.199,00" → decimal es la coma
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Solo comas: si la parte final tiene 2 o 3 dígitos, interpretamos como decimal
        frac = s.split(",")[-1]
        if len(frac) in (2, 3):
            s = s.replace(",", ".")
        else:
            # Comas como miles
            s = s.replace(",", "")
    elif "." in s:
        # Solo puntos: si hay muchos puntos o la parte final no parece decimal → miles
        parts = s.split(".")
        if len(parts) > 2 or len(parts[-1]) not in (2, 3):
            s = s.replace(".", "")

    try:
        return round(float(s), 2)
    except Exception:
        return None

def parse_products_from_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    cards: List[Dict[str, Any]] = []
    conts = soup.select("div.feature-slider-item.swiper-slide article.list-product")
    if not conts:
        conts = soup.select("article.list-product")

    for art in conts:
        img_el = art.select_one("img.second-img") or art.select_one("img.first-img")
        img_url = img_el["src"].strip() if img_el and img_el.has_attr("src") else None
        if img_url and img_url.startswith("/"):
            img_url = urljoin(BASE_URL, img_url)

        name_el = art.select_one(".nombreProducto a.inner-link span")
        name = name_el.get_text(strip=True) if name_el else None

        link_el = art.select_one(".nombreProducto a.inner-link")
        href = link_el["href"].strip() if link_el and link_el.has_attr("href") else None
        prod_url = urljoin(BASE_URL, href) if href else None

        price_el = art.select_one(".pricing-meta .current-price")
        price_text = price_el.get_text(strip=True) if price_el else None
        price = clean_price(price_text)

        form = art.select_one("form.produItem")
        data_id = form.get("data-id", "").strip() if form else ""
        data_codigo = form.get("data-codigo", "").strip() if form else ""
        data_marca = form.get("data-marca", "").strip() if form else ""
        hidden_precio = None
        if form:
            inp = form.select_one("input[name='precio']")
            if inp and inp.has_attr("value"):
                hidden_precio = clean_price(inp["value"])

        cards.append({
            "nombre": name,
            "url_producto": prod_url,
            "imagen": img_url,
            "precio_visible": price,
            "precio_texto": price_text,
            "id_interno": data_id,
            "codigo_interno": data_codigo,
            "marca_tienda": data_marca,
            "precio_hidden": hidden_precio,
        })
    return cards

def go_to(driver, url: str, wait: WebDriverWait, expect_selector: Optional[str] = None):
    driver.get(url)
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    if expect_selector:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, expect_selector)))

# — Intento genérico de paginación
def try_click_next(driver, wait: WebDriverWait) -> bool:
    # Varias heurísticas de "Siguiente"
    selectors = [
        "a[rel='next']",
        "ul.pagination li.next a",
        "a.page-link[aria-label='Next']",
        "a.page-link[title*='Siguiente']",
        "a[title*='Siguiente']",
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.1)
            el.click()
            wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(0.3)
            return True
        except Exception:
            pass
    # Plan B: por texto visible
    try:
        el = driver.find_element(By.LINK_TEXT, "Siguiente")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        el.click()
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(0.3)
        return True
    except Exception:
        return False

# =========================
# Helpers MySQL
# =========================
import numpy as np
from datetime import datetime as dt

def _price_str(val) -> Optional[str]:
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = None  # la tienda no expone EAN
    nombre = _truncate(r.get("nombre") or "", MAXLEN_NOMBRE)
    marca = _truncate(r.get("marca_tienda") or None, MAXLEN_MARCA)
    fabricante = None
    categoria = None
    subcategoria = None

    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca or ""))
        row = cur.fetchone()
        if row:
            return row[0]

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    sku = (r.get("codigo_interno") or r.get("id_interno") or None)
    record_id = sku
    url = _truncate(r.get("url_producto") or None, MAXLEN_URL)
    nombre_tienda = _truncate(r.get("nombre") or None, MAXLEN_NOMBRE_TIENDA)

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))

        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: dt):
    precio = r.get("precio_visible") if r.get("precio_visible") is not None else r.get("precio_hidden")
    precio_lista = _price_str(precio)
    precio_oferta = _price_str(precio)
    tipo_oferta = None
    promo_comentarios = _truncate(
        f"precio_texto={r.get('precio_texto') or ''}".strip(),
        MAXLEN_PROMO_COMENTARIOS
    )
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
        precio_lista, precio_oferta, tipo_oferta,
        tipo_oferta, None, None, promo_comentarios
    ))

# =========================
# Scrape por categoría
# =========================
def scrape_categoria(driver, wait: WebDriverWait, slug: str, nombre: str) -> List[Dict[str, Any]]:
    url = urljoin(BASE_URL, slug)
    print(f"➡️ Navegando a categoría: {nombre} ({url})")
    go_to(driver, url, wait, expect_selector="article.list-product, .feature-slider-item.swiper-slide")

    all_items: List[Dict[str, Any]] = []
    page_idx = 1
    while True:
        html = driver.page_source
        items = parse_products_from_html(html)
        for it in items:
            it["categoria_slug"] = slug
            it["categoria_nombre"] = nombre
        all_items.extend(items)
        print(f"   • Página {page_idx}: {len(items)} items (acumulado {len(all_items)})")

        # Intento pasar a la siguiente página (si existe)
        moved = try_click_next(driver, wait)
        if not moved:
            break
        page_idx += 1

    print(f"🛒 Total en {nombre}: {len(all_items)}")
    return all_items

# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser(description="El Abastecedor (Selenium headless VPS) → MySQL / Excel")
    parser.add_argument("--url", default=None, help="URL de categoría a scrapear (si se pasa, NO itera por la lista)")
    parser.add_argument("--outfile", default=OUT_XLSX, help="XLSX combinado de salida")
    parser.add_argument("--csv", default=OUT_CSV, help="CSV combinado adicional (opcional)")
    parser.add_argument("--no-mysql", action="store_true", help="No insertar en MySQL; solo exportar archivos")
    parser.add_argument("--headless", action="store_true", default=True, help="Headless ON (por defecto)")
    parser.add_argument("--per-category-files", action="store_true",
                        help="Además del combinado, guarda un XLSX por categoría (Listado_<slug>.xlsx)")
    parser.add_argument("--start-slug", default="almacen-aceites",
                        help="Al iterar toda la lista, comenzar desde este slug (incluido).")
    args = parser.parse_args()

    driver, profile_dir = make_driver(headless=True)
    wait = WebDriverWait(driver, PAGE_WAIT)

    from datetime import datetime as dt
    productos: List[Dict[str, Any]] = []

    try:
        # 1) Login
        driver.get(LOGIN_URL)
        email_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='email'], input#email")))
        pass_input  = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='password'], input#password")))
        email_input.clear(); email_input.send_keys(EMAIL)
        pass_input.clear();  pass_input.send_keys(PASSWORD)

        # reCAPTCHA
        try:
            WebDriverWait(driver, 5).until(lambda d: find_anchor_iframe(d) or find_bframe_iframe(d))
        except Exception:
            pass

        sitekey = detect_recaptcha_sitekey(driver)
        if sitekey:
            enterprise = is_enterprise_recaptcha(driver)
            if not CAPSOLVER_API_KEY or not CAPSOLVER_API_KEY.startswith("CAP-"):
                print("❌ CAPSOLVER_API_KEY no configurada o inválida.")
                sys.exit(2)
            task_id = caps_create_task(CAPSOLVER_API_KEY, LOGIN_URL, sitekey, is_enterprise=enterprise)
            if not task_id:
                print("❌ No se pudo crear la tarea de CapSolver.")
                sys.exit(2)
            token = caps_poll_result(CAPSOLVER_API_KEY, task_id, timeout_sec=CAPSOLVER_TIMEOUT)
            if not token:
                print("❌ No se obtuvo token del reCAPTCHA.")
                sys.exit(2)
            inject_recaptcha_token_and_trigger(driver, token)
            time.sleep(SUBMIT_WAIT_AFTER_TOKEN)

        # enviar form
        for sel in ["button#send2", "button[name='send']", "button[type='submit']"]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, sel)
                driver.execute_script("arguments[0].scrollIntoView({behavior:'instant',block:'center'});", btn)
                time.sleep(0.1)
                btn.click()
                break
            except Exception:
                continue

        try:
            WebDriverWait(driver, 30).until(lambda d: "/login" not in d.current_url)
        except Exception:
            pass

        if "/login" in driver.current_url:
            print("⚠️ No se logró iniciar sesión. Revisa credenciales/recaptcha (seguimos igual).")
        else:
            print("🎉 Sesión iniciada.")

        # 2) Scrape: una URL concreta o iterar categorías
        per_cat_results: Dict[str, List[Dict[str, Any]]] = {}

        if args.url:
            # modo single URL (como antes)
            slug_guess = args.url.rstrip("/").split("/")[-1]
            nombre_guess = next((n for s, n in CATEGORIES if s == slug_guess), slug_guess)
            items = scrape_categoria(driver, wait, slug_guess, nombre_guess)
            per_cat_results[slug_guess] = items

        else:
            # modo iteración completa, empezando en start-slug (incluido)
            # reordena la lista para arrancar desde el slug indicado
            if args.start_slug not in [s for s, _ in CATEGORIES]:
                print(f"⚠️ start-slug '{args.start_slug}' no está en la lista; se empezará desde el principio.")
                ordered = CATEGORIES[:]
            else:
                idx = [s for s, _ in CATEGORIES].index(args.start_slug)
                ordered = CATEGORIES[idx:] + CATEGORIES[:idx]

            for slug, nombre in ordered:
                try:
                    items = scrape_categoria(driver, wait, slug, nombre)
                    per_cat_results[slug] = items
                except Exception as e:
                    print(f"❌ Error en categoría {slug}: {e}")
                    continue

        # 3) Exportar a Excel / CSV (combinado)
        all_rows: List[Dict[str, Any]] = []
        for slug, items in per_cat_results.items():
            all_rows.extend(items)

        df = pd.DataFrame(all_rows, columns=[
            "categoria_slug", "categoria_nombre",
            "nombre", "url_producto", "imagen",
            "precio_visible", "precio_texto",
            "id_interno", "codigo_interno", "marca_tienda", "precio_hidden"
        ])

        #df.to_excel(args.outfile, index=False)
        print(f"✅ XLSX exportado: {args.outfile} ({len(df)} filas total)")

        if args.csv:
            #df.to_csv(args.csv, index=False, encoding="utf-8")
            print(f"✅ CSV exportado: {args.csv}")

        if args.per_category_files:
            for slug, items in per_cat_results.items():
                dfi = pd.DataFrame(items)
                outp = f"Listado_{slug}.xlsx"
                dfi.to_excel(outp, index=False)
                print(f"   • Guardado: {outp} ({len(dfi)} filas)")

        # 4) Ingesta MySQL (opcional)
        if not args.no_mysql and len(df) > 0:
            conn = None
            try:
                conn = get_conn()
                conn.autocommit = False
                cur = conn.cursor()
                tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
                capturado_en = dt.now()

                inserted = 0
                for _, r in df.iterrows():
                    rec = r.to_dict()
                    producto_id = find_or_create_producto(cur, rec)
                    pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, rec)
                    insert_historico(cur, tienda_id, pt_id, rec, capturado_en)
                    inserted += 1
                    if inserted % 50 == 0:
                        conn.commit()
                conn.commit()
                print(f"💾 MySQL OK: {inserted} filas en historico_precios.")
            except Exception:
                if conn:
                    conn.rollback()
                raise
            finally:
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception:
            pass
        print("Fin del script.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado por el usuario.")
        sys.exit(1)
