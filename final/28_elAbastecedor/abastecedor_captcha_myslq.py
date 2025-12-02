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
from typing import Optional, List, Dict, Any

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

# ======= MySQL helper =======
# Debe existir base_datos.py en tu proyecto con get_conn()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # type: ignore

# =========================
# CONFIG DEL SITIO / RUTAS
# =========================
BASE_URL   = "https://elabastecedor.com.ar/"
LOGIN_URL  = "https://elabastecedor.com.ar/login"
TARGET_URL = "https://elabastecedor.com.ar/almacen-aceites"
OUT_XLSX   = "aceites.xlsx"
OUT_CSV    = None  # si querés CSV además del XLSX, poné por ej. "aceites.csv"

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

# ====== Límites VARCHAR (ajusta si tu schema difiere) ======
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
# Utilidades varias
# =========================
def _truncate(val: Optional[Any], maxlen: int) -> Optional[str]:
    if val is None:
        return None
    s = str(val)
    return s if len(s) <= maxlen else s[:maxlen]


# =========================
# Selenium Driver (HEADLESS para VPS)
# =========================
def make_driver(headless: bool = True) -> (webdriver.Chrome, str):
    """
    Crea webdriver Chrome en modo headless y devuelve (driver, user_data_dir_tmp)
    para poder borrar el perfil al final. Se usa un perfil único para evitar
    'user data directory is already in use'.
    """
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

    # Permitir override de binarios si ya los tenés instalados en el VPS
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin
    chromedriver_path = os.getenv("CHROMEDRIVER_PATH")

    if chromedriver_path and os.path.exists(chromedriver_path):
        service = Service(chromedriver_path)
    else:
        # webdriver_manager descargará si no hay binario (requiere salida a internet)
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)

    # Anti-automation mínimo
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
    t = text.strip()
    t = t.replace("$", "").replace("ARS", "").replace("USD", "").strip()
    t = t.replace(".", "").replace(" ", "")
    t = t.replace(",", ".")
    try:
        return round(float(t), 2)
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


# =========================
# Helpers MySQL (mismo esquema que vienes usando)
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
    ean = None  # la lista no expone EAN; queda NULL
    nombre = _truncate(r.get("nombre") or "", MAXLEN_NOMBRE)
    marca = _truncate(r.get("marca_tienda") or None, MAXLEN_MARCA)
    fabricante = None
    categoria = None
    subcategoria = None

    # 1) EAN (no lo tenemos)
    # 2) nombre + marca
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca or ""))
        row = cur.fetchone()
        if row:
            pid = row[0]
            return pid

    # 3) Insert
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    # Preferimos codigo_interno; si no, id_interno
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
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    # Sin SKU: guardamos con URL/nombre
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: dt):
    # Precio final: usamos visible si existe, sino hidden
    precio = r.get("precio_visible")
    if precio is None:
        precio = r.get("precio_hidden")

    precio_lista = _price_str(precio)
    precio_oferta = _price_str(precio)
    tipo_oferta = None  # no distinguimos promo aquí

    # Comentarios con trazas útiles
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
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser(description="El Abastecedor (Selenium headless VPS) → MySQL / Excel")
    parser.add_argument("--url", default=TARGET_URL, help="URL de categoría a scrapear")
    parser.add_argument("--outfile", default=OUT_XLSX, help="XLSX de salida")
    parser.add_argument("--csv", default=OUT_CSV, help="CSV adicional de salida (opcional)")
    parser.add_argument("--no-mysql", action="store_true", help="No insertar en MySQL; solo exportar archivos")
    parser.add_argument("--headless", action="store_true", default=True, help="Ejecutar en headless (por defecto ON)")
    args = parser.parse_args()

    driver, profile_dir = make_driver(headless=True)  # forzamos headless en VPS
    wait = WebDriverWait(driver, PAGE_WAIT)

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

        # Confirmación login (no estar en /login)
        try:
            WebDriverWait(driver, 30).until(lambda d: "/login" not in d.current_url)
        except Exception:
            pass

        if "/login" in driver.current_url:
            print("⚠️ No se logró iniciar sesión. Revisa credenciales/recaptcha (se intentará igual).")
        else:
            print("🎉 Sesión iniciada.")

        # 2) Ir a la categoría
        print(f"➡️ Navegando a: {args.url}")
        go_to(driver, args.url, wait, expect_selector="article.list-product, .feature-slider-item.swiper-slide")

        # 3) Parsear productos de la página actual
        html = driver.page_source
        productos = parse_products_from_html(html)
        print(f"🛒 Productos encontrados: {len(productos)}")

        # 4) Exportar a Excel / CSV
        df = pd.DataFrame(productos, columns=[
            "nombre", "url_producto", "imagen",
            "precio_visible", "precio_texto",
            "id_interno", "codigo_interno", "marca_tienda", "precio_hidden"
        ])
        df.to_excel(args.outfile, index=False)
        print(f"✅ XLSX exportado: {args.outfile} ({len(df)} filas)")
        if args.csv:
            df.to_csv(args.csv, index=False, encoding="utf-8")
            print(f"✅ CSV exportado: {args.csv}")

        # 5) Ingesta MySQL (opcional)
        if not args.no_mysql:
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
            except Exception as e:
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
        # Borrar el perfil temporal para no dejar basura
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
