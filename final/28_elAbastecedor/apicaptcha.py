#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import requests
from urllib.parse import urlparse, parse_qs, urljoin
from typing import Optional, List, Dict

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
import pandas as pd

# =========================
# CONFIG
# =========================
BASE_URL   = "https://elabastecedor.com.ar/"
LOGIN_URL  = "https://elabastecedor.com.ar/login"
TARGET_URL = "https://elabastecedor.com.ar/almacen-aceites"
OUT_XLSX   = "aceites.xlsx"

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

# Timeouts / reintentos
PAGE_WAIT = 60
CAPSOLVER_POLL_INTERVAL = 2.0
CAPSOLVER_TIMEOUT = 180
SUBMIT_WAIT_AFTER_TOKEN = 0.8


# =========================
# Selenium Driver
# =========================
def make_driver(headless: bool = False) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--lang=es-AR")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    # Anti-automation flags mínimos
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """
    })
    return driver


# =========================
# CapSolver helpers
# =========================
def caps_create_task(api_key: str, website_url: str, sitekey: str, is_enterprise=False) -> Optional[str]:
    task_type = "ReCaptchaV2EnterpriseTaskProxyLess" if is_enterprise else "ReCaptchaV2TaskProxyLess"
    payload = {
        "clientKey": api_key,
        "task": {
            "type": task_type,
            "websiteURL": website_url,
            "websiteKey": sitekey
        }
    }
    try:
        r = requests.post(CAPSOLVER_CREATE_URL, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("errorId") == 0:
            return data.get("taskId")
        else:
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
from urllib.parse import urlparse, parse_qs

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
        if (typeof window.onSubmit === 'function') { try { window.onSubmit(); } catch(e){} }
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
    """
    Convierte '$ 13,100.00' -> 13100.00
    Maneja formatos con puntos de miles y coma decimal o viceversa.
    """
    if not text:
        return None
    t = text.strip()
    # quitar símbolo $ y espacios
    t = t.replace("$", "").replace("ARS", "").replace("USD", "").strip()
    # normalizar separadores: si hay coma y punto, asumimos: miles=., decimal=,
    # si hay solo coma y dos decimales, reemplazar coma por punto
    # eliminar separadores de miles
    t = t.replace(".", "").replace(" ", "")
    # reemplazar coma por punto
    t = t.replace(",", ".")
    try:
        return float(t)
    except:
        return None


def parse_products_from_html(html: str) -> List[Dict]:
    """
    Parsea la grilla de productos basada en tu estructura:
    <div class="feature-slider-item swiper-slide">
      <article class="list-product"> ... </article>
    </div>
    """
    soup = BeautifulSoup(html, "html.parser")

    cards = []
    # A) Contenedores más específicos
    conts = soup.select("div.feature-slider-item.swiper-slide article.list-product")
    # B) Fallbacks (por si el sitio presenta otra clase similar)
    if not conts:
        conts = soup.select("article.list-product")

    for art in conts:
        # imagen (second-img preferida, luego first-img)
        img_el = art.select_one("img.second-img") or art.select_one("img.first-img")
        img_url = img_el["src"].strip() if img_el and img_el.has_attr("src") else None
        if img_url and img_url.startswith("/"):
            img_url = urljoin(BASE_URL, img_url)

        # nombre y url relativa -> absoluta
        name_el = art.select_one(".nombreProducto a.inner-link span")
        name = name_el.get_text(strip=True) if name_el else None

        link_el = art.select_one(".nombreProducto a.inner-link")
        href = link_el["href"].strip() if link_el and link_el.has_attr("href") else None
        prod_url = urljoin(BASE_URL, href) if href else None

        # precio visible
        price_el = art.select_one(".pricing-meta .current-price")
        price_text = price_el.get_text(strip=True) if price_el else None
        price = clean_price(price_text)

        # extras útiles del <form> (cuando existan)
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
# MAIN
# =========================
def main():
    driver = make_driver(headless=False)
    wait = WebDriverWait(driver, PAGE_WAIT)

    try:
        # 1) Login
        driver.get(LOGIN_URL)
        email_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='email'], input#email")))
        pass_input  = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='password'], input#password")))
        email_input.clear(); email_input.send_keys(EMAIL)
        pass_input.clear();  pass_input.send_keys(PASSWORD)

        # reCAPTCHA (si aplica)
        try:
            wait.until(lambda d: find_anchor_iframe(d) or find_bframe_iframe(d))
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

        # enviar form si no se envió
        for sel in ["button#send2", "button[name='send']", "button[type='submit']"]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, sel)
                driver.execute_script("arguments[0].scrollIntoView({behavior:'smooth',block:'center'});", btn)
                time.sleep(0.2)
                btn.click()
                break
            except Exception:
                continue

        # Confirmación login
        try:
            WebDriverWait(driver, 30).until(
                lambda d: "/login" not in d.current_url
            )
        except Exception:
            pass

        if "/login" in driver.current_url:
            print("⚠️ No se logró iniciar sesión. Revisa credenciales/recaptcha.")
            # seguimos igual para intentar la categoría, por si quedó autenticado
        else:
            print("🎉 Sesión iniciada.")

        # 2) Ir a la categoría
        print(f"➡️ Navegando a: {TARGET_URL}")
        go_to(driver, TARGET_URL, wait, expect_selector="article.list-product, .feature-slider-item.swiper-slide")

        # 3) Parsear productos de la página actual
        html = driver.page_source
        productos = parse_products_from_html(html)
        print(f"🛒 Productos encontrados en la página: {len(productos)}")

        # (Opcional) Si la categoría tiene paginación clásica, puedes iterar:
        # next_sel = "ul.pagination li a[rel='next'], a.next, a[aria-label='Siguiente']"
        # while True:
        #     try:
        #         next_link = driver.find_element(By.CSS_SELECTOR, next_sel)
        #         driver.execute_script("arguments[0].click();", next_link)
        #         wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        #         nuevos = parse_products_from_html(driver.page_source)
        #         print(f"  + {len(nuevos)} más")
        #         productos.extend(nuevos)
        #     except Exception:
        #         break

        # 4) Exportar a Excel
        df = pd.DataFrame(productos, columns=[
            "nombre", "url_producto", "imagen",
            "precio_visible", "precio_texto",
            "id_interno", "codigo_interno", "marca_tienda", "precio_hidden"
        ])
        # Si prefieres solo columnas clave:
        # df = df[["nombre", "url_producto", "imagen", "precio_visible"]]

        df.to_excel(OUT_XLSX, index=False)
        print(f"✅ Exportado a {OUT_XLSX} ({len(df)} filas)")

        # (Opcional) Check con requests usando cookies de la sesión
        s = export_cookies_to_requests(driver)
        r = s.get(TARGET_URL, timeout=30)
        print(f"GET {TARGET_URL} con cookies -> status: {r.status_code}")

    finally:
        # driver.quit()  # Descomenta si quieres cerrar al final
        print("Fin del script.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado por el usuario.")
        sys.exit(1)
