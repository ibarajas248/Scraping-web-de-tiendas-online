#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import re
import shutil
import tempfile
from urllib.parse import urlparse, parse_qs

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

# =========================
# CONFIG DEL SITIO / RUTAS
# =========================
BASE_URL   = "https://elabastecedor.com.ar/"
LOGIN_URL  = "https://elabastecedor.com.ar/login"

ARCHIVO_IN  = "abastecedor_aux_viernes.xlsx"
ARCHIVO_OUT = "abastecedor_con_precios.xlsx"
HOJA        = 0  # o "Hoja1"

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
# Limpieza de precio
# =========================
def clean_price(text: str) -> Optional[float]:
    """
    Acepta cosas tipo:
    'De $ 3 . 199 , 00 Para $ 2 . 999 , 00'
    '$ 2.999,00'
    '$2999'
    y devuelve un float (primer precio encontrado).
    """
    if not text:
        return None

    # nos quedamos con el primer fragmento que tenga '$'
    # o sino con todo el texto
    s = text.replace("\xa0", " ")
    # buscar el primer "$ ..."
    m = re.search(r"\$\s*([0-9\.\,\s]+)", s)
    if m:
        s = m.group(1)
    else:
        # no encontró $, usamos todo
        s = text

    s = s.strip()
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return None

    # lógica similar a la que ya usabas
    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        frac = s.split(",")[-1]
        if len(frac) in (2, 3):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "." in s:
        parts = s.split(".")
        if len(parts) > 2 or len(parts[-1]) not in (2, 3):
            s = s.replace(".", "")

    try:
        return round(float(s), 2)
    except Exception:
        return None

# =========================
# Extraer precio de una página
# =========================
def extract_precio_lista_from_page(html: str) -> Optional[float]:
    """
    Busca PRECIO_LISTA en el HTML de la página de producto/categoría:
    prueba varias heurísticas de selectores de precio.
    """
    soup = BeautifulSoup(html, "html.parser")

    # candidatos de selectores; puedes ajustar si ves el HTML real del PDP
    selectors = [
        ".pricing-meta .current-price",
        ".product-item-details .price",
        ".product-info-main .price-box .price",
        ".product-info-price .price-final_price .price",
        ".precio",          # genérico en español
        ".price",           # genérico
        "[class*='price']"  # fallback general
    ]

    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if txt:
                val = clean_price(txt)
                if val is not None:
                    # print(f"DEBUG selector {sel} -> {txt} -> {val}")
                    return val

    # Fallback: buscar cualquier texto con "$"
    text = soup.get_text(" ", strip=True)
    m = re.search(r"\$\s*[0-9\.\,\s]+", text)
    if m:
        return clean_price(m.group(0))

    return None

# =========================
# Login
# =========================
def do_login(driver: webdriver.Chrome, wait: WebDriverWait):
    driver.get(LOGIN_URL)

    # esperamos a que haya al menos un formulario o algún input
    wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "form, input[type='email'], input[type='text']")
        )
    )

    # --- localizar input de email de forma tolerante ---
    email_input = None
    posibles_email = [
        "input[name='email']",
        "input#email",
        "input[type='email']",
        "input[name*='user']",
        "input[name*='login']",
        "input[name*='correo']",
        "input[name*='mail']",
    ]
    for sel in posibles_email:
        try:
            email_input = driver.find_element(By.CSS_SELECTOR, sel)
            if email_input.is_displayed():
                print(f"[LOGIN] email_input encontrado con selector: {sel}")
                break
        except Exception:
            continue

    if not email_input:
        # último recurso: primer input de texto visible
        try:
            email_input = driver.find_element(By.CSS_SELECTOR, "input[type='text']")
            print("[LOGIN] email_input usando fallback input[type='text']")
        except Exception:
            raise RuntimeError("No pude encontrar el campo de email en la página de login.")

    # --- localizar input de password de forma tolerante ---
    pass_input = None
    posibles_pass = [
        "input[name='password']",
        "input#password",
        "input[type='password']",
        "input[name*='pass']",
        "input[name*='clave']",
        "input[name*='contrasena']",
    ]
    for sel in posibles_pass:
        try:
            pass_input = driver.find_element(By.CSS_SELECTOR, sel)
            if pass_input.is_displayed():
                print(f"[LOGIN] pass_input encontrado con selector: {sel}")
                break
        except Exception:
            continue

    if not pass_input:
        raise RuntimeError("No pude encontrar el campo de contraseña en la página de login.")

    # rellenar credenciales
    email_input.clear()
    email_input.send_keys(EMAIL)
    pass_input.clear()
    pass_input.send_keys(PASSWORD)

    # detectar posible recaptcha
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
    for sel in [
        "button#send2",
        "button[name='send']",
        "button[type='submit']",
        "button[class*='login']",
        "button[title*='Ingresar']",
        "button[title*='Entrar']",
    ]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            if not btn.is_displayed():
                continue
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior:'instant',block:'center'});", btn
            )
            time.sleep(0.1)
            btn.click()
            print(f"[LOGIN] Click en botón con selector: {sel}")
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



# =========================
# MAIN
# =========================
def main():
    # 1) Cargar Excel
    df = pd.read_excel(ARCHIVO_IN, sheet_name=HOJA)

    if "URLs" not in df.columns:
        raise Exception("La columna 'URLs' no existe en el Excel.")

    if "PRECIO_LISTA" not in df.columns:
        df["PRECIO_LISTA"] = None

    # 2) Preparar driver y login
    driver, profile_dir = make_driver(headless=True)
    wait = WebDriverWait(driver, PAGE_WAIT)

    try:
        do_login(driver, wait)

        total = len(df)
        for i, url in enumerate(df["URLs"]):
            print(f"[{i+1}/{total}] URL: {url}")

            if not isinstance(url, str) or not url.strip():
                print("   → URL vacía, PRECIO_LISTA = None")
                df.at[i, "PRECIO_LISTA"] = None
                continue

            try:
                driver.get(url)
                # aseguramos que cargó
                wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
                time.sleep(1.0)  # un plus por si tarda en pintar precios

                html = driver.page_source
                precio_lista = extract_precio_lista_from_page(html)
                print(f"   → PRECIO_LISTA: {precio_lista}")
                df.at[i, "PRECIO_LISTA"] = precio_lista

            except Exception as e:
                print(f"   [ERROR] {e}")
                df.at[i, "PRECIO_LISTA"] = None

            time.sleep(0.5)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception:
            pass

    # 3) Guardar Excel de salida
    df.to_excel(ARCHIVO_OUT, index=False)
    print(f"\n✔ Listo. Archivo guardado como: {ARCHIVO_OUT}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado por el usuario.")
        sys.exit(1)

