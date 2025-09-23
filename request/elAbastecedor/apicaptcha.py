#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import requests
from urllib.parse import urlparse, parse_qs
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# CONFIG
# =========================
LOGIN_URL = "https://elabastecedor.com.ar/login"

# Credenciales (puedes dejarlas por env var o hardcodear)
EMAIL = os.getenv("ELABASTECEDOR_EMAIL", "mauro@factory-blue.com")
PASSWORD = os.getenv("ELABASTECEDOR_PASSWORD", "Compras2025")

# CapSolver
# Recomendado: export CAPSOLVER_API_KEY="CAP-xxxx"
CAPSOLVER_API_KEY = os.getenv(
    "CAPSOLVER_API_KEY",
    # Tu clave (puedes dejarla literal si lo deseas):
    "CAP-D2D4BC1B86FD4F550ED83C329898264E02F0E2A7A81E1B079F64F7F11477C8FD"
)

CAPSOLVER_CREATE_URL = "https://api.capsolver.com/createTask"
CAPSOLVER_RESULT_URL = "https://api.capsolver.com/getTaskResult"

# Timeouts / reintentos
PAGE_WAIT = 60
CAPSOLVER_POLL_INTERVAL = 2.0     # segundos entre polls
CAPSOLVER_TIMEOUT = 180           # tiempo máx esperando resultado (s)
SUBMIT_WAIT_AFTER_TOKEN = 0.8     # pequeña espera tras inyectar token


# =========================
# Selenium Driver
# =========================
def make_driver(headless: bool = False) -> webdriver.Chrome:
    opts = Options()
    if headless:
        # Tip: reCAPTCHA a veces detecta headless. Si falla, usar visible.
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
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
    """Crea una tarea CapSolver para reCAPTCHA v2 (proxyless). Devuelve taskId."""
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
    """Polea hasta obtener el gRecaptchaResponse (token)."""
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
                solution = data.get("solution", {})
                token = solution.get("gRecaptchaResponse")
                if token:
                    return token
                # Para algunos casos, la key puede estar en 'text' o similar
                token = solution.get("text")
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
    # devolver el visible (si hay)
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
    """Busca el iframe anchor de reCAPTCHA y extrae el sitekey (param k de la URL)."""
    try:
        iframe = find_anchor_iframe(driver)
        if not iframe:
            # A veces solo está el bframe inicialmente
            iframe = find_bframe_iframe(driver)
        if not iframe:
            return None
        src = iframe.get_attribute("src") or ""
        return extract_sitekey_from_iframe_src(src)
    except Exception:
        return None

def is_enterprise_recaptcha(driver) -> bool:
    """Heurística simple; si la URL contiene enterprise o render=explicit enterprise."""
    try:
        iframe = find_anchor_iframe(driver) or find_bframe_iframe(driver)
        if not iframe:
            return False
        src = iframe.get_attribute("src") or ""
        return ("enterprise" in src.lower())
    except Exception:
        return False

def inject_recaptcha_token_and_trigger(driver, token: str) -> None:
    """
    Inyecta el token en #g-recaptcha-response (y posibles textareas clonadas) y dispara eventos.
    """
    js = r"""
    (function(token) {
        function setVal(el, val){
            if (!el) return;
            el.value = val;
            el.dispatchEvent(new Event('change', { bubbles: true }));
            el.dispatchEvent(new Event('input',  { bubbles: true }));
        }

        // 1) Estándar
        var main = document.getElementById('g-recaptcha-response');
        if (main) setVal(main, token);

        // 2) Invisible/enterprise puede crear textareas clon
        var taList = document.querySelectorAll("textarea[name='g-recaptcha-response'], textarea.g-recaptcha-response");
        for (var i=0; i<taList.length; i++){
            setVal(taList[i], token);
        }

        // 3) A veces hay callback registrado en window
        if (typeof window.onSubmit === 'function') {
            try { window.onSubmit(); } catch(e){}
        }

        // 4) Forzar que google reCAPTCHA lea el token
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
# MAIN
# =========================
def main():
    driver = make_driver(headless=False)  # mejor visible para evitar falsos positivos
    wait = WebDriverWait(driver, PAGE_WAIT)

    try:
        driver.get(LOGIN_URL)

        # Campos de login
        email_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='email'], input#email")))
        pass_input  = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='password'], input#password")))

        email_input.clear(); email_input.send_keys(EMAIL)
        pass_input.clear();  pass_input.send_keys(PASSWORD)

        # Detectar reCAPTCHA y sitekey
        # Si no aparece todavía, intenta esperar el iframe del anchor/bframe
        try:
            wait.until(lambda d: find_anchor_iframe(d) or find_bframe_iframe(d))
        except Exception:
            pass

        sitekey = detect_recaptcha_sitekey(driver)
        if not sitekey:
            print("⚠️ No se detectó sitekey automáticamente. Intentaré continuar sin resolver reCAPTCHA.")
        else:
            print(f"🔑 reCAPTCHA sitekey: {sitekey}")

            # Enterprise?
            enterprise = is_enterprise_recaptcha(driver)
            print(f"🏷️ Enterprise: {enterprise}")

            # Crear tarea en CapSolver
            if not CAPSOLVER_API_KEY or not CAPSOLVER_API_KEY.startswith("CAP-"):
                print("❌ CAPSOLVER_API_KEY no configurada o inválida.")
                sys.exit(2)

            print("🧩 Creando tarea en CapSolver…")
            task_id = caps_create_task(CAPSOLVER_API_KEY, LOGIN_URL, sitekey, is_enterprise=enterprise)
            if not task_id:
                print("❌ No se pudo crear la tarea de CapSolver.")
                sys.exit(2)

            print(f"⏳ Polling resultado (taskId={task_id})…")
            token = caps_poll_result(CAPSOLVER_API_KEY, task_id, timeout_sec=CAPSOLVER_TIMEOUT)
            if not token:
                print("❌ No se obtuvo token del reCAPTCHA.")
                sys.exit(2)

            print("✅ Token recibido. Inyectando en la página…")
            inject_recaptcha_token_and_trigger(driver, token)
            time.sleep(SUBMIT_WAIT_AFTER_TOKEN)

        # Clic en el botón Acceder (si existe). Muchos formularios se envían solos al tener el token.
        # Ajusta selectores del botón según el sitio.
        submit_btn = None
        for sel in ["button#send2", "button[name='send']", "button[type='submit']"]:
            try:
                submit_btn = driver.find_element(By.CSS_SELECTOR, sel)
                break
            except Exception:
                continue

        if submit_btn:
            driver.execute_script("arguments[0].scrollIntoView({behavior:'smooth',block:'center'});", submit_btn)
            time.sleep(0.2)
            submit_btn.click()

        # Esperar a que cambie la URL o desaparezca el form
        try:
            WebDriverWait(driver, 30).until(
                lambda d: ("/login" not in d.current_url) or
                          (len(d.find_elements(By.CSS_SELECTOR, "form[action*='login' i], form[action*='login-acciones' i]")) == 0)
            )
        except Exception:
            pass

        print(f"URL tras login: {driver.current_url}")

        if "/login" in driver.current_url:
            print("⚠️ Parece que no se inició sesión. Verifica credenciales o si el sitio requiere otra validación.")
        else:
            print("🎉 Sesión iniciada (aparentemente).")

            # (Opcional) Continuar con scraping autenticado usando requests + cookies
            s = export_cookies_to_requests(driver)
            r = s.get("https://elabastecedor.com.ar/", timeout=30)
            print(f"GET / -> status: {r.status_code}")

        print("Listo.")

    finally:
        # driver.quit()  # Descomenta si quieres cerrar el navegador al terminar
        print("Fin del script.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado por el usuario.")
        sys.exit(1)
