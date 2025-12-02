#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import os
import sys

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import requests


LOGIN_URL = "https://elabastecedor.com.ar/login"

# Variables de entorno opcionales; si no existen, usa estos valores
EMAIL = os.getenv("ELABASTECEDOR_EMAIL", "mauro@factory-blue.com")
PASSWORD = os.getenv("ELABASTECEDOR_PASSWORD", "Compras2025")


def make_driver(headless=True):
    opts = Options()
    if headless:
        # reCAPTCHA bloquea a menudo headless; mejor visible
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)


def export_cookies_to_requests(driver) -> requests.Session:
    """Copia las cookies del navegador a un requests.Session para seguir autenticado."""
    s = requests.Session()
    for c in driver.get_cookies():
        s.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"), path=c.get("path", "/"))
    return s


def find_anchor_iframe(driver):
    """Iframe del checkbox (anchor)."""
    frames = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='/recaptcha/api2/anchor']")
    return frames[0] if frames else None


def find_challenge_iframe(driver):
    """Iframe del desafío (bframe), si está visible."""
    frames = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='/recaptcha/api2/bframe']")
    for f in frames:
        try:
            if f.is_displayed():
                return f
        except Exception:
            pass
    return None


def highlight_and_wait_recaptcha(driver, xpath="//*[@id='rc-anchor-container']/div[3]/div[1]", max_wait_sec=300):
    """
    NO hace clic al reCAPTCHA.
    - Enfoca y resalta el elemento indicado por `xpath` dentro del iframe anchor.
    - Mueve el mouse encima (sin clicar).
    - Espera hasta que:
        a) el checkbox quede marcado (aria-checked=true), o
        b) exista token en #g-recaptcha-response
    - También detecta si se abre el iframe de desafío (bframe) y espera a que lo resuelvas.
    """
    wait = WebDriverWait(driver, 60)

    # Desplaza hasta la zona del captcha (textarea en DOM principal)
    try:
        recaptcha_textarea = driver.find_element(By.CSS_SELECTOR, "textarea#g-recaptcha-response")
        driver.execute_script("arguments[0].scrollIntoView({behavior:'smooth',block:'center'});", recaptcha_textarea)
    except Exception:
        pass

    # Cambia al iframe del anchor
    anchor_iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='/recaptcha/api2/anchor']")))
    driver.switch_to.frame(anchor_iframe)

    # Localiza el elemento por tu XPath y lo resalta
    target = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
    driver.execute_script("arguments[0].style.outline='3px solid red'; arguments[0].style.outlineOffset='2px';", target)

    # Mueve el mouse encima (sin click)
    try:
        # Mueve el mouse encima y haz click
        ActionChains(driver) \
            .move_to_element(target) \
            .pause(0.15) \
            .click(target) \
            .perform()
    except Exception:
        pass

    # También intentamos resaltar el anchor para leer aria-checked
    try:
        anchor = driver.find_element(By.CSS_SELECTOR, "span#recaptcha-anchor")
        driver.execute_script("arguments[0].style.outline='3px solid red';", anchor)
    except Exception:
        anchor = None

    driver.switch_to.default_content()

    print("\n👉 Marca manualmente el reCAPTCHA (el área está resaltada en rojo). "
          "Si aparece un desafío de imágenes, resuélvelo. Esperaré hasta 5 minutos.\n")

    deadline = time.time() + max_wait_sec
    while time.time() < deadline:
        # ¿Token listo en DOM principal?
        token = driver.execute_script(
            "var el=document.getElementById('g-recaptcha-response');"
            "return (el && el.value) ? el.value : '';"
        )
        if token and len(token) > 0:
            print("✅ Detectado token en g-recaptcha-response.")
            return True

        # ¿Checkbox marcado (aria-checked)?
        anchor_iframe = find_anchor_iframe(driver)
        if anchor_iframe:
            try:
                driver.switch_to.frame(anchor_iframe)
                try:
                    anchor_el = anchor or driver.find_element(By.CSS_SELECTOR, "span#recaptcha-anchor")
                    if anchor_el.get_attribute("aria-checked") == "true":
                        driver.switch_to.default_content()
                        print("✅ Checkbox marcado (aria-checked=true).")
                        return True
                except Exception:
                    pass
            finally:
                driver.switch_to.default_content()

        # ¿Desafío visible?
        if find_challenge_iframe(driver):
            print("… Desafío visible. Resuélvelo por favor.")
            time.sleep(1.0)
        else:
            time.sleep(0.5)

    raise TimeoutError("El reCAPTCHA no fue resuelto dentro del tiempo límite.")


def main():
    driver = make_driver(headless=False)  # ventana visible por el reCAPTCHA
    wait = WebDriverWait(driver, 60)

    try:
        driver.get(LOGIN_URL)

        # Espera campos de email y password
        email_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='email']")))
        pass_input  = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='password']")))

        # Completa credenciales
        email_input.clear(); email_input.send_keys(EMAIL)
        pass_input.clear();  pass_input.send_keys(PASSWORD)

        # Resaltar y esperar al reCAPTCHA (con tu XPath)
        highlight_and_wait_recaptcha(driver, xpath="//*[@id='rc-anchor-container']/div[3]/div[1]", max_wait_sec=300)

        # Click en el botón Acceder (esto sí es válido)
        try:
            submit_btn = driver.find_element(By.CSS_SELECTOR, "button#send2")
        except Exception:
            submit_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[name='send']")))
        driver.execute_script("arguments[0].scrollIntoView({behavior:'smooth',block:'center'});", submit_btn)
        time.sleep(0.3)
        submit_btn.click()

        # Espera a que cambie la URL o desaparezca el formulario
        WebDriverWait(driver, 30).until(
            lambda d: "/login" not in d.current_url or
            len(d.find_elements(By.CSS_SELECTOR, "form[action*='login-acciones.php']")) == 0
        )

        print(f"URL tras login: {driver.current_url}")

        if "/login" in driver.current_url:
            print("⚠️ Parece que no se inició sesión. ¿Credenciales correctas? ¿reCAPTCHA vencido?")
        else:
            print("🎉 Sesión iniciada (aparentemente).")

            # (Opcional) Exporta cookies a requests para scraping autenticado
            s = export_cookies_to_requests(driver)
            r = s.get("https://elabastecedor.com.ar/", timeout=30)
            print(f"GET / -> status: {r.status_code}")

        print("Listo.")
    finally:
        #driver.quit()
        print("Listo.")



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado por el usuario.")
        sys.exit(1)
