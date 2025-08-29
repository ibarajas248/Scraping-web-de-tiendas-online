#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains

# =========================
# Configuración del driver
# =========================
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_experimental_option("detach", True)  # NO cerrar al terminar
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 30)

driver.get("https://www.disco.com.ar/")

def _click_with_retry(xpath: str, retries: int = 3) -> None:
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

def _type_with_retry(xpath: str, text: str, retries: int = 3) -> None:
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

# =========================
# Paso 1: clic en "Mi Cuenta"
# =========================
_click_with_retry("//span[normalize-space()='Mi Cuenta']")
time.sleep(1)

# =========================
# Paso 2: clic en "Entrar con e-mail y contraseña"
# =========================
btn_login = "//span[normalize-space()='Entrar con e-mail y contraseña']/ancestor::button[1]"
btn_login_alt = "//button[.//span[contains(normalize-space(),'Entrar con e-mail')]]"
try:
    _click_with_retry(btn_login)
except Exception:
    _click_with_retry(btn_login_alt)

# =========================
# Paso 3: escribir el e-mail
# =========================
email_xpath = "//input[@placeholder='Ej. nombre@mail.com']"
email_alt = "//input[contains(@placeholder,'nombre@mail.com')]"
try:
    _type_with_retry(email_xpath, "comercial@factory-blue.com")
except Exception:
    _type_with_retry(email_alt, "comercial@factory-blue.com")

# =========================
# Paso 4: escribir la contraseña
# =========================
# (Usamos dos selectores por si el placeholder cambia; el de bullets requiere UTF-8)
pass_xpath = "//input[@type='password' and contains(@class,'vtex-styleguide-9-x-input')]"
pass_alt = "//input[@type='password' or contains(@placeholder,'●')]"

try:
    _type_with_retry(pass_xpath, "Compras2025")
except Exception:
    _type_with_retry(pass_alt, "Compras2025")

print("✅ Contraseña cargada")

# =========================
# IMPORTANTE: nunca cerrar
# =========================
input("Navegador abierto. Presiona ENTER aquí cuando QUIERAS cerrar manualmente...")
# No usamos driver.quit() ni driver.close()
