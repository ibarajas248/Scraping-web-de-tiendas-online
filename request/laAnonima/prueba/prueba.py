#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

URL = "https://www.laanonima.com.ar/almacen/n1_512/"

def accept_cookies_if_any(driver, sec=8):
    sels = [
        (By.CSS_SELECTOR, "button#onetrust-accept-btn-handler"),
        (By.XPATH, "//button[contains(., 'Aceptar')]"),
        (By.XPATH, "//button[contains(., 'ACEPTAR')]"),
        (By.XPATH, "//button[contains(., 'Entendido')]"),
    ]
    end = time.time() + sec
    for by, sel in sels:
        try:
            btn = WebDriverWait(driver, max(1, int(end - time.time()))).until(
                EC.element_to_be_clickable((by, sel))
            )
            btn.click()
            time.sleep(0.5)
            return
        except Exception:
            pass

def gentle_scroll(driver, steps=8, pause=0.5):
    last = 0
    for _ in range(steps):
        driver.execute_script("window.scrollBy(0, 1200);")
        time.sleep(pause)
        h = driver.execute_script("return document.body.scrollHeight")
        if h == last:
            break
        last = h

def main():
    opts = Options()
    # Nos conectamos al Chrome ya abierto en 9222
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    # importante: no pongas headless
    driver = webdriver.Chrome(options=opts)

    try:
        driver.get(URL)

        # DOM listo
        WebDriverWait(driver, 40).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        accept_cookies_if_any(driver)
        gentle_scroll(driver, steps=10, pause=0.6)

        # Cualquiera de estos dos selectores debería ver productos
        any_selector = (
            "div.producto-item, div[id-codigo-producto]"
        )
        WebDriverWait(driver, 25).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, any_selector))
        )

        items = driver.find_elements(By.CSS_SELECTOR, any_selector)
        print(f"✅ Productos visibles: {len(items)}")
        time.sleep(5)  # para verlos

    finally:
        # No cierres el Chrome real; solo cierra el control Selenium
        driver.quit()

if __name__ == "__main__":
    main()
