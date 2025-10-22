#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time, re
import pandas as pd

HOME_URL = "https://www.lacoopeencasa.coop/"
BASE_CAT  = "https://www.lacoopeencasa.coop/listado/categoria/almacen/2"
PAGE_FMT  = BASE_CAT + "/pagina--{page}"   # page >= 2
MAX_PAGES = 300                             # límite de seguridad

# ---------------- Utilidades seguras ----------------

def s_find(el, by, sel):
    try:
        return el.find_element(by, sel)
    except Exception:
        return None

def s_attr(el, name):
    try:
        return el.get_attribute(name)
    except Exception:
        return None

def inner_text(driver, el):
    """Lee texto real pintado por Angular (innerText/textContent)."""
    try:
        return (driver.execute_script(
            "return (arguments[0].innerText || arguments[0].textContent || '').trim();", el
        ) or "").strip()
    except Exception:
        return ""

def limpiar_precio_a_float(entero_txt, decimal_txt):
    """
    Convierte partes de precio (entero y decimal) a float.
    entero_txt: e.g. '2.200 ' ; decimal_txt: e.g. ' 00 '
    """
    if entero_txt is None:
        return None
    e = re.sub(r"[^\d\.]", "", (entero_txt or ""))
    d = re.sub(r"[^\d]", "", (decimal_txt or ""))
    if not e:
        return None
    if not d:
        d = "00"
    e_sin_miles = e.replace(".", "")
    try:
        return float(f"{e_sin_miles}.{d}")
    except Exception:
        return None

def scroll_hasta_cargar_todo(driver, pausa=0.8, max_intentos_sin_cambio=4):
    """
    Hace scroll hasta fondo repetidamente hasta que no aumente el número de tarjetas
    durante 'max_intentos_sin_cambio' intentos.
    """
    last_count = -1
    estancados = 0
    while True:
        cards = driver.find_elements(By.CSS_SELECTOR, "col-listado-articulo div.card.hoverable")
        count = len(cards)
        if count == last_count:
            estancados += 1
        else:
            estancados = 0
        if estancados >= max_intentos_sin_cambio:
            break
        last_count = count
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pausa)

def get_nombre_desde_card(driver, card):
    """
    Intenta en orden:
    1) .card-descripcion[id^='descripcion'] p -> innerText
    2) alt/title de la imagen (corta ' - $' si viene precio junto)
    3) atributo data-nombre del <a>
    """
    # 1) principal
    nombre_el = s_find(card, By.CSS_SELECTOR, ".card-descripcion[id^='descripcion'] p")
    nombre = inner_text(driver, nombre_el) if nombre_el else ""

    # 2) alt/title
    if not nombre:
        img = s_find(card, By.CSS_SELECTOR, ".card-image img")
        alt = s_attr(img, "alt") if img else ""
        title = s_attr(img, "title") if img else ""
        candidato = (alt or title or "").strip()
        if candidato:
            nombre = re.split(r"\s+-\s*\$", candidato, maxsplit=1)[0].strip()

    # 3) data-nombre
    if not nombre:
        a_det = s_find(card, By.CSS_SELECTOR, "a[data-nombre]")
        if a_det:
            nombre = (s_attr(a_det, "data-nombre") or "").strip()

    return nombre

def extraer_tarjetas(driver):
    """
    Devuelve lista de dicts con:
    codigo, nombre, precio, precio_texto, precio_unitario, precio_sin_impuestos, imagen_url, detalle_url
    """
    cards = driver.find_elements(By.CSS_SELECTOR, "col-listado-articulo div.card.hoverable")
    filas = []
    for c in cards:
        # Código desde ids como imagen208826 / descripcion208826
        imagen_div = s_find(c, By.CSS_SELECTOR, "[id^='imagen']")
        desc_div   = s_find(c, By.CSS_SELECTOR, "[id^='descripcion']")
        codigo = None
        for divpos in (imagen_div, desc_div):
            if divpos:
                m = re.search(r"(\d+)$", s_attr(divpos, "id") or "")
                if m:
                    codigo = m.group(1)
                    break

        # Imagen
        img = s_find(c, By.CSS_SELECTOR, ".card-image img")
        imagen_url = None
        if img:
            imagen_url = s_attr(img, "src") or s_attr(img, "data-src") or s_attr(img, "data-lazy")

        # Nombre (robusto)
        nombre = get_nombre_desde_card(driver, c)

        # Precio: entero + decimal
        entero_el  = s_find(c, By.CSS_SELECTOR, ".precio-listado .precio-entero")
        decimal_el = s_find(c, By.CSS_SELECTOR, ".precio-listado .precio-complemento .precio-decimal")
        entero_txt  = inner_text(driver, entero_el) if entero_el else ""
        decimal_txt = inner_text(driver, decimal_el) if decimal_el else ""

        precio_float = limpiar_precio_a_float(entero_txt, decimal_txt)

        # Precio textual tal como se ve (reconstruido)
        precio_texto = ""
        if entero_txt:
            dec_t = decimal_txt if decimal_txt else "00"
            precio_texto = f"${entero_txt.strip()}{dec_t.strip()}".replace("  ", " ")

        # Precio unitario
        unit_el = s_find(c, By.CSS_SELECTOR, ".precio-unitario")
        precio_unitario = inner_text(driver, unit_el) if unit_el else ""

        # Precio sin impuestos (segundo span)
        psi_el = s_find(c, By.CSS_SELECTOR, ".precio-sin-impuestos span:last-child")
        precio_sin_impuestos = inner_text(driver, psi_el) if psi_el else ""

        # Link al detalle (si existe)
        a_det = s_find(c, By.CSS_SELECTOR, "a[href]")
        detalle_url = None
        if a_det:
            href = (s_attr(a_det, "href") or "").strip()
            if href.startswith("/"):
                detalle_url = "https://www.lacoopeencasa.coop" + href
            elif href.startswith("http"):
                detalle_url = href

        filas.append({
            "codigo": codigo,
            "nombre": nombre,
            "precio": precio_float,
            "precio_texto": precio_texto,
            "precio_unitario": precio_unitario,
            "precio_sin_impuestos": precio_sin_impuestos,
            "imagen_url": imagen_url,
            "detalle_url": detalle_url,
        })
    return filas

def cargar_y_extraer_pagina(driver, url, wait_cards=True):
    """
    Carga una URL de categoría/página, espera tarjetas, hace scroll y devuelve filas.
    Retorna (filas, count_cards).
    """
    driver.get(url)
    WebDriverWait(driver, 30).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

    if wait_cards:
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "col-listado-articulo div.card.hoverable"))
            )
        except Exception:
            # No hay tarjetas en esta página
            return [], 0

    # Además, esperar a que aparezca algún texto real en descripciones (si existen)
    descs = driver.find_elements(By.CSS_SELECTOR, ".card-descripcion[id^='descripcion'] p")
    if descs:
        try:
            WebDriverWait(driver, 10).until(
                lambda d: any(inner_text(d, el) for el in d.find_elements(By.CSS_SELECTOR, ".card-descripcion[id^='descripcion'] p"))
            )
        except Exception:
            pass  # si no aparece, igual seguimos

    # Scroll para cargar todas las tarjetas lazy
    scroll_hasta_cargar_todo(driver, pausa=0.8, max_intentos_sin_cambio=4)

    # Extraer
    filas = extraer_tarjetas(driver)
    return filas, len(filas)

# ---------------- Main ----------------

def main():
    # Configuración del navegador (sin headless)
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

    try:
        print("Abriendo página principal…")
        driver.get(HOME_URL)
        WebDriverWait(driver, 30).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(1.2)

        todas = []

        # Página 1 (sin sufijo)
        print("➡️ Página 1 (base):", BASE_CAT)
        filas, n = cargar_y_extraer_pagina(driver, BASE_CAT, wait_cards=True)
        print(f"   • items: {n}")
        todas.extend(filas)

        # Páginas 2..MAX_PAGES
        for page in range(2, MAX_PAGES + 1):
            url = PAGE_FMT.format(page=page)
            print(f"➡️ Página {page}: {url}")
            filas, n = cargar_y_extraer_pagina(driver, url, wait_cards=False)
            print(f"   • items: {n}")
            if n == 0:
                print("   ⛳ No hay más productos. Fin de paginación.")
                break
            todas.extend(filas)

        print(f"🛒 Total artículos capturados: {len(todas)}")

        # A XLSX
        df = pd.DataFrame(todas)
        cols = ["codigo", "nombre", "precio", "precio_texto",
                "precio_unitario", "precio_sin_impuestos",
                "imagen_url", "detalle_url"]
        df = df[[c for c in cols if c in df.columns]]

        out_xlsx = "la_coope_almacen_full.xlsx"
        df.to_excel(out_xlsx, index=False)
        print(f"✅ XLSX guardado: {out_xlsx}")

        input("Presiona ENTER para cerrar el navegador…")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
