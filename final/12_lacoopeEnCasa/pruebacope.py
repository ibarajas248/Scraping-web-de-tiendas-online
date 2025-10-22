#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, sys, time, math, traceback
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    StaleElementReferenceException,
    NoSuchElementException,
    JavascriptException,
)

# =========================
# Configuración
# =========================
BASE_LISTING = "https://www.lacoopeencasa.coop/listado/categoria/almacen/2/pagina--{page}"
WAIT_SEC = 20
PAUSE_BETWEEN = 0.2  # pequeñas pausas para estabilidad

# =========================
# Utilidades
# =========================
def setup_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        # motor moderno de headless
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1366,900")
    opts.add_argument("--lang=es-AR")
    # ayuda contra bloqueos por automation flags
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Si prefieres un chromedriver específico, usa Service(executable_path="...")
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """
    })
    return driver


def _text(el) -> str:
    try:
        return el.text.strip()
    except Exception:
        return ""


def safe_find(driver, by, sel, many=False, timeout=WAIT_SEC):
    try:
        if many:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((by, sel))
            )
            return driver.find_elements(by, sel)
        else:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, sel))
            )
            return driver.find_element(by, sel)
    except TimeoutException:
        return [] if many else None


def open_in_new_tab(driver: webdriver.Chrome, url: str):
    driver.execute_script("window.open(arguments[0], '_blank');", url)
    time.sleep(PAUSE_BETWEEN)
    driver.switch_to.window(driver.window_handles[-1])


def close_current_tab_and_back(driver: webdriver.Chrome):
    if len(driver.window_handles) > 1:
        driver.close()
        driver.switch_to.window(driver.window_handles[0])


def extract_detail(driver: webdriver.Chrome) -> Dict[str, Any]:
    """
    En la página de detalle, captura los campos clave.
    Selectores basados en los HTML que enviaste.
    """
    data = {
        "codigo_interno": "",
        "nombre": "",
        "marca": "",
        "categoria": "",
        "precio": "",
        "precio_unitario": "",
        "precio_sin_impuestos": "",
        "url_detalle": driver.current_url,
        "url_imagen": "",
    }

    # Título
    h1 = safe_find(driver, By.CSS_SELECTOR, "h1.articulo-detalle-titulo")
    if h1:
        data["nombre"] = _text(h1)

    # Marca y Categoría (h2 dentro de links)
    try:
        marca_h2 = driver.find_elements(By.CSS_SELECTOR, ".articulo-detalle-marca h2")
        if marca_h2:
            # Hay 2 spans .articulo-detalle-marca (Marca y Categoría) con sus h2
            if len(marca_h2) >= 1:
                data["marca"] = marca_h2[0].text.strip()
            if len(marca_h2) >= 2:
                data["categoria"] = marca_h2[1].text.strip()
    except Exception:
        pass

    # Precios
    precio = safe_find(driver, By.CSS_SELECTOR, ".precios .precio-detalle")
    if precio:
        data["precio"] = _text(precio)

    precio_unit = safe_find(driver, By.CSS_SELECTOR, ".precio-unitario")
    if precio_unit:
        data["precio_unitario"] = _text(precio_unit)

    # "Precio sin impuestos nacionales: " -> siguiente span
    try:
        psi_label = driver.find_elements(By.XPATH, "//div[contains(@class,'precio-sin-impuestos')]//span")
        # suele venir como: [ 'Precio sin...', '$1.818,18' ]
        if len(psi_label) >= 2:
            data["precio_sin_impuestos"] = psi_label[1].text.strip()
    except Exception:
        pass

    # Código interno
    try:
        codigo_span = driver.find_element(By.CSS_SELECTOR, ".articulo-codigo span")
        data["codigo_interno"] = codigo_span.text.strip()
    except Exception:
        pass

    # Imagen principal
    img = safe_find(driver, By.CSS_SELECTOR, "img.articulo-detalle-imagen-ppal")
    if img:
        try:
            data["url_imagen"] = img.get_attribute("src") or ""
        except Exception:
            pass

    return data


def click_card_and_scrape(driver: webdriver.Chrome, card) -> Dict[str, Any]:
    """
    Abre el detalle de una card: si hay <a href>, la abre en nueva pestaña.
    Si no, hace click JS sobre la card y luego back().
    """
    # intenta encontrar un link con href
    link = None
    try:
        # cualquier <a href> dentro de la card
        candidates = card.find_elements(By.CSS_SELECTOR, "a[href]")
        if candidates:
            link = candidates[0].get_attribute("href")
    except Exception:
        link = None

    if link:
        # abrir en nueva pestaña
        open_in_new_tab(driver, link)
        # esperar que cargue detalle
        safe_find(driver, By.CSS_SELECTOR, "h1.articulo-detalle-titulo", timeout=WAIT_SEC)
        data = extract_detail(driver)
        close_current_tab_and_back(driver)
        return data
    else:
        # click directo en card (Angular Router)
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
        time.sleep(PAUSE_BETWEEN)
        driver.execute_script("arguments[0].click();", card)
        # esperar que cargue detalle
        safe_find(driver, By.CSS_SELECTOR, "h1.articulo-detalle-titulo", timeout=WAIT_SEC)
        data = extract_detail(driver)
        # volver al listado
        driver.back()
        # esperar a que reaparezca el grid/listado
        safe_find(driver, By.CSS_SELECTOR, "col-listado-articulo .card, .col.s6.m4.l4.xl3 .card", timeout=WAIT_SEC)
        return data


def scrape_listing_page(driver: webdriver.Chrome, page_url: str, page_num: int) -> List[Dict[str, Any]]:
    driver.get(page_url)
    # esperar cards
    cards = safe_find(
        driver,
        By.CSS_SELECTOR,
        "col-listado-articulo .card, .col.s6.m4.l4.xl3 .card",
        many=True,
        timeout=WAIT_SEC
    )
    if not cards:
        return []

    results: List[Dict[str, Any]] = []
    total_cards = len(cards)
    print(f"🗂  Página {page_num}: {total_cards} cards detectadas")

    i = 0
    while i < total_cards:
        # re-localizar cada vez para evitar stale
        cards = driver.find_elements(By.CSS_SELECTOR, "col-listado-articulo .card, .col.s6.m4.l4.xl3 .card")
        if i >= len(cards):
            break
        card = cards[i]

        # robustez: si la card no está en viewport o es stale, reintenta
        for attempt in range(3):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                time.sleep(PAUSE_BETWEEN)
                data = click_card_and_scrape(driver, card)
                data["pagina"] = page_num
                data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                results.append(data)
                print(f"  ✓ [{i+1}/{total_cards}] {data.get('nombre','(sin nombre)')}")
                break
            except (StaleElementReferenceException, TimeoutException, JavascriptException) as e:
                if attempt == 2:
                    print(f"  ⚠️  Saltando card {i+1}: {type(e).__name__}")
                else:
                    time.sleep(0.5)
                    # reubica card para nuevo intento
                    cards = driver.find_elements(By.CSS_SELECTOR, "col-listado-articulo .card, .col.s6.m4.l4.xl3 .card")
                    if i < len(cards):
                        card = cards[i]
                    continue
            except Exception as e:
                print(f"  ❌ Error en card {i+1}: {e}")
                # no detener el scraping por una card
                break

        i += 1

    return results


def run(start: int, max_pages: int, out_xlsx: str, headless: bool = True, stop_on_empty: bool = True):
    driver = setup_driver(headless=headless)
    all_rows: List[Dict[str, Any]] = []
    empty_streak = 0
    try:
        for p in range(start, start + max_pages):
            url = BASE_LISTING.format(page=p)
            print(f"\n➡️  Navegando: {url}")
            rows = scrape_listing_page(driver, url, p)
            if not rows:
                print("  (sin items)")
                empty_streak += 1
                if stop_on_empty and empty_streak >= 1:
                    print("  No hay más productos. Fin.")
                    break
            else:
                empty_streak = 0
                all_rows.extend(rows)
                print(f"  ✔ Página {p} → {len(rows)} productos acumulados: {len(all_rows)}")
            time.sleep(0.3)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    if not all_rows:
        print("No se encontraron productos. Saliendo.")
        return

    df = pd.DataFrame(all_rows)
    # Orden sugerido de columnas
    cols = [
        "codigo_interno", "nombre", "marca", "categoria",
        "precio", "precio_unitario", "precio_sin_impuestos",
        "url_detalle", "url_imagen", "pagina", "timestamp"
    ]
    df = df[[c for c in cols if c in df.columns]]

    # Exportar
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_xlsx or f"Lacoope_Almacen_{ts}.xlsx"
    df.to_excel(out_file, index=False)
    print(f"\n📦 Exportado: {out_file}")
    print(f"Total filas: {len(df)}")


def parse_args():
    ap = argparse.ArgumentParser(description="Scraper La Coope (Almacén) → XLSX")
    ap.add_argument("--start", type=int, default=1, help="Página inicial (default: 1)")
    ap.add_argument("--max-pages", type=int, default=50, help="Cantidad máxima de páginas a intentar (default: 50)")
    ap.add_argument("--out", type=str, default="", help="Ruta de salida XLSX (default: Lacoope_Almacen_YYYYMMDD.xlsx)")
    ap.add_argument("--no-headless", action="store_true", help="Desactivar modo headless")
    ap.add_argument("--keep-going", action="store_true",
                    help="No cortar al encontrar una página vacía (sigue hasta max-pages)")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        start=args.start,
        max_pages=args.max_pages,
        out_xlsx=args.out,
        headless=not args.no_headless,
        stop_on_empty=not args.keep_going
    )
