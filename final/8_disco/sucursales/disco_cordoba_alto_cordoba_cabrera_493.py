#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import unicodedata
from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains

BASE = "https://www.disco.com.ar"
TIENDA = "Disco Alta Córdoba Cabrera 493"
PROVINCIA = "CORDOBA"
OUT_XLSX = f"disco_almacen_{TIENDA.replace(' ','_').replace('ó','o').replace('Ó','O')}.xlsx"

# =========================
# Configuración del driver (NO cerrar nunca)
# =========================
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_experimental_option("detach", True)  # deja la ventana viva al terminar el script
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 30)

driver.get(f"{BASE}/")

# =========================
# Helpers robustos
# =========================
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

def _normalize(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s or '') if not unicodedata.combining(c)).strip().lower()

def _select_by_text_case_insensitive(select_xpath: str, target_text: str, retries: int = 3) -> None:
    last_exc = None
    tgt = _normalize(target_text)
    for _ in range(retries):
        try:
            sel = wait.until(EC.presence_of_element_located((By.XPATH, select_xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sel)
            wait.until(lambda d: len(sel.find_elements(By.TAG_NAME, "option")) > 1)

            # 1) exacto
            try:
                Select(sel).select_by_visible_text(target_text)
                return
            except Exception:
                pass
            # 2) insensible (texto)
            value = None
            for o in sel.find_elements(By.TAG_NAME, "option"):
                if o.get_attribute("disabled"):
                    continue
                if _normalize(o.text) == tgt or tgt in _normalize(o.text):
                    value = o.get_attribute("value")
                    break
            if value is None:
                raise RuntimeError(f"Opción no encontrada: {target_text}")

            try:
                Select(sel).select_by_value(value)
                return
            except Exception:
                driver.execute_script("""
                    const s = arguments[0], val = arguments[1];
                    s.value = val; s.dispatchEvent(new Event('change', {bubbles:true}));
                """, sel, value)
                return
        except Exception as e:
            last_exc = e
            time.sleep(1)
    raise last_exc

def _parse_price(text: str):
    """Convierte '$1.912,5' / '$2.550' -> float. Devuelve (float or None, raw)."""
    raw = (text or "").strip()
    if not raw:
        return None, raw
    # quitar moneda y espacios no-numéricos salvo . y ,
    s = re.sub(r"[^\d,\.]", "", raw)
    if not s:
        return None, raw
    # Si hay ambos, el último separador suele ser decimal (formato AR)
    last_comma = s.rfind(",")
    last_dot = s.rfind(".")
    if last_comma > last_dot:
        s = s.replace(".", "")  # puntos como miles
        s = s.replace(",", ".") # coma como decimal
    else:
        s = s.replace(",", "")  # comas como miles
    try:
        return float(s), raw
    except Exception:
        return None, raw

# =========================
# Login + selección de tienda (como ya veníamos haciendo)
# =========================
# 1) Mi Cuenta
_click_with_retry("//span[normalize-space()='Mi Cuenta']")
time.sleep(1)

# 2) Entrar con e-mail y contraseña
try:
    _click_with_retry("//span[normalize-space()='Entrar con e-mail y contraseña']/ancestor::button[1]")
except Exception:
    _click_with_retry("//button[.//span[contains(normalize-space(),'Entrar con e-mail')]]")

# 3) Email
try:
    _type_with_retry("//input[@placeholder='Ej. nombre@mail.com']", "comercial@factory-blue.com")
except Exception:
    _type_with_retry("//input[contains(@placeholder,'nombre@mail.com')]", "comercial@factory-blue.com")

# 4) Password
try:
    _type_with_retry("//input[@type='password' and contains(@class,'vtex-styleguide-9-x-input')]", "Compras2025")
except Exception:
    _type_with_retry("//input[@type='password' or contains(@placeholder,'●')]", "Compras2025")

# 5) Entrar
try:
    _click_with_retry("//span[normalize-space()='Entrar']/ancestor::button[@type='submit'][1]")
except Exception:
    _click_with_retry("//div[contains(@class,'vtex-login-2-x-sendButton')]//button[@type='submit']")

time.sleep(2)

# 6) Abrir selector método entrega
try:
    _click_with_retry("//span[contains(normalize-space(),'Seleccioná') and contains(.,'método de entrega')]/ancestor::*[@role='button'][1]")
except Exception:
    _click_with_retry("//div[contains(@class,'discoargentina-delivery-modal-1-x-containerTrigger')]/ancestor::div[@role='button'][1]")

time.sleep(1)

# 7) Retirar en una tienda
try:
    _click_with_retry("//div[contains(@class,'pickUpSelectionContainer')]//button[.//p[contains(normalize-space(),'Retirar en una tienda')]]")
except Exception:
    _click_with_retry("//button[.//p[contains(normalize-space(),'Retirar en una tienda')]]")

# 8) Provincia = CORDOBA
try:
    _click_with_retry("//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar Provincia')]]//div[contains(@class,'vtex-dropdown__button')]")
except Exception:
    pass
_select_by_text_case_insensitive("//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar Provincia')]]//select", PROVINCIA)
time.sleep(1.2)

# 9) Tienda = Disco Alta Córdoba Cabrera 493
try:
    _click_with_retry("//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//div[contains(@class,'vtex-dropdown__button')]")
except Exception:
    pass
store_select_xpath = "//div[contains(@class,'vtex-dropdown__container')][.//div[contains(.,'Seleccionar tienda')]]//select"
wait.until(EC.presence_of_element_located(
    (By.XPATH, f"{store_select_xpath}/option[contains(., 'Disco Alta Córdoba Cabrera 493') or contains(., 'Disco Alta Cordoba Cabrera 493')]")
))
_select_by_text_case_insensitive(store_select_xpath, TIENDA)

# 10) Confirmar
try:
    _click_with_retry("//div[contains(@class,'discoargentina-delivery-modal-1-x-buttonStyle')]//button[.//div[normalize-space()='Confirmar']]")
except Exception:
    _click_with_retry("//div[@role='dialog']//button[.//div[normalize-space()='Confirmar'] or normalize-space()='Confirmar']")
# esperar cierre modal (no obligatorio)
time.sleep(1.2)

# =========================
# Scraping de /almacen paginado
# =========================
def _collect_product_links_on_page(timeout=10):
    """Devuelve lista de hrefs relativas '/xxx/p' encontradas en la grilla."""
    t0 = time.time()
    links = set()
    # Esperar a que haya grilla o productos
    while time.time() - t0 < timeout:
        cards = driver.find_elements(
            By.XPATH,
            # anclas de la card de producto
            "//a[contains(@class,'vtex-product-summary-2-x-clearLink') and contains(@href,'/p')]"
        )
        for a in cards:
            href = a.get_attribute("href") or ""
            if href.startswith(BASE):
                href = href[len(BASE):]
            if href.startswith("/"):
                links.add(href)
        if links:
            break
        time.sleep(0.5)
    return list(links)

def _scrape_pdp(pdp_url_rel: str) -> dict:
    """Abre la PDP y extrae campos clave; vuelve a la lista."""
    url_full = f"{BASE}{pdp_url_rel}"
    driver.get(url_full)

    # Esperas básicas para PDP
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "//h1[contains(@class,'productNameContainer')]")))
    except Exception:
        time.sleep(1)

    # Nombre
    try:
        name = driver.find_element(By.XPATH, "//h1[contains(@class,'productNameContainer')]//span").text.strip()
    except Exception:
        name = ""

    # Marca
    try:
        brand = driver.find_element(By.XPATH, "//*[contains(@class,'productBrandName')]").text.strip()
    except Exception:
        brand = ""

    # SKU
    try:
        sku = driver.find_element(
            By.XPATH,
            "//span[contains(@class,'product-identifier__label') and normalize-space()='SKU']/following-sibling::span[contains(@class,'product-identifier__value')]"
        ).text.strip()
    except Exception:
        # fallback: cualquier value cercano a 'SKU'
        try:
            sku = driver.find_element(By.XPATH, "//*[contains(@class,'product-identifier')][contains(.,'SKU')]").text
            sku = re.sub(r".*SKU\s*:\s*", "", sku, flags=re.I).strip()
        except Exception:
            sku = ""

    # Precios (actual, regular, % desc y unitario/IVA)
    try:
        price_now_text = driver.find_element(By.XPATH, "//*[@id='priceContainer']").text
    except Exception:
        # fallback: primer elemento con $ visible en box de precio
        try:
            price_now_text = driver.find_element(By.XPATH, "(//*[contains(@class,'store-theme')][contains(.,'$')])[1]").text
        except Exception:
            price_now_text = ""

    price_now, price_now_raw = _parse_price(price_now_text)

    # precio regular (tachado) suele venir en un div aparte
    try:
        price_reg_text = driver.find_element(
            By.XPATH, "(//div[contains(@class,'store-theme')][contains(text(),'$')])[2]"
        ).text
    except Exception:
        price_reg_text = ""
    price_reg, price_reg_raw = _parse_price(price_reg_text)

    # % descuento (si existe)
    try:
        discount_text = driver.find_element(
            By.XPATH, "//span[contains(text(),'%') and contains(@class,'store-theme')]"
        ).text.strip()
    except Exception:
        discount_text = ""

    # unitario y/o IVA texto (si existe)
    try:
        unit_text = driver.find_element(By.XPATH, "//*[contains(@class,'vtex-custom-unit-price')]").text.strip()
    except Exception:
        unit_text = ""
    try:
        iva_text = driver.find_element(By.XPATH, "//p[contains(@class,'iva-pdp')]").text.strip()
    except Exception:
        iva_text = ""

    return {
        "url": url_full,
        "provincia": PROVINCIA,
        "tienda": TIENDA,
        "sku": sku,
        "marca": brand,
        "nombre": name,
        "precio_actual": price_now,
        "precio_actual_raw": price_now_raw,
        "precio_regular": price_reg,
        "precio_regular_raw": price_reg_raw,
        "descuento_texto": discount_text,
        "unitario_texto": unit_text,
        "iva_texto": iva_text,
        "capturado_en": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

# Iterar paginado hasta que no haya items
data = []
page = 1
empty_pages = 0
MAX_EMPTY = 1  # detente cuando encuentres 1 página sin items

while True:
    list_url = f"{BASE}/almacen?page={page}"
    print(f"\n📄 Página: {page} -> {list_url}")
    driver.get(list_url)

    # esperar a que cargue algo (productos o evidencia de vacío)
    time.sleep(1)
    links = _collect_product_links_on_page(timeout=12)

    if not links:
        print("⚠️  Sin items en la página.")
        empty_pages += 1
        if empty_pages > MAX_EMPTY:
            print("⛔ Fin: no hay más productos.")
            break
        else:
            page += 1
            continue

    empty_pages = 0  # reset si hay productos
    print(f"🔗 Productos encontrados: {len(links)}")

    # Recorrer productos de la página (usamos GET directo a PDP para evitar stale elements)
    for i, rel in enumerate(links, 1):
        try:
            print(f"  → [{i}/{len(links)}] {rel}")
            item = _scrape_pdp(rel)
            data.append(item)
        except Exception as e:
            print(f"    × Error en {rel}: {e}")
        finally:
            # volver a la lista actual antes de seguir con el siguiente (misma sesión)
            driver.get(list_url)
            time.sleep(0.6)

    page += 1

# =========================
# Exportar a Excel
# =========================
if data:
    df = pd.DataFrame(data)
    # Orden sugerido de columnas
    cols = ["provincia","tienda","sku","nombre","marca","precio_actual","precio_actual_raw",
            "precio_regular","precio_regular_raw","descuento_texto","unitario_texto","iva_texto",
            "url","capturado_en"]
    df = df.reindex(columns=cols)
    #df.to_excel(OUT_XLSX, index=False)
    print(f"\n✅ Excel generado: {OUT_XLSX} ({len(df)} filas)")
else:
    print("\n⚠️ No se capturaron productos; no se genera Excel.")

# =========================
# IMPORTANTE: nunca cerrar
# =========================
input("\nNavegador abierto. Presiona ENTER aquí cuando QUIERAS cerrar manualmente...")
# No llamamos a driver.quit() ni driver.close()
