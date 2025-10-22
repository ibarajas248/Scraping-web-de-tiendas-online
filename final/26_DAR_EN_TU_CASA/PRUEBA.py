#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, time, re, argparse
from typing import Tuple, Dict, Any, List, Optional
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, JavascriptException
)
from webdriver_manager.chrome import ChromeDriverManager

URL  = "https://www.darentucasa.com.ar/login.asp"
WAIT = 15  # segundos

# ---------------------------------------
# Utilidades base
# ---------------------------------------

def setup_driver(headless: bool = False) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1366,900")
    opts.add_argument("--lang=es-AR")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    # ocultar webdriver flag
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    })
    return driver

def wait_js_click(driver: webdriver.Chrome, locator: Tuple[str, str], desc: str):
    el = WebDriverWait(driver, WAIT).until(EC.presence_of_element_located(locator))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    WebDriverWait(driver, WAIT).until(EC.element_to_be_clickable(locator))
    driver.execute_script("arguments[0].click();", el)
    print(f"✔ {desc}")

def try_call_js(driver: webdriver.Chrome, code: str, desc: str):
    try:
        driver.execute_script(code)
        print(f"✔ JS: {desc}")
        time.sleep(0.6)
        return True
    except JavascriptException:
        print(f"✖ JS falló: {desc}")
        return False

def parse_price(text: str) -> Optional[float]:
    """ '$3.170,00' -> 3170.00 ; '6.445,50' -> 6445.50 """
    if not text:
        return None
    t = re.sub(r"[^\d,\.]", "", text.strip())
    t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None

def text_or_empty(el) -> str:
    try:
        return el.text.strip()
    except Exception:
        return ""

def extract_code_from(li) -> Optional[str]:
    """
    - onclick COMPRAR: PCompra('0000000009043')
    - src imagen: Fotos/Articulos/0000000009043.jpg
    - onclick img: FCargaP(... Pr=0000000009043 ...)
    """
    try:
        comprar = li.find_element(By.XPATH, ".//div[contains(@class,'AgregaArt') and @onclick]")
        oc = comprar.get_attribute("onclick") or ""
        m = re.search(r"PCompra\('(\d+)'\)", oc)
        if m: return m.group(1)
    except Exception:
        pass
    try:
        img = li.find_element(By.CSS_SELECTOR, ".FotoProd img")
        src = img.get_attribute("src") or ""
        m = re.search(r"/Articulos/(\d+)\.(?:jpg|png|jpeg|gif)", src, re.I)
        if m: return m.group(1)
        oc = img.get_attribute("onclick") or ""
        m = re.search(r"Pr=(\d+)", oc)
        if m: return m.group(1)
    except Exception:
        pass
    return None

# ---------------------------------------
# Menú y descubrimiento (N1, N2, N3)
# ---------------------------------------

def open_products_menu(driver: webdriver.Chrome):
    driver.get(URL)
    try:
        wait_js_click(driver, (By.CSS_SELECTOR, "div.M2-Mdir.Dispara"), "Abrir PRODUCTOS")
    except TimeoutException:
        if not try_call_js(driver, "if (typeof Menu==='function') Menu();", "Menu()"):
            raise
    time.sleep(0.4)

def open_menu_to_n1(driver: webdriver.Chrome, n1: str):
    open_products_menu(driver)
    try:
        wait_js_click(driver, (By.CSS_SELECTOR, f"div#D{n1}.M2-N1, #D{n1}"), f"Abrir N1 {n1}")
    except TimeoutException:
        try_call_js(driver, f"if (typeof Dispara==='function') Dispara('{n1}');", f"Dispara('{n1}')")
    time.sleep(0.4)

def open_menu_to_n2(driver: webdriver.Chrome, n1: str, n2: str):
    open_menu_to_n1(driver, n1)
    try:
        wait_js_click(driver, (By.CSS_SELECTOR, f"div#D{n1}-{n2}.M2-N2Act, #D{n1}-{n2}"), f"Abrir N2 {n1}-{n2}")
    except TimeoutException:
        try_call_js(driver, f"if (typeof Dispara2==='function') Dispara2('{n1}','{n2}');", f"Dispara2('{n1}','{n2}')")
    time.sleep(0.4)

def discover_n1_routes(driver: webdriver.Chrome) -> List[Dict[str, str]]:
    """
    Extrae TODOS los N1 válidos de #Niv-1:
    - Ignora DESTACADOS (top.location ...) y OFERTAS (EnvioForm('CM')).
    Devuelve: [{"n1":"01","nombre":"ALMACÉN"}, ...]
    """
    open_products_menu(driver)
    WebDriverWait(driver, WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div#Niv-1")))
    nodes = driver.find_elements(By.CSS_SELECTOR, "div#Niv-1 .M2-N1[onclick]")
    rutas, seen = [], set()
    for el in nodes:
        oc = el.get_attribute("onclick") or ""
        nombre = (el.text or "").strip()
        # saltar DESTACADOS y OFERTAS
        if "top.location.href" in oc or "EnvioForm('CM')" in oc:
            continue
        m = re.search(r"Dispara\('(\d+)'\)", oc)
        if not m:
            continue
        n1 = m.group(1)
        if n1 in seen:
            continue
        seen.add(n1)
        rutas.append({"n1": n1, "nombre": nombre})
    print(f"🧭 N1 detectados: {len(rutas)}")
    return rutas

def discover_n2_routes(driver: webdriver.Chrome, n1: str) -> List[Dict[str, str]]:
    """
    Lee TODAS las entradas de #Niv-2 bajo un N1:
    - Folder N2: onclick="Dispara2('n1','n2')"
    - Leaf N2:   onclick="EnvioForm('Cat','n1','n2','00','00')"
    Devuelve dicts: {"n1","n2","tipo","nombre"} con tipo in {"folder","leaf"}.
    """
    open_menu_to_n1(driver, n1)
    WebDriverWait(driver, WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div#Niv-2")))
    nodes = driver.find_elements(By.CSS_SELECTOR, "div#Niv-2 .M2-N2[onclick]")
    rutas, seen = [], set()
    for el in nodes:
        oc = el.get_attribute("onclick") or ""
        nombre = (el.text or "").strip()
        m_folder = re.search(r"Dispara2\('(\d+)','(\d+)'\)", oc)
        m_leaf   = re.search(r"EnvioForm\('Cat','(\d+)','(\d+)','(\d+)','(\d+)'\)", oc)
        if m_folder:
            n1_, n2_ = m_folder.groups()
            key = (n1_, n2_, "folder")
            if key in seen: continue
            seen.add(key)
            rutas.append({"n1": n1_, "n2": n2_, "tipo": "folder", "nombre": nombre})
        elif m_leaf:
            n1_, n2_, n3_, n4_ = m_leaf.groups()
            key = (n1_, n2_, "leaf")
            if key in seen: continue
            seen.add(key)
            rutas.append({"n1": n1_, "n2": n2_, "tipo": "leaf", "nombre": nombre, "n3": n3_, "n4": n4_})
    print(f"   ↳ N2 detectados en N1={n1}: {len(rutas)}")
    return rutas

def get_n3_subcats(driver: webdriver.Chrome) -> List[Dict[str, str]]:
    """Subcategorías N3 visibles bajo #Niv-3: EnvioForm('Cat', n1,n2,n3,'00')."""
    WebDriverWait(driver, WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div#Niv-3")))
    n3_nodes = driver.find_elements(By.CSS_SELECTOR, "div#Niv-3 .M2-N3[onclick]")
    rutas, seen = [], set()
    for el in n3_nodes:
        oc = el.get_attribute("onclick") or ""
        m = re.search(r"EnvioForm\('Cat','(\d+)','(\d+)','(\d+)','(\d+)'\)", oc)
        if not m: continue
        n1, n2, n3, n4 = m.groups()
        key = (n1, n2, n3, n4)
        if key in seen: continue
        seen.add(key)
        nombre = (el.text or "").strip() or f"{n1}-{n2}-{n3}"
        rutas.append({"n1": n1, "n2": n2, "n3": n3, "n4": n4, "nombre": nombre})
    print(f"      ↳ N3 detectadas: {len(rutas)}")
    return rutas

def go_to_category(driver: webdriver.Chrome, n1: str, n2: str, n3: str, n4: str = "00"):
    """Navega a la categoría con EnvioForm y espera el listado."""
    open_menu_to_n2(driver, n1, n2)  # asegura overlay correcto
    ok = try_call_js(driver, f"EnvioForm('Cat','{n1}','{n2}','{n3}','{n4}');",
                     f"EnvioForm('Cat','{n1}','{n2}','{n3}','{n4}')")
    if not ok:
        raise TimeoutException("No se pudo ejecutar EnvioForm para la categoría")
    WebDriverWait(driver, WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds")))
    time.sleep(0.6)

# ---------------------------------------
# Extracción y paginación
# ---------------------------------------

def collect_page_products(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    items = []
    ul = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds"))
    )
    cards = ul.find_elements(By.CSS_SELECTOR, "li.cuadProd")
    for li in cards:
        try:
            desc_el = li.find_element(By.CSS_SELECTOR, ".InfoProd .desc")
            price_el = li.find_element(By.CSS_SELECTOR, ".InfoProd .precio .izq")
        except Exception:
            continue

        code      = extract_code_from(li)
        desc      = text_or_empty(desc_el)
        price_raw = text_or_empty(price_el)
        price     = parse_price(price_raw)

        is_offer = False
        try:
            li.find_element(By.CSS_SELECTOR, "#DvOferProd.OferProd, .OferProd")
            is_offer = True
        except Exception:
            pass

        img_url = None
        try:
            img_url = li.find_element(By.CSS_SELECTOR, ".FotoProd img").get_attribute("src")
        except Exception:
            pass

        items.append({
            "codigo": code,
            "descripcion": desc,
            "precio_texto": price_raw,
            "precio": price,
            "oferta": is_offer,
            "imagen": img_url
        })
    return items

def click_next(driver: webdriver.Chrome, prev_first_code: Optional[str]) -> bool:
    """Click en 'Siguiente' si existe. True si cambió de página."""
    try:
        btn = driver.find_element(By.XPATH, "//input[contains(@class,'PagArt') and @value='Siguiente']")
    except Exception:
        return False

    try:
        ul_before = driver.find_element(By.CSS_SELECTOR, "ul.listaProds")
    except Exception:
        ul_before = None

    oc = btn.get_attribute("onclick") or ""
    try:
        if oc: driver.execute_script(oc)
        else:  driver.execute_script("arguments[0].click();", btn)
    except JavascriptException:
        try:
            btn.click()
        except Exception:
            return False

    try:
        if ul_before:
            WebDriverWait(driver, WAIT).until(EC.staleness_of(ul_before))
        WebDriverWait(driver, WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds")))
        time.sleep(0.6)
        first_code = None
        try:
            li0 = driver.find_elements(By.CSS_SELECTOR, "ul.listaProds li.cuadProd")[0]
            first_code = extract_code_from(li0)
        except Exception:
            pass
        if prev_first_code and first_code and first_code == prev_first_code:
            return False
        return True
    except TimeoutException:
        return False

# ---------------------------------------
# Scrapers
# ---------------------------------------

def scrape_n1_block(driver: webdriver.Chrome, n1: str, nombre_n1: str,
                    all_rows: List[Dict[str, Any]], seen_keys: set):
    """
    Recorre un N1 completo: todas sus N2 (leaf o folders) y sus N3 si aplica.
    Acumula en all_rows; dedupe con seen_keys.
    """
    rutas_n2 = discover_n2_routes(driver, n1=n1)
    if not rutas_n2:
        print(f"No se detectaron N2 bajo N1={n1}.")
        return

    for i, r2 in enumerate(rutas_n2, 1):
        n2 = r2["n2"]
        nombre_n2 = r2["nombre"]
        tipo = r2["tipo"]
        print(f"\n=== N2 [{i}/{len(rutas_n2)}] {nombre_n2} ({n1}/{n2}) — {tipo} ===")

        if tipo == "leaf":
            n3 = r2.get("n3", "00"); n4 = r2.get("n4", "00")
            try:
                go_to_category(driver, n1, n2, n3, n4)
            except TimeoutException:
                print("  ⚠ No se pudo abrir la categoría leaf; se omite.")
                continue

            page_idx = 1
            while True:
                print(f"  📄 Página {page_idx} (leaf): extrayendo…")
                rows = collect_page_products(driver)
                nuevos = 0
                for rp in rows:
                    key = (rp.get("codigo") or f"desc::{rp.get('descripcion')}", n1, n2, n3)
                    if key in seen_keys: continue
                    seen_keys.add(key)
                    rp["cat_n0"] = n1
                    rp["cat_n2"] = n2
                    rp["cat_n3"] = n3
                    rp["cat_nombre"] = f"{nombre_n1} > {nombre_n2}"
                    all_rows.append(rp); nuevos += 1
                print(f"    → {len(rows)} encontrados, {nuevos} nuevos, total {len(all_rows)}")
                first_code = rows[0]["codigo"] if rows else None
                if not click_next(driver, prev_first_code=first_code): break
                page_idx += 1

        else:
            # folder: obtener N3 y recorrerlas
            open_menu_to_n2(driver, n1, n2)
            rutas_n3 = get_n3_subcats(driver)
            if not rutas_n3:
                print("  (sin N3 detectadas; puede ser vacía)")
                continue

            for j, r3 in enumerate(rutas_n3, 1):
                n3, n4, nombre_n3 = r3["n3"], r3["n4"], r3["nombre"]
                print(f"   → N3 [{j}/{len(rutas_n3)}] {nombre_n3} ({n1}/{n2}/{n3})")
                try:
                    go_to_category(driver, n1, n2, n3, n4)
                except TimeoutException:
                    print("     ⚠ No se pudo abrir esta N3; se omite.")
                    continue

                page_idx = 1
                while True:
                    print(f"     📄 Página {page_idx}: extrayendo…")
                    rows = collect_page_products(driver)
                    nuevos = 0
                    for rp in rows:
                        key = (rp.get("codigo") or f"desc::{rp.get('descripcion')}", n1, n2, n3)
                        if key in seen_keys: continue
                        seen_keys.add(key)
                        rp["cat_n0"] = n1
                        rp["cat_n2"] = n2
                        rp["cat_n3"] = n3
                        rp["cat_nombre"] = f"{nombre_n1} > {nombre_n2} > {nombre_n3}"
                        all_rows.append(rp); nuevos += 1
                    print(f"       → {len(rows)} encontrados, {nuevos} nuevos, total {len(all_rows)}")
                    first_code = rows[0]["codigo"] if rows else None
                    if not click_next(driver, prev_first_code=first_code): break
                    page_idx += 1

def scrape_all_n1(headless: bool, out_xlsx: str, out_csv: Optional[str]=None) -> pd.DataFrame:
    driver = setup_driver(headless=headless)
    try:
        rutas_n1 = discover_n1_routes(driver)
        if not rutas_n1:
            print("No se detectaron categorías N1.")
            return pd.DataFrame()

        all_rows: List[Dict[str, Any]] = []
        seen_keys = set()

        for k, r1 in enumerate(rutas_n1, 1):
            n1 = r1["n1"]; nombre_n1 = r1["nombre"]
            print(f"\n============================")
            print(f"=== N1 [{k}/{len(rutas_n1)}] {nombre_n1} (id {n1}) ===")
            print(f"============================")

            try:
                scrape_n1_block(driver, n1, nombre_n1, all_rows, seen_keys)
            except Exception as e:
                print(f"  ⚠ Error recorriendo N1 {n1}: {e}")

        df = pd.DataFrame(all_rows,
                          columns=["codigo","descripcion","precio","precio_texto","oferta","imagen",
                                   "cat_n0","cat_n2","cat_n3","cat_nombre"])
        if not df.empty:
            df.sort_values(by=["cat_nombre","descripcion"], inplace=True, kind="stable")
        df.to_excel(out_xlsx, index=False)
        print(f"\n✅ XLSX guardado: {out_xlsx}")
        if out_csv:
            df.to_csv(out_csv, index=False)
            print(f"✅ CSV guardado:  {out_csv}")
        return df

    finally:
        try:
            driver.quit()
        except Exception:
            pass

# ---------------------------------------
# CLI
# ---------------------------------------

def main():
    ap = argparse.ArgumentParser(description="DAR → Recorrido completo N1 → N2 (leaf y folders) → N3 (todo el catálogo)")
    ap.add_argument("--out", default="dar_catalogo_completo.xlsx", help="Ruta de salida XLSX")
    ap.add_argument("--csv", default=None, help="(Opcional) Ruta CSV adicional")
    ap.add_argument("--headless", action="store_true", help="Headless (ideal VPS)")
    args = ap.parse_args()

    scrape_all_n1(headless=args.headless, out_xlsx=args.out, out_csv=args.csv)

if __name__ == "__main__":
    try:
        main()
    except WebDriverException as e:
        print(f"❌ WebDriver error: {e}")
        sys.exit(2)
