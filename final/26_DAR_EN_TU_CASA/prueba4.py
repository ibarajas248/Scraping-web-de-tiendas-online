#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, time, re, argparse, os
from typing import Tuple, Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime

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
from mysql.connector import Error as MySQLError

# ========= Conexión MySQL =========
# Espera que tengas ../../base_datos.py con get_conn()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- tu conexión MySQL

# ========= Config Scraper =========
URL  = "https://www.darentucasa.com.ar/login.asp"
WAIT = 15  # segundos

# Identidad tienda para DB
TIENDA_CODIGO = "dar_ar"
TIENDA_NOMBRE = "DAR en tu Casa"

COMMIT_EVERY = 300  # filas
LOCK_RETRIES = 5    # reintentos 1205 por execute

# ---------------------------------------
# Utilidades base (Selenium)
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
# Scrapers (recorre todo como el tuyo)
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

def scrape_all_n1(headless: bool) -> pd.DataFrame:
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
        print(f"\n✅ Total capturado: {len(df)}")
        return df

    finally:
        try:
            driver.quit()
        except Exception:
            pass

# ---------------------------------------
# Helpers MySQL (patrón Coto)
# ---------------------------------------

def _clip255(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    return s[:255] if len(s) > 255 else s

def price_to_text(val: Any) -> Optional[str]:
    """Precio como texto 2 decimales para evitar out-of-range NUMERIC."""
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return None
    try:
        f = float(val)
    except Exception:
        return None
    if abs(f) > 999999999:
        return None
    return f"{round(f, 2):.2f}"

def exec_retry(cur, sql: str, params: tuple, retries=LOCK_RETRIES):
    wait = 0.5
    for i in range(retries):
        try:
            cur.execute(sql, params)
            return
        except MySQLError as e:
            if getattr(e, "errno", None) == 1205:
                print(f"[LOCK] errno=1205 retry {i+1}/{retries} in {wait:.2f}s")
                time.sleep(wait)
                wait *= 2
                continue
            raise

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, _clip255(nombre))
    )
    exec_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, nombre: Optional[str], marca: Optional[str],
                            categoria: Optional[str], subcategoria: Optional[str],
                            ean: Optional[str] = None) -> int:
    nombre = _clip255(nombre or "")
    marca  = _clip255(marca or "")
    categoria = _clip255(categoria or None)
    subcategoria = _clip255(subcategoria or None)
    ean = _clip255(ean or None)

    if ean:
        exec_retry(cur, "SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_retry(cur, """
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (nombre, marca, categoria, subcategoria, pid))
            return pid

    exec_retry(cur, """SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
               (nombre, marca))
    row = cur.fetchone()
    if row:
        pid = row[0]
        exec_retry(cur, """
            UPDATE productos SET
              categoria = COALESCE(%s, categoria),
              subcategoria = COALESCE(%s, subcategoria)
            WHERE id=%s
        """, (categoria, subcategoria, pid))
        return pid

    exec_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), NULLIF(%s,''), %s, %s)
    """, (ean, nombre, marca, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int,
                           sku: Optional[str], record_id: Optional[str],
                           url: Optional[str], nombre_tienda: Optional[str]) -> int:
    sku = _clip255(sku or "")
    record_id = _clip255(record_id or "")
    url = _clip255(url or "")
    nombre_tienda = _clip255(nombre_tienda or "")

    # DAR: usamos codigo como SKU y también como record_id
    if sku:
        exec_retry(cur, """
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id or sku, url, nombre_tienda))
        return cur.lastrowid

    if record_id:
        exec_retry(cur, """
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    exec_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int,
                     precio_lista: Any, precio_oferta: Any, capturado_en: datetime):
    pl = price_to_text(precio_lista)
    po = price_to_text(precio_oferta)
    exec_retry(cur, """
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta)
    """, (tienda_id, producto_tienda_id, capturado_en, pl, po))

# ---------------------------------------
# Ingesta a MySQL
# ---------------------------------------

def ingest_df_to_mysql(df: pd.DataFrame, tienda_codigo=TIENDA_CODIGO, tienda_nombre=TIENDA_NOMBRE):
    if df is None or df.empty:
        print("ℹ️ Nada para insertar.")
        return 0

    conn = None
    total_hist = 0
    capturado_en = datetime.now()

    try:
        conn = get_conn()
        # settings suaves para locks
        try:
            cset = conn.cursor()
            cset.execute("SET SESSION innodb_lock_wait_timeout = 15")
            cset.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
            cset.close()
        except Exception:
            pass

        cur = conn.cursor()
        tienda_id = upsert_tienda(cur, tienda_codigo, tienda_nombre)
        conn.commit()

        batch = 0
        for _, r in df.iterrows():
            # Mapear a nuestro modelo Coto
            codigo = (r.get("codigo") or "") and str(r.get("codigo"))
            nombre = r.get("descripcion") or None
            # DAR no expone marca / ean: se dejan en blanco
            marca = None
            ean = None

            # Categorías: usaremos cat_nombre como "categoria".
            categoria = r.get("cat_nombre") or None
            # subcategoria: intentamos N3 si existe
            subcategoria = r.get("cat_n3") or None

            # Precio: DAR nos da un solo precio. Lo guardamos como oferta (y lista None).
            precio = r.get("precio")
            precio_lista = None
            precio_oferta = precio

            # URL de detalle no está en listing; si querés, podría construirse si la UI la expone en otro lado.
            url = None

            try:
                pid = find_or_create_producto(cur, nombre, marca, categoria, subcategoria, ean=ean)
                ptid = upsert_producto_tienda(cur, tienda_id, pid, sku=codigo, record_id=codigo, url=url, nombre_tienda=nombre)
                insert_historico(cur, tienda_id, ptid, precio_lista, precio_oferta, capturado_en)
                total_hist += 1
                batch += 1
            except MySQLError as e:
                errno = getattr(e, "errno", None)
                if errno == 1264:
                    # out of range → omitir fila y seguir
                    print(f"[SKIP-1264] codigo={codigo} precio={precio} -> {e}")
                    try: conn.rollback()
                    except: pass
                elif errno == 1205:
                    # lock persistente tras reintentos de exec_retry (poco probable): rollback y seguir
                    print(f"[SKIP-1205] codigo={codigo} -> {e}")
                    try: conn.rollback()
                    except: pass
                else:
                    print(f"[SKIP] errno={errno} codigo={codigo} -> {e}")
                    try: conn.rollback()
                    except: pass

            if batch >= COMMIT_EVERY:
                try:
                    conn.commit()
                except Exception:
                    conn.rollback()
                batch = 0

        if batch:
            try:
                conn.commit()
            except Exception:
                conn.rollback()

        print(f"💾 Insertado en MySQL: {total_hist} registros de histórico para {tienda_nombre} ({capturado_en})")
        return total_hist

    except MySQLError as e:
        if conn:
            try: conn.rollback()
            except: pass
        print(f"❌ Error MySQL: {e}")
        return total_hist
    finally:
        if conn:
            try: conn.close()
            except: pass

# ---------------------------------------
# CLI
# ---------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="DAR → Recorrido completo N1 → N2 (leaf y folders) → N3 (todo el catálogo) + Ingesta MySQL"
    )
    ap.add_argument("--headless", action="store_true", help="Headless (ideal VPS)")
    ap.add_argument("--no-db", action="store_true", help="Solo scrapea (no ingresa a la base)")
    ap.add_argument("--xlsx", default=None, help="(Opcional) Guardar XLSX con lo scrapeado")
    args = ap.parse_args()

    df = scrape_all_n1(headless=args.headless)

    if args.xlsx and not df.empty:
        df.to_excel(args.xlsx, index=False)
        print(f"✅ XLSX guardado: {args.xlsx}")

    if not args.no_db and not df.empty:
        ingest_df_to_mysql(df)

if __name__ == "__main__":
    try:
        main()
    except WebDriverException as e:
        print(f"❌ WebDriver error: {e}")
        sys.exit(2)
