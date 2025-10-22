#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DAR (darentucasa.com.ar) — Scrape completo (N1/N2/N3) + Ingesta MySQL

- Recorre N1 → N2 (leaf/folder) → N3, pagina y junta todos los productos.
- Columnas base: codigo, descripcion, precio, precio_texto, oferta, imagen,
                 cat_n0, cat_n2, cat_n3, cat_nombre
- Ingesta en tablas: tiendas, productos, producto_tienda, historico_precios

Requisitos:
  pip install selenium webdriver-manager beautifulsoup4 lxml pandas mysql-connector-python

NOTA VPS/headless:
  - Por defecto corre en headless. Si necesitás ver la UI: --no-headless
  - Asegurate de tener Google Chrome/Chromium instalado en el VPS.

Config MySQL:
  - Debe existir base_datos.py con:
        import mysql.connector
        def get_conn():
            return mysql.connector.connect(
                host="...", user="...", password="...", database="...", charset="utf8mb4"
            )
"""

import sys, time, re, argparse
from typing import Tuple, Dict, Any, List, Optional
import pandas as pd
from datetime import datetime as dt

# ---------- Selenium / parsing ----------
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

# ---------- MySQL ----------
import mysql.connector
from mysql.connector import errors as myerr

# ---------- Conexión (tu helper) ----------
from base_datos import get_conn

# =========================
# Config tienda
# =========================
TIENDA_CODIGO = "dar"
TIENDA_NOMBRE = "Dar en tu Casa"

URL  = "https://www.darentucasa.com.ar/login.asp"
WAIT = 15  # segundos

# =========================
# Límite de longitudes (ajustá a tu esquema)
# =========================
MAXLEN_TIPO_OFERTA     = 64
MAXLEN_COMENTARIOS     = 255
MAXLEN_NOMBRE          = 255
MAXLEN_CATEGORIA       = 120
MAXLEN_SUBCATEGORIA    = 200
MAXLEN_NOMBRE_TIENDA   = 255

# =========================
# Helpers genéricos
# =========================
def _truncate(s: Optional[str], n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]

def _price_str(val) -> Optional[str]:
    if val is None:
        return None
    try:
        f = float(val)
        if pd.isna(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

# =========================
# SQL con reintentos ante locks
# =========================
LOCK_ERRNOS = {1205, 1213}  # lock wait timeout, deadlock

def exec_with_retry(cur, sql, params=None, max_retries=5, base_sleep=0.4):
    """
    Ejecuta una sentencia SQL con reintentos ante lock timeout/deadlock.
    NO hace commit (se maneja fuera).
    """
    attempt = 0
    while True:
        try:
            cur.execute(sql, params or ())
            return
        except myerr.DatabaseError as e:
            code = getattr(e, 'errno', None)
            if code in LOCK_ERRNOS and attempt < max_retries:
                wait = base_sleep * (2 ** attempt)
                print(f"[LOCK] errno={code} reintento {attempt+1}/{max_retries} en {wait:.2f}s")
                time.sleep(wait)
                attempt += 1
                continue
            raise

# =========================
# Selenium base
# =========================
def setup_driver(headless: bool = True) -> webdriver.Chrome:
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

# =========================
# Menú y descubrimiento
# =========================
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

# =========================
# Extracción y paginación
# =========================
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

# =========================
# Scrapers
# =========================
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

# =========================
# Mapeo → MySQL
# =========================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_with_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    exec_with_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def split_categoria_sub(cat_nombre: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    cat_nombre viene como 'N1 > N2' o 'N1 > N2 > N3'.
    Devolvemos: (categoria=N1, subcategoria='N2' o 'N2 > N3')
    """
    if not cat_nombre:
        return None, None
    parts = [p.strip() for p in str(cat_nombre).split(">") if p.strip()]
    if not parts:
        return None, None
    categoria = parts[0]
    sub = " > ".join(parts[1:]) if len(parts) > 1 else None
    return categoria, sub

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = None  # no lo tenemos
    nombre = _truncate(r.get("descripcion") or "", MAXLEN_NOMBRE)
    marca = None
    fabricante = None
    categoria, subcategoria = split_categoria_sub(r.get("cat_nombre"))
    categoria = _truncate(categoria or "", MAXLEN_CATEGORIA)
    subcategoria = _truncate(subcategoria or "", MAXLEN_SUBCATEGORIA)

    # (nombre, marca) como clave "suave"
    if nombre:
        exec_with_retry(cur,
            "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
            (nombre, marca or "")
        )
        row = cur.fetchone()
        if row:
            pid = row[0]
            exec_with_retry(cur, """
                UPDATE productos SET
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (categoria, subcategoria, pid))
            return pid

    exec_with_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), %s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    sku = r.get("codigo") or None
    record_id = sku
    url = None
    nombre_tienda = _truncate(r.get("descripcion") or None, MAXLEN_NOMBRE_TIENDA)

    if sku:
        exec_with_retry(cur, """
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    exec_with_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en):
    precio = r.get("precio")
    precio_lista = _price_str(precio)
    precio_oferta = _price_str(precio)
    tipo_oferta = _truncate(("Oferta" if r.get("oferta") else None), MAXLEN_TIPO_OFERTA)
    promo_comentarios = _truncate(f"precio_texto={r.get('precio_texto') or ''}", MAXLEN_COMENTARIOS)

    exec_with_retry(cur, """
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = VALUES(promo_tipo),
          promo_texto_regular = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios = VALUES(promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista, precio_oferta, tipo_oferta,
        tipo_oferta, None, None, promo_comentarios
    ))

def ingest_to_mysql(df: pd.DataFrame):
    if df.empty:
        print("⚠ No hay filas para insertar en MySQL.")
        return

    conn = None
    try:
        conn = get_conn()

        # Sugerencias de sesión para reducir bloqueos
        try:
            with conn.cursor() as cset:
                cset.execute("SET SESSION innodb_lock_wait_timeout = 5")
                cset.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
        except Exception:
            pass

        conn.autocommit = False
        cur = conn.cursor(buffered=True)

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        capturado_en = dt.now()

        # Dedupe básico por (codigo, descripcion)
        if "codigo" in df.columns and "descripcion" in df.columns:
            df = df.drop_duplicates(subset=["codigo", "descripcion"], keep="first")

        batch = 0
        total = 0
        for _, r in df.iterrows():
            rec = r.to_dict()
            pid = find_or_create_producto(cur, rec)
            ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)
            insert_historico(cur, tienda_id, ptid, rec, capturado_en)

            batch += 1
            total += 1
            if batch >= 20:
                conn.commit()
                batch = 0

        if batch:
            conn.commit()

        print(f"✅ MySQL: {total} registros de histórico insertados/actualizados.")

    except mysql.connector.Error as e:
        if conn:
            conn.rollback()
        print(f"❌ MySQL error {getattr(e,'errno',None)}: {e}")
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

# =========================
# CLI
# =========================
def main():
    ap = argparse.ArgumentParser(description="DAR → Scrape completo + Ingesta MySQL")
    ap.add_argument("--out", default="dar_catalogo_completo.xlsx", help="Ruta de salida XLSX (debug/auditoría)")
    ap.add_argument("--csv", default=None, help="(Opcional) Ruta CSV adicional")
    ap.add_argument("--no-headless", action="store_true", help="Desactivar headless (para debugar con UI)")
    ap.add_argument("--no-ingest", action="store_true", help="Solo scrape (no ingesta)")
    args = ap.parse_args()

    df = scrape_all_n1(headless=(not args.no_headless), out_xlsx=args.out, out_csv=args.csv)
    if not args.no_ingest and not df.empty:
        ingest_to_mysql(df)

if __name__ == "__main__":
    try:
        main()
    except WebDriverException as e:
        print(f"❌ WebDriver error: {e}")
        sys.exit(2)
