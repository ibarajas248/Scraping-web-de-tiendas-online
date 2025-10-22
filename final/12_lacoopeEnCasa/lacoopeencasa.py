#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Requisitos:
#   pip install selenium beautifulsoup4 pandas mysql-connector-python numpy webdriver-manager bs4

import time
import re
import json
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException,
    ElementClickInterceptedException, InvalidSelectorException, WebDriverException
)
from mysql.connector import Error as MySQLError
import sys, os
from webdriver_manager.chrome import ChromeDriverManager

import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # debes tenerlo configurado

# ------------------ Config ------------------
BASE = "https://www.lacoopeencasa.coop"
CATEGORY = "almacen"    # solo se usa como fallback
CAT_ID = 2

PAGE_START = 2
PAGE_END = 20000             # alto por si hay muchas páginas
WAIT = 25
HUMAN_SLEEP = 0.2
OUT_XLSX = "la_coope_almacen.xlsx"

CSS_CARD_ANCHOR = "col-listado-articulo a[id^='listadoArt']"
MONEY_RX = re.compile(r"[^\d,.\-]")
ID_RX = re.compile(r"/(\d+)(?:$|[/?#])")  # captura el código al final del href

# ---- Identidad de tienda ----
TIENDA_CODIGO = "la_coope"
TIENDA_NOMBRE = "La Coope en Casa"

# ------------------ Driver ------------------
def make_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--window-size=1366,900")
    options.add_argument("--lang=es-AR")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    driver.set_page_load_timeout(45)
    return driver

# ------------------ Helpers gen ------------------
def clean(val):
    if val is None: return None
    s = str(val).strip()
    return s if s else None

def money_to_float(txt: str):
    """Convierte '$4.099,00' -> 4099.00; devuelve None si no puede."""
    if not txt:
        return None
    t = MONEY_RX.sub("", str(txt)).strip()
    if not t:
        return None
    # miles con punto, decimales coma
    t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None

def to_price_text(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        v = float(x)
    else:
        v = money_to_float(str(x))
        if v is None:
            return None
    if isinstance(v, float) and np.isnan(v):
        return None
    return f"{round(float(v), 2)}"

# ------------------ Helpers DOM ------------------
def wait_cards(driver, timeout=WAIT):
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CSS_CARD_ANCHOR))
        )
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_any_elements_located((By.CSS_SELECTOR, CSS_CARD_ANCHOR))
        )
        return True
    except TimeoutException:
        return False

def get_page_cards(driver):
    try:
        anchors = driver.find_elements(By.CSS_SELECTOR, CSS_CARD_ANCHOR)
    except WebDriverException:
        anchors = []
    items = []
    for a in anchors:
        try:
            href = a.get_attribute("href") or a.get_attribute("ng-reflect-router-link")
            if href and href.startswith("/"):
                href = BASE + href
            title = a.get_attribute("title") or a.text.strip()
            if href:
                items.append((href, (title or "").strip()))
        except (StaleElementReferenceException, WebDriverException):
            continue
    return items

def product_code_from_href(href: str) -> str:
    m = ID_RX.search(href or "")
    return m.group(1) if m else ""

def to_relative_path(href: str) -> str:
    p = urlparse(href or "")
    if p.scheme and p.netloc:
        return p.path
    return href or ""

def css_escape_attr_value(v: str) -> str:
    if v is None:
        return ""
    return v.replace("\\", "\\\\").replace('"', r'\"')

def sel_href_eq(v: str) -> str:
    return f'a[href="{css_escape_attr_value(v)}"]'

def lazy_scroll_find(driver, selector: str, max_steps: int = 10, pause: float = 0.18):
    """Scrollea por partes buscando selector; vuelve arriba al final."""
    try:
        for i in range(max_steps):
            try:
                els = driver.find_elements(By.CSS_SELECTOR, selector)
            except InvalidSelectorException:
                return None
            except WebDriverException:
                els = []
            if els:
                return els[0]
            try:
                driver.execute_script(
                    "window.scrollTo(0, (document.body.scrollHeight/arguments[0])*arguments[1]);",
                    max_steps, i + 1
                )
            except WebDriverException:
                pass
            time.sleep(pause)
        try:
            driver.execute_script("window.scrollTo(0,0);")
        except WebDriverException:
            pass
        try:
            els = driver.find_elements(By.CSS_SELECTOR, selector)
        except (InvalidSelectorException, WebDriverException):
            return None
        return els[0] if els else None
    except Exception:
        return None

def smart_open_product(driver, list_url: str, href: str, timeout: int = 25, retries: int = 2) -> bool:
    rel = to_relative_path(href)
    code = product_code_from_href(href)

    try:
        if not driver.current_url.startswith(list_url):
            driver.get(list_url)
            if not wait_cards(driver, timeout):
                return False
    except WebDriverException:
        return False

    for attempt in range(retries + 1):
        mode = None
        sel_href = f"{sel_href_eq(rel)}, {sel_href_eq(href)}"
        el = lazy_scroll_find(driver, sel_href)
        if el:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.06)
                driver.execute_script("arguments[0].click();", el)
                mode = "href"
            except (ElementClickInterceptedException, WebDriverException, StaleElementReferenceException):
                try:
                    driver.get(href)
                    mode = "get"
                except WebDriverException:
                    mode = "error"
        else:
            if code:
                sel_id = f"a#listadoArt{code}, a[id='listadoArt{code}']"
                el2 = lazy_scroll_find(driver, sel_id)
                if el2:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el2)
                        time.sleep(0.05)
                        driver.execute_script("arguments[0].click();", el2)
                        mode = "id"
                    except (ElementClickInterceptedException, WebDriverException, StaleElementReferenceException):
                        try:
                            driver.get(href)
                            mode = "get"
                        except WebDriverException:
                            mode = "error"
            if not mode:
                try:
                    driver.get(href)
                    mode = "get"
                except WebDriverException:
                    mode = "error"

        try:
            WebDriverWait(driver, timeout).until(EC.url_contains("/producto/"))
            print(f"      ↪️  abierto por: {mode} (intento {attempt+1})")
            return True
        except TimeoutException:
            print(f"      ⚠️  no abrió detalle (mode={mode}, intento {attempt+1})")
            time.sleep(0.3 + attempt * 0.3)
            try:
                driver.get(list_url)
                wait_cards(driver, timeout)
            except WebDriverException:
                pass

    return False

# ------------------ Helpers parsing ------------------
def safe_select_text(soup, selector, attr=None):
    el = soup.select_one(selector)
    if not el:
        return ""
    if attr:
        return (el.get(attr) or "").strip()
    return el.get_text(" ", strip=True)

def first_text(soup, selectors, attr=None):
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return ((el.get(attr) or "") if attr else el.get_text(" ", strip=True)).strip()
    return ""

def extract_product_fields(driver):
    """Extrae campos del detalle, mapeando:
       - precio_lista = tachado (precio regular) si existe
       - precio_oferta = precio grande si hay regular; si no, None
    """
    try:
        WebDriverWait(driver, WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".articulo-detalle-titulo, .precio-detalle"))
        )
    except TimeoutException:
        pass

    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return {}

    # ---------- Título, marca ----------
    titulo = first_text(soup, ["h1.articulo-detalle-titulo"])
    marca = first_text(soup, [".articulo-detalle-marca a h2", ".articulo-detalle-marca h2"])

    # ---------- Breadcrumb -> categoría / subcategoría ----------
    bc_items = [a.get_text(" ", strip=True) for a in soup.select("col-breadcrumb .breadcrumb")]
    categoria = ""
    subcategoria = ""
    if len(bc_items) >= 3:
        # [INICIO, ALMACÉN, SNACKS, PAPAS FRITAS, Producto]
        categoria = bc_items[1]
        if len(bc_items) >= 4:
            subcategoria = bc_items[-2]  # penúltimo antes del producto
    # Fallbacks
    if not categoria:
        categoria = first_text(soup, [".articulo-detalle-marca a[href*='/listado/categoria/'] h2"]) or CATEGORY
    if not subcategoria:
        subcategoria = first_text(soup, [".articulo-detalle-marca a[href*='/listado/categoria/'] h2"])

    # ---------- Precios ----------
    precio_actual_txt = first_text(soup, [".precio.precio-detalle"])
    precio_actual = money_to_float(precio_actual_txt)

    precio_regular_txt = first_text(soup, [
        ".precio-regular .valor",
        ".precio-regular .precio-tachado",
        ".precio-regular.precio-detalle .valor",
    ])
    precio_regular = money_to_float(precio_regular_txt)

    if (precio_regular is not None) and (precio_actual is not None) and precio_regular > 0:
        precio_lista = precio_regular       # 6.990,00 en tu ejemplo
        precio_oferta = precio_actual       # 4.199,00 en tu ejemplo
    else:
        precio_lista = precio_actual
        precio_oferta = None

    # ---------- Precio unitario y sin impuestos ----------
    precio_unitario_txt = first_text(soup, [".precio-unitario"])
    precio_unitario = None
    if precio_unitario_txt:
        m = re.search(r"([\d\.\,]+)", precio_unitario_txt)
        if m:
            precio_unitario = money_to_float(m.group(1))

    precio_sin_imp_txt = first_text(soup, [".precio-sin-impuestos span:nth-of-type(2)", ".precio-sin-impuestos"])
    precio_sin_impuestos = money_to_float(precio_sin_imp_txt)

    # ---------- Código interno ----------
    codigo_interno = first_text(soup, [".articulo-codigo span"])

    # ---------- Imagen ----------
    imagen_url = first_text(soup, [".articulo-detalle-imagen-ppal"], attr="src")
    if not imagen_url:
        imagen_url = first_text(soup, [".articulo-detalle-imagen-contenedor img"], attr="src")

    # ---------- URL actual ----------
    try:
        url_producto = driver.current_url
    except WebDriverException:
        url_producto = ""

    # ---------- Tipo de oferta y vigencia ----------
    tipo_oferta = first_text(soup, [".iconos-ofertas img.icono1"], attr="alt")  # p.ej. "Ahorrón"
    promo_vigencia = first_text(soup, [".vigencia_promo strong"])               # "14/08/2025 - 27/08/2025"
    precio_regular_label = "Precio regular" if precio_regular_txt else ""

    return {
        "nombre": titulo,
        "marca": marca or "",
        "categoria": (categoria or "").strip(),
        "subcategoria": (subcategoria or "").strip(),
        "precio_lista": precio_lista,
        "precio_oferta": precio_oferta,
        "tipo_oferta": tipo_oferta or "",
        "promo_tipo": precio_regular_label,           # opcional
        "precio_regular_promo": precio_regular_txt,   # texto crudo tachado
        "precio_descuento": precio_actual_txt,        # texto crudo visible
        "comentarios_promo": promo_vigencia,          # rango de fechas si existe
        "codigo_interno": codigo_interno,
        "ean": None,                                  # no visible
        "fabricante": "",
        "url": url_producto,
        "imagen_url": imagen_url,
        "precio_txt": precio_actual_txt,
        "precio_unitario": precio_unitario,
        "precio_unitario_txt": precio_unitario_txt,
        "precio_sin_impuestos": precio_sin_impuestos,
        "precio_sin_impuestos_txt": precio_sin_imp_txt,
    }

# ------------------ Helpers BD (upserts) ------------------
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: dict) -> int:
    ean = clean(p.get("ean"))
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(NULLIF(%s,''), marca),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("nombre") or "", p.get("marca") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    cur.execute("""
        SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1
    """, (p.get("nombre") or "", p.get("marca") or ""))
    row = cur.fetchone()
    if row:
        pid = row[0]
        cur.execute("""
            UPDATE productos SET
              ean = COALESCE(NULLIF(%s,''), ean),
              marca = COALESCE(NULLIF(%s,''), marca),
              fabricante = COALESCE(NULLIF(%s,''), fabricante),
              categoria = COALESCE(NULLIF(%s,''), categoria),
              subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
            WHERE id=%s
        """, (
            p.get("ean") or "", p.get("marca") or "", p.get("fabricante") or "",
            p.get("categoria") or "", p.get("subcategoria") or "", pid
        ))
        return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", p.get("nombre") or "", p.get("marca") or "",
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: dict) -> int:
    sku = clean(p.get("codigo_interno"))
    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULL, NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              producto_id=VALUES(producto_id),
              url_tienda=COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda=COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, p.get("url") or "", p.get("nombre") or ""))
        cur.execute("SELECT id FROM producto_tienda WHERE tienda_id=%s AND sku_tienda=%s LIMIT 1",
                    (tienda_id, sku))
        return cur.fetchone()[0]

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, p.get("url") or "", p.get("nombre") or ""))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: dict, capturado_en: datetime):
    precio_lista_txt  = to_price_text(p.get("precio_lista"))
    precio_oferta_txt = to_price_text(p.get("precio_oferta")) if p.get("precio_oferta") is not None else None

    cur.execute("""
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
        precio_lista_txt, precio_oferta_txt,
        p.get("tipo_oferta") or None, p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None, p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# ------------------ Main ------------------
def run(headless=True):
    driver = make_driver(headless=headless)
    data = []
    seen_codes = set()

    t0 = time.time()
    try:
        for page in range(PAGE_START, PAGE_END + 1):
            url = f"{BASE}/listado/categoria/{CATEGORY}/{CAT_ID}/pagina--{page}"
            print(f"\n📄 Página {page}: {url}")

            try:
                driver.get(url)
            except WebDriverException as e:
                print(f"⚠️  No se pudo cargar la página {page}: {e}")
                continue

            if not wait_cards(driver):
                print("⚠️  No se detectaron productos en esta página. Deteniendo.")
                break

            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.25);")
            except WebDriverException:
                pass
            time.sleep(0.3)

            cards = get_page_cards(driver)
            print(f"🧾 Encontrados {len(cards)} productos en la grilla de la página {page}")
            if not cards:
                print("⚠️  Sin tarjetas; deteniendo.")
                break

            for i, (href, title) in enumerate(cards, start=1):
                print(f"   {i:02d}/{len(cards)} -> abrir: {title or '(sin título)'} | {href}")

                ok = smart_open_product(driver, url, href, timeout=WAIT, retries=2)
                if not ok:
                    print("   ⚠️  No navegó al detalle (timeout). Sigo con el siguiente.")
                    continue

                try:
                    row = extract_product_fields(driver)
                    if not row or not (row.get("nombre") or row.get("codigo_interno")):
                        print("      ⚠️  Extracción vacía; skipping.")
                    else:
                        code = (row.get("codigo_interno") or "").strip()
                        if code and code in seen_codes:
                            print(f"      ℹ️  Duplicado cod {code}; skipping.")
                        else:
                            data.append(row)
                            if code:
                                seen_codes.add(code)
                            precolog = to_price_text(row.get("precio_lista")) or "-"
                            print(f"      ✅ {row.get('nombre','(sin título)')} | ${precolog} | cod {code or '-'}")
                except Exception as e:
                    print(f"      ⚠️  Error extrayendo: {e}")

                time.sleep(HUMAN_SLEEP)
                try:
                    driver.back()
                    wait_cards(driver)
                except WebDriverException:
                    try:
                        driver.get(url)
                        wait_cards(driver)
                    except WebDriverException:
                        pass
                time.sleep(0.1)

            time.sleep(0.4)

        if not data:
            print("\nℹ️ No se recolectó ningún dato.")
            return

        capturado_en = datetime.now()

        conn = None
        try:
            conn = get_conn()
            conn.autocommit = False
            cur = conn.cursor()

            tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

            insertados = 0
            for p in data:
                producto_id = find_or_create_producto(cur, p)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                insertados += 1

            conn.commit()
            print(f"\n💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

        except MySQLError as e:
            if conn: conn.rollback()
            print(f"❌ Error MySQL: {e}")
        finally:
            try:
                if conn: conn.close()
            except Exception:
                pass

        try:
            df = pd.DataFrame(data)
            cols = [
                "nombre","marca","categoria","subcategoria",
                "precio_lista","precio_txt","precio_unitario","precio_unitario_txt",
                "precio_sin_impuestos","precio_sin_impuestos_txt",
                "codigo_interno","imagen_url","url"
            ]
            df = df.reindex(columns=cols)
            df.to_excel(OUT_XLSX, index=False)
            print(f"💾 Exportado: {OUT_XLSX} | filas: {len(df)}")
        except Exception as e:
            print(f"⚠️ Error exportando XLSX: {e}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print(f"⏱️ Tiempo total: {time.time() - t0:.2f} s")

if __name__ == "__main__":
    # en VPS normalmente True (headless)
    run(headless=True)
