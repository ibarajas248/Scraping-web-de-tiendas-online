#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Anónima (categoría -> detalle) → MySQL + PROXY

 todo el tráfico del navegador pasa por un proxy.
- Soporta:
    * Backconnect residencial/móvil (una sola URL con rotación automática en el proveedor)
    * Lista de proxies con fallback/rotación local
- Usa selenium-wire para manejar autenticación de proxy sin extensiones.
- Si detecta bloqueo (403/timeout/sin cards) rota de proxy y reintenta.

Requisitos:
  pip install selenium-wire selenium webdriver-manager beautifulsoup4 lxml pandas numpy mysql-connector-python
"""

import re
import time
import html
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urljoin
from datetime import datetime

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

# 🔁 Selenium + Proxy (selenium-wire)
from seleniumwire import webdriver as sw_webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from mysql.connector import Error as MySQLError
from base_datos import get_conn  # <- tu conexión MySQL

# ================= Config scraping =================
BASE = "https://supermercado.laanonimaonline.com"
START = f"{BASE}/almacen/n1_1/pag/1/"

HEADLESS = True
PAGE_LOAD_TIMEOUT = 25
IMPLICIT_WAIT = 2
SCROLL_PAUSES = [300, 600, 900]
SLEEP_BETWEEN_PAGES = 1.2
SLEEP_BETWEEN_PRODUCTS = 0.8
MAX_PAGES: Optional[int] = None  # None = todas

TIENDA_CODIGO = "laanonima"
TIENDA_NOMBRE = "La Anónima Online"

# ================ PROXY ================
# Opción A: backconnect (rotación del proveedor)
#   Formato: scheme://USER:PASS@HOST:PORT
BACKCONNECT_PROXY: Optional[str] = None  # ej. "http://user:pass@br.backconnect.proxy:12345"

# Opción B: pool local para rotar cuando haya bloqueo
PROXY_POOL: List[str] = [
    # Rellena estos si no usas backconnect. Ejemplos:
    # "http://user:pass@res.proxy-1:8000",
    # "http://user:pass@res.proxy-2:8000",
]
MAX_PROXY_RETRIES = 5  # cuántos intentos totales (rotando) por fase

def build_seleniumwire_options(proxy_url: str) -> dict:
    """
    Devuelve seleniumwire_options con auth si corresponde.
    """
    if not proxy_url:
        return {}  # sin proxy

    # Acepta http/https para ambos esquemas
    return {
        'proxy': {
            'http': proxy_url,
            'https': proxy_url,
            'no_proxy': 'localhost,127.0.0.1'
        },
        # si el proxy hace MITM con cert propio y no quieres validar:
        'verify_ssl': False
    }

def setup_driver_with_proxy(proxy_url: Optional[str]) -> sw_webdriver.Chrome:
    """Crea un Chrome con (o sin) proxy."""
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,1800")
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--allow-running-insecure-content")
    opts.add_argument("--mute-audio")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())

    sw_options = build_seleniumwire_options(proxy_url or "")
    driver = sw_webdriver.Chrome(service=service, options=opts, seleniumwire_options=sw_options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.implicitly_wait(IMPLICIT_WAIT)
    return driver

# ================= Utils Selenium =================
def wait_dom(driver, css: str, timeout: int = 15):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, css)))

def try_click_cookies(driver):
    for sel in [
        "button#onetrust-accept-btn-handler",
        "button[aria-label='Aceptar']",
        "button.cookie-accept",
        "div.cookie a.btn, div.cookie button",
    ]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if el.is_displayed():
                el.click()
                time.sleep(0.2)
                return
        except Exception:
            pass

def smooth_scroll(driver):
    for y in SCROLL_PAUSES:
        driver.execute_script(f"window.scrollTo(0, {y});")
        time.sleep(0.3)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(0.4)

def get_list_links_from_page(driver) -> List[str]:
    soup = BeautifulSoup(driver.page_source, "lxml")
    links = []
    for card in soup.select("div.producto.item a[href*='/almacen/'][href*='/art_']"):
        href = card.get("href") or ""
        if not href:
            continue
        full = urljoin(BASE, href)
        if "/art_" in full and full not in links:
            links.append(full)
    return links

def get_next_page_url(current_url: str, soup: BeautifulSoup) -> Optional[str]:
    m = re.search(r"/pag/(\d+)/", current_url)
    if not m:
        return None
    cur = int(m.group(1))
    guess = re.sub(r"/pag/\d+/", f"/pag/{cur + 1}/", current_url)
    a = soup.select_one(f"a[href*='/pag/{cur + 1}/']")
    return urljoin(BASE, a["href"]) if a and a.has_attr("href") else guess

def parse_price_text(txt: str) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip().replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None

def text_or_none(node) -> Optional[str]:
    if not node:
        return None
    return re.sub(r"\s+", " ", node.get_text(strip=True)) or None

def parse_detail(html_source: str, url: str) -> Dict:
    soup = BeautifulSoup(html_source, "lxml")

    nombre = text_or_none(soup.select_one("h1.titulo_producto.principal"))

    cod_txt = text_or_none(soup.select_one("div.codigo"))  # "Cod. 3115185"
    sku = None
    if cod_txt:
        m = re.search(r"(\d+)", cod_txt)
        if m:
            sku = m.group(1)

    sku_hidden = soup.select_one("input[id^='sku_item_imetrics_'][value]")
    if sku_hidden and not sku:
        sku = sku_hidden.get("value")

    id_item = None
    id_item_hidden = soup.select_one("input#id_item[value], input[id^='id_item_'][value]")
    if id_item_hidden:
        id_item = id_item_hidden.get("value")

    marca_hidden = soup.select_one("input[id^='brand_item_imetrics_'][value]")
    marca = marca_hidden.get("value") if marca_hidden else None

    cat_hidden = soup.select_one("input[id^='categorias_item_imetrics_'][value]")
    categorias = None
    if cat_hidden:
        categorias = html.unescape(cat_hidden.get("value") or "").strip()

    descripcion = text_or_none(soup.select_one("div.descripcion div.texto"))

    precio_lista = None
    caja_lista = soup.select_one("div.precio.anterior")
    if caja_lista:
        precio_lista = parse_price_text(caja_lista.get_text(" ", strip=True))
    else:
        alt = soup.select_one(".precio_complemento .precio.destacado, .precio.destacado")
        if alt:
            precio_lista = parse_price_text(alt.get_text(" ", strip=True))

    precio_plus = None
    plus_node = soup.select_one(".precio-plus .precio b, .precio-plus .precio")
    if plus_node:
        precio_plus = parse_price_text(plus_node.get_text(" ", strip=True))

    imagenes = []
    for im in soup.select("#img_producto img[src], #galeria_img img[src]"):
        src = im.get("src")
        if src and src not in imagenes:
            imagenes.append(src)

    return {
        "url": url,
        "nombre": nombre,
        "sku": sku,
        "id_item": id_item,
        "marca": marca,
        "categorias": categorias,
        "precio_lista": precio_lista,
        "precio_plus": precio_plus,
        "descripcion": descripcion,
        "imagenes": " | ".join(imagenes) if imagenes else None,
    }

# ------- detección de bloqueo -------
def looks_blocked(driver) -> bool:
    """
    Heurística ligera para detectar bloqueo o respuesta inválida:
      - HTTP 403 en últimos requests
      - No hay cards de producto en listado
      - Página de error/captcha
    """
    try:
        # Revisa las últimas respuestas (selenium-wire)
        for req in reversed(driver.requests[-10:]):
            if req.response and req.response.status_code in (403, 429, 503):
                return True
    except Exception:
        pass

    src = driver.page_source.lower()
    if any(p in src for p in ["captcha", "access denied", "forbidden", "error 403"]):
        return True
    # Si estamos en listado y no hay productos visibles
    try:
        soup = BeautifulSoup(driver.page_source, "lxml")
        cards = soup.select("div.producto.item")
        if len(cards) == 0:
            return True
    except Exception:
        pass
    return False

def grab_category_with_rotation(start_url: str) -> List[Dict[str, Any]]:
    """
    Intenta scrapear la categoría rotando proxy si hay bloqueo.
    Retorna filas agregadas (detalle de producto).
    """
    attempts = 0

    # Fuente de proxy: backconnect fijo o pool
    candidate_proxies: List[Optional[str]] = []
    if BACKCONNECT_PROXY:
        candidate_proxies = [BACKCONNECT_PROXY] * MAX_PROXY_RETRIES
    else:
        candidate_proxies = (PROXY_POOL or [None]) * MAX_PROXY_RETRIES  # si no hay proxy, intenta sin

    last_error: Optional[str] = None

    for proxy_url in candidate_proxies:
        attempts += 1
        print(f"[PROXY] Intento {attempts}/{MAX_PROXY_RETRIES} usando: {proxy_url or 'SIN PROXY'}")
        driver = None
        try:
            driver = setup_driver_with_proxy(proxy_url)
            rows = grab_category(driver, start_url)
            # si bloqueó, rows será vacío o muy bajo; revisa también heurística
            if not rows or looks_blocked(driver):
                last_error = "Bloqueo o respuesta vacía."
                print("  ⚠️ Posible bloqueo con esta IP. Rotando…")
                continue
            return rows
        except Exception as e:
            last_error = str(e)
            print(f"  ⚠️ Error en intento con proxy {proxy_url}: {e}")
            continue
        finally:
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass

    print(f"❌ No se pudo scrapear tras {attempts} intentos. Último error: {last_error}")
    return []

# ==============================================
#  (tu lógica de paginado y parseo por página)
# ==============================================
def grab_category(driver, start_url: str) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    visited = set()
    page_url = start_url
    page_idx = 0

    while True:
        page_idx += 1
        if MAX_PAGES is not None and page_idx > MAX_PAGES:
            break

        try:
            driver.get(page_url)
        except (TimeoutException, WebDriverException):
            try:
                driver.get(page_url)
            except Exception:
                print(f"⚠️ No se pudo cargar: {page_url}")
                break

        try_click_cookies(driver)
        try:
            wait_dom(driver, "div.producto.item")
        except TimeoutException:
            print(f"⚠️ Sin productos visibles en: {page_url}")
            break

        smooth_scroll(driver)
        time.sleep(0.3)
        links = get_list_links_from_page(driver)
        print(f"🔗 P{page_idx} {len(links)} productos - {page_url}")
        if not links:
            break

        for href in links:
            if href in visited:
                continue
            visited.add(href)
            try:
                driver.get(href)
                wait_dom(driver, "h1.titulo_producto.principal")
                time.sleep(0.25)
                row = parse_detail(driver.page_source, href)
                all_rows.append(row)
                print(f"  ✔ {row.get('nombre') or ''} [{row.get('sku') or ''}]")
                time.sleep(SLEEP_BETWEEN_PRODUCTS)
            except TimeoutException:
                print(f"  ⚠️ Timeout detalle: {href}")
            except Exception as e:
                print(f"  ⚠️ Error detalle: {href} -> {e}")

        soup = BeautifulSoup(driver.page_source, "lxml")
        next_url = get_next_page_url(page_url, soup)
        if not next_url or next_url == page_url:
            break
        page_url = next_url
        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_rows

# ================= MySQL helpers =================
def clean_txt(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def price_to_varchar(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        v = float(x)
        if np.isnan(v):
            return None
        return f"{round(v, 2)}"
    except Exception:
        s = str(x).strip()
        return s if s else None

def split_categoria(categorias: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Parte la cadena 'A > B > C' en (A, B)."""
    if not categorias:
        return None, None
    parts = [p.strip() for p in re.split(r">|›", categorias) if p.strip()]
    cat = parts[0] if len(parts) > 0 else None
    sub = parts[1] if len(parts) > 1 else None
    return cat, sub

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    """
    No hay EAN → ean=NULL.
    Match preferente por (nombre, marca). Fallback por nombre.
    Guarda categoría/subcategoría derivadas de 'categorias'.
    """
    nombre = clean_txt(r.get("nombre"))
    marca = clean_txt(r.get("marca"))
    cat, sub = split_categoria(clean_txt(r.get("categorias")))

    if nombre and marca:
        cur.execute("SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1", (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (cat or "", sub or "", pid))
            return pid

    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  marca = COALESCE(NULLIF(%s,''), marca),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (marca or "", cat or "", sub or "", pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULLIF(%s,''), NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (nombre or "", marca or "", cat or "", sub or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    """
    Clave natural preferida: (tienda_id, sku_tienda=sku).
    Respaldo: (tienda_id, record_id_tienda=id_item) o (tienda_id, url_tienda).
    """
    sku = clean_txt(r.get("sku"))
    record_id = clean_txt(r.get("id_item"))
    url = clean_txt(r.get("url"))
    nombre_tienda = clean_txt(r.get("nombre"))

    if sku:
        cur.execute("""
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

    if record_id:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, record_id, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: datetime):
    """
    Mapeo precios:
      - precio_lista: el 'precio principal' del detalle
      - precio_plus: si está presente, se guarda como 'precio_oferta' con tipo_oferta='PLUS'
    """
    precio_lista = price_to_varchar(r.get("precio_lista"))
    precio_plus = price_to_varchar(r.get("precio_plus"))
    tipo_oferta = "PLUS" if precio_plus is not None else None

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
        precio_lista, precio_plus, tipo_oferta,
        None, None, None, None
    ))

# ================= Orquestación =================
def main():
    print("[INFO] Scrapeando La Anónima con proxy…")
    rows = grab_category_with_rotation(START)

    if not rows:
        print("[INFO] No se extrajeron registros (bloqueo persistente o sin datos).")
        return

    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for r in rows:
            producto_id = find_or_create_producto(cur, r)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, r)
            insert_historico(cur, tienda_id, pt_id, r, capturado_en)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn:
            conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
