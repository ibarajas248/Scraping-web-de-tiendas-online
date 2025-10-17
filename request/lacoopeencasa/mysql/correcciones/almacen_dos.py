# -*- coding: utf-8 -*-
# Requisitos:
#   pip install selenium webdriver-manager beautifulsoup4 pandas mysql-connector-python numpy beautifulsoup4

import time
import re
import json
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException,
    ElementClickInterceptedException, InvalidSelectorException, WebDriverException
)
from mysql.connector import Error as MySQLError
import logging

from base_datos import get_conn  # debes tenerlo configurado

# ------------------ Logging ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
print("🔧 Logging configurado a nivel INFO")

# ------------------ Config ------------------
BASE = "https://www.lacoopeencasa.coop"
CATEGORY = "almacen"
CAT_ID = 2

PAGE_START = 1
PAGE_END = 20000
WAIT = 25
HUMAN_SLEEP = 0.2
OUT_XLSX = "la_coope_almacen.xlsx"

# NUEVO: selectores del listado (no hay <a>, es web component)
CARD_CONTAINER_SELECTOR = "col-listado-articulo .card.hoverable"
CARD_TITLE_SELECTOR = ".card-descripcion p.ajustar-altura-texto-desc"  # nombre en la tarjeta

MONEY_RX = re.compile(r"[^\d,.\-]")
CARD_CODE_RX = re.compile(r"(?:imagen|descripcion)?\s*([0-9]{3,})")  # imagen292718 -> 292718

# ---- Identidad de tienda ----
TIENDA_CODIGO = "la_coope"
TIENDA_NOMBRE = "La Coope en Casa"

# ------------------ Driver ------------------
def make_driver(headless=False):
    print(f"🚗 [make_driver] Inicializando ChromeDriver | headless={headless}")
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=es-AR")
    options.add_argument("--window-size=1366,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(45)
    print("✅ [make_driver] Driver listo")
    return driver

# ------------------ Helpers gen ------------------
def clean(val):
    if val is None: return None
    s = str(val).strip()
    return s if s else None

def money_to_float(txt: str):
    try:
        if not txt:
            return None
        t = MONEY_RX.sub("", txt).strip()
        t = t.replace(".", "").replace(",", ".")
        return float(t)
    except Exception as e:
        print(f"⚠️ [money_to_float] No se pudo convertir '{txt}': {e}")
        logging.exception("money_to_float fallo con txt=%s", txt)
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

# ------------------ LISTADO ------------------
def wait_cards(driver, timeout=WAIT):
    print(f"⏳ [wait_cards] Esperando tarjetas selector='{CARD_CONTAINER_SELECTOR}', timeout={timeout}")
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CARD_CONTAINER_SELECTOR))
        )
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_any_elements_located((By.CSS_SELECTOR, CARD_CONTAINER_SELECTOR))
        )
        print("✅ [wait_cards] Tarjetas presentes y visibles")
        return True
    except TimeoutException:
        print("⚠️ [wait_cards] Timeout esperando tarjetas (revisar selector o lazy render)")
        return False

def _extract_code_from_card(card_el):
    """Toma ids como imagen292718 / descripcion292718 dentro de la tarjeta y devuelve 292718."""
    try:
        ids = []
        for css in ("[id^='imagen']", "[id^='descripcion']"):
            try:
                node = card_el.find_element(By.CSS_SELECTOR, css)
                if node:
                    vid = node.get_attribute("id") or ""
                    if vid:
                        ids.append(vid)
            except Exception:
                pass
        if not ids:
            descendants = card_el.find_elements(By.CSS_SELECTOR, "*[id]")
            for d in descendants:
                vid = d.get_attribute("id") or ""
                if vid:
                    ids.append(vid)
        for vid in ids:
            m = CARD_CODE_RX.search(vid)
            if m:
                code = m.group(1)
                if code:
                    print(f"🏷️ [_extract_code_from_card] id='{vid}' -> code={code}")
                    return code
    except Exception as e:
        print(f"⚠️ [_extract_code_from_card] Error: {e}")
    return ""

def get_page_cards(driver):
    """
    Devuelve lista de (codigo, title). La URL se construye luego.
    """
    print("🔎 [get_page_cards] Buscando tarjetas en el listado…")
    try:
        cards = driver.find_elements(By.CSS_SELECTOR, CARD_CONTAINER_SELECTOR)
    except WebDriverException as e:
        print(f"⚠️ [get_page_cards] Error buscando tarjetas: {e}")
        return []

    items = []
    print(f"📦 [get_page_cards] Tarjetas detectadas: {len(cards)}")
    for idx, card in enumerate(cards, start=1):
        try:
            title = ""
            try:
                title_el = card.find_element(By.CSS_SELECTOR, CARD_TITLE_SELECTOR)
                title = (title_el.text or "").strip()
            except Exception:
                pass
            code = _extract_code_from_card(card)
            items.append((code, title))
            print(f"   ↪️ Card {idx:02d}: title='{title[:60]}', code='{code or '-'}'")
        except Exception as e:
            print(f"⚠️ [get_page_cards] Error card {idx}: {e}")
            continue
    return items

# ------------------ Navegación a detalle ------------------
DETAIL_PATHS = [
    "/producto/{id}",
    "/articulo/{id}",
    "/detalle/{id}",
]

def smart_open_product(driver, list_url: str, code: str, timeout: int = 25, retries: int = 2) -> bool:
    """
    No hay <a> clickeable en el listado. Construimos href probando rutas comunes.
    """
    print(f"🧭 [smart_open_product] code={code or '(vacío)'} | retries={retries}")
    if not code:
        print("❌ [smart_open_product] No hay code; no se puede abrir detalle")
        return False

    for attempt in range(retries + 1):
        for path in DETAIL_PATHS:
            href = f"{BASE}{path.format(id=code)}"
            print(f"🌐 [smart_open_product] Intento {attempt+1}/{retries+1} -> GET {href}")
            try:
                driver.get(href)
            except WebDriverException as e:
                print(f"❌ [smart_open_product] driver.get falló: {e}")
                continue

            try:
                # condición robusta: título de detalle presente
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".articulo-detalle-titulo"))
                )
                print(f"      ↪️  Detalle abierto en {href}")
                return True
            except TimeoutException:
                print(f"      ⚠️  No cargó detalle con {path}; probando siguiente ruta…")
                continue

        # si ninguna ruta abrió, volver al listado y reintentar
        try:
            driver.get(list_url)
            wait_cards(driver, timeout)
        except WebDriverException as e:
            print(f"⚠️ [smart_open_product] Error volviendo al listado: {e}")
        time.sleep(0.3 + attempt * 0.3)

    print("❌ [smart_open_product] Agotados los intentos para abrir detalle")
    return False

# ------------------ Parsing de detalle ------------------
def safe_select_text(soup, selector, attr=None):
    el = soup.select_one(selector)
    if not el:
        return ""
    if attr:
        return (el.get(attr) or "").strip()
    return el.get_text(" ", strip=True)

def extract_product_fields(driver):
    """Extrae campos desde la página de detalle (según el DOM que enviaste)."""
    print("🧪 [extract_product_fields] Extrayendo campos del detalle")
    try:
        WebDriverWait(driver, WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".articulo-detalle-titulo"))
        )
    except TimeoutException:
        print("⚠️ [extract_product_fields] Timeout esperando título de detalle")

    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        print(f"❌ [extract_product_fields] Error obteniendo page_source/BS4: {e}")
        return {}

    titulo = safe_select_text(soup, "h1.articulo-detalle-titulo")

    # Marca y categoría (están como h2 dentro de enlaces)
    marca = safe_select_text(soup, ".articulo-detalle-marca h2")
    categoria = ""
    try:
        bloques_marca = soup.select(".articulo-detalle-marca")
        if len(bloques_marca) >= 2:
            h2 = bloques_marca[1].select_one("h2")
            if h2:
                categoria = h2.get_text(" ", strip=True)
    except Exception:
        pass

    precio_txt = safe_select_text(soup, ".precio.precio-detalle")
    precio = money_to_float(precio_txt)

    precio_unitario_txt = safe_select_text(soup, ".precio-unitario")
    precio_unitario = None
    if precio_unitario_txt:
        m = re.search(r"\$?\s*([\d\.\,]+)", precio_unitario_txt)
        if m:
            precio_unitario = money_to_float(m.group(1))

    precio_sin_imp_txt = safe_select_text(soup, ".precio-sin-impuestos span:nth-of-type(2)")
    precio_sin_impuestos = money_to_float(precio_sin_imp_txt)

    # Código interno visible en el detalle
    codigo_interno = safe_select_text(soup, ".articulo-codigo span")

    # Imagen principal
    imagen_url = safe_select_text(soup, ".articulo-detalle-imagen-ppal", attr="src")
    if not imagen_url:
        imagen_url = safe_select_text(soup, ".articulo-detalle-imagen-contenedor img", attr="src")

    try:
        url_producto = driver.current_url
    except WebDriverException:
        url_producto = ""

    out = {
        "nombre": titulo,
        "marca": marca,
        "categoria": "almacen",
        "subcategoria": categoria,
        "precio_lista": precio,
        "precio_oferta": None,
        "tipo_oferta": "",
        "promo_tipo": "",
        "precio_regular_promo": "",
        "precio_descuento": "",
        "comentarios_promo": "",
        "codigo_interno": codigo_interno,
        "ean": None,
        "fabricante": "",
        "url": url_producto,
        "imagen_url": imagen_url,
        "precio_txt": precio_txt,
        "precio_unitario": precio_unitario,
        "precio_unitario_txt": precio_unitario_txt,
        "precio_sin_impuestos": precio_sin_impuestos,
        "precio_sin_impuestos_txt": precio_sin_imp_txt,
    }
    print(f"🧾 [extract_product_fields] Extraído: nombre='{out.get('nombre','')[:50]}' | cod='{out.get('codigo_interno','')}' | precio='{out.get('precio_lista')}'")
    return out

# ------------------ BD (upserts) ------------------
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    print(f"🏬 [upsert_tienda] Upsert tienda codigo='{codigo}', nombre='{nombre}'")
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    tid = cur.fetchone()[0]
    print(f"✅ [upsert_tienda] tienda_id={tid}")
    return tid

def find_or_create_producto(cur, p: dict) -> int:
    print("🔎 [find_or_create_producto] Buscando/creando producto…")
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
            print(f"✅ [find_or_create_producto] Match por EAN -> id={pid}")
            return pid

    # Sin EAN: match por (nombre, marca)
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
        print(f"✅ [find_or_create_producto] Match por (nombre,marca) -> id={pid}")
        return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", p.get("nombre") or "", p.get("marca") or "",
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    new_id = cur.lastrowid
    print(f"🆕 [find_or_create_producto] Insertado nuevo producto id={new_id}")
    return new_id

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: dict) -> int:
    print(f"🔗 [upsert_producto_tienda] Vinculando producto {producto_id} con tienda {tienda_id}")
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
        ptid = cur.fetchone()[0]
        print(f"✅ [upsert_producto_tienda] pt_id={ptid} (por sku)")
        return ptid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, p.get("url") or "", p.get("nombre") or ""))
    ptid = cur.lastrowid
    print(f"✅ [upsert_producto_tienda] pt_id={ptid} (sin sku)")
    return ptid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: dict, capturado_en: datetime):
    print(f"🧮 [insert_historico] Insertando histórico pt_id={producto_tienda_id}")
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
    print("✅ [insert_historico] Histórico upsert OK")

# ------------------ Main ------------------
def run(headless=False):
    print(f"🚀 [run] Inicio | headless={headless}")
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
                print(f"✅ [run] GET ok página {page}")
            except WebDriverException as e:
                print(f"⚠️  [run] No se pudo cargar la página {page}: {e}")
                continue

            if not wait_cards(driver):
                print("⚠️  [run] No se detectaron productos en esta página. Deteniendo.")
                break

            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.25);")
            except WebDriverException as e:
                print(f"⚠️ [run] Scroll inicial falló: {e}")
            time.sleep(0.3)

            cards = get_page_cards(driver)
            print(f"🧾 Encontrados {len(cards)} productos en la grilla de la página {page}")
            if not cards:
                print("⚠️  [run] Sin tarjetas; deteniendo.")
                break

            for i, (code, title) in enumerate(cards, start=1):
                print(f"   {i:02d}/{len(cards)} -> abrir: {title or '(sin título)'} | code={code or '(sin code)'}")

                ok = smart_open_product(driver, url, code, timeout=WAIT, retries=2)
                if not ok:
                    print("   ⚠️  [run] No navegó al detalle. Sigo con el siguiente.")
                    continue

                try:
                    row = extract_product_fields(driver)
                    if not row or not (row.get("nombre") or row.get("codigo_interno")):
                        print("      ⚠️  [run] Extracción vacía; skipping.")
                    else:
                        code_detail = (row.get("codigo_interno") or "").strip()
                        if code_detail and code_detail in seen_codes:
                            print(f"      ℹ️  [run] Duplicado cod {code_detail}; skipping.")
                        else:
                            data.append(row)
                            if code_detail:
                                seen_codes.add(code_detail)
                            precolog = to_price_text(row.get("precio_lista")) or "-"
                            print(f"      ✅ [run] {row.get('nombre','(sin título)')} | ${precolog} | cod {code_detail or '-'}")
                except Exception as e:
                    print(f"      ⚠️  [run] Error extrayendo: {e}")

                time.sleep(HUMAN_SLEEP)
                try:
                    driver.back()
                    wait_cards(driver)
                except WebDriverException as e:
                    print(f"⚠️ [run] driver.back() falló: {e} -> re-cargando listado")
                    try:
                        driver.get(url)
                        wait_cards(driver)
                    except WebDriverException as e2:
                        print(f"❌ [run] Falló re-cargar listado: {e2}")
                time.sleep(0.1)

            time.sleep(0.4)

        # ====== Inserción directa en MySQL ======
        print(f"\n📦 [run] Total filas recolectadas: {len(data)}")
        if not data:
            print("\nℹ️ [run] No se recolectó ningún dato.")
            return

        capturado_en = datetime.now()
        print(f"🕒 [run] capturado_en={capturado_en}")

        conn = None
        try:
            print("🔌 [run] Abriendo conexión MySQL…")
            conn = get_conn()
            conn.autocommit = False
            cur = conn.cursor()

            tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

            insertados = 0
            for idx, p in enumerate(data, start=1):
                print(f"➡️  [run] ({idx}/{len(data)}) Upserts/insert histórico")
                producto_id = find_or_create_producto(cur, p)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                insertados += 1

            conn.commit()
            print(f"\n💾 [run] Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

        except MySQLError as e:
            if conn:
                try:
                    conn.rollback()
                    print("↩️ [run] Rollback ejecutado por error MySQL")
                except Exception as e2:
                    print(f"⚠️ [run] Error en rollback: {e2}")
            print(f"❌ [run] Error MySQL: {e}")
        finally:
            try:
                if conn:
                    conn.close()
                    print("🔒 [run] Conexión MySQL cerrada")
            except Exception as e:
                print(f"⚠️ [run] Error cerrando conexión: {e}")

        # (opcional) export XLSX como respaldo local
        try:
            print("📝 [run] Exportando a XLSX…")
            df = pd.DataFrame(data)
            cols = [
                "nombre","marca","categoria","subcategoria",
                "precio_lista","precio_txt","precio_unitario","precio_unitario_txt",
                "precio_sin_impuestos","precio_sin_impuestos_txt",
                "codigo_interno","imagen_url","url"
            ]
            df = df.reindex(columns=cols)
            df.to_excel(OUT_XLSX, index=False)
            print(f"💾 [run] Exportado: {OUT_XLSX} | filas: {len(df)}")
        except Exception as e:
            print(f"⚠️ [run] Error exportando XLSX: {e}")

    finally:
        try:
            driver.quit()
            print("🛑 [run] Driver cerrado")
        except Exception as e:
            print(f"⚠️ [run] Error cerrando driver: {e}")

    total = time.time() - t0
    print(f"⏱️ [run] Tiempo total: {total:.2f} s")

if __name__ == "__main__":
    # Cambia a True si lo quieres headless en servidor
    run(headless=False)
