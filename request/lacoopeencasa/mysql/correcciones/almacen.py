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
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException,
    ElementClickInterceptedException, InvalidSelectorException, WebDriverException
)
from mysql.connector import Error as MySQLError
import logging  # logging opcional

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
PAGE_END = 20000             # sube este valor para cubrir más páginas
WAIT = 25
HUMAN_SLEEP = 0.2
OUT_XLSX = "la_coope_almacen.xlsx"

# Listado: ya no hay <a>; las tarjetas están en un web component Angular
CARD_CONTAINER_SELECTOR = "col-listado-articulo .card.hoverable"
CARD_TITLE_SELECTOR = ".card-descripcion p.ajustar-altura-texto-desc"  # texto de la tarjeta
CARD_CODE_RX = re.compile(r"(?:imagen|descripcion)?\s*([0-9]{3,})")     # imagen292718 -> 292718

MONEY_RX = re.compile(r"[^\d,.\-]")

# ---- Identidad de tienda ----
TIENDA_CODIGO = "la_coope"
TIENDA_NOMBRE = "La Coope en Casa"

# ------------------ Driver ------------------
def make_driver(headless=False):
    print(f"🚗 [make_driver] Inicializando ChromeDriver | headless={headless}")
    logging.info("Inicializando ChromeDriver (headless=%s)", headless)
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
    logging.info("Driver listo")
    return driver

# ------------------ Helpers gen ------------------
def clean(val):
    if val is None: return None
    s = str(val).strip()
    return s if s else None

def money_to_float(txt: str):
    """Convierte '$4.099,00' -> 4099.00; devuelve None si no puede."""
    try:
        if not txt:
            return None
        t = MONEY_RX.sub("", txt).strip()
        t = t.replace(".", "").replace(",", ".")
        v = float(t)
        return v
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
    logging.info("Esperando tarjetas (timeout=%s)", timeout)
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CARD_CONTAINER_SELECTOR))
        )
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_any_elements_located((By.CSS_SELECTOR, CARD_CONTAINER_SELECTOR))
        )
        print("✅ [wait_cards] Tarjetas presentes y visibles")
        logging.info("Tarjetas presentes y visibles")
        return True
    except TimeoutException:
        print("⚠️ [wait_cards] Timeout esperando tarjetas (revisar selector o lazy render)")
        logging.warning("Timeout esperando tarjetas")
        return False

def _try_click(driver, el, label=""):
    """Hace scroll al centro y click vía JS; si falla, intenta ActionChains y .click()."""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.06)
    except Exception:
        pass
    try:
        driver.execute_script("arguments[0].click();", el)
        print(f"🖱️ [_try_click] JS click OK en {label}")
        return True
    except Exception as e_js:
        print(f"⚠️ [_try_click] JS click falló en {label}: {e_js}")
        try:
            ActionChains(driver).move_to_element(el).pause(0.05).click(el).perform()
            print(f"🖱️ [_try_click] ActionChains click OK en {label}")
            return True
        except Exception as e_ac:
            print(f"⚠️ [_try_click] ActionChains falló en {label}: {e_ac}")
            try:
                el.click()
                print(f"🖱️ [_try_click] .click() directo OK en {label}")
                return True
            except Exception as e_direct:
                print(f"❌ [_try_click] .click() directo falló en {label}: {e_direct}")
                return False

def open_card_by_click(driver, list_url: str, card_el, timeout: int = 20) -> bool:
    """
    Intenta abrir el detalle clickeando elementos dentro de la tarjeta:
    1) imagen dentro de .card-image
    2) el div con id^='imagen'
    3) la descripción (id^='descripcion')
    4) la card completa
    Valida apertura esperando h1.articulo-detalle-titulo o url con '/producto/'.
    """
    candidates = [
        (".card-image img", "card-image img"),
        ("[id^='imagen']", "div[id^='imagen']"),
        ("[id^='descripcion']", "div[id^='descripcion']"),
        (".card", "card root"),
    ]

    for css, label in candidates:
        try:
            target = card_el.find_element(By.CSS_SELECTOR, css)
        except Exception:
            continue

        print(f"🔎 [open_card_by_click] Probando click en: {label}")
        if not _try_click(driver, target, label):
            continue

        # validar apertura
        opened = False
        t0 = time.time()
        while time.time() - t0 < timeout:
            cur = ""
            try:
                cur = driver.current_url
            except Exception:
                pass
            # condición 1: título del detalle presente
            try:
                if driver.find_elements(By.CSS_SELECTOR, ".articulo-detalle-titulo"):
                    opened = True
                    break
            except Exception:
                pass
            # condición 2: url contiene /producto/
            if "/producto/" in (cur or ""):
                opened = True
                break
            time.sleep(0.2)

        if opened:
            print(f"✅ [open_card_by_click] Detalle abierto haciendo click en: {label}")
            return True
        else:
            print(f"⚠️ [open_card_by_click] Click en {label} no llevó a detalle; probando siguiente target…")

    # si ninguno abrió, devolvemos False
    print("❌ [open_card_by_click] No se pudo abrir el detalle desde la tarjeta por click")
    return False

def get_list_cards(driver):
    """
    Devuelve la lista de WebElements de tarjetas del listado (para iterar y hacer click).
    """
    print("🔎 [get_list_cards] Buscando tarjetas en la grilla…")
    try:
        cards = driver.find_elements(By.CSS_SELECTOR, CARD_CONTAINER_SELECTOR)
    except WebDriverException as e:
        print(f"⚠️ [get_list_cards] Error buscando tarjetas: {e}")
        logging.exception("Error buscando tarjetas")
        return []

    print(f"📦 [get_list_cards] Tarjetas detectadas: {len(cards)}")
    return cards

# ------------------ Parsing de detalle ------------------
def safe_select_text(soup, selector, attr=None):
    el = soup.select_one(selector)
    if not el:
        return ""
    if attr:
        return (el.get(attr) or "").strip()
    return el.get_text(" ", strip=True)

def extract_product_fields(driver):
    """Desde la página de detalle ya cargada, extrae los campos solicitados."""
    print("🧪 [extract_product_fields] Extrayendo campos del detalle")
    logging.info("Extrayendo campos del detalle")
    try:
        WebDriverWait(driver, WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".articulo-detalle-titulo"))
        )
    except TimeoutException:
        print("⚠️ [extract_product_fields] Timeout esperando título de detalle")
        logging.warning("Timeout esperando título de detalle")

    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        print(f"❌ [extract_product_fields] Error obteniendo page_source/BS4: {e}")
        logging.exception("Error preparando soup")
        return {}

    titulo = safe_select_text(soup, "h1.articulo-detalle-titulo")

    # Marca y categoría (enlaces con h2)
    marca = safe_select_text(soup, ".articulo-detalle-marca h2")
    categoria = ""
    try:
        bloques_marca = soup.select(".articulo-detalle-marca")
        if len(bloques_marca) >= 2:
            h2 = bloques_marca[1].select_one("h2")
            if h2:
                categoria = h2.get_text(" ", strip=True)
    except Exception as e:
        print(f"⚠️ [extract_product_fields] No se pudo derivar subcategoria: {e}")

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
        "subcategoria": categoria,  # esta página no lo muestra claro
        "precio_lista": precio,              # usamos precio normal como lista
        "precio_oferta": None,               # si detectas promos, setea aquí
        "tipo_oferta": "",                   # si detectas texto de promo, setéalo
        "promo_tipo": "",
        "precio_regular_promo": "",
        "precio_descuento": "",
        "comentarios_promo": "",
        "codigo_interno": codigo_interno,    # lo usaremos como sku_tienda
        "ean": None,                         # este site no lo expone
        "fabricante": "",                    # no disponible
        "url": url_producto,
        "imagen_url": imagen_url,
        "precio_txt": precio_txt,
        "precio_unitario": precio_unitario,
        "precio_unitario_txt": precio_unitario_txt,
        "precio_sin_impuestos": precio_sin_impuestos,
        "precio_sin_impuestos_txt": precio_sin_imp_txt,
    }
    print(f"🧾 [extract_product_fields] Extraído: nombre='{out.get('nombre','')[:50]}' | cod='{out.get('codigo_interno','')}' | precio='{out.get('precio_lista')}'")
    logging.info("Campos extraidos ok")
    return out

# ------------------ BD (upserts) ------------------
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    print(f"🏬 [upsert_tienda] Upsert tienda codigo='{codigo}', nombre='{nombre}'")
    logging.info("Upsert tienda %s", codigo)
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
    sku = clean(p.get("codigo_interno"))    # usamos código interno como sku_tienda

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

    # Sin sku: registramos por URL/nombre
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, p.get("url") or "", p.get("nombre") or ""))
    ptid = cur.lastrowid
    print(f"✅ [upsert_producto_tienda] pt_id={ptid} (sin sku)")
    return ptid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: dict, capturado_en: datetime):
    print(f"🧮 [insert_historico] Insertando histórico pt_id={producto_tienda_id}")
    # Si no detectamos oferta, guardamos precio_lista y dejamos oferta NULL
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

            # pequeño scroll para disparar lazy loading si aplica
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.25);")
            except WebDriverException as e:
                print(f"⚠️ [run] Scroll inicial falló: {e}")
            time.sleep(0.3)

            cards = get_list_cards(driver)
            print(f"🧾 Encontrados {len(cards)} productos en la grilla de la página {page}")
            if not cards:
                print("⚠️  [run] Sin tarjetas; deteniendo.")
                break

            for i, card in enumerate(cards, start=1):
                # (opcional) log del título en tarjeta
                try:
                    title = card.find_element(By.CSS_SELECTOR, CARD_TITLE_SELECTOR).text.strip()
                except Exception:
                    title = ""
                print(f"   {i:02d}/{len(cards)} -> intentar click: {title or '(sin título)'}")

                ok = open_card_by_click(driver, url, card, timeout=WAIT)
                if not ok:
                    print("   ⚠️  [run] No se abrió el detalle por click. Sigo con la siguiente tarjeta.")
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
                # volver al listado
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
