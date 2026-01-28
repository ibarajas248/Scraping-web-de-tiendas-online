#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DAR (darentucasa.com.ar) — Búsqueda por palabra clave + Ingesta MySQL (cron-friendly)

Flujo:
- Abre la página principal.
- Escribe una palabra clave en el buscador (input#cpoBuscar) y envía el form#FormBus.
- Pagina resultados (ul.listaProds li.cuadProd) y junta todos los productos.
- Columnas base: codigo, descripcion, precio, precio_texto, oferta, imagen,
                 cat_n0, cat_n2, cat_n3, cat_nombre
- Ingesta en tablas: tiendas, productos, producto_tienda, historico_precios

NOTAS:
  - Por defecto corre en headless. Para ver UI: --no-headless
  - Evita “user data dir in use” creando un perfil temporal de Chrome.
  - Debe existir base_datos.py con get_conn() -> mysql.connector.connect(...)
  - Sin lecturas de stdin / sin hilos: apto para cron.
"""

import os, sys, time, re, argparse, random, tempfile, shutil
from typing import Tuple, Dict, Any, List, Optional
from datetime import datetime as dt
from contextlib import contextmanager

import pandas as pd
import numpy as np

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
from selenium.webdriver.common.keys import Keys  # para ENTER en el buscador

# Usa webdriver_manager si está disponible; si no, intenta con chromedriver del PATH
try:
    from webdriver_manager.chrome import ChromeDriverManager
    _USE_WDM = True
except Exception:
    _USE_WDM = False

# ---------- MySQL ----------
import mysql.connector
from mysql.connector import errors as myerr

# ---------- Conexión (tu helper) ----------
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# =========================
# Config tienda
# =========================
TIENDA_CODIGO = "darentucasa"
TIENDA_NOMBRE = "Supermercados Dar"

URL  = "https://www.darentucasa.com.ar/login.asp"
WAIT = 25  # segundos

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
def _page_signature(driver) -> str:
    """Firma determinística de la página de resultados (dentro del frame de listado)."""
    try:
        lis = driver.find_elements(By.CSS_SELECTOR, "ul.listaProds li.cuadProd")
        codes = []
        for li in lis:
            c = extract_code_from(li)
            if c:
                codes.append(c)
        return "|".join(codes)
    except Exception:
        return ""

def _ensure_full_list_loaded(driver, min_loops: int = 2, max_loops: int = 8):
    """Carga perezosa estable: scrollea hasta estabilizar la cantidad de tarjetas."""
    last_count = -1
    loops = 0
    while loops < max_loops:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.35 + random.random()*0.20)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.15 + random.random()*0.10)

        cards = driver.find_elements(By.CSS_SELECTOR, "ul.listaProds li.cuadProd")
        count = len(cards)
        if count == last_count and loops >= min_loops:
            break
        last_count = count
        loops += 1

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
        if pd.isna(f) or np.isinf(f):
            return None
        if abs(f) > 999999999:
            return None
        return f"{round(f, 2):.2f}"
    except Exception:
        return None

# =========================
# SQL con reintentos ante locks
# =========================
LOCK_ERRNOS = {1205, 1213}  # lock wait timeout, deadlock

def exec_with_retry(cur, sql, params=None, max_retries=5, base_sleep=0.4):
    """Ejecuta una sentencia SQL con reintentos ante lock timeout/deadlock (sin commit)."""
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
_TEMP_PROFILE_DIR: Optional[str] = None

def _resolve_chromedriver_service() -> Service:
    """Permite usar CHROMEDRIVER_PATH si existe; si no, webdriver_manager; y si falla, Service() vacío."""
    path_env = os.environ.get("CHROMEDRIVER_PATH")
    if path_env and os.path.isfile(path_env):
        return Service(path_env)
    if _USE_WDM:
        try:
            return Service(ChromeDriverManager().install())
        except Exception as e:
            print(f"[WARN] webdriver_manager falló ({e}). Intentando Service() por defecto (requiere chromedriver en PATH).")
    return Service()  # requiere chromedriver en PATH

def setup_driver(headless: bool = True) -> webdriver.Chrome:
    global _TEMP_PROFILE_DIR
    _TEMP_PROFILE_DIR = tempfile.mkdtemp(prefix="dar_chrome_profile_")

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
    opts.add_argument("--disable-features=AutomationControlled,TranslateUI,CalculateNativeWinOcclusion")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument("--force-device-scale-factor=1")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument(f"--user-data-dir={_TEMP_PROFILE_DIR}")
    opts.add_argument("--profile-directory=Default")

    service = _resolve_chromedriver_service()
    driver = webdriver.Chrome(service=service, options=opts)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        })
    except Exception:
        pass
    return driver

def _cleanup_profile_dir():
    global _TEMP_PROFILE_DIR
    if _TEMP_PROFILE_DIR and os.path.isdir(_TEMP_PROFILE_DIR):
        try:
            shutil.rmtree(_TEMP_PROFILE_DIR, ignore_errors=True)
        except Exception:
            pass
    _TEMP_PROFILE_DIR = None

# =========================
# FRAME / WINDOW helpers
# =========================
def _switch_to_default(driver):
    try:
        driver.switch_to.default_content()
    except Exception:
        pass

def _switch_to_last_window(driver):
    try:
        handles = driver.window_handles
        if handles:
            driver.switch_to.window(handles[-1])
    except Exception:
        pass

def _find_frame_with_css(driver, css_selector: str) -> Optional[int]:
    """
    Busca un frame/iframe que contenga css_selector. Retorna:
      -1 = documento principal
      idx>=0 = índice del frame
      None = no encontrado
    """
    _switch_to_last_window(driver)
    _switch_to_default(driver)

    # documento principal
    try:
        WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
        return -1
    except TimeoutException:
        pass

    frames = driver.find_elements(By.TAG_NAME, "iframe") + driver.find_elements(By.TAG_NAME, "frame")
    for idx, fr in enumerate(frames):
        try:
            driver.switch_to.frame(fr)
            WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
            return idx
        except TimeoutException:
            _switch_to_default(driver)
            continue
        except Exception:
            _switch_to_default(driver)
            continue
    return None

@contextmanager
def switch_into_frame_with(driver, css_selector: str):
    """Context manager: entra al frame que contiene css_selector y vuelve al default al salir."""
    idx = _find_frame_with_css(driver, css_selector)
    entered = False
    try:
        if idx is None:
            yield False
        else:
            if idx == -1:
                entered = True
                yield True
            else:
                frames = driver.find_elements(By.TAG_NAME, "iframe") + driver.find_elements(By.TAG_NAME, "frame")
                if 0 <= idx < len(frames):
                    driver.switch_to.frame(frames[idx])
                    entered = True
                    yield True
                else:
                    yield False
    finally:
        if entered:
            _switch_to_default(driver)

# =========================
# Buscar por palabra clave
# =========================
def open_search_and_submit(driver: webdriver.Chrome, term: str):
    """
    Abre la URL principal, escribe `term` en el input de búsqueda y envía el formulario.
    Luego deja el driver en el contexto donde existe ul.listaProds.
    """
    driver.get(URL)
    time.sleep(0.8)
    _switch_to_last_window(driver)

    # Buscar el input dentro de form#FormBus (puede estar en principal o en un frame)
    with switch_into_frame_with(driver, "form#FormBus input#cpoBuscar") as ok:
        if not ok:
            # Intento directo en el documento principal
            try:
                inp = WebDriverWait(driver, WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#cpoBuscar"))
                )
            except TimeoutException:
                raise TimeoutException("No se encontró el input de búsqueda #cpoBuscar")

            try:
                form = driver.find_element(By.ID, "FormBus")
            except Exception:
                form = None
        else:
            inp = WebDriverWait(driver, WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input#cpoBuscar"))
            )
            try:
                form = driver.find_element(By.ID, "FormBus")
            except Exception:
                form = None

        # Escribir término de búsqueda
        inp.clear()
        inp.send_keys(term)

        # Enviar el formulario
        try:
            if form is not None:
                driver.execute_script("arguments[0].submit();", form)
            else:
                inp.send_keys(Keys.ENTER)
        except Exception:
            inp.send_keys(Keys.ENTER)

    # Esperar a que aparezca el listado de productos
    time.sleep(0.8)
    _switch_to_last_window(driver)
    with switch_into_frame_with(driver, "ul.listaProds") as ok2:
        if not ok2:
            time.sleep(0.8)
            with switch_into_frame_with(driver, "ul.listaProds") as ok3:
                if not ok3:
                    raise TimeoutException("No se encontró el listado de productos (ul.listaProds) después de buscar.")
    time.sleep(0.4)

# =========================
# Extracción y paginación (resultados de búsqueda)
# =========================
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

def collect_page_products(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    """Extrae todos los productos de la página actual de resultados de búsqueda."""
    items = []
    with switch_into_frame_with(driver, "ul.listaProds") as ok:
        if not ok:
            print("⚠ No se pudo entrar al frame del listado.")
            return items
        ul = WebDriverWait(driver, WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds"))
        )
        _ensure_full_list_loaded(driver)
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

def click_next(driver: webdriver.Chrome, prev_sig: Optional[str]) -> bool:
    """Clic en botón 'Siguiente' de la paginación (si existe)."""
    with switch_into_frame_with(driver, "ul.listaProds") as ok:
        if not ok:
            return False
        try:
            btn = driver.find_element(
                By.XPATH,
                "//input[contains(@class,'PagArt') and (contains(@value,'Siguiente') or contains(@value,'Sig') or contains(@value,'>'))]"
            )
        except Exception:
            return False

        try:
            ul_before = driver.find_element(By.CSS_SELECTOR, "ul.listaProds")
        except Exception:
            ul_before = None

        oc = btn.get_attribute("onclick") or ""
        try:
            if oc:
                driver.execute_script(oc)
            else:
                driver.execute_script("arguments[0].click();", btn)
        except JavascriptException:
            try:
                btn.click()
            except Exception:
                return False

    # re-entrar al frame tras el render
    _switch_to_last_window(driver)
    with switch_into_frame_with(driver, "ul.listaProds") as ok2:
        if not ok2:
            return False
        try:
            if ul_before:
                WebDriverWait(driver, WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds"))
                )
            time.sleep(0.6)
            _ensure_full_list_loaded(driver)
            sig = _page_signature(driver)
            return (prev_sig is None) or (sig != prev_sig)
        except TimeoutException:
            return False

# =========================
# Scraper completo por búsqueda
# =========================
def scrape_by_search_term(headless: bool, out_xlsx: str, out_csv: Optional[str], term: str) -> pd.DataFrame:
    """
    Usa el buscador del sitio para `term` y scrapea todas las páginas de resultados.
    """
    driver = setup_driver(headless=headless)
    try:
        print(f"🔍 Buscando productos con el término: {term!r}")
        open_search_and_submit(driver, term)

        all_rows: List[Dict[str, Any]] = []
        seen_keys = set()
        page_idx = 1

        while True:
            print(f"📄 Página {page_idx} (búsqueda='{term}'): extrayendo…")
            rows = collect_page_products(driver)
            nuevos = 0
            for rp in rows:
                key = (rp.get("codigo") or f"desc::{rp.get('descripcion')}", page_idx)
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                rp["cat_n0"] = None
                rp["cat_n2"] = None
                rp["cat_n3"] = None
                rp["cat_nombre"] = f"BUSQUEDA: {term}"

                all_rows.append(rp)
                nuevos += 1

            print(f"   → {len(rows)} encontrados, {nuevos} nuevos, total {len(all_rows)}")

            sig = _page_signature(driver)
            if not click_next(driver, prev_sig=sig):
                break
            page_idx += 1

        df = pd.DataFrame(
            all_rows,
            columns=["codigo","descripcion","precio","precio_texto","oferta","imagen",
                     "cat_n0","cat_n2","cat_n3","cat_nombre"]
        )
        if not df.empty:
            df.sort_values(by=["descripcion"], inplace=True, kind="stable")

        # Guardado (auditoría)
        #df.to_excel(out_xlsx, index=False)
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
        _cleanup_profile_dir()

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
    """'N1 > N2' o 'N1 > N2 > N3' → (N1, 'N2' o 'N2 > N3')."""
    if not cat_nombre:
        return None, None
    parts = [p.strip() for p in str(cat_nombre).split(">") if p.strip()]
    if not parts:
        return None, None
    categoria = parts[0]
    sub = " > ".join(parts[1:]) if len(parts) > 1 else None
    return categoria, sub

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = None  # no lo tenemos en esta tienda
    nombre = _truncate(r.get("descripcion") or "", MAXLEN_NOMBRE)
    marca = None
    fabricante = None
    categoria, subcategoria = split_categoria_sub(r.get("cat_nombre"))
    categoria = _truncate(categoria or "", MAXLEN_CATEGORIA)
    subcategoria = _truncate(subcategoria or "", MAXLEN_SUBCATEGORIA)

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
            INSERT INTO producto_tienda 
                (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))

        return cur.lastrowid

    exec_with_retry(cur, """
        INSERT INTO producto_tienda 
            (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
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

        # Reducir bloqueos
        try:
            with conn.cursor() as cset:
                cset.execute("SET SESSION innodb_lock_wait_timeout = 8")
                cset.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception:
            pass

        conn.autocommit = False
        cur = conn.cursor(buffered=True)

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        capturado_en = dt.now()

        total = 0
        batch = 0

        for _, r in df.iterrows():
            rec = r.to_dict()
            try:
                pid  = find_or_create_producto(cur, rec)
                ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)
                insert_historico(cur, tienda_id, ptid, rec, capturado_en)
                total += 1
                batch += 1
                if batch >= 50:
                    conn.commit()
                    batch = 0
            except myerr.DatabaseError as e:
                errno = getattr(e, "errno", None)
                if errno in LOCK_ERRNOS:
                    try: conn.rollback()
                    except: pass
                    print(f"[WARN] lock en fila (codigo={rec.get('codigo')}), continúo…")
                    continue
                elif errno == 1264:
                    try: conn.rollback()
                    except: pass
                    print(f"[DOWNGRADE] 1264 en (codigo={rec.get('codigo')}). Reinsertando con precios NULL.")
                    rec2 = dict(rec)
                    rec2["precio"] = None
                    try:
                        pid  = find_or_create_producto(cur, rec2)
                        ptid = upsert_producto_tienda(cur, tienda_id, pid, rec2)
                        insert_historico(cur, tienda_id, ptid, rec2, capturado_en)
                        total += 1
                        batch += 1
                        if batch >= 50:
                            conn.commit()
                            batch = 0
                    except Exception as e2:
                        try: conn.rollback()
                        except: pass
                        print(f"[SKIP] persistente tras downgrade: {e2}")
                        continue
                else:
                    try: conn.rollback()
                    except: pass
                    print(f"[SKIP] MySQL errno={errno} en fila (codigo={rec.get('codigo')}): {e}")
                    continue

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
    ap = argparse.ArgumentParser(description="DAR → Búsqueda por palabra clave + Ingesta MySQL")
    ap.add_argument("--out", default="dar_busqueda.xlsx", help="Ruta de salida XLSX (debug/auditoría)")
    ap.add_argument("--csv", default=None, help="(Opcional) Ruta CSV adicional")
    ap.add_argument("--no-headless", action="store_true", help="Desactivar headless (para debugar con UI)")
    ap.add_argument("--no-ingest", action="store_true", help="Solo scrape (no ingesta)")
    args = ap.parse_args()

    # pedir el término por consola
    term = input("Ingrese término de búsqueda para cpoBuscar: ").strip()
    if not term:
        print("❌ No se ingresó ningún término de búsqueda. Saliendo.")
        sys.exit(1)

    df = scrape_by_search_term(
        headless=(not args.no_headless),
        out_xlsx=args.out,
        out_csv=args.csv,
        term=term
    )

    if not args.no_ingest and not df.empty:
        ingest_to_mysql(df)


if __name__ == "__main__":
    try:
        main()
    except WebDriverException as e:
        print(f"❌ WebDriver error: {e}")
        sys.exit(2)
