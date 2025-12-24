#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Coope en Casa (frescos) → MySQL (robusto contra “tab crashed”)
CORREGIDO para tu lógica estándar:

✅ Reglas de ingesta:
1) Si existe SKU (codigo) en producto_tienda para esa tienda:
   - NO crea producto nuevo
   - reutiliza el producto_id ya asociado
2) En upsert_producto_tienda con SKU:
   - NO actualiza producto_id en ON DUPLICATE KEY UPDATE

Resto igual:
- precio_lista = precio capturado (VARCHAR)
- precio_oferta = NULL
- tipo_oferta = NULL
- promo_comentarios: precio_texto / unitario / sin_impuestos

Requisitos:
  pip install selenium webdriver-manager beautifulsoup4 lxml pandas mysql-connector-python
  base_datos.py con get_conn() -> mysql.connector.connect(...)
"""

import os
import re
import sys
import time
import argparse
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime as dt

import pandas as pd
import mysql.connector
from mysql.connector import errors as myerr

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)

# ---- helper de conexión ----
from base_datos import get_conn

# =========================
# Config tienda / sitio
# =========================
TIENDA_CODIGO = "lacoope"
TIENDA_NOMBRE = "La Coope en Casa"

HOME_URL = "https://www.lacoopeencasa.coop/"
BASE_CAT  = "https://www.lacoopeencasa.coop/listado/categoria/frescos/3"
PAGE_FMT  = BASE_CAT + "/pagina--{page}"   # page >= 2
MAX_PAGES = 300

# =========================
# Límites de campos (ajusta a tu esquema)
# =========================
MAXLEN_NOMBRE      = 255
MAXLEN_URL         = 512
MAXLEN_COMENTARIOS = 255

# =========================
# MySQL helpers (locks/deadlocks)
# =========================
LOCK_ERRNOS = {1205, 1213}  # lock wait timeout, deadlock

def exec_retry(cur, sql: str, params: tuple = (), max_retries: int = 5, base_sleep: float = 0.4):
    att = 0
    while True:
        try:
            cur.execute(sql, params)
            return
        except myerr.DatabaseError as e:
            code = getattr(e, "errno", None)
            if code in LOCK_ERRNOS and att < max_retries:
                wait = base_sleep * (2 ** att)
                print(f"[LOCK] errno={code} retry {att+1}/{max_retries} in {wait:.2f}s")
                time.sleep(wait)
                att += 1
                continue
            raise

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
# Selenium (headless VPS y flags livianas)
# =========================
def make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")  # evita /dev/shm chico
    opts.add_argument("--window-size=1200,800")
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--disable-notifications")
    # bloquear imágenes para bajar RAM/CPU
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    # ocultar webdriver flag
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    })
    return driver

# =========================
# Utilidades DOM seguras
# =========================
def s_find(el, by, sel):
    try:
        return el.find_element(by, sel)
    except Exception:
        return None

def s_attr(el, name):
    try:
        return el.get_attribute(name)
    except Exception:
        return None

def inner_text(driver, el):
    try:
        return (driver.execute_script(
            "return (arguments[0].innerText || arguments[0].textContent || '').trim();", el
        ) or "").strip()
    except Exception:
        return ""

def limpiar_precio_a_float(entero_txt, decimal_txt):
    """
    Convierte partes de precio (entero y decimal) a float.
    entero_txt: '2.200 ' ; decimal_txt: ' 00 '
    """
    if entero_txt is None:
        return None
    e = re.sub(r"[^\d\.]", "", (entero_txt or ""))
    d = re.sub(r"[^\d]", "", (decimal_txt or ""))
    if not e:
        return None
    if not d:
        d = "00"
    e_sin_miles = e.replace(".", "")
    try:
        return float(f"{e_sin_miles}.{d}")
    except Exception:
        return None

def scroll_hasta_cargar_todo(driver, pausa=0.7, max_intentos_sin_cambio=3, max_scrolls=30):
    last_h = 0
    estancados = 0
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pausa)
        h = driver.execute_script("return document.body.scrollHeight || 0;")
        if h <= last_h:
            estancados += 1
            if estancados >= max_intentos_sin_cambio:
                break
        else:
            estancados = 0
            last_h = h

def get_nombre_desde_card(driver, card):
    # 1) principal
    nombre_el = s_find(card, By.CSS_SELECTOR, ".card-descripcion[id^='descripcion'] p")
    nombre = inner_text(driver, nombre_el) if nombre_el else ""

    # 2) alt/title
    if not nombre:
        img = s_find(card, By.CSS_SELECTOR, ".card-image img")
        alt = s_attr(img, "alt") if img else ""
        title = s_attr(img, "title") if img else ""
        candidato = (alt or title or "").strip()
        if candidato:
            nombre = re.split(r"\s+-\s*\$", candidato, maxsplit=1)[0].strip()

    # 3) data-nombre
    if not nombre:
        a_det = s_find(card, By.CSS_SELECTOR, "a[data-nombre]")
        if a_det:
            nombre = (s_attr(a_det, "data-nombre") or "").strip()

    return nombre

def extraer_tarjetas(driver) -> List[Dict[str, Any]]:
    cards = driver.find_elements(By.CSS_SELECTOR, "col-listado-articulo div.card.hoverable")
    filas = []
    for c in cards:
        try:
            # Código desde ids como imagen208826 / descripcion208826
            codigo = None
            iddiv = s_find(c, By.CSS_SELECTOR, "[id^='imagen'], [id^='descripcion']")
            if iddiv:
                m = re.search(r"(\d+)$", s_attr(iddiv, "id") or "")
                if m:
                    codigo = m.group(1)

            # Imagen
            imagen_url = None
            img = s_find(c, By.CSS_SELECTOR, ".card-image img")
            if img:
                imagen_url = s_attr(img, "src") or s_attr(img, "data-src") or s_attr(img, "data-lazy")

            # Nombre
            nombre = get_nombre_desde_card(driver, c)

            # Precio
            entero_el  = s_find(c, By.CSS_SELECTOR, ".precio-listado .precio-entero")
            decimal_el = s_find(c, By.CSS_SELECTOR, ".precio-listado .precio-complemento .precio-decimal")
            entero_txt  = inner_text(driver, entero_el) if entero_el else ""
            decimal_txt = inner_text(driver, decimal_el) if decimal_el else ""
            precio_float = limpiar_precio_a_float(entero_txt, decimal_txt)

            # Precio textual (reconstruido)
            precio_texto = f"${entero_txt.strip()}{(decimal_txt or '00').strip()}" if entero_txt else ""

            # Precio unitario
            unit_el = s_find(c, By.CSS_SELECTOR, ".precio-unitario")
            precio_unitario = inner_text(driver, unit_el) if unit_el else ""

            # Precio sin impuestos
            psi_el = s_find(c, By.CSS_SELECTOR, ".precio-sin-impuestos span:last-child")
            precio_sin_impuestos = inner_text(driver, psi_el) if psi_el else ""

            # Link al detalle
            detalle_url = None
            a_det = s_find(c, By.CSS_SELECTOR, "a[href]")
            if a_det:
                href = (s_attr(a_det, "href") or "").strip()
                if href.startswith("/"):
                    detalle_url = "https://www.lacoopeencasa.coop" + href
                elif href.startswith("http"):
                    detalle_url = href

            filas.append({
                "codigo": codigo,
                "nombre": nombre,
                "precio": precio_float,
                "precio_texto": precio_texto,
                "precio_unitario": precio_unitario,
                "precio_sin_impuestos": precio_sin_impuestos,
                "imagen_url": imagen_url,
                "detalle_url": detalle_url,
            })
        except Exception:
            continue
    return filas

def cargar_y_extraer_pagina(driver, url, wait_cards=True, retries=2) -> Tuple[List[Dict[str, Any]], int]:
    for att in range(retries + 1):
        try:
            driver.get(url)
            WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")

            if wait_cards:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "col-listado-articulo div.card.hoverable"))
                )

            # dar tiempo a que se rellenen descripciones (evita nombres vacíos)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".card-descripcion[id^='descripcion'] p"))
                )
            except Exception:
                pass

            scroll_hasta_cargar_todo(driver, pausa=0.7, max_intentos_sin_cambio=3, max_scrolls=30)
            filas = extraer_tarjetas(driver)
            return filas, len(filas)

        except WebDriverException as e:
            msg = (str(e) or "").lower()
            if any(k in msg for k in ["tab crashed", "target closed", "disconnected", "cannot determine loading status"]):
                if att < retries:
                    try:
                        driver.refresh()
                        time.sleep(1)
                        continue
                    except Exception:
                        pass
                raise
            else:
                if att < retries:
                    try:
                        driver.refresh()
                        time.sleep(1)
                        continue
                    except Exception:
                        pass
                raise

# =========================
# Ingesta MySQL (CORREGIDA)
# =========================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    exec_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def get_producto_id_from_pt_by_sku(cur, tienda_id: int, sku: str) -> Optional[int]:
    """
    Si el SKU ya existe en producto_tienda para esa tienda,
    devolvemos el producto_id ya asociado para NO crear producto nuevo.
    """
    exec_retry(cur, """
        SELECT producto_id
        FROM producto_tienda
        WHERE tienda_id=%s AND sku_tienda=%s
        LIMIT 1
    """, (tienda_id, sku))
    r = cur.fetchone()
    return r[0] if r else None

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    # Para esta fuente no tenemos EAN ni marca: clave blanda = nombre
    nombre = _truncate(r.get("nombre") or "", MAXLEN_NOMBRE)
    if nombre:
        exec_retry(cur, "SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        row = cur.fetchone()
        if row:
            return row[0]

    exec_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, %s, NULL, NULL, NULL, NULL)
    """, (nombre,))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    """
    Regla estándar:
    - Si hay SKU: NO actualizar producto_id en ON DUPLICATE (ancla el SKU)
    """
    sku = r.get("codigo") or None
    nombre_tienda = _truncate(r.get("nombre") or None, MAXLEN_NOMBRE)
    url = _truncate(r.get("detalle_url") or None, MAXLEN_URL)

    if sku:
        exec_retry(cur, """
            INSERT INTO producto_tienda
              (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              -- producto_id = VALUES(producto_id),  -- REMOVIDO a propósito
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, sku, url, nombre_tienda))
        return cur.lastrowid

    # Sin SKU: último recurso (puede actualizar producto_id si tu UNIQUE cae aquí)
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

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: dt):
    precio = r.get("precio")
    precio_lista = _price_str(precio)          # SOLO precio_lista
    precio_oferta = None                       # sin precio oferta
    tipo_oferta = None                         # sin tipo_oferta

    comentarios_parts = []
    if r.get("precio_texto"): comentarios_parts.append(f"txt={r['precio_texto']}")
    if r.get("precio_unitario"): comentarios_parts.append(f"unit={r['precio_unitario']}")
    if r.get("precio_sin_impuestos"): comentarios_parts.append(f"sinimp={r['precio_sin_impuestos']}")
    promo_comentarios = _truncate(" | ".join(comentarios_parts), MAXLEN_COMENTARIOS) if comentarios_parts else None

    exec_retry(cur, """
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
        None, None, None, promo_comentarios
    ))

def ingest_to_mysql(rows: List[Dict[str, Any]]):
    if not rows:
        print("⚠ No hay filas para insertar.")
        return

    conn = None
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cset:
                cset.execute("SET SESSION innodb_lock_wait_timeout = 5")
                cset.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
                cset.execute("SET SESSION sql_safe_updates = 0")
        except Exception:
            pass

        conn.autocommit = False
        cur = conn.cursor(buffered=True)

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        capturado_en = dt.now()

        # Dedupe preferente por codigo (SKU). Si no hay, cae a (detalle_url,nombre)
        df = pd.DataFrame(rows)
        if not df.empty:
            if 'codigo' in df.columns:
                df['codigo_norm'] = df['codigo'].fillna("").astype(str).str.strip()
            else:
                df['codigo_norm'] = ""

            if 'detalle_url' in df.columns:
                df['url_norm'] = df['detalle_url'].fillna("").astype(str).str.strip()
            else:
                df['url_norm'] = ""

            if 'nombre' in df.columns:
                df['nombre_norm'] = df['nombre'].fillna("").astype(str).str.strip()
            else:
                df['nombre_norm'] = ""

            # si hay codigo, dedupe por codigo; si no, por url+nombre
            con_sku = df[df['codigo_norm'] != ""].drop_duplicates(subset=['codigo_norm'], keep='first')
            sin_sku = df[df['codigo_norm'] == ""].drop_duplicates(subset=['url_norm', 'nombre_norm'], keep='first')
            df = pd.concat([con_sku, sin_sku], ignore_index=True)

            # volver a dict sin cols auxiliares
            rows = df.drop(columns=[c for c in ['codigo_norm','url_norm','nombre_norm'] if c in df.columns]).to_dict(orient='records')

        batch = 0
        total = 0
        for rec in rows:
            sku = (rec.get("codigo") or "").strip() if rec.get("codigo") is not None else ""

            # 1) Si el SKU ya existe en producto_tienda -> NO crear producto nuevo
            pid = None
            if sku:
                pid = get_producto_id_from_pt_by_sku(cur, tienda_id, sku)

            # 2) Si no existía link previo -> crear/buscar por nombre
            if pid is None:
                pid = find_or_create_producto(cur, rec)

            # 3) Upsert producto_tienda (SKU no reasigna producto_id)
            ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)

            # 4) Histórico
            insert_historico(cur, tienda_id, ptid, rec, capturado_en)

            batch += 1
            total += 1
            if batch >= 20:
                conn.commit()
                batch = 0

        if batch:
            conn.commit()

        print(f"✅ MySQL: {total} registros insertados/actualizados en histórico.")

    except mysql.connector.Error as e:
        if conn:
            conn.rollback()
        print(f"❌ MySQL error {getattr(e,'errno',None)}: {e}")
        raise
    except Exception:
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
# Scrape + Run (con reinicios preventivos)
# =========================
def scrape_categoria(headless: bool = True, restart_every: int = 6) -> List[Dict[str, Any]]:
    def _new_driver():
        return make_driver(headless=headless)

    driver = _new_driver()
    try:
        print("Abriendo home…")
        driver.get(HOME_URL)
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(1.0)

        todas: List[Dict[str, Any]] = []

        # Página 1
        print(f"➡️ Página 1: {BASE_CAT}")
        filas, n = cargar_y_extraer_pagina(driver, BASE_CAT, wait_cards=True)
        print(f"   • items: {n}")
        todas.extend(filas)

        # Páginas siguientes
        page = 2
        while page <= MAX_PAGES:
            if (page - 2) % restart_every == 0:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = _new_driver()

            url = PAGE_FMT.format(page=page)
            print(f"➡️ Página {page}: {url}")
            try:
                filas, n = cargar_y_extraer_pagina(driver, url, wait_cards=False)
                print(f"   • items: {n}")
            except WebDriverException as e:
                if "tab crashed" in (str(e).lower()):
                    print(f"⚠️ Renderer crash en pág {page}. Recreando Chrome…")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = _new_driver()
                    filas, n = cargar_y_extraer_pagina(driver, url, wait_cards=False)
                    print(f"   • items (retry): {n}")
                else:
                    raise

            if n == 0:
                print("   ⛳ Fin de paginación.")
                break

            todas.extend(filas)
            page += 1

        print(f"🛒 Total capturados: {len(todas)}")
        return todas

    finally:
        try:
            driver.quit()
        except Exception:
            pass

def main():
    ap = argparse.ArgumentParser(description="La Coope (frescos) → MySQL (solo precio)")
    ap.add_argument("--no-headless", action="store_true", help="Desactivar headless (UI visible)")
    ap.add_argument("--out-xlsx", default=None, help="Guardar XLSX (opcional)")
    ap.add_argument("--restart-every", type=int, default=6, help="Reiniciar Chrome cada N páginas (default: 6)")
    args = ap.parse_args()

    rows = scrape_categoria(headless=(not args.no_headless), restart_every=args.restart_every)

    if args.out_xlsx:
        pd.DataFrame(rows).to_excel(args.out_xlsx, index=False)
        print(f"📄 XLSX guardado: {args.out_xlsx}")

    ingest_to_mysql(rows)

if __name__ == "__main__":
    main()
