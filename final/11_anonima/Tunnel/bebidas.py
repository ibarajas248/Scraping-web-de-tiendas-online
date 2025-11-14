#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
import re
import sys, os
from datetime import datetime
from typing import Dict, Any, List, Tuple

import pandas as pd
import numpy as np
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from sshtunnel import SSHTunnelForwarder
import mysql.connector
from mysql.connector import Error as MySQLError

# =================== Identificador script (para logs) ===================
SCRIPT_TAG = "[LAANONIMA_BEBIDAS_513]"

# =================== Config scraper ===================
URL = "https://www.laanonima.com.ar/bebidas/n1_513/"
POSTAL_CODE = "8300"

# Permite que cada instancia ponga su propio nombre de archivo por ENV
OUT_XLSX = os.getenv("OUT_XLSX", "lacteos.xlsx")

# Si quieres ver el navegador, ponlo en False. En servidor, True.
HEADLESS = False

# Dominios/recursos que bloquearemos para aligerar
BLOCK_URL_PATTERNS = [
    "*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp", "*.svg",
    "*.woff", "*.woff2", "*.ttf", "*.otf", "*.eot",
    "*.css.map", "*.js.map",
    "*doubleclick.net*", "*googletagmanager.com*", "*google-analytics.com*",
    "*facebook.net*", "*hotjar.com*", "*newrelic.com*", "*optimizely.com*"
]

# =================== Config SSH / MySQL ===================
SSH_HOST = "scrap.intelligenceblue.com.ar"
SSH_USER = "scrap-ssh"
SSH_PASS = "gLqqVHswm42QjbdvitJ0"

DB_HOST = "127.0.0.1"
DB_USER = "userscrap"
DB_PASS = "UY8rMSGcHUunSsyJE4c7"
DB_NAME = "scrap"
DB_PORT = 3306

# =================== Config tienda ===================
TIENDA_CODIGO = "laanonima"
TIENDA_NOMBRE = "La Anónima"

# =================== Utils comunes ===================
_price_clean_re = re.compile(r"[^\d,.\-]")
_NULLLIKE = {"", "null", "none", "nan", "na"}


def log(msg: str):
    print(f"{SCRIPT_TAG} {msg}")


def clean(val):
    """Normaliza texto: trim, colapsa espacios, filtra null-likes."""
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s


def parse_price(val) -> float:
    """Parsea números con separadores locales; devuelve float o np.nan."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return np.nan
    s = _price_clean_re.sub("", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan


def parse_money_to_number(txt: str):
    """Convierte textos tipo '$ 1.234,56' / '1.234' / '1,234.56' a float."""
    if txt is None:
        return None
    txt = str(txt).strip()
    if not txt:
        return None

    t = re.sub(r"[^\d.,-]", "", txt)
    if not t:
        return None

    # Caso con punto y coma
    if "." in t and "," in t:
        # Formato AR típico: 1.234,56
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            # Formato US típico: 1,234.56
            t = t.replace(",", "")

    # Solo coma
    elif "," in t:
        frac = t.split(",")[-1]
        # Si la parte decimal tiene 1 o 2 dígitos: coma decimal
        if len(frac) in (1, 2):
            t = t.replace(".", "").replace(",", ".")
        else:
            # Probablemente separador de miles
            t = t.replace(",", "")

    # Solo punto
    elif "." in t:
        parts = t.split(".")
        frac = parts[-1]
        # Si hay grupos de 3 (p.ej. '1.234'), trátalo como separador de miles
        if len(frac) == 3 and len("".join(parts)) > 3:
            t = "".join(parts)
        # Si no, lo dejamos como decimal (12.34, 0.99, etc.)

    try:
        return float(t)
    except Exception:
        return None


# =================== Conexión MySQL con túnel ===================
def get_conn() -> Tuple[mysql.connector.connection.MySQLConnection, SSHTunnelForwarder]:
    """
    Devuelve una conexión a MySQL a través de un túnel SSH.
    IMPORTANTE: hay que cerrar tanto conn como tunnel en el caller.
    Cada script/instancia usa su propio túnel y su propio puerto local.
    """
    log("🚂 Iniciando túnel SSH...")
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, 22),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASS,
        remote_bind_address=(DB_HOST, DB_PORT),
        # puerto local aleatorio libre -> seguro para varios scripts a la vez
        local_bind_address=("127.0.0.1", 0),
    )
    tunnel.start()
    local_port = tunnel.local_bind_port
    log(f"🔐 Túnel SSH activo en localhost:{local_port} -> {DB_HOST}:{DB_PORT}")

    log("🛰  Intentando conectar a MySQL a través del túnel...")
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=local_port,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        connection_timeout=20,  # para evitar cuelgues eternos
    )
    log("✅ Conexión MySQL establecida.")
    return conn, tunnel


# =================== Selenium helpers ===================
def setup_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        # Headless moderno (más estable/rápido)
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1366,900")
    else:
        opts.add_argument("--start-maximized")

    # Ahorro de recursos
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    # No cargar imágenes por si se cuela alguna
    opts.add_argument("--blink-settings=imagesEnabled=false")

    # Suavizar huellas
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.images": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )

    # Ocultar navigator.webdriver
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    })

    # Bloquear recursos pesados / analytics
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": BLOCK_URL_PATTERNS})
    except Exception:
        pass

    return driver


def apply_postal_code(driver: webdriver.Chrome, wait: WebDriverWait, postal_code: str):
    try:
        cp_input = wait.until(
            EC.presence_of_element_located((By.ID, "idCodigoPostalUnificado"))
        )
        cp_input.clear()
        cp_input.send_keys(postal_code)
        cp_input.send_keys(Keys.ENTER)

        driver.execute_script("""
            const inp = document.getElementById('idCodigoPostalUnificado');
            if (inp) { inp.dispatchEvent(new Event('input', { bubbles: true })); }
        """)

        # Intentar cerrar modal
        try:
            close_btn = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((By.ID, "btnCerrarCodigoPostal"))
            )
            close_btn.click()
        except Exception:
            pass

        sleep(1.0)

        # 🔄 Recargar página para aplicar el CP
        log("🔄 Recargando la página para aplicar el código postal...")
        driver.refresh()

        # Esperar a que aparezcan tarjetas otra vez
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.card a[data-codigo]"))
        )

        sleep(1.0)

    except Exception as e:
        log(f"[AVISO] No se pudo interactuar con el modal de CP: {e}")


def smart_infinite_scroll(driver: webdriver.Chrome, wait_css: str, pause=0.9, max_plateaus=5):
    """
    Hace scroll hasta el fondo mientras el número de tarjetas siga creciendo.
    Se detiene tras 'max_plateaus' rondas sin incremento de tarjetas.
    """
    WebDriverWait(driver, 25).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
    )

    last_count = 0
    plateaus = 0

    while plateaus < max_plateaus:
        # Scroll suave: 3 saltos para disparar distintos lazy-loaders
        for _ in range(3):
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight/3);")
            sleep(pause)

        # Rebote final
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(pause)

        # Medimos progreso por número de tarjetas
        count = len(driver.find_elements(By.CSS_SELECTOR, wait_css))
        if count <= last_count:
            plateaus += 1
        else:
            plateaus = 0
            last_count = count

    # pequeño ajuste final
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sleep(0.6)


JS_EXTRACT = """
return Array.from(document.querySelectorAll('div.card a[data-codigo]')).map(a => {
  const card = a.closest('.card');
  const q = sel => {
    const el = card ? card.querySelector(sel) : null;
    return el ? el.textContent.trim() : '';
  };
  const img = card ? card.querySelector('.imagen img') : null;
  return {
    codigo: a.dataset.codigo || '',
    nombre_data: a.dataset.nombre || '',
    marca: a.dataset.marca || '',
    modelo: a.dataset.modelo || '',
    ruta_categorias: a.dataset.rutacategorias || '',
    data_precio: a.dataset.precio || '',
    data_precio_anterior: a.dataset.precioAnterior || '',
    data_precio_oferta: a.dataset.precioOferta || '',
    data_precio_desde: a.dataset.precioDesde || '',
    data_precio_hasta: a.dataset.precioHasta || '',
    data_precio_minimo: a.dataset.precioMinimo || '',
    data_precio_maximo: a.dataset.precioMaximo || '',
    data_es_padre_matriz: a.dataset.esPadreMatriz || '',
    data_primer_hijo_stock: a.dataset.primerHijoStock || '',
    titulo_card: q('.titulo'),
    precio_tachado_txt: q('.precio-anterior .tachado'),
    precio_visible_txt: q('.precio span'),
    impuestos_nacionales_txt: q('.impuestos-nacionales'),
    detalle_url: a.href || '',
    img_url: img ? (img.getAttribute('src') || img.getAttribute('data-src') || '') : ''
  };
});
"""

# =================== MySQL helpers (mismo formato que Coto) ===================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    # 1) Intentar leer sin bloquear en escritura
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    row = cur.fetchone()
    if row:
        return row[0]

    # 2) Si no existe, insertarla (solo la primera vez)
    cur.execute(
        "INSERT IGNORE INTO tiendas (codigo, nombre) VALUES (%s, %s)",
        (codigo, nombre)
    )
    # 3) Volver a leer el id
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    row = cur.fetchone()
    return row[0]



def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
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

    nombre = clean(p.get("nombre")) or ""
    marca = clean(p.get("marca")) or ""
    if nombre and marca:
        cur.execute("""
            SELECT id FROM productos
            WHERE nombre=%s AND IFNULL(marca,'')=%s
            LIMIT 1
        """, (nombre, marca))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(NULLIF(%s,''), ean),
                  fabricante = COALESCE(NULLIF(%s,''), fabricante),
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (
                p.get("ean") or "", p.get("fabricante") or "",
                p.get("categoria") or "", p.get("subcategoria") or "", pid
            ))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        p.get("ean") or "", nombre, marca,
        p.get("fabricante") or "", p.get("categoria") or "", p.get("subcategoria") or ""
    ))
    return cur.lastrowid


def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    """Upsert que devuelve ID con LAST_INSERT_ID para evitar SELECT extra."""
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre") or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid

    if rec:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, rec, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid


def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
    def to_txt_or_none(x):
        if x is None:
            return None
        v = parse_price(x)
        if isinstance(v, float) and np.isnan(v):
            return None
        return f"{round(float(v), 2)}"  # guardamos como VARCHAR

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
        to_txt_or_none(p.get("precio_lista")), to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None, p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None, p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))


# =================== Main ===================
def main():
    log("🔎 Iniciando scraping ...")
    # ---------- SCRAPING ----------
    driver = setup_driver()
    driver.get(URL)
    wait = WebDriverWait(driver, 25)

    # 1) CP si aparece
    apply_postal_code(driver, wait, POSTAL_CODE)

    # 2) Scroll eficiente controlando conteo de tarjetas
    css_card_anchor = "div.card a[data-codigo]"
    smart_infinite_scroll(driver, css_card_anchor, pause=0.8, max_plateaus=5)

    # 3) Extracción masiva en **una sola llamada JS**
    rows = driver.execute_script(JS_EXTRACT) or []

    # 4) Cerrar navegador
    driver.quit()

    # 5) Limpieza numérica y regla de precios
    for r in rows:
        r["precio_visible_num"] = parse_money_to_number(r.get("precio_visible_txt", ""))
        r["precio_tachado_num"] = parse_money_to_number(r.get("precio_tachado_txt", ""))
        r["impuestos_sin_nacionales_num"] = parse_money_to_number(r.get("impuestos_nacionales_txt", ""))
        r["data_precio_num"] = parse_money_to_number(r.get("data_precio", ""))
        r["data_precio_anterior_num"] = parse_money_to_number(r.get("data_precio_anterior", ""))
        r["data_precio_oferta_num"] = parse_money_to_number(r.get("data_precio_oferta", ""))
        r["data_precio_minimo_num"] = parse_money_to_number(r.get("data_precio_minimo", ""))
        r["data_precio_maximo_num"] = parse_money_to_number(r.get("data_precio_maximo", ""))

        # ====== REGLA DE PRECIOS ======
        pt_txt_present = bool((r.get("precio_tachado_txt") or "").strip())
        pt = r.get("precio_tachado_num")
        if pt is None:
            pt = parse_money_to_number(r.get("data_precio_anterior") or "")
        hay_tachado = pt_txt_present or (pt is not None and pt > 0)

        pv = r.get("precio_visible_num")
        if pv is None:
            if hay_tachado:
                pv = parse_money_to_number(r.get("data_precio_oferta") or "") \
                     or parse_money_to_number(r.get("data_precio") or "")
            else:
                pv = parse_money_to_number(r.get("data_precio") or "")

        if hay_tachado and pv is not None:
            precio_lista_num = pt if (pt is not None and pt > 0) else None
            if precio_lista_num is None:
                precio_lista_num = parse_money_to_number(r.get("data_precio_anterior") or "")
            if precio_lista_num is None:
                precio_lista_num = pv
            r["precio_lista"] = precio_lista_num
            r["precio_oferta"] = pv
        else:
            base_lista = pv if pv is not None else parse_money_to_number(r.get("data_precio") or "")
            r["precio_lista"] = base_lista
            r["precio_oferta"] = None
        # ====== FIN REGLA DE PRECIOS ======

    # De-dup por "codigo"
    seen = set()
    dedup: List[Dict[str, Any]] = []
    for r in rows:
        c = r.get("codigo", "")
        if c and c in seen:
            continue
        seen.add(c)
        dedup.append(r)

    # ----- DataFrame / XLSX opcional -----
    df = pd.DataFrame(dedup)
    prefer = [
        "codigo", "titulo_card", "nombre_data", "marca", "modelo", "ruta_categorias",
        "detalle_url", "img_url",
        "precio_visible_txt", "precio_tachado_txt", "impuestos_nacionales_txt",
        "precio_visible_num", "precio_tachado_num", "impuestos_sin_nacionales_num",
        "data_precio", "data_precio_anterior", "data_precio_oferta",
        "data_precio_desde", "data_precio_hasta", "data_precio_minimo", "data_precio_maximo",
        "data_precio_num", "data_precio_anterior_num", "data_precio_oferta_num",
        "data_precio_minimo_num", "data_precio_maximo_num",
        "data_es_padre_matriz", "data_primer_hijo_stock",
        "precio_lista", "precio_oferta",
    ]
    cols = [c for c in prefer if c in df.columns] + [c for c in df.columns if c not in prefer]
    df = df[cols]

    df.to_excel(OUT_XLSX, index=False)
    log(f"✅ Capturados: {len(df)} productos")
    log(f"📄 XLSX: {OUT_XLSX}")

    # ====== Mapear a formato estándar para MySQL ======
    productos: List[Dict[str, Any]] = []

    for r in dedup:
        ruta = (r.get("ruta_categorias") or "").strip()
        categoria = subcategoria = None
        if ruta:
            partes = [x.strip() for x in re.split(r">|/", ruta) if x.strip()]
            if partes:
                categoria = partes[0]
                if len(partes) > 1:
                    subcategoria = partes[-1]

        nombre = (r.get("titulo_card") or r.get("nombre_data") or "").strip()

        p = {
            "sku": (r.get("codigo") or "").strip(),
            "record_id": None,
            "ean": None,  # no tenemos EAN aquí
            "nombre": nombre,
            "marca": (r.get("marca") or "").strip(),
            "fabricante": None,
            "precio_lista": r.get("precio_lista"),
            "precio_oferta": r.get("precio_oferta"),
            "tipo_oferta": "OFERTA" if r.get("precio_oferta") not in (None, 0) else None,
            "promo_tipo": None,
            "precio_regular_promo": None,
            "precio_descuento": None,
            "comentarios_promo": None,
            "categoria": categoria,
            "subcategoria": subcategoria,
            "url": r.get("detalle_url") or "",
        }

        if p["sku"] or p["precio_lista"] or p["precio_oferta"]:
            productos.append(p)

    if not productos:
        log("⚠️ No hay productos para insertar en MySQL.")
        return

    # ====== Inserción en MySQL con get_conn (túnel SSH) ======
    capturado_en = datetime.now()

    conn = None
    tunnel = None
    try:
        conn, tunnel = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        log("📝 Upsert tienda...")
        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        log(f"🆔 tienda_id = {tienda_id}")

        insertados = 0
        total = len(productos)
        for i, p in enumerate(productos, start=1):
            if i % 100 == 0:
                log(f"   → Procesando producto {i}/{total}")

            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            insertados += 1

        conn.commit()
        log(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn:
            conn.rollback()
        log(f"❌ Error MySQL: {e}")
    except Exception as e:
        log(f"❌ Error general: {e}")
    finally:
        if conn:
            try:
                conn.close()
                log("🔌 Conexión MySQL cerrada.")
            except Exception:
                pass
        if tunnel:
            try:
                tunnel.stop()
                log("🔚 Túnel SSH cerrado.")
            except Exception:
                pass


if __name__ == "__main__":
    main()
