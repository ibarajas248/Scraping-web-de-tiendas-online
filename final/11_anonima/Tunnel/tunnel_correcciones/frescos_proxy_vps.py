#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
import re
import sys, os
import random
import json
import tempfile
import zipfile
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# IMPORTANTE:
# En VPS es MUCHO mejor usar chromedriver del sistema (/usr/bin/chromedriver)
# y NO webdriver_manager (porque descarga y a veces rompe). Lo dejamos como fallback.
try:
    from webdriver_manager.chrome import ChromeDriverManager
    _HAS_WDM = True
except Exception:
    _HAS_WDM = False

from sshtunnel import SSHTunnelForwarder
import mysql.connector
from mysql.connector import Error as MySQLError


# =================== Identificador script (para logs) ===================
SCRIPT_TAG = "[LAANONIMA_CONGELADOS_766]"

# =================== Config scraper ===================
URL = "https://www.laanonima.com.ar/congelados/n1_766/"
POSTAL_CODE = "8300"

# Permite que cada instancia ponga su propio nombre de archivo por ENV
OUT_XLSX = os.getenv("OUT_XLSX", "congelados.xlsx")

# VPS headless
HEADLESS = True  # <- pedido

# =================== PROXY DataImpulse (pedido) ===================
PROXY_HOST = "gw.dataimpulse.com"
PROXY_PORT = 823
PROXY_USER = "a1d102f8514e7ff8eea7"
PROXY_PASS = "ad339fe6c2486f3c"

# =================== Config SSH / MySQL (pedido) ===================
SSH_HOST = "scrap.intelligenceblue.com.ar"
SSH_USER = "scrap-ssh"
SSH_PASS = "gLqqVHswm42QjbdvitJ0"

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_USER = os.getenv("DB_USER", "userscrap")
DB_PASS = os.getenv("DB_PASS", "UY8rMSGcHUunSsyJE4c7")
DB_NAME = os.getenv("DB_NAME", "scrap")
DB_PORT = int(os.getenv("DB_PORT", "3306"))

# =================== Config tienda ===================
TIENDA_CODIGO = "laanonima"
TIENDA_NOMBRE = "La Anónima"

# =================== Rendimiento / robustez MySQL ===================
BATCH_COMMIT = int(os.getenv("BATCH_COMMIT", "150"))  # commit cada N productos
MAX_DB_RETRIES = int(os.getenv("MAX_DB_RETRIES", "8"))  # reintentos por 1205/1213
BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "0.8"))  # segundos
SESSION_LOCK_WAIT_TIMEOUT = int(os.getenv("SESSION_LOCK_WAIT_TIMEOUT", "60"))  # seconds

# =================== Chrome bin + driver paths (VPS) ===================
# Ajusta si tu VPS los tiene en otro lugar
CHROME_BINARY = os.getenv("CHROME_BINARY", "/usr/bin/google-chrome")
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")

# =================== Bloqueo de recursos Selenium (CDP blocked urls) ===================
BLOCK_URL_PATTERNS = [
    "*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp", "*.svg",
    "*.woff", "*.woff2", "*.ttf", "*.otf", "*.eot",
    "*.css.map", "*.js.map",
    "*doubleclick.net*", "*googletagmanager.com*", "*google-analytics.com*",
    "*facebook.net*", "*hotjar.com*", "*newrelic.com*", "*optimizely.com*"
]

# =================== Utils comunes ===================
_price_clean_re = re.compile(r"[^\d,.\-]")
_NULLLIKE = {"", "null", "none", "nan", "na"}

def log(msg: str):
    print(f"{SCRIPT_TAG} {msg}", flush=True)

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

def parse_money_to_number(txt: str) -> Optional[float]:
    """Convierte textos tipo '$ 1.234,56' / '1.234' / '1,234.56' a float."""
    if txt is None:
        return None
    txt = str(txt).strip()
    if not txt:
        return None

    t = re.sub(r"[^\d.,-]", "", txt)
    if not t:
        return None

    if "." in t and "," in t:
        if t.rfind(",") > t.rfind("."):     # AR: 1.234,56
            t = t.replace(".", "").replace(",", ".")
        else:                               # US: 1,234.56
            t = t.replace(",", "")
    elif "," in t:
        frac = t.split(",")[-1]
        if len(frac) in (1, 2):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
    elif "." in t:
        parts = t.split(".")
        frac = parts[-1]
        if len(frac) == 3 and len("".join(parts)) > 3:
            t = "".join(parts)

    try:
        return float(t)
    except Exception:
        return None


# =================== Chrome extension MV3 (Proxy Auth) ===================
# Chrome 138+ deshabilita MV2, por eso lo hacemos MV3 con webRequestAuthProvider.
# Ref: Chrome docs webRequest + webRequestAuthProvider para onAuthRequired.
def build_proxy_auth_extension_mv3(proxy_user: str, proxy_pass: str) -> str:
    """
    Crea una extensión (ZIP) Manifest V3 que responde credenciales en onAuthRequired
    SOLO cuando details.isProxy == true.
    Devuelve ruta al .zip para cargar con ChromeOptions.add_extension().
    """
    manifest = {
        "name": "Proxy Auth (MV3) - DataImpulse",
        "version": "1.0.0",
        "manifest_version": 3,
        "permissions": ["webRequest", "webRequestAuthProvider"],
        "host_permissions": ["<all_urls>"],
        "background": {"service_worker": "background.js"},
    }

    # IMPORTANTE: usar asyncBlocking en MV3 service worker
    background_js = f"""
const PROXY_USER = {json.dumps(proxy_user)};
const PROXY_PASS = {json.dumps(proxy_pass)};

chrome.webRequest.onAuthRequired.addListener(
  (details, callback) => {{
    try {{
      if (details && details.isProxy) {{
        return callback({{ authCredentials: {{ username: PROXY_USER, password: PROXY_PASS }} }});
      }}
      return callback({{}});
    }} catch (e) {{
      return callback({{}});
    }}
  }},
  {{ urls: ["<all_urls>"] }},
  ["asyncBlocking"]
);
""".strip()

    tmp_dir = tempfile.mkdtemp(prefix="proxy_auth_mv3_")
    zip_path = os.path.join(tmp_dir, "proxy_auth_mv3.zip")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", json.dumps(manifest, indent=2))
        z.writestr("background.js", background_js)

    return zip_path


# =================== Conexión MySQL con túnel ===================
def get_conn() -> Tuple[mysql.connector.connection.MySQLConnection, SSHTunnelForwarder]:
    """
    Devuelve una conexión a MySQL a través de un túnel SSH.
    IMPORTANTE: cerrar conn y tunnel en el caller.
    """
    log("🚂 Iniciando túnel SSH...")
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, 22),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASS,
        remote_bind_address=(DB_HOST, DB_PORT),
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
        connection_timeout=20,
        autocommit=False,
        buffered=True,
    )
    log("✅ Conexión MySQL establecida.")

    # Ajustes por sesión (no tocan el servidor global)
    try:
        cur = conn.cursor()
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cur.execute(f"SET SESSION innodb_lock_wait_timeout = {int(SESSION_LOCK_WAIT_TIMEOUT)}")
        cur.close()
    except Exception:
        pass

    return conn, tunnel


# =================== Selenium helpers ===================
def setup_driver() -> webdriver.Chrome:
    """
    Selenium + Chrome headless NEW + proxy DataImpulse + extensión MV3 para auth.
    Incluye user-data-dir en /tmp para evitar el error de permisos de snap/chromium.
    """
    opts = Options()

    # Binario Chrome (VPS)
    if CHROME_BINARY and os.path.exists(CHROME_BINARY):
        opts.binary_location = CHROME_BINARY

    # Headless
    if HEADLESS:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1366,900")
    else:
        opts.add_argument("--start-maximized")

    # Estabilidad en VPS
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--blink-settings=imagesEnabled=false")

    # PERF: perfil en /tmp (evita "cannot create user data directory" en VPS)
    profile_dir = os.path.join("/tmp", f"selenium_profile_{os.getpid()}_{random.randint(1000,9999)}")
    os.makedirs(profile_dir, exist_ok=True)
    opts.add_argument(f"--user-data-dir={profile_dir}")

    # Proxy (host:port)
    proxy_url = f"http://{PROXY_HOST}:{PROXY_PORT}"
    opts.add_argument(f"--proxy-server={proxy_url}")

    # Extensión MV3 para inyectar credenciales del proxy
    ext_zip = build_proxy_auth_extension_mv3(PROXY_USER, PROXY_PASS)
    opts.add_extension(ext_zip)

    # anti-detección básica
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.images": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    # Service chromedriver
    if CHROMEDRIVER_PATH and os.path.exists(CHROMEDRIVER_PATH):
        service = Service(CHROMEDRIVER_PATH)
    else:
        if not _HAS_WDM:
            raise RuntimeError(
                "No existe CHROMEDRIVER_PATH y webdriver_manager no está instalado. "
                "Instala chromedriver o define CHROMEDRIVER_PATH."
            )
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    })

    # Bloquear recursos (si CDP está disponible)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": BLOCK_URL_PATTERNS})
    except Exception:
        pass

    return driver


def apply_postal_code(driver: webdriver.Chrome, wait: WebDriverWait, postal_code: str):
    try:
        cp_input = wait.until(EC.presence_of_element_located((By.ID, "idCodigoPostalUnificado")))
        cp_input.clear()
        cp_input.send_keys(postal_code)
        cp_input.send_keys(Keys.ENTER)

        driver.execute_script("""
            const inp = document.getElementById('idCodigoPostalUnificado');
            if (inp) { inp.dispatchEvent(new Event('input', { bubbles: true })); }
        """)

        try:
            close_btn = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((By.ID, "btnCerrarCodigoPostal"))
            )
            close_btn.click()
        except Exception:
            pass

        sleep(1.0)

        log("🔄 Recargando la página para aplicar el código postal...")
        driver.refresh()

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.card a[data-codigo]"))
        )
        sleep(1.0)

    except Exception as e:
        log(f"[AVISO] No se pudo interactuar con el modal de CP: {e}")


def smart_infinite_scroll(driver: webdriver.Chrome, wait_css: str, pause=0.9, max_plateaus=5):
    WebDriverWait(driver, 25).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
    )

    last_count = 0
    plateaus = 0

    while plateaus < max_plateaus:
        for _ in range(3):
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight/3);")
            sleep(pause)

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(pause)

        count = len(driver.find_elements(By.CSS_SELECTOR, wait_css))
        if count <= last_count:
            plateaus += 1
        else:
            plateaus = 0
            last_count = count

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


# =================== MySQL helpers ===================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT IGNORE INTO tiendas (codigo, nombre) VALUES (%s, %s)",
        (codigo, nombre)
    )
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
    """
    Upsert que devuelve ID con LAST_INSERT_ID.
    IMPORTANTE: NO actualiza producto_id si el registro ya existe (regla Kilbel).
    Requiere UNIQUE KEY:
      - (tienda_id, sku_tienda) cuando sku_tienda existe
      - (tienda_id, record_id_tienda) si usas record_id como alternativa
    """
    sku = clean(p.get("sku"))
    rec = clean(p.get("record_id"))
    url = p.get("url") or ""
    nombre_tienda = p.get("nombre") or ""

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda
              (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES
              (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda       = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda    = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, rec, url, nombre_tienda))
        return cur.lastrowid

    if rec:
        cur.execute("""
            INSERT INTO producto_tienda
              (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES
              (%s, %s, NULL, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              url_tienda    = COALESCE(VALUES(url_tienda), url_tienda),
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
        return f"{round(float(v), 2)}"

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
        p.get("tipo_oferta") or None,
        p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None,
        p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))


# =================== Retry wrapper para 1205/1213 ===================
def is_retryable_mysql_error(e: MySQLError) -> bool:
    try:
        return int(getattr(e, "errno", -1)) in (1205, 1213)
    except Exception:
        return False


def run_with_retry_db(fn, conn, *, max_retries=MAX_DB_RETRIES, base_backoff=BASE_BACKOFF):
    """
    Ejecuta fn() dentro de una transacción.
    Si hay 1205/1213: rollback, backoff, retry.
    """
    attempt = 0
    while True:
        try:
            return fn()
        except MySQLError as e:
            if not is_retryable_mysql_error(e) or attempt >= max_retries:
                raise
            try:
                conn.rollback()
            except Exception:
                pass
            sleep_s = base_backoff * (2 ** attempt) + random.uniform(0, 0.35)
            log(f"⏳ MySQL retryable ({getattr(e,'errno',None)}): {e}. Reintento en {sleep_s:.2f}s (attempt {attempt+1}/{max_retries})")
            sleep(sleep_s)
            attempt += 1


# =================== Main ===================
def main():
    log("🔎 Iniciando scraping (VPS headless + proxy DataImpulse) ...")
    log(f"🕸️  URL: {URL}")
    log(f"🧠 HEADLESS={HEADLESS} | PROXY={PROXY_HOST}:{PROXY_PORT}")

    driver = None
    rows = []
    try:
        driver = setup_driver()
        driver.get(URL)
        wait = WebDriverWait(driver, 25)

        apply_postal_code(driver, wait, POSTAL_CODE)

        css_card_anchor = "div.card a[data-codigo]"
        smart_infinite_scroll(driver, css_card_anchor, pause=0.8, max_plateaus=5)

        rows = driver.execute_script(JS_EXTRACT) or []

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    # ---------- Limpieza / regla de precios ----------
    for r in rows:
        r["precio_visible_num"] = parse_money_to_number(r.get("precio_visible_txt", ""))
        r["precio_tachado_num"] = parse_money_to_number(r.get("precio_tachado_txt", ""))
        r["impuestos_sin_nacionales_num"] = parse_money_to_number(r.get("impuestos_nacionales_txt", ""))
        r["data_precio_num"] = parse_money_to_number(r.get("data_precio", ""))
        r["data_precio_anterior_num"] = parse_money_to_number(r.get("data_precio_anterior", ""))
        r["data_precio_oferta_num"] = parse_money_to_number(r.get("data_precio_oferta", ""))
        r["data_precio_minimo_num"] = parse_money_to_number(r.get("data_precio_minimo", ""))
        r["data_precio_maximo_num"] = parse_money_to_number(r.get("data_precio_maximo", ""))

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

    # ---------- De-dup por codigo ----------
    seen = set()
    dedup: List[Dict[str, Any]] = []
    for r in rows:
        c = r.get("codigo", "")
        if c and c in seen:
            continue
        seen.add(c)
        dedup.append(r)

    # ---------- XLSX ----------
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

    # ---------- Mapear a formato estándar ----------
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
            "ean": None,
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

    # ---------- Inserción MySQL robusta ----------
    capturado_en = datetime.now()
    conn = None
    tunnel = None
    cur = None

    try:
        conn, tunnel = get_conn()
        cur = conn.cursor()

        log("📝 Upsert tienda...")
        def tx_upsert_tienda():
            tienda_id_local = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
            conn.commit()
            return tienda_id_local

        tienda_id = run_with_retry_db(tx_upsert_tienda, conn)
        log(f"🆔 tienda_id = {tienda_id}")

        total = len(productos)
        insertados = 0

        for start in range(0, total, BATCH_COMMIT):
            batch = productos[start:start+BATCH_COMMIT]
            batch_num = (start // BATCH_COMMIT) + 1

            def tx_batch():
                nonlocal insertados
                for p in batch:
                    producto_id = find_or_create_producto(cur, p)
                    pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
                    insert_historico(cur, tienda_id, pt_id, p, capturado_en)
                    insertados += 1
                conn.commit()

            run_with_retry_db(tx_batch, conn)
            log(f"💾 Batch {batch_num} OK → {min(start+BATCH_COMMIT, total)}/{total}")

        log(f"✅ Guardado en MySQL: {insertados} históricos para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        log(f"❌ Error MySQL: {e}")
        log("👉 Si sigue pasando, es porque hay mucha concurrencia en cron o faltan UNIQUE/INDEX en producto_tienda/historico_precios.")
    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        log(f"❌ Error general: {e}")
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
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
