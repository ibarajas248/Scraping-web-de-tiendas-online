#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
import re, json, os, sys
import pandas as pd
from urllib.parse import urljoin
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

# ===== MySQL / conexión
import numpy as np
from mysql.connector import Error as MySQLError

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ===== Selenium (selenium-wire para proxy con auth)
from seleniumwire import webdriver as wire_webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# =================== Config scraping ===================
URL = "https://www.laanonima.com.ar/tiempo-libre-deporte-y-entretenimiento/n1_2/"
POSTAL_CODE = "1001"
OUT_XLSX = "laanonima_hogar_jardin_automotor_optimizado.xlsx"

# En VPS: headless recomendado
HEADLESS = True

# Dominios/recursos que bloquearemos para aligerar
BLOCK_URL_PATTERNS = [
    "*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp", "*.svg",
    "*.woff", "*.woff2", "*.ttf", "*.otf", "*.eot",
    "*.css.map", "*.js.map",
    "*doubleclick.net*", "*googletagmanager.com*", "*google-analytics.com*",
    "*facebook.net*", "*hotjar.com*", "*newrelic.com*", "*optimizely.com*"
]

# =================== Config MySQL (tienda) ===================
TIENDA_CODIGO = "laanonima"
TIENDA_NOMBRE = "La Anónima"
# Política de precios:
# - precio_lista: "precio_tachado" si existe; si no, "precio_visible".
# - precio_oferta: "precio_visible" si hay tachado; si no, NULL.

# =================== Proxy DataImpulse ===================
PROXY_HOST = "gw.dataimpulse.com"
PROXY_PORT = 823
PROXY_USER_BASE = "2cf8063dbace06f69df4"
PROXY_PASS = "61425d26fb3c7287"
# Geotarget opcional: "__cr.ar" (Argentina) o "" para desactivar
PROXY_COUNTRY_SUFFIX = "__cr.ar"

def build_proxy_url():
    user = PROXY_USER_BASE + (PROXY_COUNTRY_SUFFIX or "")
    return f"http://{user}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

# =================== Utils ===================
_slug_spaces = re.compile(r"\s+")

def parse_money_to_number(txt: str):
    if not txt:
        return None
    t = re.sub(r"[^\d.,-]", "", txt)
    if not t:
        return None
    # Formato AR: coma decimal
    if "," in t and t.rfind(",") > t.rfind("."):
        t = t.replace(".", "").replace(",", ".")
    else:
        t = t.replace(",", "")
    try:
        return float(t)
    except Exception:
        return None

def to_txt_price_or_none(v) -> Optional[str]:
    if v is None:
        return None
    try:
        f = float(v)
        if np.isnan(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

def clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    s = _slug_spaces.sub(" ", s)
    return s or None

def split_categoria(ruta: str) -> Tuple[Optional[str], Optional[str]]:
    if not ruta:
        return None, None
    parts = [p.strip() for p in re.split(r">\s*", ruta.replace("&gt;", ">")) if p.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[-1]

def looks_like_blocked(html: str) -> bool:
    """Heurística simple para detectar captcha/challenge."""
    if not html:
        return False
    needles = [
        "cf-challenge", "captcha", "hcaptcha", "cloudflare", "verifica que eres humano",
        "Access Denied", "Request unsuccessful", "Temporarily unavailable"
    ]
    html_low = html.lower()
    return any(n in html_low for n in needles)

def setup_driver() -> wire_webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1366,900")
    else:
        opts.add_argument("--start-maximized")

    # Ahorro de recursos / huella baja
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    # User Agent realista
    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    )

    # Suavizar huellas
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.images": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    # Proxy con selenium-wire
    proxy_url = build_proxy_url()
    sw_options = {
        'proxy': {
            'http': proxy_url,
            'https': proxy_url,
            'no_proxy': 'localhost,127.0.0.1'
        },
        'verify_ssl': False,   # por si hay MITM TLS
        'request_storage': 'memory',
    }

    driver = wire_webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
        seleniumwire_options=sw_options
    )

    # Parcheo de webdriver + UA + idioma
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        })
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {"userAgent": UA, "acceptLanguage": "es-AR,es;q=0.9,en;q=0.8", "platform": "Win32"}
        )
        driver.execute_cdp_cmd("Emulation.setLocaleOverride", {"locale": "es-AR"})
        driver.execute_cdp_cmd("Emulation.setTimezoneOverride", {"timezoneId": "America/Argentina/Buenos_Aires"})
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": BLOCK_URL_PATTERNS})
    except Exception:
        pass

    return driver

def apply_postal_code(driver, wait: WebDriverWait, postal_code: str):
    """
    Tolerante: intenta por ID conocido y por otros posibles selectores;
    si no encuentra, simplemente sigue (no bloquea el flujo).
    """
    try:
        # Variante 1: ID conocido
        cp_input = wait.until(EC.presence_of_element_located((By.ID, "idCodigoPostalUnificado")))
    except Exception:
        # Variante 2: inputs alternativos (por si cambió el ID/estructura)
        try:
            cp_input = wait.until(EC.presence_of_element_located((
                By.CSS_SELECTOR, "input[name*='CodigoPostal'], input[placeholder*='Postal']"
            )))
        except Exception as e:
            print(f"[AVISO] No se pudo interactuar con el modal de CP (no encontrado): {e}")
            return

    try:
        cp_input.clear()
        cp_input.send_keys(postal_code)
        cp_input.send_keys(Keys.ENTER)
        # disparamos input event por si usan listeners
        driver.execute_script("""
            const inp = arguments[0];
            if (inp) { inp.dispatchEvent(new Event('input', { bubbles: true })); }
        """, cp_input)

        # intentamos cerrar modal si aparece botón de cerrar
        try:
            close_btn = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#btnCerrarCodigoPostal, .btn-cerrar, .modal-close, button.close"))
            )
            close_btn.click()
        except Exception:
            pass

        sleep(1.0)
    except Exception as e:
        print(f"[AVISO] No se pudo interactuar con el modal de CP: {e}")

def wait_for_cards_or_diagnose(driver, css_card_anchor: str, timeout: int = 30) -> bool:
    """
    Espera cartas; si no aparecen, guarda HTML y screenshot para diagnóstico
    y retorna False.
    """
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_card_anchor))
        )
        return True
    except Exception:
        # Dump para diagnóstico
        try:
            html = driver.page_source
            with open("debug_laaanonima.html", "w", encoding="utf-8") as f:
                f.write(html)
            driver.save_screenshot("debug_laaanonima.png")
            print("🧪 Guardado debug_laaanonima.html y debug_laaanonima.png")
            print(f"URL actual: {driver.current_url}")
            print(f"Título: {driver.title}")
            if looks_like_blocked(html):
                print("⚠️ Señal de captcha/bloqueo detectada en el HTML.")
        except Exception:
            pass
        return False

def smart_infinite_scroll(driver, wait_css: str, pause=0.9, max_plateaus=5):
    """
    Hace scroll hasta el fondo mientras el número de tarjetas siga creciendo.
    Si al inicio no hay tarjetas, intenta un scroll “forzado” y reintenta una vez.
    """
    # Primer intento de espera
    ok = wait_for_cards_or_diagnose(driver, wait_css, timeout=30)
    if not ok:
        # Scroll forzado y reintento
        for _ in range(6):
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight/2);")
            sleep(pause)
        ok = wait_for_cards_or_diagnose(driver, wait_css, timeout=20)
        if not ok:
            # No hay cartas; devolvemos sin lanzar excepción → el llamador decidirá.
            print("⚠️ No se detectaron tarjetas; se continúa para diagnóstico.")
            return

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
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = (p.get("ean") or "").strip() or None
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

    nombre = (p.get("nombre") or "").strip()
    marca  = (p.get("marca") or "").strip()

    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca))
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
    sku = (p.get("sku") or "").strip() or None
    rec = None
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

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, p: Dict[str, Any], capturado_en: datetime):
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
        p.get("precio_lista") or None,
        p.get("precio_oferta") or None,
        p.get("tipo_oferta") or None,
        p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None,
        p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# =================== MAIN ===================
def main():
    driver = setup_driver()
    driver.get(URL)
    wait = WebDriverWait(driver, 30)

    # 1) CP (tolerante)
    apply_postal_code(driver, wait, POSTAL_CODE)

    # 2) Scroll eficiente
    css_card_anchor = "div.card a[data-codigo]"
    smart_infinite_scroll(driver, css_card_anchor, pause=0.9, max_plateaus=5)

    # 3) Extracción masiva
    rows = driver.execute_script(JS_EXTRACT) or []

    # 4) Cerrar navegador
    driver.quit()

    # 5) Limpieza y enriquecimiento
    dedup = []
    seen = set()
    for r in rows:
        r["precio_visible_num"] = parse_money_to_number(r.get("precio_visible_txt", ""))
        r["precio_tachado_num"] = parse_money_to_number(r.get("precio_tachado_txt", ""))
        r["impuestos_sin_nacionales_num"] = parse_money_to_number(r.get("impuestos_nacionales_txt", ""))
        r["data_precio_num"] = parse_money_to_number(r.get("data_precio", ""))
        r["data_precio_anterior_num"] = parse_money_to_number(r.get("data_precio_anterior", ""))
        r["data_precio_oferta_num"] = parse_money_to_number(r.get("data_precio_oferta", ""))
        r["data_precio_minimo_num"] = parse_money_to_number(r.get("data_precio_minimo", ""))
        r["data_precio_maximo_num"] = parse_money_to_number(r.get("data_precio_maximo", ""))

        codigo = (r.get("codigo") or "").strip()
        if codigo and codigo in seen:
            continue
        seen.add(codigo)
        dedup.append(r)

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
    ]
    cols = [c for c in prefer if c in df.columns] + [c for c in df.columns if c not in prefer]
    if not df.empty:
        df = df[cols]
        #df.to_excel(OUT_XLSX, index=False)
    print(f"✅ Capturados: {len(df)} productos")
    print(f"📄 XLSX: {OUT_XLSX}")

    if df.empty:
        print("⚠️ No hay datos para insertar en MySQL.")
        return

    # 6) Map a modelo MySQL (con lógica de precios validada)
    mapped: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        ruta = r.get("ruta_categorias") or ""
        cat, subcat = split_categoria(ruta)

        nombre = clean_text(r.get("titulo_card")) or clean_text(r.get("nombre_data")) or None
        marca  = clean_text(r.get("marca")) or None

        pt_txt_present = bool((r.get("precio_tachado_txt") or "").strip())
        pt_num = r.get("precio_tachado_num")
        if pt_num is None:
            pt_num = parse_money_to_number(r.get("data_precio_anterior") or "")
        hay_oferta = pt_txt_present or (pt_num is not None and pt_num > 0)

        pv = r.get("precio_visible_num")
        if pv is None:
            if hay_oferta:
                pv = parse_money_to_number(r.get("data_precio_oferta") or "") \
                     or parse_money_to_number(r.get("data_precio") or "")
            else:
                pv = parse_money_to_number(r.get("data_precio") or "")

        if hay_oferta and pv is not None:
            precio_lista = to_txt_price_or_none(pt_num if (pt_num is not None and pt_num > 0) else None)
            if precio_lista is None:
                precio_lista = to_txt_price_or_none(parse_money_to_number(r.get("data_precio_anterior") or ""))
            if precio_lista is None:
                precio_lista = to_txt_price_or_none(pv)
            precio_oferta = to_txt_price_or_none(pv)
        else:
            base_lista = pv if pv is not None else parse_money_to_number(r.get("data_precio") or "")
            precio_lista  = to_txt_price_or_none(base_lista)
            precio_oferta = None

        p: Dict[str, Any] = {
            "ean": None,
            "nombre": nombre,
            "marca": marca,
            "fabricante": None,
            "categoria": clean_text(cat),
            "subcategoria": clean_text(subcat),
            "sku": clean_text(str(r.get("codigo") or "")),
            "record_id": None,
            "url": clean_text(r.get("detalle_url") or ""),
            "nombre_tienda": nombre,
            "precio_lista": precio_lista,
            "precio_oferta": precio_oferta,
            "tipo_oferta": None,
            "promo_tipo": None,
            "precio_regular_promo": None,
            "precio_descuento": None,
            "comentarios_promo": None,
        }
        mapped.append(p)

    # 7) INSERTAR EN MySQL
    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for p in mapped:
            producto_id = find_or_create_producto(cur, p)
            pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, p)
            insert_historico(cur, tienda_id, pt_id, p, capturado_en)
            insertados += 1

        conn.commit()
        print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn: conn.rollback()
        print(f"❌ Error MySQL: {e}")
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
