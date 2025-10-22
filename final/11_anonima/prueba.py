#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
import re
import pandas as pd
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://www.laanonima.com.ar/hogar-jardin-y-automotor/n1_1/"
POSTAL_CODE = "1001"
OUT_XLSX = "laanonima_hogar_jardin_automotor_optimizado.xlsx"

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

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

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
    except Exception as e:
        print(f"[AVISO] No se pudo interactuar con el modal de CP: {e}")

def smart_infinite_scroll(driver: webdriver.Chrome, wait_css: str, pause=0.9, max_plateaus=5):
    """
    Hace scroll hasta el fondo mientras el número de tarjetas siga creciendo.
    Se detiene tras 'max_plateaus' rondas sin incremento de tarjetas.
    """
    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_css)))

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

def main():
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

    # 5) Limpieza numérica y guardado
    for r in rows:
        r["precio_visible_num"] = parse_money_to_number(r.get("precio_visible_txt", ""))
        r["precio_tachado_num"] = parse_money_to_number(r.get("precio_tachado_txt", ""))
        r["impuestos_sin_nacionales_num"] = parse_money_to_number(r.get("impuestos_nacionales_txt", ""))
        # por si quieres números "data_*"
        r["data_precio_num"] = parse_money_to_number(r.get("data_precio", ""))
        r["data_precio_anterior_num"] = parse_money_to_number(r.get("data_precio_anterior", ""))
        r["data_precio_oferta_num"] = parse_money_to_number(r.get("data_precio_oferta", ""))
        r["data_precio_minimo_num"] = parse_money_to_number(r.get("data_precio_minimo", ""))
        r["data_precio_maximo_num"] = parse_money_to_number(r.get("data_precio_maximo", ""))

        # =======================
        # REGLA DE PRECIOS (solo esto)
        # - Si hay tachado (DOM) o data_precio_anterior > 0 => hay oferta:
        #     precio_lista   = tachado (o data_precio_anterior; fallback visible)
        #     precio_oferta  = visible (o data_precio_oferta; fallback data_precio)
        # - Si NO hay tachado => no hay oferta:
        #     precio_lista   = visible (o data_precio)
        #     precio_oferta  = None
        # =======================
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

    # De-dup por "codigo" (por si la página duplica componentes)
    seen = set()
    dedup = []
    for r in rows:
        c = r.get("codigo", "")
        if c and c in seen:
            continue
        seen.add(c)
        dedup.append(r)

    df = pd.DataFrame(dedup)
    # orden sugerido
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
        # añadimos las dos nuevas columnas calculadas:
        "precio_lista", "precio_oferta",
    ]
    cols = [c for c in prefer if c in df.columns] + [c for c in df.columns if c not in prefer]
    df = df[cols]

    df.to_excel(OUT_XLSX, index=False)
    print(f"✅ Capturados: {len(df)} productos")
    print(f"📄 XLSX: {OUT_XLSX}")

if __name__ == "__main__":
    main()
