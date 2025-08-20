#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time, re, requests, pandas as pd, unicodedata
from bs4 import BeautifulSoup
from html import unescape
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
)
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ====================== Configuración general ======================
URL = "https://www.disco.com.ar"
EMAIL = "ivanbarajashurtado@gmail.com"

HEADLESS = True          # <-- pon False para ver el navegador
PAGELOAD_TIMEOUT = 45
IMPLICIT_WAIT = 2

# Scraper VTEX
BASE = "https://www.disco.com.ar"
SEARCH = f"{BASE}/api/catalog_system/pub/products/search"
FACETS = f"{BASE}/api/catalog_system/pub/facets/search/*?map=c"
STEP = 50
SLEEP_BASE = 0.1
TIMEOUT = 25
MAX_EMPTY_PAGES = 2
RETRIES = 3
MAX_WORKERS = 1          # usa 1 para estabilidad con la misma sesión/cookies
MAX_DEPTH = None

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
OUT_XLSX = "disco_formato.xlsx"
OUT_CSV  = None  # p.ej. "disco_formato.csv"

COLS_FINAL = [
    "EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Marca",
    "Fabricante","Precio de Lista","Precio de Oferta","Tipo de Oferta","URL"
]
ILLEGAL_XLSX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

# ====================== Selenium helpers ======================
def setup_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    else:
        opts.add_experimental_option("detach", True)  # deja la ventana abierta
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,900")
    opts.add_argument("--lang=es-AR")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    opts.add_argument(f"user-agent={ua}")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    driver.implicitly_wait(IMPLICIT_WAIT)
    return driver

def wait_css(driver, css: str, timeout: int = 25):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css))
    )

def wait_clickable(driver, by, sel, timeout: int = 25):
    return WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, sel)))

def click_js(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    time.sleep(0.05)
    driver.execute_script("arguments[0].click();", el)

def try_accept_cookies(driver):
    for sel in [
        "#onetrust-accept-btn-handler",
        "button[aria-label*='Aceptar']",
        "div#onetrust-button-group button",
    ]:
        try:
            btn = WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
            click_js(driver, btn)
            time.sleep(0.2)
            return
        except Exception:
            pass

def open_delivery_modal(driver):
    # Por texto visible
    try:
        el = wait_clickable(
            driver, By.XPATH,
            "//span[contains(.,'Seleccioná el método de entrega')]/ancestor::*[@role='button'][1]",
            timeout=20
        )
        try: el.click()
        except Exception: click_js(driver, el)
        return
    except TimeoutException:
        pass
    # Trigger genérico VTEX
    triggers = driver.find_elements(By.CSS_SELECTOR, ".vtex-modal-layout-0-x-triggerContainer")
    for t in triggers:
        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable(t))
            try: t.click()
            except Exception: click_js(driver, t)
            return
        except Exception:
            continue
    raise TimeoutException("No se encontró el disparador del modal de entrega.")

def fill_email_and_send(driver, email: str):
    # input email
    for sel in [
        "input[type='email'][placeholder*='correo']",
        "input[type='email'][placeholder*='Correo']",
        "input[type='email']",
        "input[name='email']",
    ]:
        try:
            email_input = wait_clickable(driver, By.CSS_SELECTOR, sel, timeout=20)
            break
        except TimeoutException:
            email_input = None
    if not email_input:
        email_input = wait_clickable(driver, By.XPATH, "//input[@type='email']", timeout=25)
    driver.execute_script("arguments[0].focus();", email_input)
    email_input.clear()
    email_input.send_keys(email)
    time.sleep(0.2)
    # botón Enviar
    for by, sel in [
        (By.XPATH, "//button[.//div[contains(.,'Enviar')] or contains(.,'Enviar')]"),
        (By.XPATH, "//button[contains(.,'Enviar')]"),
        (By.CSS_SELECTOR, "button.vtex-button"),
    ]:
        try:
            send_btn = wait_clickable(driver, by, sel, timeout=15)
            break
        except TimeoutException:
            send_btn = None
    if not send_btn:
        raise TimeoutException("No se encontró el botón 'Enviar' en el modal.")
    try: send_btn.click()
    except Exception: click_js(driver, send_btn)

def pick_retire_in_store(driver):
    # Botón/Tile "Retirar en una tienda"
    try:
        btn = wait_clickable(
            driver, By.XPATH,
            "//button[.//p[contains(.,'Retirar en una tienda')]"
            " or .//div[contains(.,'Retirar en una tienda')]]",
            timeout=25
        )
        try: btn.click()
        except Exception: click_js(driver, btn)
    except TimeoutException:
        # fallback al contenedor
        container = wait_clickable(
            driver, By.CSS_SELECTOR,
            ".discoargentina-delivery-modal-1-x-pickUpSelectionContainer button",
            timeout=15
        )
        try: container.click()
        except Exception: click_js(driver, container)

# ====================== Helpers de selects robustos ======================
def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.strip().lower().split())

def _focus_and_dispatch(driver, el):
    driver.execute_script("""
        const el = arguments[0];
        el.focus();
        el.dispatchEvent(new Event('focus',  {bubbles:true}));
        el.dispatchEvent(new Event('click',  {bubbles:true}));
        el.dispatchEvent(new Event('input',  {bubbles:true}));
        el.dispatchEvent(new Event('change', {bubbles:true}));
    """, el)

def _find_select_by_placeholder(driver, placeholder_text: str, timeout: int = 40):
    """
    Devuelve el <select> cuyo primer <option> contiene el placeholder (p.ej. 'Seleccionar tienda').
    """
    target = _norm(placeholder_text)
    def _cond(drv):
        selects = drv.find_elements(By.TAG_NAME, "select")
        for s in selects:
            try:
                opts = s.find_elements(By.TAG_NAME, "option")
                if not opts: continue
                first_txt = _norm(opts[0].text)
                if target in first_txt:
                    return s
            except StaleElementReferenceException:
                continue
        return False
    return WebDriverWait(driver, timeout).until(_cond)

def _wait_select_has_options(driver, select_el, min_count: int = 2, timeout: int = 75):
    """
    Espera a que el <select> tenga al menos min_count <option>. Maneja re-render.
    """
    end = time.time() + timeout
    while time.time() < end:
        try:
            opts = select_el.find_elements(By.TAG_NAME, "option")
            if len(opts) >= min_count:
                return True
        except StaleElementReferenceException:
            try:
                select_el = _find_select_by_placeholder(driver, "Seleccionar tienda", timeout=5)
            except Exception:
                pass
        time.sleep(0.25)
    return False

def _list_select_texts(select_el):
    try:
        return [o.text.strip() for o in select_el.find_elements(By.TAG_NAME, "option")]
    except Exception:
        return []

def _dispatch_all(driver, select_el, value):
    driver.execute_script("""
        const sel = arguments[0];
        const val = arguments[1];
        sel.value = val;
        sel.dispatchEvent(new Event('change', {bubbles:true}));
        sel.dispatchEvent(new Event('input',  {bubbles:true}));
        sel.dispatchEvent(new Event('blur',   {bubbles:true}));
    """, select_el, value)

def choose_province(driver, province_text: str = "CABA"):
    """
    Selecciona provincia en el <select> cuyo placeholder es 'Seleccionar Provincia'
    """
    sel_prov = _find_select_by_placeholder(driver, "Seleccionar Provincia", timeout=35)
    # intenta vía Select; si no, JS + eventos
    try:
        Select(sel_prov).select_by_visible_text(province_text)
    except Exception:
        opts = sel_prov.find_elements(By.TAG_NAME, "option")
        tgt = _norm(province_text); val = None
        for o in opts:
            if _norm(o.text) == tgt or tgt in _norm(o.text):
                val = o.get_attribute("value"); break
        if not val:
            raise TimeoutException(f"No se pudo seleccionar la provincia '{province_text}'.")
        _dispatch_all(driver, sel_prov, val)
    # forzar el fetch de tiendas
    _focus_and_dispatch(driver, sel_prov)
    time.sleep(0.8)

def choose_store(driver, store_text: str = "Disco Alto Palermo"):
    """
    Selecciona tienda en el <select> 'Seleccionar tienda' (o tarjetas).
    Matching tolerante: exacto -> contiene 'disco alto palermo' -> contiene 'alto palermo'.
    """
    normalized_goal = _norm(store_text)

    # 1) Intento vía SELECT "Seleccionar tienda"
    try:
        sel_store = _find_select_by_placeholder(driver, "Seleccionar tienda", timeout=45)
        # focus/click en wrapper por si el select está opaco
        try:
            wrapper = sel_store.find_element(By.XPATH, "..")
            try: wrapper.click()
            except Exception: click_js(driver, wrapper)
        except Exception:
            pass
        _focus_and_dispatch(driver, sel_store)

        ok = _wait_select_has_options(driver, sel_store, min_count=2, timeout=75)
        if ok:
            options = sel_store.find_elements(By.TAG_NAME, "option")
            # 1.1 exacto
            for o in options:
                if _norm(o.text) == normalized_goal:
                    _dispatch_all(driver, sel_store, o.get_attribute("value"))
                    time.sleep(0.6); return
            # 1.2 contiene 'disco alto palermo'
            for o in options:
                if "disco alto palermo" in _norm(o.text):
                    _dispatch_all(driver, sel_store, o.get_attribute("value"))
                    time.sleep(0.6); return
            # 1.3 contiene 'alto palermo'
            for o in options:
                if "alto palermo" in _norm(o.text):
                    _dispatch_all(driver, sel_store, o.get_attribute("value"))
                    time.sleep(0.6); return
            # si no encontró por select, sigue al plan B (cards)
    except TimeoutException:
        pass  # no está el select; probamos cards

    # 2) Plan B: UI con tarjetas/lista de tiendas (no select)
    xpath_card = (
        "//*[contains(translate(normalize-space(.), 'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöü', "
        "'AEIOUAEIOUaeiouaeiou'), 'disco alto palermo')"
        " or contains(translate(normalize-space(.), 'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöü', "
        "'AEIOUAEIOUaeiouaeiou'), 'alto palermo')]"
    )
    try:
        cand = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.XPATH, xpath_card))
        )
        for el in cand:
            try:
                btn = el if el.tag_name.lower() == "button" else el.find_element(By.XPATH, "ancestor::button[1]")
            except Exception:
                btn = el
            try: btn.click()
            except Exception: click_js(driver, btn)
            time.sleep(0.6)
            return
    except TimeoutException:
        pass

    # 3) Debug & error
    try:
        sel_store = _find_select_by_placeholder(driver, "Seleccionar tienda", timeout=5)
        visibles = [o.text.strip() for o in sel_store.find_elements(By.TAG_NAME, "option")]
        print("⚠️ Opciones visibles en 'Seleccionar tienda':", visibles)
    except Exception:
        print("⚠️ No hay <select> de tienda visible; puede ser UI de tarjetas.")
    driver.save_screenshot("tiendas_debug.png")
    raise TimeoutException(f"No se pudo seleccionar la tienda '{store_text}'.")

def click_confirm(driver):
    """
    Click en 'Confirmar' cuando deja de estar deshabilitado.
    """
    target = WebDriverWait(driver, 40).until(
        EC.presence_of_element_located((
            By.XPATH, "//button[.//div[contains(.,'Confirmar')] or contains(.,'Confirmar')]"
        ))
    )
    WebDriverWait(driver, 40).until(
        lambda d: (target.get_attribute("disabled") is None) or (target.get_attribute("disabled") == "false")
    )
    try: target.click()
    except Exception: click_js(driver, target)
    time.sleep(1.2)

def selenium_to_cookiejar(driver) -> requests.cookies.RequestsCookieJar:
    jar = requests.cookies.RequestsCookieJar()
    for c in driver.get_cookies():
        name = c.get("name"); value = c.get("value")
        dom = c.get("domain") or "www.disco.com.ar"
        path = c.get("path") or "/"
        jar.set(name, value, domain=dom, path=path)
    return jar

# ====================== Scraper VTEX ======================
def clean_text_fast(v):
    if v is None: return ""
    if not isinstance(v, str): return v
    if "<" in v and ">" in v:
        try:
            v = BeautifulSoup(unescape(v), "html.parser").get_text(" ", strip=True)
        except Exception:
            pass
    return ILLEGAL_XLSX.sub("", v)

def first(lst, default=None):
    return lst[0] if isinstance(lst, list) and lst else default

def split_cat(path: str):
    if not path: return "", ""
    parts = [p for p in path.strip("/").split("/") if p]
    fix = lambda s: s.replace("-", " ").strip().title()
    cat = fix(parts[0]) if parts else ""
    sub = fix(parts[1]) if len(parts) > 1 else ""
    return cat, sub

def tipo_de_oferta(offer: dict, list_price: float, price: float) -> str:
    try:
        dh = offer.get("DiscountHighLight") or []
        if dh and isinstance(dh, list):
            name = (dh[0].get("Name") or "").strip()
            if name: return name
    except Exception:
        pass
    return "Descuento" if (price or 0) < (list_price or 0) else "Precio regular"

def make_session(cookies: Optional[requests.cookies.RequestsCookieJar]=None,
                 user_agent: Optional[str]=None):
    s = requests.Session()
    retry = Retry(total=RETRIES, backoff_factor=0.5,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET"], raise_on_status=False)
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry)
    s.mount("http://", adapter); s.mount("https://", adapter)
    headers = HEADERS.copy()
    if user_agent: headers["User-Agent"] = user_agent
    headers.setdefault("Referer", BASE + "/")
    s.headers.update(headers)
    if cookies is not None: s.cookies.update(cookies)
    return s

SESSION: Optional[requests.Session] = None

def _link_to_segments(link: str):
    if not link: return []
    link = unquote(link)
    path = link.split("?", 1)[0].strip("/")
    if not path: return []
    return [s.strip().lower() for s in path.split("/") if s.strip()]

def _walk_categories(node, results):
    link = (node.get("Link") or node.get("link") or "").strip()
    segs = _link_to_segments(link)
    if segs: results.add(tuple(segs))
    for ch in (node.get("Children") or node.get("children") or []):
        _walk_categories(ch, results)

def get_category_paths(max_depth=None):
    r = SESSION.get(FACETS, timeout=TIMEOUT); r.raise_for_status()
    data = r.json()
    results = set()
    for n1 in (data.get("CategoriesTrees") or []):
        _walk_categories(n1, results)
    if not results:
        for dep in data.get("Departments", []):
            segs = _link_to_segments(dep.get("Link") or dep.get("link") or "")
            if segs: results.add(tuple(segs))
    paths = sorted(results, key=lambda t: (len(t), t))
    if max_depth: paths = [p for p in paths if len(p) <= max_depth]
    return paths

def fetch_page_by_path(path_segments, offset, sleep_holder):
    path = "/".join(path_segments)
    map_str = ",".join(["c"] * len(path_segments))
    url = f"{SEARCH}/{path}?map={map_str}&_from={offset}&_to={offset + STEP - 1}"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
    except Exception:
        time.sleep(sleep_holder[0]); return []
    if r.status_code in (200,206):
        try: return r.json()
        except Exception:
            time.sleep(sleep_holder[0]); return []
    if r.status_code == 429:
        sleep_holder[0] = min(1.0, sleep_holder[0] + 0.2)
        time.sleep(sleep_holder[0]); return []
    if r.status_code in (500,503):
        time.sleep(sleep_holder[0]); return []
    return []

def build_key(ean: str, item_id: str, url: str) -> str:
    ean = (ean or "").strip()
    if ean: return f"E:{ean}"
    iid = (item_id or "").strip()
    if iid: return f"I:{iid}"
    return f"U:{(url or '').strip()}"

def rows_from_product(p: dict):
    rows = []
    categories = p.get("categories") or []
    cat, sub = ("","")
    if categories and isinstance(categories, list) and isinstance(categories[0], str):
        cat, sub = split_cat(categories[0])
    slug = p.get("linkText")
    base_url = f"{BASE}/{slug}/p" if slug else (p.get("link") or "")
    product_name = clean_text_fast(p.get("productName"))
    brand = clean_text_fast(p.get("brand"))
    manufacturer = p.get("manufacturer") or ""
    for it in (p.get("items") or []):
        sellers = it.get("sellers") or []
        s0 = sellers[0] if sellers else {}
        offer = s0.get("commertialOffer") or {}
        list_price = float(offer.get("ListPrice") or 0)
        price      = float(offer.get("Price") or 0)
        row = {
            "EAN": it.get("ean") or first(p.get("EAN")),
            "Código Interno": it.get("itemId") or p.get("productId"),
            "Nombre Producto": product_name,
            "Categoría": cat,
            "Subcategoría": sub,
            "Marca": brand,
            "Fabricante": manufacturer,
            "Precio de Lista": round(list_price, 2),
            "Precio de Oferta": round(price, 2),
            "Tipo de Oferta": tipo_de_oferta(offer, list_price, price),
            "URL": base_url,
        }
        rows.append(row)
    return rows

def scrape_category(segs, seen_keys: set):
    etiqueta = "/".join(segs)
    out = []
    offset = 0
    empty_streak = 0
    sleep_holder = [SLEEP_BASE]
    while True:
        data = fetch_page_by_path(segs, offset, sleep_holder)
        if not data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES: break
            offset += STEP; continue
        empty_streak = 0
        for p in data:
            try:
                for row in rows_from_product(p):
                    key = build_key(row["EAN"], row["Código Interno"], row["URL"])
                    if key in seen_keys: continue
                    seen_keys.add(key); out.append(row)
            except Exception:
                continue
        if len(data) < STEP: break
        offset += STEP
        time.sleep(sleep_holder[0])
    print(f"🗂️ {etiqueta}: +{len(out)} filas únicas")
    return etiqueta, out

def scrape_all(max_workers=MAX_WORKERS, max_depth=MAX_DEPTH):
    paths = get_category_paths(max_depth=max_depth)
    print(f"🔎 {len(paths)} rutas a scrapear (workers={max_workers})")
    seen_keys = set(); all_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(scrape_category, segs, seen_keys): segs for segs in paths}
        for fut in as_completed(futures):
            etiqueta, rows = fut.result()
            all_rows.extend(rows)
    df = pd.DataFrame(all_rows)
    for c in COLS_FINAL:
        if c not in df.columns: df[c] = pd.NA
    df["EAN"] = df["EAN"].astype("string")
    for c in ["Precio de Lista","Precio de Oferta"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
    df = df[COLS_FINAL]
    return df

def postprocess_and_save(df: pd.DataFrame):
    if df.empty:
        print("⚠️ No se obtuvieron productos.")
        return df
    # Dedupe final por EAN / Código / URL
    df["_k"] = df["EAN"].fillna("").str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "Código Interno"].fillna("").astype(str).str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "URL"].fillna("").astype(str).str.strip()
    before = len(df)
    df = df.drop_duplicates(subset=["_k"]).drop(columns=["_k"]).reset_index(drop=True)
    print(f"🧹 Dedupe final: -{before-len(df)} duplicados → {len(df)} únicos")
    # Excel
    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="productos")
        wb=w.book; ws=w.sheets["productos"]
        money=wb.add_format({"num_format":"0.00"})
        text=wb.add_format({"num_format":"@"})
        col={n:i for i,n in enumerate(COLS_FINAL)}
        ws.set_column(col["EAN"], col["EAN"], 18, text)
        ws.set_column(col["Nombre Producto"], col["Nombre Producto"], 52)
        for c in ["Categoría","Subcategoría","Marca","Fabricante"]:
            ws.set_column(col[c], col[c], 20)
        ws.set_column(col["Precio de Lista"], col["Precio de Lista"], 14, money)
        ws.set_column(col["Precio de Oferta"], col["Precio de Oferta"], 14, money)
        ws.set_column(col["URL"], col["URL"], 46)
    if OUT_CSV:
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"💾 XLSX: {OUT_XLSX} ({len(df)} filas)")
    return df

# ====================== Main ======================
if __name__ == "__main__":
    t0 = time.time()
    driver = setup_driver()
    try:
        driver.get(URL)
        wait_css(driver, "body")
        time.sleep(1.0)

        try_accept_cookies(driver)
        open_delivery_modal(driver)
        fill_email_and_send(driver, EMAIL)

        # Paso intermedio: Retiro en tienda -> CABA -> Disco Alto Palermo -> Confirmar
        time.sleep(0.8)
        pick_retire_in_store(driver)
        choose_province(driver, "CABA")
        choose_store(driver, "Disco Alto Palermo")
        click_confirm(driver)

        # Deja que VTEX aplique el segmento
        time.sleep(1.5)

        # Cookies → requests.Session
        cookiejar = selenium_to_cookiejar(driver)
        ua_header = driver.execute_script("return navigator.userAgent;")
        SESSION = make_session(cookies=cookiejar, user_agent=ua_header)
        print("✅ Segmento configurado (Retiro en tienda: CABA / Disco Alto Palermo).")

    finally:
        try:
            # Si quieres mantener la ventana abierta en modo no headless,
        # comenta la línea siguiente y asegúrate de tener detach=True.
            driver.quit()
        except Exception:
            pass

    # Scraping VTEX con esa sesión
    df = scrape_all(max_workers=MAX_WORKERS, max_depth=MAX_DEPTH)
    df = postprocess_and_save(df)
    print(f"⏱️ Tiempo total: {time.time() - t0:.1f}s")
