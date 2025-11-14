#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DAR (darentucasa.com.ar) — Scrape completo (N1/N2/N3) + Ingesta MySQL (login+iframes+fallbacks)

Novedades clave:
- Login opcional (--user/--password) cuando cae en Login.asp.
- Descubrimiento robusto de categorías:
    * Overlay “PRODUCTOS” normal
    * Regex sobre page_source (Dispara('NN'))
    * Búsqueda recursiva en iframes/frames
    * Brute-force de N1/N2/N3 con EnvioForm('Cat', ...)
    * Rutas directas/ búsqueda (busca.asp?texto=...)
- Paginación extendida (input/a “Siguiente”, “Ver más/Cargar más”)
- ENTER → ingesta inmediata

Requisitos:
  pip install selenium webdriver-manager mysql-connector-python pandas numpy
  Debe existir base_datos.py con get_conn()
"""

import os, sys, time, re, argparse, random, threading, tempfile, shutil
from copy import deepcopy
from typing import Tuple, Dict, Any, List, Optional
from datetime import datetime as dt

import pandas as pd
import numpy as np

# ---------- Selenium ----------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, JavascriptException, NoSuchFrameException
from webdriver_manager.chrome import ChromeDriverManager

# ---------- MySQL ----------
import mysql.connector
from mysql.connector import errors as myerr

# ---------- Conexión ----------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn

TIENDA_CODIGO = "darentucasa"
TIENDA_NOMBRE = "DAR en tu Casa"
HOME_URL      = "https://www.darentucasa.com.ar/"
LOGIN_URL     = "https://www.darentucasa.com.ar/Login.asp"

MAXLEN_TIPO_OFERTA   = 64
MAXLEN_COMENTARIOS   = 255
MAXLEN_NOMBRE        = 255
MAXLEN_CATEGORIA     = 120
MAXLEN_SUBCATEGORIA  = 200
MAXLEN_NOMBRE_TIENDA = 255

LOCK_ERRNOS = {1205, 1213}

def _truncate(s: Optional[str], n: int) -> Optional[str]:
    if s is None: return None
    s = str(s);  return s if len(s) <= n else s[:n]

def _price_str(val) -> Optional[str]:
    if val is None: return None
    try:
        f = float(val)
        if pd.isna(f) or np.isinf(f) or abs(f) > 999999999: return None
        return f"{round(f,2):.2f}"
    except Exception:
        return None

def parse_price(text: str) -> Optional[float]:
    if not text: return None
    t = re.sub(r"[^\d,\.]", "", text.strip()).replace(".", "").replace(",", ".")
    try:    return float(t)
    except: return None

def exec_with_retry(cur, sql, params=None, max_retries=5, base_sleep=0.4):
    attempt = 0
    while True:
        try:
            cur.execute(sql, params or ())
            return
        except myerr.DatabaseError as e:
            code = getattr(e, "errno", None)
            if code in LOCK_ERRNOS and attempt < max_retries:
                wait = base_sleep * (2 ** attempt)
                print(f"[LOCK] errno={code} reintento {attempt+1}/{max_retries} en {wait:.2f}s")
                time.sleep(wait); attempt += 1; continue
            raise

# =========================
# Selenium base
# =========================
_TEMP_PROFILE_DIR: Optional[str] = None

def setup_driver(headless: bool = True, ua: Optional[str] = None) -> webdriver.Chrome:
    global _TEMP_PROFILE_DIR
    _TEMP_PROFILE_DIR = tempfile.mkdtemp(prefix="dar_chrome_")
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox"); opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1366,900"); opts.add_argument("--lang=es-AR")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-features=AutomationControlled,TranslateUI,CalculateNativeWinOcclusion")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument(f"--user-data-dir={_TEMP_PROFILE_DIR}")
    opts.add_argument("--profile-directory=Default")
    opts.add_argument("--force-device-scale-factor=1")
    opts.add_argument("--disable-renderer-backgrounding")
    if ua:
        opts.add_argument(f"--user-agent={ua}")
    else:
        opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
                               {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"})
    except Exception: pass
    return driver

def _cleanup_profile_dir():
    global _TEMP_PROFILE_DIR
    if _TEMP_PROFILE_DIR and os.path.isdir(_TEMP_PROFILE_DIR):
        try: shutil.rmtree(_TEMP_PROFILE_DIR, ignore_errors=True)
        except Exception: pass
    _TEMP_PROFILE_DIR = None

# =========================
# Utilidades de frames y espera
# =========================
def find_in_all_frames(driver, by, selector, wait_s) -> Optional[Any]:
    """Busca el primer elemento que exista en cualquier frame/iframe (baja un nivel por simplicidad)."""
    # main
    try:
        return WebDriverWait(driver, 1).until(EC.presence_of_element_located((by, selector)))
    except Exception:
        pass
    # iframes
    frames = driver.find_elements(By.CSS_SELECTOR, "frame, iframe")
    for i, fr in enumerate(frames):
        try:
            driver.switch_to.frame(fr)
            try:
                el = WebDriverWait(driver, 1).until(EC.presence_of_element_located((by, selector)))
                driver.switch_to.default_content()
                return el
            except Exception:
                driver.switch_to.default_content()
        except NoSuchFrameException:
            driver.switch_to.default_content()
    return None

def run_js_all_frames(driver, script: str) -> bool:
    """Intenta ejecutar JS en main y en cada iframe (útil para EnvioForm/Menu)."""
    # main
    try:
        driver.execute_script(script); time.sleep(0.4); return True
    except Exception:
        pass
    # iframes
    frames = driver.find_elements(By.CSS_SELECTOR, "frame, iframe")
    for fr in frames:
        try:
            driver.switch_to.frame(fr)
            try:
                driver.execute_script(script); time.sleep(0.4)
                driver.switch_to.default_content(); return True
            except Exception:
                driver.switch_to.default_content()
        except NoSuchFrameException:
            driver.switch_to.default_content()
    return False

# =========================
# Login opcional
# =========================
def maybe_login(driver, user: Optional[str], password: Optional[str], wait_s: int) -> None:
    driver.get(HOME_URL)
    time.sleep(0.8)
    # ¿redirigió a Login.asp?
    cur = driver.current_url.lower()
    if "login.asp" not in cur and "Login.asp" not in cur:
        print("ℹ️  No parece requerir login inicial.")
        return

    if not user or not password:
        print("⚠ En Login.asp y sin credenciales — intento seguir en modo invitado.")
        return

    print("→ Intentando login…")
    # buscar inputs típicos (id/name)
    candidates_user = ["usuario", "email", "correo", "login", "user"]
    candidates_pass = ["clave", "password", "pass", "contrasena", "contraseña"]

    ok_user = ok_pass = False
    for inp in driver.find_elements(By.CSS_SELECTOR, "input"):
        attr = (inp.get_attribute("id") or "") + " " + (inp.get_attribute("name") or "") + " " + (inp.get_attribute("placeholder") or "")
        low = attr.lower()
        if not ok_user and any(k in low for k in candidates_user):
            inp.clear(); inp.send_keys(user); ok_user = True
        if not ok_pass and any(k in low for k in candidates_pass):
            inp.clear(); inp.send_keys(password); ok_pass = True

    # botón ingresar
    btn = None
    for el in driver.find_elements(By.CSS_SELECTOR, "input[type=submit], button, a"):
        t = (el.text or el.get_attribute("value") or "").strip().lower()
        if any(x in t for x in ["ingresar", "entrar", "login", "acceder"]):
            btn = el; break
    if btn:
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(1.5)
    else:
        # último recurso: submit por JS
        run_js_all_frames(driver, "if (document.forms && document.forms[0]) document.forms[0].submit();")
        time.sleep(1.5)

    # si tras login seguimos en Login.asp, seguimos igual (modo invitado)
    print(f"↪ URL tras login: {driver.current_url}")

# =========================
# Navegación menú / alternativas
# =========================
def open_products_menu(driver, wait_s: int):
    # intenta abrir overlay
    driver.get(HOME_URL); time.sleep(0.8)
    # 1) click visible
    el = find_in_all_frames(driver, By.CSS_SELECTOR, "div.M2-Mdir.Dispara", wait_s)
    if el:
        try:
            driver.execute_script("arguments[0].click();", el); time.sleep(0.6)
            print("✔ Menú PRODUCTOS abierto (click)")
            return True
        except Exception:
            pass
    # 2) Menu() por JS
    if run_js_all_frames(driver, "if (typeof Menu==='function') Menu();"):
        print("✔ Menú PRODUCTOS abierto (Menu() JS)")
        return True
    print("✖ No pude abrir overlay PRODUCTOS (seguimos con métodos alternativos).")
    return False

def discover_n1_from_dom_or_source(driver, wait_s: int, include_destacados: bool) -> List[Dict[str,str]]:
    rutas, seen = [], set()

    # DOM (overlay)
    nodes = driver.find_elements(By.CSS_SELECTOR, "div#Niv-1 .M2-N1[onclick]")
    for el in nodes:
        oc = el.get_attribute("onclick") or ""
        nombre = (el.text or "").strip()
        if not include_destacados and ("top.location.href" in oc or "EnvioForm('CM')" in oc):
            continue
        m = re.search(r"Dispara\('(\d+)'\)", oc)
        if not m:
            # id="D01"…
            m2 = re.search(r"D(\d+)", el.get_attribute("id") or "")
            if not m2: continue
            n1 = m2.group(1)
        else:
            n1 = m.group(1)
        if n1 in seen: continue
        seen.add(n1); rutas.append({"n1": n1, "nombre": nombre or f"N1 {n1}"})

    if rutas:
        print(f"🧭 N1 detectados (DOM): {len(rutas)}")
        return rutas

    # Regex sobre page_source
    html = driver.page_source or ""
    cand = set(re.findall(r"Dispara\('(\d{2})'\)", html))
    if cand:
        for n1 in sorted(cand):
            rutas.append({"n1": n1, "nombre": f"N1 {n1}"})
        print(f"🧭 N1 detectados (regex en HTML): {len(rutas)}")
        return rutas

    print("🫥 No hay N1 por DOM ni regex.")
    return []

def open_menu_to_n1(driver, n1: str, wait_s: int):
    # intenta DOM
    el = find_in_all_frames(driver, By.CSS_SELECTOR, f"div#D{n1}.M2-N1, #D{n1}", wait_s)
    if el:
        try:
            driver.execute_script("arguments[0].click();", el); time.sleep(0.4); return True
        except Exception: pass
    # intenta JS
    if run_js_all_frames(driver, f"if (typeof Dispara==='function') Dispara('{n1}');"):
        return True
    return False

def open_menu_to_n2(driver, n1: str, n2: str, wait_s: int):
    el = find_in_all_frames(driver, By.CSS_SELECTOR, f"div#D{n1}-{n2}.M2-N2Act, #D{n1}-{n2}", wait_s)
    if el:
        try:
            driver.execute_script("arguments[0].click();", el); time.sleep(0.4); return True
        except Exception: pass
    if run_js_all_frames(driver, f"if (typeof Dispara2==='function') Dispara2('{n1}','{n2}');"):
        return True
    return False

def go_to_category(driver, n1: str, n2: str, n3: str, n4: str, wait_s: int):
    if not open_menu_to_n2(driver, n1, n2, wait_s):
        # seguimos igual; probamos EnvioForm directo
        pass
    if run_js_all_frames(driver, f"if (typeof EnvioForm==='function') EnvioForm('Cat','{n1}','{n2}','{n3}','{n4}');"):
        try:
            WebDriverWait(driver, wait_s).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds")))
        except Exception:
            pass
        time.sleep(0.6); return True
    return False

def brute_force_categories(driver, wait_s: int, max_n1: int = 30, max_n2: int = 30, max_n3: int = 30) -> List[Tuple[str,str,str,str]]:
    print("🔎 Brute-force de categorías (EnvioForm) — límites bajos para probar existencia…")
    found = []
    for n1 in [f"{i:02d}" for i in range(1, max_n1+1)]:
        for n2 in [f"{j:02d}" for j in range(0, max_n2+1)]:
            for n3 in [f"{k:02d}" for k in range(0, max_n3+1)]:
                ok = run_js_all_frames(driver, f"if (typeof EnvioForm==='function') EnvioForm('Cat','{n1}','{n2}','{n3}','00');")
                if not ok:
                    continue
                try:
                    WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds")))
                    # pequeño chequeo de tarjetas
                    if driver.find_elements(By.CSS_SELECTOR, "ul.listaProds li.cuadProd"):
                        found.append((n1,n2,n3,"00"))
                        print(f"  ✓ Existe Cat {n1}/{n2}/{n3}/00")
                except Exception:
                    pass
    return found

# =========================
# Extracción y paginación
# =========================
def _page_signature(driver) -> str:
    try:
        lis = driver.find_elements(By.CSS_SELECTOR, "ul.listaProds li.cuadProd")
        codes = []
        for li in lis:
            c = extract_code_from(li)
            if c: codes.append(c)
        return "|".join(codes)
    except Exception:
        return ""

def _ensure_full_list_loaded(driver, min_loops: int = 3, max_loops: int = 12):
    last_count = -1; loops = 0
    while loops < max_loops:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.35 + random.random()*0.20)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.15 + random.random()*0.10)
        count = len(driver.find_elements(By.CSS_SELECTOR, "ul.listaProds li.cuadProd"))
        if count == last_count and loops >= min_loops: break
        last_count = count; loops += 1

def text_or_empty(el) -> str:
    try:    return el.text.strip()
    except: return ""

def extract_code_from(li) -> Optional[str]:
    try:
        comprar = li.find_element(By.XPATH, ".//div[contains(@class,'AgregaArt') and @onclick]")
        oc = comprar.get_attribute("onclick") or ""
        m = re.search(r"PCompra\('(\d+)'\)", oc)
        if m: return m.group(1)
    except Exception: pass
    try:
        img = li.find_element(By.CSS_SELECTOR, ".FotoProd img")
        src = img.get_attribute("src") or ""
        m = re.search(r"/Articulos/(\d+)\.(?:jpg|png|jpeg|gif)", src, re.I)
        if m: return m.group(1)
        oc = img.get_attribute("onclick") or ""
        m = re.search(r"Pr=(\d+)", oc)
        if m: return m.group(1)
    except Exception: pass
    return None

def collect_page_products(driver, min_loops: int, max_loops: int, wait_s: int) -> List[Dict[str, Any]]:
    items = []
    # intenta encontrar listado en cualquier frame
    ul = find_in_all_frames(driver, By.CSS_SELECTOR, "ul.listaProds", wait_s)
    if not ul:
        # intenta de nuevo en main por si se perdió el handle
        try:
            ul = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds")))
        except Exception:
            return items
    _ensure_full_list_loaded(driver, min_loops=min_loops, max_loops=max_loops)
    cards = driver.find_elements(By.CSS_SELECTOR, "ul.listaProds li.cuadProd")
    for li in cards:
        try:
            desc_el  = li.find_element(By.CSS_SELECTOR, ".InfoProd .desc")
            price_el = li.find_element(By.CSS_SELECTOR, ".InfoProd .precio .izq")
        except Exception:
            continue
        code      = extract_code_from(li)
        desc      = text_or_empty(desc_el)
        price_raw = text_or_empty(price_el)
        price     = parse_price(price_raw)
        is_offer  = False
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
            "codigo": code, "descripcion": desc,
            "precio_texto": price_raw, "precio": price,
            "oferta": is_offer, "imagen": img_url
        })
    return items

def click_next(driver, prev_sig: Optional[str], wait_s: int, min_loops: int, max_loops: int) -> bool:
    btns = driver.find_elements(By.XPATH, "//input[contains(@class,'PagArt') and (contains(@value,'Siguiente') or contains(@value,'Sig') or contains(@value,'>'))]")
    links = driver.find_elements(By.XPATH, "//a[contains(@class,'PagArt') and (contains(.,'Siguiente') or contains(.,'>') or @onclick)]")
    ver_mas = driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(translate(., 'VERMASCARGAR', 'vermascargar'), 'ver más') or contains(translate(., 'VERMASCARGAR', 'vermascargar'), 'cargar más')]")
    candidates = btns + links + ver_mas
    if not candidates: return False
    ul_before = None
    try: ul_before = driver.find_element(By.CSS_SELECTOR, "ul.listaProds")
    except Exception: pass
    clicked = False
    for el in candidates:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            oc = el.get_attribute("onclick") or ""
            if oc: driver.execute_script(oc)
            else:  driver.execute_script("arguments[0].click();", el)
            clicked = True; break
        except Exception: continue
    if not clicked: return False
    try:
        if ul_before: WebDriverWait(driver, wait_s).until(EC.staleness_of(ul_before))
        WebDriverWait(driver, wait_s).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds")))
        time.sleep(0.8); _ensure_full_list_loaded(driver, min_loops=min_loops, max_loops=max_loops)
        sig = _page_signature(driver);  return (prev_sig is None) or (sig != prev_sig)
    except TimeoutException:
        return False

# =========================
# Scrape
# =========================
def split_categoria_sub(cat_nombre: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not cat_nombre: return None, None
    parts = [p.strip() for p in str(cat_nombre).split(">") if p.strip()]
    if not parts: return None, None
    categoria = parts[0]; sub = " > ".join(parts[1:]) if len(parts) > 1 else None
    return categoria, sub

def scrape_tree(driver, wait_s: int, include_destacados: bool, min_loops: int, max_loops: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str,Any]] = []
    opened = open_products_menu(driver, wait_s)
    rutas_n1 = discover_n1_from_dom_or_source(driver, wait_s, include_destacados)
    if not rutas_n1 and not opened:
        # último recurso: brute-force de categorías
        found = brute_force_categories(driver, wait_s, max_n1=20, max_n2=30, max_n3=30)
        for (n1,n2,n3,n4) in found:
            print(f"\n[BF] N1/N2/N3 {n1}/{n2}/{n3}")
            if not go_to_category(driver, n1, n2, n3, n4, wait_s):
                continue
            page_idx = 1
            while True:
                print(f"  📄 Página {page_idx} (BF): extrayendo…")
                items = collect_page_products(driver, min_loops, max_loops, wait_s)
                for it in items:
                    it["cat_n0"] = n1; it["cat_n2"] = n2; it["cat_n3"] = n3
                    it["cat_nombre"] = f"N1 {n1} > N2 {n2} > N3 {n3}"
                    rows.append(it)
                sig = _page_signature(driver)
                if not click_next(driver, prev_sig=sig, wait_s=wait_s, min_loops=min_loops, max_loops=max_loops):
                    break
                page_idx += 1
        return rows

    # Si hay N1, intentar N2/N3 navegando con EnvioForm
    for r1 in rutas_n1:
        n1 = r1["n1"]; nombre_n1 = r1["nombre"]
        print(f"\n=== N1 {n1} — {nombre_n1} ===")
        # intentar listar N2 por DOM/regex
        # si no aparece N2, brute-force N2/N3
        got_any = False
        # brute N2/N3 (hasta 40) porque DOM puede no estar en overlay real
        for n2 in [f"{j:02d}" for j in range(0, 40)]:
            # abre y prueba N3=00 de entrada
            if not go_to_category(driver, n1, n2, "00", "00", wait_s):
                continue
            got_any = True
            page_idx = 1
            while True:
                print(f"  📄 Página {page_idx} (n2={n2}): extrayendo…")
                items = collect_page_products(driver, min_loops, max_loops, wait_s)
                for it in items:
                    it["cat_n0"] = n1; it["cat_n2"] = n2; it["cat_n3"] = "00"
                    it["cat_nombre"] = f"{nombre_n1} > N2 {n2}"
                    rows.append(it)
                sig = _page_signature(driver)
                if not click_next(driver, prev_sig=sig, wait_s=wait_s, min_loops=min_loops, max_loops=max_loops):
                    break
                page_idx += 1

            # intentar N3=01..30
            for n3 in [f"{k:02d}" for k in range(1, 31)]:
                if not go_to_category(driver, n1, n2, n3, "00", wait_s):
                    continue
                page_idx = 1
                while True:
                    print(f"    📄 Página {page_idx} (n2={n2}, n3={n3}): extrayendo…")
                    items = collect_page_products(driver, min_loops, max_loops, wait_s)
                    for it in items:
                        it["cat_n0"] = n1; it["cat_n2"] = n2; it["cat_n3"] = n3
                        it["cat_nombre"] = f"{nombre_n1} > N2 {n2} > N3 {n3}"
                        rows.append(it)
                    sig = _page_signature(driver)
                    if not click_next(driver, prev_sig=sig, wait_s=wait_s, min_loops=min_loops, max_loops=max_loops):
                        break
                    page_idx += 1

        if not got_any:
            print("  (No hubo N2/N3 navegables para este N1)")

    return rows

# =========================
# Guardado e ingesta
# =========================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_with_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)", (codigo, nombre))
    exec_with_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = None
    nombre = _truncate(r.get("descripcion") or "", MAXLEN_NOMBRE)
    marca = fabricante = None
    categoria, subcategoria = split_categoria_sub(r.get("cat_nombre"))
    categoria = _truncate(categoria or "", MAXLEN_CATEGORIA)
    subcategoria = _truncate(subcategoria or "", MAXLEN_SUBCATEGORIA)
    if nombre:
        exec_with_retry(cur,
            "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
            (nombre, marca or ""))
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
    record_id = sku; url = None
    nombre_tienda = _truncate(r.get("descripcion") or None, MAXLEN_NOMBRE_TIENDA)
    if sku:
        exec_with_retry(cur, """
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
    exec_with_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en):
    precio = r.get("precio")
    precio_lista = _price_str(precio); precio_oferta = _price_str(precio)
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
    """, (tienda_id, producto_tienda_id, capturado_en,
          precio_lista, precio_oferta, tipo_oferta,
          tipo_oferta, None, None, promo_comentarios))

def ingest_to_mysql(df: pd.DataFrame):
    if df.empty:
        print("⚠ No hay filas para insertar en MySQL."); return
    conn = None
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cset:
                cset.execute("SET SESSION innodb_lock_wait_timeout = 8")
                cset.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception: pass
        conn.autocommit = False
        cur = conn.cursor(buffered=True)
        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        capturado_en = dt.now()
        total = batch = 0
        for _, r in df.iterrows():
            rec = r.to_dict()
            try:
                pid  = find_or_create_producto(cur, rec)
                ptid = upsert_producto_tienda(cur, tienda_id, pid, rec)
                insert_historico(cur, tienda_id, ptid, rec, capturado_en)
                total += 1; batch += 1
                if batch >= 50: conn.commit(); batch = 0
            except myerr.DatabaseError as e:
                errno = getattr(e, "errno", None)
                if errno in LOCK_ERRNOS:
                    try: conn.rollback()
                    except: pass
                    print(f"[WARN] lock en fila (codigo={rec.get('codigo')}), continúo…"); continue
                elif errno == 1264:
                    try: conn.rollback()
                    except: pass
                    print(f"[DOWNGRADE] 1264 (codigo={rec.get('codigo')}). Reinsertando con precios NULL.")
                    rec2 = dict(rec); rec2["precio"] = None
                    try:
                        pid  = find_or_create_producto(cur, rec2)
                        ptid = upsert_producto_tienda(cur, tienda_id, pid, rec2)
                        insert_historico(cur, tienda_id, ptid, rec2, capturado_en)
                        total += 1; batch += 1
                        if batch >= 50: conn.commit(); batch = 0
                    except Exception as e2:
                        try: conn.rollback()
                        except: pass
                        print(f"[SKIP] persistente tras downgrade: {e2}"); continue
                else:
                    try: conn.rollback()
                    except: pass
                    print(f"[SKIP] MySQL errno={errno} en (codigo={rec.get('codigo')}): {e}"); continue
        if batch: conn.commit()
        print(f"✅ MySQL: {total} registros de histórico insertados/actualizados.")
    finally:
        try:
            if conn: conn.close()
        except Exception: pass

# === ENTER ingest ===
_ROWS_LOCK = threading.Lock()
_SHARED_ROWS: Dict[str, Any] = {"ref": None}

def _snapshot_df_for_ingest() -> pd.DataFrame:
    with _ROWS_LOCK:
        ref = _SHARED_ROWS.get("ref") or []; rows = deepcopy(ref)
    cols = ["codigo","descripcion","precio","precio_texto","oferta","imagen","cat_n0","cat_n2","cat_n3","cat_nombre"]
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    if not df.empty:
        try: df.sort_values(by=["cat_nombre","descripcion"], inplace=True, kind="stable")
        except Exception: pass
    return df

def _enter_listener_loop():
    print("💡 Tip: presioná ENTER en cualquier momento para INGESTAR a MySQL lo acumulado hasta ahora.")
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                time.sleep(0.4); continue
            df = _snapshot_df_for_ingest()
            if df.empty:
                print("↩️  ENTER recibido, pero todavía no hay filas para ingestar."); continue
            print(f"↩️  ENTER: ingesto {len(df)} filas acumuladas…")
            try:
                ingest_to_mysql(df); print("✔ Ingesta por ENTER completada.\n")
            except Exception as e:
                print(f"❌ Error en ingesta por ENTER: {e}\n")
        except Exception:
            time.sleep(0.4)

def _start_enter_listener():
    t = threading.Thread(target=_enter_listener_loop, daemon=True)
    t.start()

# =========================
# CLI principal
# =========================
def main():
    ap = argparse.ArgumentParser(description="DAR → Scrape completo + Ingesta MySQL (robusto)")
    ap.add_argument("--out", default="dar_catalogo_completo.xlsx")
    ap.add_argument("--csv", default=None)
    ap.add_argument("--no-headless", action="store_true")
    ap.add_argument("--no-ingest", action="store_true")
    ap.add_argument("--include-destacados", action="store_true")
    ap.add_argument("--wait", type=int, default=45)
    ap.add_argument("--min-loops", type=int, default=3)
    ap.add_argument("--max-loops", type=int, default=12)
    ap.add_argument("--extra-queries", type=str, default="", help="Palabras para búsqueda extra (espacio)")
    ap.add_argument("--user", type=str, default=None, help="Usuario/correo para Login.asp (opcional)")
    ap.add_argument("--password", type=str, default=None, help="Contraseña para Login.asp (opcional)")
    args = ap.parse_args()

    headless  = not args.no_headless
    wait_s    = max(12, args.wait)
    min_loops = max(1, args.min_loops)
    max_loops = max(min_loops, args.max_loops)
    extra_q   = [q for q in (args.extra_queries.split() if args.extra_queries else []) if q.strip()]

    driver = setup_driver(headless=headless)
    try:
        _start_enter_listener()
        # 1) login si aplica
        maybe_login(driver, args.user, args.password, wait_s)

        # 2) scrape árbol/alternativas
        all_rows = scrape_tree(driver, wait_s, args.include_destacados, min_loops, max_loops)

        # 3) búsquedas adicionales
        if extra_q:
            print(f"\n=== EXTRA QUERIES === {extra_q}")
            for q in extra_q:
                driver.get(f"https://www.darentucasa.com.ar/busca.asp?texto={q}")
                try:
                    WebDriverWait(driver, wait_s).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.listaProds")))
                except Exception:
                    continue
                page_idx = 1
                while True:
                    print(f"  📄 Página {page_idx} [search='{q}']: extrayendo…")
                    rows = collect_page_products(driver, min_loops, max_loops, wait_s)
                    for rp in rows:
                        rp["cat_n0"] = f"Q::{q}"; rp["cat_n2"] = "00"; rp["cat_n3"] = "00"
                        rp["cat_nombre"] = f"Búsqueda: {q}"
                        all_rows.append(rp)
                    sig = _page_signature(driver)
                    if not click_next(driver, prev_sig=sig, wait_s=wait_s, min_loops=min_loops, max_loops=max_loops):
                        break
                    page_idx += 1

        # 4) export
        df = pd.DataFrame(all_rows, columns=["codigo","descripcion","precio","precio_texto","oferta","imagen","cat_n0","cat_n2","cat_n3","cat_nombre"])
        if not df.empty:
            df.sort_values(by=["cat_nombre","descripcion"], inplace=True, kind="stable")
        df.to_excel(args.out, index=False)
        print(f"\n✅ XLSX guardado: {args.out}")
        if args.csv:
            df.to_csv(args.csv, index=False); print(f"✅ CSV guardado: {args.csv}")

        if not args.no_ingest and not df.empty:
            ingest_to_mysql(df)

    finally:
        try: driver.quit()
        except Exception: pass
        _cleanup_profile_dir()

if __name__ == "__main__":
    try:
        main()
    except WebDriverException as e:
        print(f"❌ WebDriver error: {e}")
        sys.exit(2)
