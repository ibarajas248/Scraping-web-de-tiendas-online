#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
from html import unescape
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== Config ==================
BASE = "https://www.dinoonline.com.ar"
START_URL = (
    "https://www.dinoonline.com.ar/super/categoria"
    "?_dyncharset=utf-8&Dy=1&Nty=1&minAutoSuggestInputLength=3"
    "&autoSuggestServiceUrl=%2Fassembler%3FassemblerContentCollection%3D%2Fcontent%2FShared%2FAuto-Suggest+Panels%26format%3Djson"
    "&searchUrl=%2Fsuper&containerClass=search_rubricator"
    "&defaultImage=%2Fimages%2Fno_image_auto_suggest.png&rightNowEnabled=false&Ntt="
)
OUT_XLSX = "dinoonline_productos.xlsx"

TIMEOUT = 25
SLEEP_BETWEEN_PAGES = 0.6
MAX_SEEN_PAGES = 1000   # safety
# ============================================


# -------- Sesión robusta --------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    retry = Retry(
        total=5, connect=5, read=5, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


# -------- Utilidades --------
def clean_money(txt: str):
    """Normaliza $1.190,50 / $1,190.50 -> float"""
    if not txt:
        return None
    t = re.sub(r"[^\d,.\-]", "", txt)
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None

def text_or_none(el):
    return el.get_text(strip=True) if el else None

def absolute_url(href: str):
    if not href:
        return None
    href = unescape(href)  # <-- clave para &amp;
    return urljoin(BASE, href)

def build_url_with_params(current_url: str, **overrides):
    """Devuelve current_url con params modificados por overrides (strings)."""
    p = urlparse(current_url)
    q = parse_qs(p.query, keep_blank_values=True)
    for k, v in overrides.items():
        q[str(k)] = [str(v)]
    new_query = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


# -------- Parseo de productos --------
def parse_items(soup: BeautifulSoup):
    rows = []
    # Los items están en <div class="item col-lg-3 col-md-3 col-sm-4 col-xs-6">
    boxes = soup.select("div.item.col-lg-3, div.item.col-md-3, div.item.col-sm-4, div.item.col-xs-6")
    for box in boxes:
        prod = box.select_one("div.product")
        if not prod:
            continue

        prod_id = prod.get("id")  # ej. prod3390039
        pesable = prod.get("pesable")
        cantbulto = prod.get("cantbulto")
        categoryrec = prod.get("categoryrec")

        a = box.select_one(".image a[href]")
        href = absolute_url(a["href"]) if a else None

        img = box.select_one(".image img")
        img_src = img.get("src") if img else None
        if img_src and img_src.startswith("//"):
            img_src = "https:" + img_src
        img_alt = img.get("alt") if img else None

        precio_unidad_span = box.select_one(".precio-unidad span")
        precio_unidad_txt = text_or_none(precio_unidad_span)
        precio_unidad = clean_money(precio_unidad_txt)

        pu_div = box.select_one(".precio-unidad")
        precio_sin_imp = None
        precio_antes = None
        if pu_div:
            pu_text = pu_div.get_text(" ", strip=True)
            m1 = re.search(r"Precio\s*s/Imp.*?:\s*\$?\s*([\d\.,]+)", pu_text, re.I)
            if m1:
                precio_sin_imp = clean_money(m1.group(1))
            m2 = re.search(r"\bantes\s*\$?\s*([\d\.,]+)", pu_text, re.I)
            if m2:
                precio_antes = clean_money(m2.group(1))

        descripcion_div = box.select_one(".description")
        nombre = text_or_none(descripcion_div) or img_alt

        precio_ref_txt = text_or_none(box.select_one(".precio-referencia"))
        precio_ref_val = None
        unidad_ref = None
        if precio_ref_txt:
            m3 = re.search(r"\$?\s*([\d\.,]+)\s*x\s*(.+)", precio_ref_txt)
            if m3:
                precio_ref_val = clean_money(m3.group(1))
                unidad_ref = m3.group(2).strip()

        # imprime en tiempo real
        print(f"🛒 {nombre} - ${precio_unidad if precio_unidad is not None else 'N/D'} - URL: {href}")

        rows.append({
            "prod_id": prod_id,
            "nombre": nombre,
            "precio_unidad": precio_unidad,
            "precio_sin_imp": precio_sin_imp,
            "precio_antes": precio_antes,
            "precio_ref_valor": precio_ref_val,
            "precio_ref_unidad": unidad_ref,
            "precio_unidad_raw": precio_unidad_txt,
            "url": href,
            "img": img_src,
            "img_alt": img_alt,
            "pesable": pesable,
            "cantbulto": cantbulto,
            "categoryrec": categoryrec
        })
    return rows, len(boxes)


# -------- Paginación --------
def find_next_url_by_icon(soup: BeautifulSoup):
    """Intenta encontrar el botón '>' por el icono fa-angle-right."""
    caret = soup.select_one("a i.fa.fa-angle-right, a i.fa-angle-right")
    if caret and caret.parent and caret.parent.name == "a":
        return absolute_url(caret.parent.get("href"))
    # Fallback: buscar cualquier <a> que contenga el icono
    for a in soup.select("a[href]"):
        i = a.select_one("i.fa-angle-right, i.fa.fa-angle-right")
        if i:
            return absolute_url(a.get("href"))
    return None

def detect_nrpp_and_base(current_url: str, soup: BeautifulSoup, page_items_found: int):
    """Intenta detectar Nrpp y deja listos los params estables para paginar por No."""
    # 1) Intentar leer de un link de paginación
    cand = find_next_url_by_icon(soup)
    if cand:
        p = urlparse(cand)
        q = parse_qs(p.query, keep_blank_values=True)
        nrpp = None
        if "Nrpp" in q and q["Nrpp"]:
            try:
                nrpp = int(q["Nrpp"][0])
            except Exception:
                pass
        # base estable: todos los params salvo No (índice)
        stable_params = {k: v[0] for k, v in q.items()}
        stable_params.pop("No", None)
        return (p.scheme + "://" + p.netloc + p.path, nrpp, stable_params)

    # 2) Si no hay link, usar la URL actual
    p = urlparse(current_url)
    q = parse_qs(p.query, keep_blank_values=True)
    nrpp = None
    if "Nrpp" in q and q["Nrpp"]:
        try:
            nrpp = int(q["Nrpp"][0])
        except Exception:
            nrpp = None
    if not nrpp:
        # fallback: usar lo que realmente vimos en la página
        nrpp = page_items_found if page_items_found else 36

    stable_params = {k: v[0] for k, v in q.items()}
    stable_params.pop("No", None)  # No se recalcula cada vez
    base_path = p.scheme + "://" + p.netloc + p.path
    return base_path, nrpp, stable_params

def next_url_by_no(base_path: str, nrpp: int, stable_params: dict, page_index: int):
    """Construye siguiente URL: No = page_index * nrpp."""
    params = stable_params.copy()
    params["No"] = str(page_index * nrpp)
    # Asegurar que Nrpp esté presente
    if "Nrpp" not in params:
        params["Nrpp"] = str(nrpp)
    query = urlencode(params, doseq=False)
    return f"{base_path}?{query}"


# -------- Scraper principal --------
def get_with_fix(session: requests.Session, url: str):
    """GET con fix para &amp; -> & en caso de 404."""
    r = session.get(url, timeout=TIMEOUT)
    if r.status_code == 404 and "&amp;" in url:
        fixed = url.replace("&amp;", "&")
        print(f"[WARN] 404 con &amp;, reintentando: {fixed}")
        r = session.get(fixed, timeout=TIMEOUT)
        return r, fixed
    return r, url

def scrape_all(start_url=START_URL, limit_pages=None):
    s = make_session()
    url = start_url
    all_rows = []
    seen_urls = set()
    page_num = 1

    # Variables para paginación por No/Nrpp cuando el botón falle
    base_path = None
    nrpp = None
    stable_params = None
    page_index_for_no = 1  # ya que la primera página es No=0

    while url and page_num <= MAX_SEEN_PAGES:
        if url in seen_urls:
            print("[STOP] URL repetida, deteniendo.")
            break
        seen_urls.add(url)

        print(f"\n🌐 Página {page_num}: {url}")
        r, url = get_with_fix(s, url)
        if r.status_code == 404:
            print(f"[ERROR] 404 definitivo: {url}")
            break
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        rows, items_on_page = parse_items(soup)
        all_rows.extend(rows)

        # detectar Nrpp/base en la primera pasada (o si aún no lo tenemos)
        if base_path is None or nrpp is None or stable_params is None:
            base_path, nrpp, stable_params = detect_nrpp_and_base(url, soup, items_on_page)

        # límite manual para pruebas
        if limit_pages and page_num >= limit_pages:
            break

        # 1) Intentar con el botón ">"
        next_by_icon = find_next_url_by_icon(soup)
        if next_by_icon:
            next_by_icon = absolute_url(next_by_icon)
            if next_by_icon and next_by_icon not in seen_urls:
                url = next_by_icon
                page_num += 1
                time.sleep(SLEEP_BETWEEN_PAGES)
                continue

        # 2) Si falla, intentar construyendo con No = k*Nrpp
        if items_on_page == 0:
            print("[STOP] Página sin productos; fin.")
            break

        # Construir siguiente página por índice
        url = next_url_by_no(base_path, nrpp, stable_params, page_index_for_no)
        page_index_for_no += 1
        page_num += 1
        time.sleep(SLEEP_BETWEEN_PAGES)

    df = pd.DataFrame(all_rows)
    # ordenar columnas de forma amigable
    cols = [
        "prod_id", "nombre",
        "precio_unidad", "precio_sin_imp", "precio_antes",
        "precio_ref_valor", "precio_ref_unidad",
        "precio_unidad_raw",
        "url", "img", "img_alt",
        "pesable", "cantbulto", "categoryrec",
    ]
    cols = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
    df = df[cols]
    return df


# -------- Ejecutar --------
if __name__ == "__main__":
    df = scrape_all(START_URL)  # usa limit_pages=2 para probar rápido
    df.to_excel(OUT_XLSX, index=False)
    print(f"\n✅ Exportado a {OUT_XLSX} con {len(df)} productos.")
