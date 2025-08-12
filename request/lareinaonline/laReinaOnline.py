#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import logging
from threading import Lock
from urllib.parse import urljoin, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

# ================== Config ==================
BASE = "https://www.lareinaonline.com.ar"
HOME = f"{BASE}/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 25
SLEEP_BETWEEN = 0.2
MAX_WORKERS = 6
PRINT_ROWS = True               # imprimir cada producto ni bien se obtiene
OUT_CSV = "lareina_productos.csv"

# ================== Utilidades ==================
# Captura $6.900, 00 ó $2.956,00 (nota: permite espacio después de la coma)
MONEY_RX  = re.compile(r"\$\s*[\d\.]+(?:,\s*\d{2})?")
SPACES_RX = re.compile(r"\s+")

_row_log_lock = Lock()

def norm_text(s: str) -> str:
    return SPACES_RX.sub(" ", (s or "").strip())

def money_to_decimal(txt: str) -> float | None:
    if not txt:
        return None
    m = MONEY_RX.search(txt)
    if not m:
        return None
    # "$ 1.234, 56" -> "1234.56"
    val = (
        m.group(0)
        .replace("$", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", ".")
        .strip()
    )
    try:
        return float(val)
    except Exception:
        return None

def get_qp(url: str, key: str) -> str | None:
    try:
        return parse_qs(urlparse(url).query).get(key, [None])[0]
    except Exception:
        return None

def is_ean(s: str | None) -> bool:
    return bool(s) and s.isdigit() and len(s) in (8, 13)

def mk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def fetch(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    # Fuerza una decodificación fiable (útil en sitios ASP/Windows-1252)
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "latin-1"):
        r.encoding = r.apparent_encoding
    time.sleep(SLEEP_BETWEEN)
    return r.text

def log_row(row: dict) -> None:
    campos = ("EAN", "Código Interno", "Nombre Producto", "Precio de Lista", "Precio de Oferta", "URL")
    texto = " | ".join(f"{k}: {row.get(k)}" for k in campos)
    with _row_log_lock:
        logging.info("    ✓ %s", texto)

# ================== Descubrimiento ==================
def discover_category_urls(session: requests.Session) -> list[tuple[str, str, str]]:
    """
    Devuelve una lista de tuples [(nl, url, etiqueta)] únicas encontradas en el HOME.
    """
    html = fetch(session, HOME)
    soup = BeautifulSoup(html, "html.parser")
    found: dict[str, tuple[str, str, str]] = {}
    for a in soup.select('a[href*="productosnl.asp"]'):
        href = a.get("href") or ""
        if "nl=" not in href:
            continue
        url = urljoin(BASE, href)
        nl = get_qp(url, "nl")
        if not nl:
            continue
        label = norm_text(a.get_text(" ", strip=True))
        if nl not in found:
            found[nl] = (nl, url, label)
    return list(found.values())

def collect_category_pages(session: requests.Session, nl_url: str, nl_code: str) -> list[str]:
    """
    Lee la página inicial de la subcategoría y detecta URLs de paginación del mismo nl.
    """
    html = fetch(session, nl_url)
    soup = BeautifulSoup(html, "html.parser")
    pages = set([nl_url])
    for a in soup.select('a[href*="productosnl.asp"]'):
        href = a.get("href") or ""
        if "nl=" in href and nl_code in href:
            pages.add(urljoin(BASE, href))
    return sorted(pages)

# ================== Listado ==================
def extract_list_cards(html: str) -> list[dict]:
    """
    Extrae cada <li class="cuadProd"> con:
      - link detalle desde .FotoProd a[href*='productosdet.asp']
      - nombre desde .InfoProd .desc  (fallback: alt de la imagen)
      - precio desde .InfoProd .precio (maneja coma + centavos en <b>)
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    # Preferimos el patrón del sitio
    for li in soup.select("li.cuadProd"):
        a = li.select_one(".FotoProd a[href*='productosdet.asp']")
        if not a:
            continue
        det_url = urljoin(BASE, a.get("href", ""))
        if not det_url or det_url in seen:
            continue
        seen.add(det_url)

        # Nombre
        name_el = li.select_one(".InfoProd .desc")
        name = norm_text(name_el.get_text(" ", strip=True)) if name_el else None
        if not name:
            # Fallback: alt de la imagen (suele venir en mayúsculas)
            img = li.select_one(".FotoProd img[alt]")
            if img and img.get("alt"):
                name = norm_text(img.get("alt"))

        # Precio
        p_block = li.select_one(".InfoProd .precio") or li
        money_text = norm_text(p_block.get_text(" ", strip=True))
        # Unir "coma + centavos" si vienen con espacio: ", 00" -> ",00"
        money_text = re.sub(r",\s*(\d{2})", r",\1", money_text)
        price = money_to_decimal(money_text)

        is_offer = ("OFERTA" in money_text.upper())

        items.append({
            "url_detalle": det_url,
            "nombre_listado": name,
            "precio_listado": price,
            "oferta_listado": is_offer
        })

    # Fallback genérico si no encontramos la estructura anterior
    if not items:
        for a in soup.select('a[href*="productosdet.asp"]'):
            href = a.get("href") or ""
            det_url = urljoin(BASE, href)
            if det_url in seen:
                continue
            seen.add(det_url)

            cont = a
            for _ in range(3):
                if cont and cont.parent:
                    cont = cont.parent
            text_block = norm_text(cont.get_text(" ", strip=True)) if cont else ""

            name = None
            name_el = cont.select_one(".desc") if cont else None
            if name_el:
                name = norm_text(name_el.get_text(" ", strip=True))
            if not name and cont:
                el = cont.select_one("h1, h2, h3, strong, b")
                if el:
                    name = norm_text(el.get_text(" ", strip=True))
            if not name:
                for chunk in [t.strip() for t in re.split(r"\s{2,}|\|", text_block) if t.strip()]:
                    if "$" in chunk:
                        continue
                    up = chunk.upper()
                    if up in ("AGREGAR", "OFERTA", "VOLVER"):
                        continue
                    if len(chunk) >= 6:
                        name = chunk
                        break

            money_text = re.sub(r",\s*(\d{2})", r",\1", text_block)
            price = money_to_decimal(money_text)
            is_offer = ("OFERTA" in text_block.upper())

            items.append({
                "url_detalle": det_url,
                "nombre_listado": name,
                "precio_listado": price,
                "oferta_listado": is_offer
            })

    return items

# ================== Detalle ==================
def parse_detail(session: requests.Session, url: str) -> dict:
    html = fetch(session, url)
    soup = BeautifulSoup(html, "html.parser")

    # Contenedor principal del detalle (o cae a soup)
    right = soup.select_one(".DetallDer") or soup
    desc  = right.select_one(".DetallDesc") or right

    # ---------- Nombre ----------
    name = None
    elb = desc.find("b") if desc else None
    if elb:
        name = norm_text(elb.get_text(" ", strip=True))
    if not name and desc:
        el = desc.select_one("h1, h2, h3, strong")
        if el:
            name = norm_text(el.get_text(" ", strip=True))
    if not name and desc:
        raw = norm_text(desc.get_text(" ", strip=True))
        for chunk in re.split(r"\s{2,}|\|", raw):
            c = chunk.strip()
            up = c.upper()
            if c and "$" not in c and up not in ("AGREGAR", "OFERTA", "VOLVER") and len(c) >= 6:
                name = c
                break
    if not name and soup.title:
        name = norm_text(soup.title.get_text(strip=True))

    # ---------- Marca ----------
    brand = None
    tag_brand = (desc.select_one(".DetallMarc") if desc else None) or right.select_one(".DetallMarc")
    if tag_brand:
        brand = norm_text(tag_brand.get_text(" ", strip=True))

    # ---------- Categoría / Subcategoría ----------
    cats = [norm_text(a.get_text(" ", strip=True)) for a in soup.select('a[href*="productosnl.asp"]')]
    categoria = cats[0] if cats else None
    subcategoria = cats[-1] if len(cats) >= 2 else None

    # ---------- Precios ----------
    prec_block = right.select_one(".DetallPrec") or right
    money_text = norm_text(prec_block.get_text(" ", strip=True))
    money_text = re.sub(r",\s*(\d{2})", r",\1", money_text)
    prices = [money_to_decimal(m.group(0)) for m in MONEY_RX.finditer(money_text)]
    prices = [p for p in prices if p is not None]
    precio_lista = precio_oferta = None
    if prices:
        if len(prices) == 1:
            precio_lista = prices[0]
        else:
            precio_lista, precio_oferta = max(prices), min(prices)
    oferta_flag = "OFERTA" in right.get_text(" ", strip=True).upper()

    # ---------- EAN / Código Interno ----------
    ean = codigo_interno = None

    agre = right.select_one(".DetallAgre[onclick]")
    if agre:
        m = re.search(r"FLaCompDet\('(\d+)'\)", agre.get("onclick", ""))
        if m:
            pr = m.group(1)
            ean = pr if is_ean(pr) else None
            codigo_interno = pr if not ean else None

    if not ean and not codigo_interno:
        inp = right.select_one("input[id^='c'][name^='c']")
        if inp:
            pr = (inp.get("id") or "").lstrip("c")
            if pr:
                ean = pr if is_ean(pr) else None
                codigo_interno = pr if not ean else None

    if not ean and not codigo_interno:
        for sc in right.select("script"):
            m = re.search(r"ProductoEnTicket\.asp\?Prod=(\d+)", sc.text or "")
            if m:
                pr = m.group(1)
                ean = pr if is_ean(pr) else None
                codigo_interno = pr if not ean else None
                break

    if not ean and not codigo_interno:
        pr = get_qp(url, "Pr")
        if pr:
            ean = pr if is_ean(pr) else None
            codigo_interno = pr if not ean else None

    return {
        "EAN": ean,
        "Código Interno": codigo_interno,
        "Nombre Producto (detalle)": name,
        "Marca": brand,
        "Precio de Lista": precio_lista,
        "Precio de Oferta": precio_oferta,
        "Tipo de Oferta": "Oferta" if oferta_flag else None,
        "Categoría": categoria,
        "Subcategoría": subcategoria,
        "URL": url
    }

# ================== Pipeline ==================
def scrape_lareina() -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    session = mk_session()

    logging.info("Descubriendo subcategorías…")
    cats = discover_category_urls(session)
    logging.info("Encontradas %d subcategorías", len(cats))

    rows = []
    for nl, url_cat, label in cats:
        logging.info("📂 Subcategoría %s (%s)", label or "", nl)
        try:
            page_urls = collect_category_pages(session, url_cat, nl)
        except Exception as e:
            logging.warning("No pude leer páginas de %s: %s", url_cat, e)
            continue

        for purl in page_urls:
            try:
                html = fetch(session, purl)
            except Exception as e:
                logging.warning("Fallo leyendo %s: %s", purl, e)
                continue

            cards = extract_list_cards(html)
            logging.info("  • %s → %d productos (página)", purl, len(cards))

            # En paralelo, completar info con el detalle
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = {ex.submit(parse_detail, session, c["url_detalle"]): c for c in cards}
                for fut in as_completed(futs):
                    base = futs[fut]
                    try:
                        det = fut.result()
                    except Exception as e:
                        logging.warning("    - Error detalle %s: %s", base["url_detalle"], e)
                        continue

                    nombre = base.get("nombre_listado") or det.get("Nombre Producto (detalle)")
                    precio_lista = det.get("Precio de Lista") or base.get("precio_listado")
                    precio_oferta = det.get("Precio de Oferta")
                    tipo_oferta = det.get("Tipo de Oferta") or ("Oferta" if base.get("oferta_listado") else None)

                    row = {
                        "EAN": det["EAN"],
                        "Código Interno": det["Código Interno"],
                        "Nombre Producto": nombre,
                        "Categoría": det["Categoría"],
                        "Subcategoría": det["Subcategoría"],
                        "Marca": det["Marca"],
                        "Fabricante": None,
                        "Precio de Lista": precio_lista,
                        "Precio de Oferta": precio_oferta,
                        "Tipo de Oferta": tipo_oferta,
                        "URL": det["URL"],
                    }

                    if PRINT_ROWS:
                        log_row(row)

                    rows.append(row)

    df = pd.DataFrame(rows).drop_duplicates(subset=["URL"]).reset_index(drop=True)
    return df

# ================== Main ==================
if __name__ == "__main__":
    df = scrape_lareina()
    print("\n✅ Filas obtenidas:", len(df))
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("Guardado:", OUT_CSV)
