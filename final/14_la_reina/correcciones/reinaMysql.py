#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Reina (lareinaonline.com.ar) — Crawl completo + Listado + Detalle + MySQL

• Descubrimiento ampliado de subcategorías "nl":
    - Semillas: HOME (/), /rubros.asp, /ofertas.asp
    - Además, durante el crawling de cada página, se agregan nls nuevos
      encontrados (BFS simple limitado por umbrales).
• Paginación por subcategoría (productosnl.asp?nl=...&pag=...).
• Listado: nombre + precio (con parsing robusto de coma/centavos).
• Detalle: nombre, marca, categoría, subcategoría, EAN/código interno, precios.
• Dedupe: prioridad por EAN -> Código Interno -> URL.
• Ingesta en MySQL: tiendas, productos, producto_tienda, historico_precios.
  - precio_lista/oferta se guardan como VARCHAR (compatibles con tu esquema).

Requisitos:
  pip install requests urllib3 beautifulsoup4 pandas numpy mysql-connector-python

Notas:
  - Usa un crawl respetuoso (SLEEP_BETWEEN) y reintentos HTTP (Retry).
  - Ajusta ACTIVE_P_VALUES para cubrir más sucursales (P=...).
"""

import re
import time
import logging
from threading import Lock
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Set

from mysql.connector import Error as MySQLError
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ================== Config ==================
BASE = "https://www.lareinaonline.com.ar"
HOME = f"{BASE}/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 25
SLEEP_BETWEEN = 0.25
MAX_WORKERS = 6
PRINT_ROWS = True
OUT_CSV = "lareina_productos.csv"

# Identidad de tienda en DB
TIENDA_CODIGO = "lareina"
TIENDA_NOMBRE = "La Reina Online"

# Límites del crawler
MAX_NL_DISCOVER = 2000        # máximo de nls a visitar
MAX_PAGES_PER_NL = 200        # máximo de páginas por nl
CRAWL_SEEDS = [HOME, f"{BASE}/rubros.asp", f"{BASE}/ofertas.asp"]

# Recorrer múltiples plazas/sucursales (param P=)
ACTIVE_P_VALUES = [1, 2]      # agrega más si hace falta

# ================== Utilidades ==================
MONEY_RX  = re.compile(r"\$\s*[\d\.]+(?:,\s*\d{2})?")
SPACES_RX = re.compile(r"\s+")
_price_clean_re = re.compile(r"[^\d,.\-]")
_NULLLIKE = {"", "null", "none", "nan", "na"}

_row_log_lock = Lock()

def norm_text(s: str) -> str:
    return SPACES_RX.sub(" ", (s or "").strip())

def money_to_decimal(txt: str) -> Optional[float]:
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

def parse_price(val) -> float:
    """Convierte a float o np.nan, aceptando formatos con coma/punto."""
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

def clean(val):
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

def get_qp(url: str, key: str) -> Optional[str]:
    try:
        return parse_qs(urlparse(url).query).get(key, [None])[0]
    except Exception:
        return None

def set_qp(url: str, key: str, value: str) -> str:
    """Devuelve url con query param key=value (reemplazando si ya existe)."""
    u = urlparse(url)
    q = parse_qs(u.query)
    q[key] = [str(value)]
    new_q = urlencode({k: v[0] for k, v in q.items()}, doseq=False)
    return u._replace(query=new_q).geturl()

def ensure_p(url: str, pval: int) -> str:
    """Asegura que la URL lleve P=pval."""
    cur = get_qp(url, "P")
    if cur and str(cur) == str(pval):
        return url
    return set_qp(url, "P", str(pval))

def mk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=0.6,
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
def discover_nl_on_page(html: str) -> List[Tuple[str, str, str]]:
    """
    Extrae tuplas (nl, url, etiqueta) de una página cualquiera.
    """
    soup = BeautifulSoup(html, "html.parser")
    found: Dict[str, Tuple[str, str, str]] = {}
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

def collect_category_pages(session: requests.Session, nl_url: str, nl_code: str, pval: int) -> List[str]:
    """
    Lee la primera página de la subcategoría y detecta URLs de paginación del mismo nl y misma P.
    """
    first = ensure_p(nl_url, pval)
    html = fetch(session, first)
    soup = BeautifulSoup(html, "html.parser")
    pages: Set[str] = {first}
    # Paginación típica del sitio
    for a in soup.select('a[href*="productosnl.asp"]'):
        href = a.get("href") or ""
        if "nl=" in href and nl_code in href:
            pages.add(ensure_p(urljoin(BASE, href), pval))
    # Fallback: construir páginas por "pag=" si aparecen
    if len(pages) == 1:
        for a in soup.find_all("a"):
            href = a.get("href") or ""
            if "productosnl.asp" in href and "nl=" in href:
                url = urljoin(BASE, href)
                if nl_code in url:
                    pages.add(ensure_p(url, pval))
    # Seguridad: ordenar y recortar
    pages = sorted(list(pages))
    if len(pages) > MAX_PAGES_PER_NL:
        pages = pages[:MAX_PAGES_PER_NL]
    return pages

def discover_all_categories(session: requests.Session, pval: int) -> List[Tuple[str, str, str]]:
    """
    BFS simple por plaza P:
      - parte de CRAWL_SEEDS (forzados con P)
      - en cada página descubre nls y las añade al índice
      - límite por MAX_NL_DISCOVER
    """
    logging.info("Iniciando descubrimiento ampliado de subcategorías (nl) para P=%s…", pval)
    queue: List[str] = [ensure_p(u, pval) for u in dict.fromkeys(CRAWL_SEEDS)]
    visited_pages: Set[str] = set()
    found_nl: Dict[str, Tuple[str, str, str]] = {}

    while queue and len(found_nl) < MAX_NL_DISCOVER:
        url = queue.pop(0)
        if url in visited_pages:
            continue
        visited_pages.add(url)
        try:
            html = fetch(session, url)
        except Exception as e:
            logging.warning("No pude leer %s: %s", url, e)
            continue

        # Extraer y acumular nl de esta página
        for nl, nl_url, label in discover_nl_on_page(html):
            if nl not in found_nl:
                # conservar URL como vino; P lo forzamos al entrar
                found_nl[nl] = (nl, nl_url, label)

        # Empujar nuevas páginas internas con P forzado
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            abs_url = urljoin(BASE, href)
            if not abs_url.startswith(BASE):
                continue
            # Evitar loops en login, carrito, etc.
            if any(bad in abs_url.lower() for bad in ("login", "carrito", "mi_cuenta", "ticket", "pdf")):
                continue
            # Limitar scope: páginas que probablemente enlacen a más categorías
            if ("rubros" in abs_url.lower()
                or "ofertas" in abs_url.lower()
                or "productos" in abs_url.lower()
                or "busqueda" in abs_url.lower()
                or "productosnl" in abs_url.lower()):
                abs_url = ensure_p(abs_url, pval)
                if abs_url not in visited_pages and abs_url not in queue:
                    queue.append(abs_url)

        logging.info("  • [P=%s] Descubiertos hasta ahora: %d nls (pendientes de visitar: %d)",
                     pval, len(found_nl), len(queue))

        if len(visited_pages) > 5000:  # hard cap de páginas visitadas
            logging.warning("Se alcanzó el límite de páginas visitadas del crawler (5000).")
            break

    cats = list(found_nl.values())
    logging.info("Descubrimiento finalizado (P=%s). NL totales: %d", pval, len(cats))
    return cats

# ================== Listado ==================
def extract_list_cards_for_p(html: str, pval: int) -> List[dict]:
    """
    Extrae cada <li class="cuadProd"> con:
      - link detalle desde .FotoProd a[href*='productosdet.asp'] (inyectando P)
      - nombre desde .InfoProd .desc  (fallback: alt de la imagen)
      - precio desde .InfoProd .precio (maneja coma + centavos en <b>)
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    for li in soup.select("li.cuadProd"):
        a = li.select_one(".FotoProd a[href*='productosdet.asp']")
        if not a:
            continue
        det_url = ensure_p(urljoin(BASE, a.get("href", "")), pval)
        if not det_url or det_url in seen:
            continue
        seen.add(det_url)

        # Nombre
        name_el = li.select_one(".InfoProd .desc")
        name = norm_text(name_el.get_text(" ", strip=True)) if name_el else None
        if not name:
            img = li.select_one(".FotoProd img[alt]")
            if img and img.get("alt"):
                name = norm_text(img.get("alt"))

        # Precio
        p_block = li.select_one(".InfoProd .precio") or li
        money_text = norm_text(p_block.get_text(" ", strip=True))
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
            det_url = ensure_p(urljoin(BASE, href), pval)
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
def parse_detail(session: requests.Session, url: str, pval: int) -> dict:
    url = ensure_p(url, pval)
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
            # en el sitio suele mostrarse precio tachado + oferta
            precio_lista, precio_oferta = max(prices), min(prices)
    oferta_flag = "OFERTA" in right.get_text(" ", strip=True).upper()

    # ---------- EAN / Código Interno ----------
    ean = codigo_interno = None

    agre = right.select_one(".DetallAgre[onclick]")
    if agre:
        m = re.search(r"FLaCompDet\('(\d+)'\)", agre.get("onclick", ""))
        if m:
            pr = m.group(1)
            ean = pr if (pr.isdigit() and len(pr) in (8, 13)) else None
            codigo_interno = pr if not ean else None

    if not ean and not codigo_interno:
        inp = right.select_one("input[id^='c'][name^='c']")
        if inp:
            pr = (inp.get("id") or "").lstrip("c")
            if pr:
                ean = pr if (pr.isdigit() and len(pr) in (8, 13)) else None
                codigo_interno = pr if not ean else None

    if not ean and not codigo_interno:
        for sc in right.select("script"):
            m = re.search(r"ProductoEnTicket\.asp\?Prod=(\d+)", sc.text or "")
            if m:
                pr = m.group(1)
                ean = pr if (pr.isdigit() and len(pr) in (8, 13)) else None
                codigo_interno = pr if not ean else None
                break

    if not ean and not codigo_interno:
        pr = get_qp(url, "Pr")
        if pr:
            ean = pr if (pr.isdigit() and len(pr) in (8, 13)) else None
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
        "URL": url  # normalizada con P
    }

# ================== Pipeline (scraping) ==================
def scrape_lareina() -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    session = mk_session()
    all_rows = []

    for pval in ACTIVE_P_VALUES:
        cats = discover_all_categories(session, pval)
        logging.info("P=%s → subcategorías a procesar: %d", pval, len(cats))

        rows = []
        visited_nl: Set[str] = set()

        for nl, url_cat, label in cats:
            if nl in visited_nl:
                continue
            visited_nl.add(nl)

            logging.info("📂 [P=%s] Subcategoría %s (%s)", pval, label or "", nl)
            try:
                page_urls = collect_category_pages(session, url_cat, nl, pval)
            except Exception as e:
                logging.warning("No pude leer páginas de %s: %s", url_cat, e)
                continue

            # Seguridad
            if len(page_urls) > MAX_PAGES_PER_NL:
                page_urls = page_urls[:MAX_PAGES_PER_NL]

            for purl in page_urls:
                try:
                    html = fetch(session, purl)  # purl ya trae P
                except Exception as e:
                    logging.warning("Fallo leyendo %s: %s", purl, e)
                    continue

                # Mientras listamos, también podríamos descubrir nls (no reenfilamos aquí)
                cards = extract_list_cards_for_p(html, pval)
                logging.info("  • [P=%s] %s → %d productos (página)", pval, purl, len(cards))
                if not cards:
                    continue

                # En paralelo, completar info con el detalle
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                    futs = {ex.submit(parse_detail, session, c["url_detalle"], pval): c for c in cards}
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

        all_rows.extend(rows)

    # Dedupe: prioriza EAN -> Código Interno -> URL
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df["_k"] = df["EAN"].fillna("").astype(str).str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "Código Interno"].fillna("").astype(str).str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "URL"].fillna("").astype(str).str.strip()
    df = df.drop_duplicates(subset=["_k"]).drop(columns=["_k"]).reset_index(drop=True)
    return df

# ================== MySQL helpers (upserts estilo Coto) ==================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

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
    marca  = clean(p.get("marca")) or ""
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
    """
    Para La Reina:
      - sku_tienda = código interno (cuando exista)
      - record_id_tienda = None
    """
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
        v = parse_price(x)
        if x is None: return None
        if isinstance(v, float) and np.isnan(v): return None
        return f"{round(float(v), 2)}"  # guardado como VARCHAR

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
          promo_tipo = COALESCE(VALUES(promo_tipo), promo_tipo),
          promo_texto_regular = COALESCE(VALUES(promo_texto_regular), promo_texto_regular),
          promo_texto_descuento = COALESCE(VALUES(promo_texto_descuento), promo_texto_descuento),
          promo_comentarios = COALESCE(VALUES(promo_comentarios), promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        to_txt_or_none(p.get("precio_lista")), to_txt_or_none(p.get("precio_oferta")),
        p.get("tipo_oferta") or None, p.get("promo_tipo") or None,
        p.get("precio_regular_promo") or None, p.get("precio_descuento") or None,
        p.get("comentarios_promo") or None
    ))

# ================== Main (scrape + inserción) ==================
if __name__ == "__main__":
    df = scrape_lareina()
    print("\n✅ Filas obtenidas:", len(df))
    if df.empty:
        print("Sin datos, fin.")
        raise SystemExit(0)

    # ---- Inserción en MySQL ----
    capturado_en = datetime.now()
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        insertados = 0
        for _, r in df.iterrows():
            tipo_oferta = r.get("Tipo de Oferta")
            tipo_oferta = "Oferta" if (tipo_oferta and str(tipo_oferta).strip()) else "Precio regular"

            p = {
                "sku": clean(r.get("Código Interno")),          # usamos Código Interno como SKU
                "record_id": None,                               # no hay id estable
                "ean": clean(r.get("EAN")),
                "nombre": clean(r.get("Nombre Producto")),
                "marca": clean(r.get("Marca")),
                "fabricante": None,
                "categoria": clean(r.get("Categoría")),
                "subcategoria": clean(r.get("Subcategoría")),
                "precio_lista": r.get("Precio de Lista"),
                "precio_oferta": r.get("Precio de Oferta"),
                "tipo_oferta": tipo_oferta,
                "promo_tipo": None,
                "precio_regular_promo": None,
                "precio_descuento": None,
                "comentarios_promo": None,
                "url": clean(r.get("URL")),
            }

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

    # Respaldo CSV
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("Guardado:", OUT_CSV)
