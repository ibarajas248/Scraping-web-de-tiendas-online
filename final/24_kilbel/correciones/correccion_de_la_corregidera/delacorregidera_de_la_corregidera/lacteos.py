#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kilbel (kilbelonline.com) – Almacén n1_1 (modo súper respetuoso) + MySQL (lógica Coto)

- Recorre /almacen/n1_1/pag/1/, /2/, ... hasta que no haya productos.
- Extrae desde el listado y completa en el detalle.
- Persiste en MySQL siguiendo tu patrón:
  upsert_tienda -> (reuso por SKU) -> find_or_create_producto -> upsert_producto_tienda -> insert_historico
- Regla especial Kilbel:
  ✅ Si ya existe producto_tienda con (tienda_id, sku_tienda):
     - NO crea producto nuevo
     - Reusa el producto_id asociado
  ✅ En upsert_producto_tienda con SKU:
     - NO actualizar producto_id en ON DUPLICATE KEY UPDATE
"""

import re
import os
import sys
import time
import random
import argparse
from typing import List, Dict, Optional, Tuple, Any
from urllib.parse import urljoin
from datetime import datetime, time as dtime

import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import urllib.robotparser as robotparser

from mysql.connector import Error as MySQLError

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <- tu conexión MySQL

# ================== Config sitio ==================
BASE = "https://www.kilbelonline.com"
LISTING_FMT = "/lacteos/n1_994/pag/{page}/"

TIENDA_CODIGO = "kilbel"
TIENDA_NOMBRE = "Kilbel Online"

# ================== Headers / Rotación ==================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]
ACCEPT_LANGS = [
    "es-AR,es;q=0.9,en;q=0.8",
    "es-ES,es;q=0.9,en;q=0.8",
    "es-419,es;q=0.9,en;q=0.8",
]
HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

RE_CARD = re.compile(r"^prod_(\d+)$")

# ================== Utils texto/precio ==================
_NULLLIKE = {"", "null", "none", "nan", "na"}

def clean(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return None if s.lower() in _NULLLIKE else s

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())

def parse_price(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = re.sub(r"[^\d,.\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def price_to_varchar_2dec(x) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            return f"{float(x):.2f}"
        except Exception:
            return None
    v = parse_price(str(x))
    if v is None:
        return None
    return f"{float(v):.2f}"

# ================== Sesión HTTP respetuosa ==================
def build_session(timeout_connect=10, timeout_read=25) -> Tuple[requests.Session, Tuple[int, int]]:
    s = requests.Session()
    retry = Retry(
        total=5, connect=3, read=3, status=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS_BASE)
    return s, (timeout_connect, timeout_read)

def pick_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": random.choice(ACCEPT_LANGS),
        **HEADERS_BASE,
    }

def now_in_window(start: Optional[str], end: Optional[str]) -> bool:
    if not start and not end:
        return True

    def to_time(hhmm: str) -> dtime:
        hh, mm = hhmm.split(":")
        return dtime(int(hh), int(mm))

    now = datetime.now().time()
    if start and end:
        t1, t2 = to_time(start), to_time(end)
        if t1 <= t2:
            return t1 <= now <= t2
        return now >= t1 or now <= t2
    if start:
        return now >= to_time(start)
    if end:
        return now <= to_time(end)
    return True

def respect_robots_crawl_delay() -> Optional[float]:
    rp = robotparser.RobotFileParser()
    rp.set_url(urljoin(BASE, "/robots.txt"))
    try:
        rp.read()
        return rp.crawl_delay(USER_AGENTS[0])
    except Exception:
        return None

def polite_sleep(base_sleep: float, jitter: float, rps: Optional[float], crawl_delay: Optional[float]):
    sleeps = []
    if rps and rps > 0:
        sleeps.append(1.0 / rps)
    if crawl_delay and crawl_delay > 0:
        sleeps.append(crawl_delay)
    sleeps.append(base_sleep + random.uniform(0, jitter))
    time.sleep(max(sleeps))

def polite_get(session: requests.Session, url: str, timeouts: Tuple[int, int],
               base_sleep: float, jitter: float,
               adaptive_state: Dict[str, Any],
               tries: int = 5) -> Optional[requests.Response]:
    last_exc = None
    for attempt in range(1, tries + 1):
        session.headers.update(pick_headers())
        try:
            r = session.get(url, timeout=timeouts)
        except Exception as e:
            last_exc = e
            wait = base_sleep * adaptive_state["sleep_mult"] + random.uniform(0, jitter)
            time.sleep(wait)
            adaptive_state["sleep_mult"] = min(adaptive_state["sleep_mult"] * 1.5, adaptive_state["max_sleep_mult"])
            adaptive_state["errors_row"] += 1
            continue

        if r.status_code in (403, 429) or (500 <= r.status_code < 600):
            wait = base_sleep * adaptive_state["sleep_mult"] + random.uniform(0, jitter)
            sys.stderr.write(f"[WARN] {url} -> {r.status_code}. Backoff {wait:.2f}s (try {attempt}/{tries})\n")
            time.sleep(wait)
            adaptive_state["sleep_mult"] = min(adaptive_state["sleep_mult"] * 2.0, adaptive_state["max_sleep_mult"])
            adaptive_state["errors_row"] += 1
            continue

        adaptive_state["sleep_mult"] = max(1.0, adaptive_state["sleep_mult"] * 0.9)
        adaptive_state["errors_row"] = 0
        return r

    sys.stderr.write(f"[ERROR] GET falló: {url}; último error: {last_exc}\n")
    return None

# ================== Parse listado ==================
def parse_listing_products(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    productos = []
    cards = soup.select("div.producto[id^=prod_]")
    print(f"  • Cards encontradas en listado: {len(cards)}")

    for idx, prod in enumerate(cards, 1):
        pid = prod.get("id", "")
        m = RE_CARD.match(pid)
        codigo_interno = m.group(1) if m else None

        a = prod.select_one(".col1_listado .titulo02 a")
        nombre_list = clean_text(a.get_text()) if a else None
        href = a.get("href") if a else None
        url_detalle = urljoin(BASE, href) if href else None

        img = prod.select_one(".ant_imagen img")
        img_url = img.get("data-src") or (img.get("src") if img else None)

        sku_input = prod.select_one(f"input#id_item_{codigo_interno}") if codigo_interno else None
        sku_tienda = sku_input.get("value") if sku_input else None

        precio_lista_list = None
        precio_oferta_list = None
        el_prev = prod.select_one(".precio_complemento .precio.anterior")
        if el_prev:
            precio_lista_list = parse_price(el_prev.get_text())

        el_act = prod.select_one(".precio_complemento .precio.aux1")
        if el_act:
            precio_oferta_list = parse_price(el_act.get_text())

        promo = None
        promo_span = prod.select_one("span.promocion")
        if promo_span:
            mm = re.search(r"promocion(\d+)-off", " ".join(promo_span.get("class", [])))
            if mm:
                promo = f"{mm.group(1)}% OFF"

        precio_x_kg_list = None
        sin_imp_list = None
        for cod in prod.select(".precio_complemento .codigo"):
            txt = cod.get_text(" ", strip=True)
            if "Precio por" in txt:
                precio_x_kg_list = parse_price(txt)
            if "imp" in txt.lower():
                sin_imp_list = parse_price(txt)

        print(f"    [{idx:03}] LISTADO  CODINT={codigo_interno}  SKU_TIENDA={sku_tienda}  NOMBRE='{nombre_list}'  URL={url_detalle}")

        productos.append({
            "CodigoInterno_list": codigo_interno,
            "NombreProducto_list": nombre_list,
            "URL": url_detalle,
            "Imagen": img_url,
            "SKU_Tienda": sku_tienda,
            "RecordId_Tienda": sku_tienda,  # en Kilbel suele coincidir
            "PrecioLista_list": precio_lista_list,
            "PrecioOferta_list": precio_oferta_list,
            "TipoOferta": promo,
            "PrecioPorKg_list": precio_x_kg_list,
            "PrecioSinImpuestos_list": sin_imp_list,
        })
    return productos

# ================== Parse detalle ==================
def parse_detail(session: requests.Session, url: str, timeouts: Tuple[int, int],
                 base_sleep: float, jitter: float, adaptive_state: Dict[str, Any]) -> Dict[str, Any]:
    res = {
        "NombreProducto": None, "CodigoInterno_det": None, "PrecioLista": None,
        "PrecioOferta": None, "PrecioPorKg": None, "PrecioSinImpuestos": None,
        "Categoria": None, "Subcategoria": None, "Subsubcategoria": None,
        "EAN": None, "Marca": None, "Fabricante": None,
    }
    r = polite_get(session, url, timeouts, base_sleep, jitter, adaptive_state)
    if not r or r.status_code >= 400:
        print("           ⚠ No pude cargar el detalle.")
        return res
    soup = BeautifulSoup(r.text, "lxml")

    h1 = soup.select_one("#detalle_producto h1.titulo_producto")
    if h1:
        res["NombreProducto"] = clean_text(h1.get_text())

    cod_box = soup.find(string=re.compile(r"COD\.\s*\d+"))
    if cod_box:
        m = re.search(r"COD\.\s*(\d+)", cod_box)
        if m:
            res["CodigoInterno_det"] = m.group(1)

    prev = soup.select_one("#detalle_producto .precio.anterior")
    if prev:
        res["PrecioLista"] = parse_price(prev.get_text())

    act = soup.select_one("#detalle_producto .precio.aux1")
    if act:
        res["PrecioOferta"] = parse_price(act.get_text())

    for div in soup.select("#detalle_producto .codigo"):
        t = div.get_text(" ", strip=True)
        if "Precio por" in t:
            res["PrecioPorKg"] = parse_price(t)
        if "sin impuestos" in t.lower() or "imp.nac" in t.lower():
            res["PrecioSinImpuestos"] = parse_price(t)

    onclick_node = soup.find(attrs={"onclick": re.compile(r"agregarLista_dataLayerPush")})
    if onclick_node:
        on = onclick_node.get("onclick", "")
        mm = re.search(r"agregarLista_dataLayerPush\('([^']+)'", on)
        if mm:
            ruta = clean_text(mm.group(1).replace("&gt;", ">"))
            partes = [clean_text(p) for p in ruta.split(">") if p.strip()]
            if partes:
                res["Categoria"] = partes[0] if len(partes) > 0 else None
                res["Subcategoria"] = partes[1] if len(partes) > 1 else None
                res["Subsubcategoria"] = partes[2] if len(partes) > 2 else None

    # EAN: si aparece en texto
    m_ean = re.search(r"\b(\d{13})\b", soup.get_text(" ", strip=True))
    if m_ean:
        res["EAN"] = m_ean.group(1)

    return res

# ================== MySQL helpers (lógica Coto + regla Kilbel) ==================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def get_producto_id_from_producto_tienda_by_sku(cur, tienda_id: int, sku: str) -> Optional[int]:
    """
    ✅ Regla Kilbel:
    Si ya existe producto_tienda para (tienda_id, sku_tienda),
    devolvemos su producto_id y NO creamos producto nuevo.
    """
    cur.execute("""
        SELECT producto_id
        FROM producto_tienda
        WHERE tienda_id=%s AND sku_tienda=%s
        LIMIT 1
    """, (tienda_id, sku))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] else None

def find_or_create_producto(cur, p: Dict[str, Any]) -> int:
    ean = clean(p.get("EAN"))
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
                (p.get("NombreProducto") or ""),
                (p.get("Marca") or ""),
                (p.get("Fabricante") or ""),
                (p.get("Categoria") or ""),
                (p.get("Subcategoria") or ""),
                pid
            ))
            return pid

    nombre = clean(p.get("NombreProducto")) or ""
    marca  = clean(p.get("Marca")) or ""
    if nombre and marca:
        cur.execute("SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1", (nombre, marca))
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
                (p.get("EAN") or ""),
                (p.get("Fabricante") or ""),
                (p.get("Categoria") or ""),
                (p.get("Subcategoria") or ""),
                pid
            ))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
    """, (
        (p.get("EAN") or ""),
        nombre,
        marca,
        (p.get("Fabricante") or ""),
        (p.get("Categoria") or ""),
        (p.get("Subcategoria") or "")
    ))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, p: Dict[str, Any]) -> int:
    """
    Upsert estilo Coto con LAST_INSERT_ID(id).
    ✅ Regla Kilbel: si hay SKU, NO actualizar producto_id en el DUPLICATE.
    """
    sku = clean(p.get("SKU_Tienda"))
    rec = clean(p.get("RecordId_Tienda"))
    url = (p.get("URL") or "")
    nombre_tienda = (p.get("NombreProducto") or "")

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
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
        price_to_varchar_2dec(p.get("PrecioLista")),
        price_to_varchar_2dec(p.get("PrecioOferta")),
        (p.get("TipoOferta") or None),
        None, None, None, None
    ))

# ================== Runner ==================
def run(max_pages: int,
        start_page: int,
        outfile: str,
        csv: Optional[str],
        rps: float,
        sleep_item: Tuple[float, float],
        sleep_page: Tuple[float, float],
        long_nap_every_items: int,
        long_nap_seconds: float,
        long_nap_every_pages: int,
        proxy: Optional[str],
        resume_file: Optional[str],
        autosave_every: int,
        window_start: Optional[str],
        window_end: Optional[str],
        max_consecutive_errors: int):

    if not now_in_window(window_start, window_end):
        print(f"⏳ Fuera de ventana horaria permitida ({window_start or '-'}–{window_end or '-'})")
        return

    session, timeouts = build_session()
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})

    crawl_delay = respect_robots_crawl_delay()
    if crawl_delay:
        sys.stderr.write(f"[INFO] robots.txt Crawl-delay: {crawl_delay}s\n")

    # Reanudación Excel (solo para evitar repetir URLs)
    resultados: List[Dict[str, Any]] = []
    vistos = set()
    if resume_file and os.path.exists(resume_file):
        try:
            prev = pd.read_excel(resume_file)
            if "URL" in prev.columns:
                vistos = set(prev["URL"].dropna().tolist())
                resultados = prev.to_dict(orient="records")
                sys.stderr.write(f"[INFO] Reanudación: {len(vistos)} URLs ya existentes de {resume_file}\n")
        except Exception as e:
            sys.stderr.write(f"[WARN] No se pudo leer resume '{resume_file}': {e}\n")

    adaptive_state = {"sleep_mult": 1.0, "max_sleep_mult": 10.0, "errors_row": 0}
    total_items = 0
    consecutive_errors = 0

    # ====== MySQL: una corrida = un capturado_en ======
    capturado_en = datetime.now()
    conn = None
    cur = None

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)

        for page in range(start_page, max_pages + 1):
            if not now_in_window(window_start, window_end):
                print(f"⏳ Fuera de ventana horaria ({window_start or '-'}–{window_end or '-'}) — pausa 60s")
                time.sleep(60)
                continue

            url_list = urljoin(BASE, LISTING_FMT.format(page=page))
            print(f"\n=== Página {page} -> {url_list}")

            polite_sleep(base_sleep=0, jitter=0, rps=rps, crawl_delay=crawl_delay)
            r = polite_get(session, url_list, timeouts, base_sleep=0.6, jitter=0.8, adaptive_state=adaptive_state)
            if not r or r.status_code >= 400:
                print("   (fin: sin contenido o HTTP de corte)")
                break

            soup = BeautifulSoup(r.text, "lxml")
            rows = parse_listing_products(soup)
            if not rows:
                print("   (fin: no hay cards de productos)")
                break

            for rprod in rows:
                if not now_in_window(window_start, window_end):
                    print(f"⏳ Fuera de ventana horaria — pausa 60s")
                    time.sleep(60)
                    continue

                url = rprod.get("URL")
                if not url:
                    continue
                if url in vistos:
                    continue

                base_sleep = random.uniform(*sleep_item)
                polite_sleep(base_sleep, jitter=0.4, rps=rps, crawl_delay=crawl_delay)

                d = parse_detail(session, url, timeouts, base_sleep=0.5, jitter=0.7, adaptive_state=adaptive_state)
                if all(v is None for v in d.values()):
                    consecutive_errors += 1
                else:
                    consecutive_errors = 0

                merged = {
                    "EAN": d.get("EAN"),
                    "CodigoInterno": d.get("CodigoInterno_det") or rprod.get("CodigoInterno_list"),
                    "NombreProducto": d.get("NombreProducto") or rprod.get("NombreProducto_list"),
                    "Categoria": d.get("Categoria"),
                    "Subcategoria": d.get("Subcategoria"),
                    "Marca": d.get("Marca"),
                    "Fabricante": d.get("Fabricante"),
                    "PrecioLista": d.get("PrecioLista") if d.get("PrecioLista") is not None else rprod.get("PrecioLista_list"),
                    "PrecioOferta": d.get("PrecioOferta") if d.get("PrecioOferta") is not None else rprod.get("PrecioOferta_list"),
                    "TipoOferta": rprod.get("TipoOferta"),
                    "PrecioPorKg": d.get("PrecioPorKg") if d.get("PrecioPorKg") is not None else rprod.get("PrecioPorKg_list"),
                    "PrecioSinImpuestos": d.get("PrecioSinImpuestos") if d.get("PrecioSinImpuestos") is not None else rprod.get("PrecioSinImpuestos_list"),
                    "URL": rprod.get("URL"),
                    "Imagen": rprod.get("Imagen"),
                    "SKU_Tienda": rprod.get("SKU_Tienda"),
                    "RecordId_Tienda": rprod.get("RecordId_Tienda"),
                    "Pagina": page,
                }

                # ========= Persistencia MySQL (lógica Coto + regla Kilbel) =========
                sku = clean(merged.get("SKU_Tienda"))
                producto_id = None

                if sku:
                    producto_id = get_producto_id_from_producto_tienda_by_sku(cur, tienda_id, sku)

                if not producto_id:
                    producto_id = find_or_create_producto(cur, merged)

                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, merged)
                insert_historico(cur, tienda_id, pt_id, merged, capturado_en)

                # ========= salida local =========
                resultados.append(merged)
                vistos.add(url)
                total_items += 1

                if long_nap_every_items and total_items % long_nap_every_items == 0:
                    nap = long_nap_seconds + random.uniform(0, 2)
                    sys.stderr.write(f"[INFO] Siesta larga por items: {nap:.1f}s\n")
                    time.sleep(nap)

                if autosave_every and total_items % autosave_every == 0:
                    try:
                        pd.DataFrame(resultados).to_excel(outfile, index=False)
                        sys.stderr.write(f"[INFO] Autosave -> {outfile} ({len(resultados)} filas)\n")
                    except Exception as e:
                        sys.stderr.write(f"[WARN] Autosave falló: {e}\n")

                if max_consecutive_errors and consecutive_errors >= max_consecutive_errors:
                    sys.stderr.write(f"[ERROR] Demasiados errores consecutivos ({consecutive_errors}). Abortando para respetar el sitio.\n")
                    break

            if long_nap_every_pages and page % long_nap_every_pages == 0:
                nap = long_nap_seconds + random.uniform(0, 2)
                sys.stderr.write(f"[INFO] Siesta larga por páginas: {nap:.1f}s\n")
                time.sleep(nap)

            time.sleep(random.uniform(*sleep_page))

            if max_consecutive_errors and consecutive_errors >= max_consecutive_errors:
                break

        if not resultados:
            print("\n⚠ No se obtuvieron productos. No se generará Excel.")
            conn.rollback()
            return

        conn.commit()
        print(f"💾 Guardado en MySQL: {total_items} históricos para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        if conn:
            conn.rollback()
        print(f"❌ Error MySQL: {e}")

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    # ====== Excel/CSV final ======
    df = pd.DataFrame(resultados)
    cols = [
        "EAN", "CodigoInterno", "NombreProducto",
        "Categoria", "Subcategoria", "Marca", "Fabricante",
        "PrecioLista", "PrecioOferta", "TipoOferta",
        "PrecioPorKg", "PrecioSinImpuestos",
        "URL", "Imagen", "SKU_Tienda", "RecordId_Tienda", "Pagina"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df.to_excel(outfile, index=False)
    if csv:
        df.to_csv(csv, index=False, encoding="utf-8-sig")

    print(f"\n✔ Guardado: {outfile} (filas: {len(df)})" + (f" | CSV: {csv}" if csv else ""))

# ================== CLI ==================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scraper Kilbel Almacén n1_1 (modo respetuoso) + MySQL (lógica Coto).")
    ap.add_argument("--max-pages", type=int, default=300)
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--outfile", type=str, default="kilbel_almacen_n1_1.xlsx")
    ap.add_argument("--csv", type=str, default=None)

    ap.add_argument("--rps", type=float, default=0.33, help="Requests por segundo (0 = sin tope).")
    ap.add_argument("--sleep-item-min", type=float, default=0.5)
    ap.add_argument("--sleep-item-max", type=float, default=1.2)
    ap.add_argument("--sleep-page-min", type=float, default=1.0)
    ap.add_argument("--sleep-page-max", type=float, default=2.0)
    ap.add_argument("--long-nap-every-items", type=int, default=40)
    ap.add_argument("--long-nap-every-pages", type=int, default=8)
    ap.add_argument("--long-nap-seconds", type=float, default=10.0)

    ap.add_argument("--proxy", type=str, default=None)
    ap.add_argument("--resume", type=str, default=None)
    ap.add_argument("--autosave-every", type=int, default=50)

    ap.add_argument("--window-start", type=str, default=None)
    ap.add_argument("--window-end", type=str, default=None)
    ap.add_argument("--max-consecutive-errors", type=int, default=12)

    args = ap.parse_args()

    run(
        max_pages=args.max_pages,
        start_page=args.start_page,
        outfile=args.outfile,
        csv=args.csv,
        rps=(None if args.rps and args.rps <= 0 else args.rps),
        sleep_item=(args.sleep_item_min, args.sleep_item_max),
        sleep_page=(args.sleep_page_min, args.sleep_page_max),
        long_nap_every_items=args.long_nap_every_items,
        long_nap_seconds=args.long_nap_seconds,
        long_nap_every_pages=args.long_nap_every_pages,
        proxy=args.proxy,
        resume_file=args.resume,
        autosave_every=args.autosave_every,
        window_start=args.window_start,
        window_end=args.window_end,
        max_consecutive_errors=args.max_consecutive_errors,
    )
