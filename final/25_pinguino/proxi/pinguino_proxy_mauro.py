#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Supermercado Pingüino → MySQL y/o Excel/CSV

Mejoras clave:
- Paginación completa y robusta: 1) primera página sin 'pag', 2) luego pag=0..N.
- Anti-cache: parámetro '_ts' + cabeceras no-cache y opción de rotar User-Agent.
- Precio de lista = Precio de oferta (mismo valor final).
- Números sin separador decimal: últimos 2 dígitos = centavos.
- Exporta ids y nombres de categoría y subcategoría.
- Ingesta MySQL: tiendas, productos, producto_tienda, historico_precios.
- Recortes preventivos para columnas VARCHAR según tu schema.
- Dedupe más seguro por PLU/URL y fallback a (título, cat, subcat).
- Opciones:
  * --all-sucursales: recorrer todas las sucursales detectadas.
  * --cats-only: scrapear solo las categorías (evita vista de departamento).
  * --no-dedupe: no deduplicar (útil para medir techo bruto).
  * --bruteforce + --suc-range/--ciu-range: probar combos ciudad×sucursal y usar los mejores.
  * --di-country: override de país en el proxy DataImpulse (ar, uy, py, cl, ...).
  * --ua-rotate: rotar User-Agent entre páginas para romper cache/CDN.
"""

import re
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
import os, sys

# ===== Conexión MySQL =====
# Debe existir base_datos.py con get_conn() que retorne mysql.connector.connect(...)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # type: ignore

# ===== Identidad de la tienda =====
TIENDA_CODIGO = "pinguino"
TIENDA_NOMBRE = "Pingüino"

# ===== Base del sitio =====
BASE = "https://www.pinguino.com.ar"
INDEX = f"{BASE}/web/index.r"
MENU_CAT = f"{BASE}/web/menuCat.r"
PROD = f"{BASE}/web/productos.r"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# ===== Proxy DataImpulse (HTTP/HTTPS) =====
# Variables de entorno opcionales:
#   DI_USER, DI_PASS, DI_HOST, DI_PORT, DI_COUNTRY
DI_USER_BASE = os.getenv("DI_USER", "2cf8063dbace06f69df4")
DI_PASS      = os.getenv("DI_PASS", "61425d26fb3c7287")
DI_HOST      = os.getenv("DI_HOST", "gw.dataimpulse.com")
DI_PORT      = int(os.getenv("DI_PORT", "823"))
DI_COUNTRY   = os.getenv("DI_COUNTRY", "ar")  # geotarget por defecto (sufijo __cr.<país>)

def _build_proxy_dict() -> Dict[str, str]:
    """Construye el diccionario proxies para requests usando DataImpulse."""
    from urllib.parse import quote
    user_full = f"{DI_USER_BASE}__cr.{DI_COUNTRY.lower()}" if DI_COUNTRY else DI_USER_BASE
    user_enc = quote(user_full, safe="")
    pass_enc = quote(DI_PASS, safe="")
    proxy_url = f"http://{user_enc}:{pass_enc}@{DI_HOST}:{DI_PORT}"
    return {"http": proxy_url, "https": proxy_url}

# ===== Límites de columnas (ajusta a tu schema si difiere) =====
MAXLEN_NOMBRE = 255
MAXLEN_MARCA = 128
MAXLEN_FABRICANTE = 128
MAXLEN_CATEGORIA = 128
MAXLEN_SUBCATEGORIA = 128
MAXLEN_URL = 512
MAXLEN_NOMBRE_TIENDA = 255
MAXLEN_TIPO_OFERTA = 190
MAXLEN_PROMO_COMENTARIOS = 480

def _truncate(val: Optional[Any], maxlen: int) -> Optional[str]:
    if val is None:
        return None
    s = str(val)
    return s if len(s) <= maxlen else s[:maxlen]

# ---------------------------
# Utils: rangos, UA rotate, override DI country
# ---------------------------
def parse_range(spec: str) -> List[int]:
    spec = (spec or "").strip()
    out: List[int] = []
    if not spec:
        return out
    chunks = [s.strip() for s in spec.split(",")]
    for c in chunks:
        if "-" in c:
            a, b = c.split("-", 1)
            if a.isdigit() and b.isdigit():
                out.extend(list(range(int(a), int(b) + 1)))
        elif c.isdigit():
            out.append(int(c))
    return sorted(list(set(out)))

_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]

def rotate_ua(session: requests.Session, n: int):
    if n >= 0:
        session.headers["User-Agent"] = _UAS[n % len(_UAS)]

def set_di_country_runtime(country: Optional[str]):
    global DI_COUNTRY
    if country:
        DI_COUNTRY = country.lower()

# ---------------------------
# Sesión + Sucursal/Ciudad
# ---------------------------
def new_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False  # usamos solo el proxy configurado
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": INDEX,
        # anti-cache
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    retry = Retry(total=5, backoff_factor=0.7,
                  status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    # Proxy DataImpulse (usa DI_COUNTRY actual)
    s.proxies.update(_build_proxy_dict())
    # Primer toque para setear cookies básicas del sitio
    try:
        s.get(INDEX, timeout=20)
    except requests.RequestException:
        pass
    return s

def set_ciudad_sucursal(session: requests.Session, ciudad_id: int = 1, sucursal_id: int = 4) -> None:
    session.cookies.set("ciudad", str(ciudad_id), domain="www.pinguino.com.ar", path="/")
    session.cookies.set("sucursal", str(sucursal_id), domain="www.pinguino.com.ar", path="/")
    # refresco rápido
    try:
        session.get(INDEX, timeout=20)
    except requests.RequestException:
        pass

def discover_sucursales(session: requests.Session) -> List[int]:
    """Descubre las sucursales listadas en el INDEX."""
    try:
        r = session.get(INDEX, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        opts = soup.select('select[name="sucursal"] option[value]')
        ids = []
        for o in opts:
            v = (o.get("value") or "").strip()
            if v.isdigit():
                ids.append(int(v))
        return sorted(set(ids)) or [4]
    except requests.RequestException:
        return [4]

# ---------------------------
# Normalización de precios
# ---------------------------
def tidy_space(txt: str) -> str:
    return re.sub(r"\s+", " ", txt or "").strip()

def parse_price_value(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip().replace("\u202f", "").replace(" ", "")
    if not s:
        return None
    comma_count = s.count(',')
    dot_count = s.count('.')
    if comma_count or dot_count:
        dec_sep = thou_sep = None
        if comma_count and dot_count:
            if s.rfind(',') > s.rfind('.'):
                dec_sep, thou_sep = ',', '.'
            else:
                dec_sep, thou_sep = '.', ','
        elif comma_count:
            parts = s.split(',')
            if comma_count == 1 and len(parts[-1]) <= 2:
                dec_sep, thou_sep = ',', '.'
            else:
                dec_sep, thou_sep = ',', ','
        elif dot_count:
            parts = s.split('.')
            if dot_count == 1 and len(parts[-1]) <= 2:
                dec_sep, thou_sep = '.', ','
            else:
                dec_sep, thou_sep = '.', '.'
        normalized = s
        if thou_sep and thou_sep != dec_sep:
            normalized = normalized.replace(thou_sep, '')
        if dec_sep:
            normalized = normalized.replace(dec_sep, '.')
        if dec_sep and dec_sep == thou_sep:
            last = normalized.rfind('.')
            if last != -1:
                normalized = normalized.replace('.', '')
                normalized = normalized[:last] + '.' + normalized[last:]
        try:
            return round(float(normalized), 2)
        except ValueError:
            return None
    if s.isdigit():
        if len(s) == 1:
            return round(float('0.0' + s), 2)
        if len(s) == 2:
            return round(float('0.' + s), 2)
        entero, dec = s[:-2], s[-2:]
        try:
            return round(float(f"{entero}.{dec}"), 2)
        except ValueError:
            return None
    return None

def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    money_re = re.compile(
        r"(?:\$|\bARS\b|\bAR\$?\b)?\s*([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})|[0-9]+(?:\.[0-9]{1,2})?|[0-9]+)"
    )
    m = money_re.search(text.replace("\xa0", " "))
    if not m:
        return None
    num = m.group(1)
    val = parse_price_value(num)
    if val is not None:
        return val
    cleaned = num.replace(" ", "").replace("\u202f", "")
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return round(float(cleaned), 2)
    except ValueError:
        return None

# ---------------------------
# Descubrimiento de catálogo
# ---------------------------
def get_departments_details(session: requests.Session) -> List[Dict[str, Any]]:
    deps: List[Dict[str, Any]] = []
    try:
        r = session.get(INDEX, timeout=30)
        if not r.ok:
            return deps
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select(".dpto a[data-d]"):
            dep_id = a.get("data-d")
            try:
                dep_id_int = int(dep_id)
            except (TypeError, ValueError):
                continue
            name = tidy_space(a.get_text()) if a.get_text() else str(dep_id_int)
            deps.append({"id": dep_id_int, "nombre": name})
        seen = set()
        uniq = []
        for d in deps:
            if d["id"] not in seen:
                seen.add(d["id"])
                uniq.append(d)
        return uniq
    except requests.RequestException:
        return deps

def get_categories_details(session: requests.Session, dep_id: int) -> List[Dict[str, Any]]:
    cats: List[Dict[str, Any]] = []
    try:
        r = session.get(MENU_CAT, params={"dep": str(dep_id)}, timeout=30)
        if not r.ok:
            return cats
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a[data-c]"):
            cat_id = a.get("data-c")
            try:
                cat_id_int = int(cat_id)
            except (TypeError, ValueError):
                continue
            name = tidy_space(a.get_text()) if a.get_text() else str(cat_id_int)
            cats.append({"id": cat_id_int, "nombre": name})
        seen = set()
        uniq = []
        for c in cats:
            if c["id"] not in seen:
                seen.add(c["id"])
                uniq.append(c)
        return uniq
    except requests.RequestException:
        return cats

def parse_product_cards_enriched(
    html: str,
    dep_id: int,
    cat_id: Optional[int] = None,
    dep_name: Optional[str] = None,
    cat_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = list(soup.select('[id^="prod-"]'))
    if not cards:
        cards = soup.select(".item-prod, .producto, .prod, .card, .item, .row .col-12")
    for node in soup.select('[data-pre]'):
        if node not in cards:
            cards.append(node)

    products: List[Dict[str, Any]] = []
    for node in cards:
        plu = None
        node_id = node.get("id")
        if node_id and node_id.startswith("prod-"):
            plu = node_id.split("-", 1)[-1].strip()

        ean = None
        for key in ["data-ean", "data-ean13", "data-barcode", "data-bar"]:
            val = node.get(key)
            if val:
                ean = str(val).strip()
                break

        data_prelista = node.get("data-prelista") or node.get("data-precio")
        data_preofe = node.get("data-preofe") or node.get("data-oferta")
        data_pre = node.get("data-pre")
        precio = None
        for raw_val in [data_preofe, data_pre, data_prelista]:
            p = parse_price_value(raw_val)
            if p is not None:
                precio = p
                break
        if precio is None:
            for sel in ['[class*="precio"]', '[class*="price"]', 'span', 'div']:
                price_node = node.select_one(sel)
                if price_node:
                    candidate = parse_price(price_node.get_text(" "))
                    if candidate is not None:
                        precio = candidate
                        break
        if precio is None:
            precio = parse_price(node.get_text(" "))

        precio = round(float(precio), 2) if precio is not None else None
        precio_texto = f"{precio:.2f}" if precio is not None else ""

        img = None
        data_img = node.get("data-img")
        if data_img:
            img = data_img if data_img.startswith("http") else (BASE + data_img if data_img.startswith("/") else data_img)
        img_node = node.select_one("img[src]")
        title_candidates: List[str] = []
        if not img and img_node:
            alt = img_node.get("alt")
            if alt:
                title_candidates.append(tidy_space(alt))
            src = img_node.get("src")
            if src:
                img = src if not src.startswith("/") else (BASE + src)

        data_des = node.get("data-des") or node.get("data-name")
        if data_des:
            title_candidates.append(tidy_space(str(data_des)))
        for a_tag in node.select("a[title]"):
            t = a_tag.get("title")
            if t:
                title_candidates.append(tidy_space(t))
        for sel in ['h1', 'h2', 'h3', 'h4', 'h5', '[class*="tit"][class!="precio"]', '[class*="desc"]']:
            tag = node.select_one(sel)
            if tag:
                text = tidy_space(tag.get_text(strip=True))
                if text:
                    title_candidates.append(text)

        title = None
        price_pattern = re.compile(r"\$\s*\d")
        for cand in title_candidates:
            if price_pattern.search(cand):
                continue
            lower = cand.lower()
            if "carrito" in lower or "agreg" in lower:
                continue
            title = cand
            break
        if not title:
            raw_text = tidy_space(node.get_text(" "))
            if precio_texto:
                raw_text = raw_text.replace(precio_texto, "")
            raw_text = re.sub(r"\$\s*[0-9]+(?:[.,][0-9]+)*(?:\s*[a-zA-Z]|)", "", raw_text)
            raw_text = re.sub(r"agregaste.*", "", raw_text, flags=re.IGNORECASE)
            raw_text = re.sub(r"agregar.*", "", raw_text, flags=re.IGNORECASE)
            raw_text = re.sub(r"\+\s*-", "", raw_text)
            cleaned = tidy_space(raw_text)
            if len(cleaned) > 180:
                cleaned = cleaned[:177] + "..."
            title = cleaned

        url = None
        data_href = node.get("data-href")
        if data_href:
            url = data_href if data_href.startswith("http") else (BASE + data_href if data_href.startswith("/") else data_href)
        if not url:
            for a_tag in node.select("a[href]"):
                href = a_tag.get("href")
                if not href:
                    continue
                href_l = href.lower()
                if href_l == "#" or "javascript" in href_l:
                    continue
                if any(tok in href_l for tok in ["agregar", "addcart", "accioncarrito", "ticket"]):
                    continue
                url = href if href.startswith("http") else (BASE + href if href.startswith("/") else href)
                break

        tipo_descuento = None
        if precio is not None:
            texto_inf = node.get_text(" ").lower()
            if "x" in texto_inf and "%" not in texto_inf:
                m = re.search(r"(\d+)\s*x\s*(\d+)", texto_inf)
                if m:
                    tipo_descuento = f"{m.group(1)}x{m.group(2)}"
            elif "%" in texto_inf:
                m = re.search(r"(\d+)%", texto_inf)
                if m:
                    tipo_descuento = f"{m.group(1)}%"

        products.append({
            "ean": ean,
            "titulo": title or "",
            "precio_lista": precio,
            "precio_oferta": precio,  # lista = oferta
            "tipo_descuento": tipo_descuento,
            "categoria_id": dep_id,
            "categoria_nombre": dep_name,
            "subcategoria_id": cat_id,
            "subcategoria_nombre": cat_name,
            "url": url,
            "imagen": img,
            "plu": plu,
            "precio_texto": precio_texto,
        })
    return [p for p in products if p["titulo"] or (p["precio_oferta"] is not None or p["precio_lista"] is not None)]

def fetch_all_pages_for_dep_cat(
    session: requests.Session,
    dep_id: int,
    dep_name: Optional[str] = None,
    cat_id: Optional[int] = None,
    cat_name: Optional[str] = None,
    save_prefix: Optional[Path] = None,
    max_pages: int = 600,
    sleep: float = 0.8,
) -> List[Dict[str, Any]]:
    """
    1) Intenta primera página SIN 'pag'.
    2) Luego recorre pag=0..N (no 1..N), cortando por vacío o HTML repetido.
    3) Rompe caches con _ts.
    4) Opcionalmente rota el User-Agent para esquivar CDN cache.
    """
    out: List[Dict[str, Any]] = []
    seen_html_sigs = set()

    def _pull(page_param: Optional[int]) -> List[Dict[str, Any]]:
        params = {"dep": str(dep_id), "_ts": str(int(time.time() * 1000))}
        if cat_id is not None:
            params["cat"] = str(cat_id)
        if page_param is not None:
            params["pag"] = str(page_param)
        r = session.get(PROD, params=params, timeout=40)
        if not r.ok:
            return []
        html = r.text
        if save_prefix:
            save_prefix.parent.mkdir(parents=True, exist_ok=True)
            suffix = "root" if page_param is None else f"p{page_param}"
            (save_prefix.with_name(f"{save_prefix.stem}_{suffix}.html")).write_text(html, encoding="utf-8")
        sig = hash(html)
        if sig in seen_html_sigs:
            return []  # repetido → fin
        seen_html_sigs.add(sig)
        return parse_product_cards_enriched(html, dep_id, cat_id, dep_name, cat_name)

    # Primera tirada SIN 'pag'
    first = _pull(page_param=None)
    out.extend(first)
    if sleep > 0:
        time.sleep(sleep)
    if hasattr(session, "ua_rotate_flag") and session.ua_rotate_flag:
        rotate_ua(session, -1)  # primer cambio leve

    # Luego pag=0..N
    empties = 0
    for page in range(0, max_pages + 1):
        if hasattr(session, "ua_rotate_flag") and session.ua_rotate_flag:
            rotate_ua(session, page)
        prods = _pull(page_param=page)
        if not prods:
            empties += 1
            if empties >= 2:
                break
        else:
            empties = 0
            out.extend(prods)
        if sleep > 0:
            time.sleep(sleep)

    return out

# ---------------------------
# Helpers MySQL
# ---------------------------
def _parse_price_num(val) -> Optional[str]:
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f):
            return None
        return f"{round(f, 2)}"
    except Exception:
        return None

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any]) -> int:
    ean = (r.get("ean") or None)
    nombre = _truncate((r.get("titulo") or ""), MAXLEN_NOMBRE)
    marca = _truncate((r.get("marca") or None), MAXLEN_MARCA)  # Pingüino no provee marca
    fabricante = _truncate((r.get("fabricante") or None), MAXLEN_FABRICANTE)
    categoria = _truncate((r.get("categoria_nombre") or None), MAXLEN_CATEGORIA)
    subcategoria = _truncate((r.get("subcategoria_nombre") or None), MAXLEN_SUBCATEGORIA)

    # 1) Por EAN
    if ean:
        cur.execute("SELECT id FROM productos WHERE ean=%s LIMIT 1", (ean,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  nombre = COALESCE(NULLIF(%s,''), nombre),
                  marca = COALESCE(%s, marca),
                  fabricante = COALESCE(%s, fabricante),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (nombre, marca, fabricante, categoria, subcategoria, pid))
            return pid

    # 2) Por (nombre, marca) si existe
    if nombre and marca:
        cur.execute("""SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                    (nombre, marca or ""))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  ean = COALESCE(%s, ean),
                  fabricante = COALESCE(%s, fabricante),
                  categoria = COALESCE(%s, categoria),
                  subcategoria = COALESCE(%s, subcategoria)
                WHERE id=%s
            """, (ean, fabricante, categoria, subcategoria, pid))
            return pid

    # 3) Insert nuevo
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    sku = (r.get("plu") or None)  # PLU como SKU de tienda
    record_id = sku               # mismo valor como respaldo
    url = _truncate((r.get("url") or None), MAXLEN_URL)
    nombre_tienda = _truncate((r.get("titulo") or None), MAXLEN_NOMBRE_TIENDA)

    if sku:
        cur.execute("""
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

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda
              (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: datetime):
    precio_lista = _parse_price_num(r.get("precio_lista"))
    precio_oferta = _parse_price_num(r.get("precio_oferta"))
    tipo_oferta = _truncate((r.get("tipo_descuento") or None), MAXLEN_TIPO_OFERTA)
    promo_comentarios = _truncate(
        f"cat_id={r.get('categoria_id')}; cat_nombre={r.get('categoria_nombre') or ''}; "
        f"subcat_id={r.get('subcategoria_id')}; subcat_nombre={r.get('subcategoria_nombre') or ''}",
        MAXLEN_PROMO_COMENTARIOS
    )

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
        precio_lista, precio_oferta, tipo_oferta,
        tipo_oferta, None, None, promo_comentarios
    ))

# ---------------------------
# Dedupe
# ---------------------------
def dedupe_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Preferimos PLU único
    if "plu" in df.columns and df["plu"].notna().any():
        df = df.drop_duplicates(subset=["plu"], keep="first")
    elif "url" in df.columns and df["url"].notna().any():
        df = df.drop_duplicates(subset=["url"], keep="first")
    else:
        df = df.drop_duplicates(subset=["titulo", "categoria_id", "subcategoria_id"], keep="first")
    return df

# ---------------------------
# Probing ciudad×sucursal (para bruteforce)
# ---------------------------
def probe_combo_count(session: requests.Session, ciudad_id: int, sucursal_id: int, sleep: float = 0.6) -> int:
    """
    Mide cuántos productos devuelve (rápido) con una muestra de depto/cat.
    """
    try:
        set_ciudad_sucursal(session, ciudad_id=ciudad_id, sucursal_id=sucursal_id)
        deps = get_departments_details(session)
        if not deps:
            return 0
        dep = deps[0]
        dep_id = dep["id"]
        sample = fetch_all_pages_for_dep_cat(session, dep_id=dep_id, dep_name=dep.get("nombre"),
                                             save_prefix=None, max_pages=0, sleep=sleep)
        cats = get_categories_details(session, dep_id)
        if cats:
            cat = cats[0]
            sample += fetch_all_pages_for_dep_cat(session, dep_id=dep_id, dep_name=dep.get("nombre"),
                                                  cat_id=cat["id"], cat_name=cat.get("nombre"),
                                                  save_prefix=None, max_pages=0, sleep=sleep)
        return len(sample)
    except Exception:
        return 0

def bruteforce_city_store(session: requests.Session, ciu_range: List[int], suc_range: List[int], sleep: float = 0.5, top_k: int = 5) -> List[Tuple[int,int]]:
    """
    Prueba combinaciones ciudad×sucursal y devuelve las mejores top_k según cantidad estimada.
    """
    scores: List[Tuple[Tuple[int,int], int]] = []
    for ciu in ciu_range:
        for suc in suc_range:
            cnt = probe_combo_count(session, ciu, suc, sleep=sleep)
            scores.append(((ciu, suc), cnt))
            print(f"[probe] ciudad={ciu} sucursal={suc} -> {cnt}")
    scores.sort(key=lambda x: x[1], reverse=True)
    best = scores[:top_k]
    print("Top combos (ciu,suc)->count:", best)
    return [c for (c, _) in best]

# ---------------------------
# Runner
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Pingüino → MySQL / Excel / CSV")
    ap.add_argument("--out", default="Productos_Pinguino.xlsx", help="Archivo XLSX de salida (opcional)")
    ap.add_argument("--csv", default=None, help="CSV adicional (opcional)")
    ap.add_argument("--sleep", type=float, default=1.2, help="Espera (seg) entre páginas")
    ap.add_argument("--only-ofertas", action="store_true", help="Solo ofertas (ofe=1)")
    ap.add_argument("--debug-html", action="store_true", help="Guardar HTML por depto/categoría en ./_html")
    ap.add_argument("--no-mysql", action="store_true", help="No insertar en MySQL; solo archivos")
    ap.add_argument("--all-sucursales", action="store_true", help="Recorrer todas las sucursales detectadas")
    ap.add_argument("--cats-only", action="store_true", help="Scrapear solo categorías (evita vista del depto)")
    # Nuevas opciones de cobertura
    ap.add_argument("--suc-range", default="1-40", help="Rango de sucursales a probar: ej. 1-40 o 2,4,7")
    ap.add_argument("--ciu-range", default="1-5", help="Rango de ciudades a probar: ej. 1-5 o 1,3")
    ap.add_argument("--bruteforce", action="store_true", help="Probar todas las combinaciones ciudad×sucursal y usar top-k")
    ap.add_argument("--no-dedupe", action="store_true", help="No deduplicar (para medir techo bruto)")
    ap.add_argument("--di-country", default=None, help="Sobrescribe DI_COUNTRY en runtime: ar, uy, py, cl, ...")
    ap.add_argument("--ua-rotate", action="store_true", help="Rotar user-agent entre páginas para romper cache/CDN")
    args = ap.parse_args()

    # Override de país del proxy si se pidió
    set_di_country_runtime(args.di_country)

    # Nueva sesión (usa DI_COUNTRY actual)
    s = new_session()
    # flag para rotación UA
    s.ua_rotate_flag = bool(args.ua_rotate)

    rows: List[Dict[str, Any]] = []
    html_dir = Path("_html") if args.debug_html else None
    if html_dir:
        html_dir.mkdir(exist_ok=True)

    # Construcción de combos ciudad×sucursal
    if args.bruteforce:
        suc_range = parse_range(getattr(args, "suc_range"))
        ciu_range = parse_range(getattr(args, "ciu_range"))
        # Intersecar con sucursales descubiertas (si hay)
        discovered = set(discover_sucursales(s))
        if discovered:
            suc_range = [x for x in suc_range if x in discovered] or list(discovered)
        combos = bruteforce_city_store(s, ciu_range, suc_range, sleep=0.4, top_k=5)
    else:
        if args.all_sucursales:
            combos = [(1, sid) for sid in discover_sucursales(s)]
        else:
            combos = [(1, 4)]  # compat con valor por defecto original

    total_por_combo: Dict[Tuple[int,int], int] = {}

    for (ciu_id, suc_id) in combos:
        set_ciudad_sucursal(s, ciudad_id=ciu_id, sucursal_id=suc_id)
        print(f"== Ciudad {ciu_id} · Sucursal {suc_id} ==")
        combo_rows_ini = len(rows)

        if args.only_ofertas:
            prefix = (html_dir / f"ofertas_c{ciu_id}_s{suc_id}.html") if html_dir else None
            rows.extend(
                fetch_all_pages_for_dep_cat(
                    s, dep_id=999, dep_name="Ofertas",
                    save_prefix=prefix,
                    max_pages=200, sleep=args.sleep
                )
            )
            total_por_combo[(ciu_id, suc_id)] = len(rows) - combo_rows_ini
            print(f"[ciu {ciu_id} suc {suc_id}] total ofertas: {total_por_combo[(ciu_id, suc_id)]}")
            continue

        # Descubre departamentos
        deps = get_departments_details(s)
        if not deps:
            print("No se encontraron departamentos; revisa cookies/sucursal.")
            total_por_combo[(ciu_id, suc_id)] = 0
            continue

        for dep in deps:
            dep_id = dep["id"]
            dep_name = dep.get("nombre")
            print(f"[c{ciu_id} s{suc_id} dep {dep_id}] {dep_name}")

            # Vista del depto (si no se pidió cats-only)
            if not args.cats_only:
                prefix = (html_dir / f"c{ciu_id}_s{suc_id}_dep_{dep_id}.html") if html_dir else None
                dep_prods = fetch_all_pages_for_dep_cat(
                    s, dep_id=dep_id, dep_name=dep_name,
                    save_prefix=prefix,
                    max_pages=600, sleep=args.sleep
                )
                print(f"  [depto] productos: {len(dep_prods)}")
                rows.extend(dep_prods)

            # Páginas de cada categoría del depto
            cats = get_categories_details(s, dep_id)
            for cat in cats:
                cat_id = cat["id"]
                cat_name = cat.get("nombre")
                prefix = (html_dir / f"c{ciu_id}_s{suc_id}_dep_{dep_id}_cat_{cat_id}.html") if html_dir else None
                cat_prods = fetch_all_pages_for_dep_cat(
                    s, dep_id=dep_id, dep_name=dep_name,
                    cat_id=cat_id, cat_name=cat_name,
                    save_prefix=prefix,
                    max_pages=600, sleep=args.sleep
                )
                print(f"  [cat {cat_id}] {cat_name} -> {len(cat_prods)}")
                rows.extend(cat_prods)

        total_por_combo[(ciu_id, suc_id)] = len(rows) - combo_rows_ini
        print(f"[ciu {ciu_id} suc {suc_id}] total bruto acumulado: {total_por_combo[(ciu_id, suc_id)]}")

    if not rows:
        print("No se extrajo ningún producto. Revisa cookies/sucursal o ajusta selectores.")
        return

    # DataFrame y columnas esperadas
    df = pd.DataFrame(rows)
    cols = [
        "ean", "titulo", "precio_lista", "precio_oferta", "tipo_descuento",
        "categoria_id", "categoria_nombre", "subcategoria_id", "subcategoria_nombre",
        "url", "imagen", "plu", "precio_texto",
    ]
    # Asegurar columnas aunque falten
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df.reindex(columns=cols)

    # Instrumentación antes de dedupe
    print(f"Total bruto antes de dedupe: {len(df)}")
    no_ean = df[df["ean"].isna()]
    if not no_ean.empty:
        sample_titles = no_ean["titulo"].dropna().head(10).tolist()
        print("Ejemplo títulos sin EAN (10):", sample_titles)

    # Dedupe opcional
    if getattr(args, "no_dedupe"):
        print("AVISO: --no-dedupe activo (no se eliminarán duplicados).")
    else:
        df_dedup = dedupe_products(df)
        print(f"Total tras dedupe: {len(df_dedup)} (perdidos: {len(df) - len(df_dedup)})")
        df = df_dedup

    # ===== Ingesta MySQL =====
    if not args.no_mysql:
        conn = None
        try:
            conn = get_conn()
            conn.autocommit = False
            cur = conn.cursor()
            tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
            capturado_en = datetime.now()

            inserted = 0
            for _, r in df.iterrows():
                rec = r.to_dict()
                producto_id = find_or_create_producto(cur, rec)
                pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, rec)
                insert_historico(cur, tienda_id, pt_id, rec, capturado_en)
                inserted += 1
                if inserted % 50 == 0:
                    conn.commit()
            conn.commit()
            print(f"✅ MySQL: {inserted} filas de histórico insertadas/actualizadas ({TIENDA_NOMBRE}).")
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"❌ Error MySQL: {e}")
            raise
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    # ===== Salidas locales opcionales =====
    if args.out:
        out_path = Path(args.out)
        with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
            df.to_excel(xw, index=False, sheet_name="Productos")
        print(f"📄 XLSX: {out_path.resolve()} (filas: {len(df)})")
    if args.csv:
        csv_path = Path(args.csv)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"📄 CSV:  {csv_path.resolve()}")

    # Resumen por combo (si aplicó bruteforce/all-sucursales)
    if total_por_combo:
        print("Resumen bruto por (ciudad, sucursal):", total_por_combo)

if __name__ == "__main__":
    main()
