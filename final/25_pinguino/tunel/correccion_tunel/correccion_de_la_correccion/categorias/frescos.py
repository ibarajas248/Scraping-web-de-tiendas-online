#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Supermercado Pingüino (solo LÁCTEOS) → MySQL y/o Excel/CSV (con túnel SSH al VPS)

- Precio de lista = Precio de oferta (mismo valor final).
- Números sin separador decimal: últimos 2 dígitos = centavos.
- Exporta ids y nombres de:
    * departamento (Lácteos)
    * categoría (CREMA, LECHES, etc.)
    * sub-subcategoría (FLUIDAS, SABORIZADAS, etc. cuando existan)
- Ingesta MySQL en tablas: tiendas, productos, producto_tienda, historico_precios.
- Recortes preventivos para columnas VARCHAR según tu schema.
- Conexión MySQL vía túnel SSH al VPS.

Uso:
  python pinguino_lacteos_mysql_ssh.py --out Productos_Pinguino_Lacteos.xlsx
  python pinguino_lacteos_mysql_ssh.py --no-mysql --out out.xlsx --csv out.csv
"""

import re
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
import os
import sys

# ====== imports para MySQL + túnel SSH ======
import mysql.connector
from mysql.connector import errors as mysql_errors
from sshtunnel import SSHTunnelForwarder


# ===================================================
#            CONFIG SSH + BASE DE DATOS
# ===================================================

# Datos del VPS (SSH)
SSH_HOST = "scrap.intelligenceblue.com.ar"
SSH_PORT = 22
SSH_USER = "scrap-ssh"
SSH_PASS = "gLqqVHswm42QjbdvitJ0"

# Datos de la base de datos en el VPS
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "userscrap"
DB_PASS = "UY8rMSGcHUunSsyJE4c7"
DB_NAME = "scrap"

MAXLEN_SKU_TIENDA = 80  # tu columna sku_tienda es VARCHAR(80)


# Túnel global (se reutiliza en todo el proceso)
_tunnel = None


def _ensure_tunnel() -> int:
    """
    Crea (o reutiliza) un SSHTunnelForwarder global hacia el VPS
    y devuelve el puerto local al que está ligado.
    """
    global _tunnel
    if _tunnel is None or not _tunnel.is_active:
        _tunnel = SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASS,
            remote_bind_address=(DB_HOST, DB_PORT),
        )
        _tunnel.start()
    return _tunnel.local_bind_port


def get_conn():
    """
    Devuelve un mysql.connector.connect() apuntando al puerto local
    del túnel SSH. El resto del código no necesita enterarse del túnel.
    """
    local_port = _ensure_tunnel()
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=local_port,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
    )
    return conn


def open_db():
    """
    Abre conexión y cursor con autocommit desactivado.
    """
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    return conn, cur


# ===================================================
#            IDENTIDAD TIENDA + WEB
# ===================================================

TIENDA_CODIGO = "pinguino"
TIENDA_NOMBRE = "Supermercado Pingüino"

BASE = "https://www.pinguino.com.ar"
INDEX = f"{BASE}/web/index.r"
MENU_CAT = f"{BASE}/web/menuCat.r"
PROD = f"{BASE}/web/productos.r"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# ===== Límites de columnas (ajusta a tu schema si difiere) =====
MAXLEN_NOMBRE = 255
MAXLEN_MARCA = 128
MAXLEN_FABRICANTE = 128
MAXLEN_CATEGORIA = 128
MAXLEN_SUBCATEGORIA = 128
MAXLEN_SUBSUBCATEGORIA = 128
MAXLEN_URL = 512
MAXLEN_NOMBRE_TIENDA = 255
MAXLEN_TIPO_OFERTA = 190
MAXLEN_PROMO_COMENTARIOS = 480


def _truncate(val: Optional[Any], maxlen: int) -> Optional[str]:
    if val is None:
        return None
    s = str(val)
    return s if len(s) <= maxlen else s[:maxlen]


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": INDEX,
    })
    retry = Retry(total=5, backoff_factor=0.5,
                  status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    # Cookies mínimas para ver productos (ajusta si cambia)
    s.cookies.set("ciudad", "1", domain="www.pinguino.com.ar", path="/")
    s.cookies.set("sucursal", "4", domain="www.pinguino.com.ar", path="/")
    try:
        s.get(INDEX, timeout=20)
    except requests.RequestException:
        pass
    return s


def tidy_space(txt: str) -> str:
    return re.sub(r"\s+", " ", txt or "").strip()


def parse_price_value(val: Any) -> Optional[float]:
    if val is None:
        return None

    # Normalizamos espacios raros y separadores finos
    s = str(val).strip().replace("\u202f", "").replace(" ", "")
    if not s:
        return None

    comma_count = s.count(',')
    dot_count = s.count('.')

    # Caso con separadores (coma o punto)
    if comma_count or dot_count:
        dec_sep = thou_sep = None

        if comma_count and dot_count:
            # Elegimos el último como separador decimal
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

        # Quitamos separador de miles
        if thou_sep and thou_sep != dec_sep:
            normalized = normalized.replace(thou_sep, '')

        # Reemplazamos separador decimal por punto
        if dec_sep:
            normalized = normalized.replace(dec_sep, '.')

        # Caso raro: mismo símbolo para miles y decimales (por seguridad)
        if dec_sep and dec_sep == thou_sep:
            last = normalized.rfind('.')
            if last != -1:
                normalized = normalized.replace('.', '')
                normalized = normalized[:last] + '.' + normalized[last:]

        try:
            return round(float(normalized), 2)
        except ValueError:
            return None

    # ✅ Si NO hay puntos ni comas y son solo dígitos,
    # lo tratamos como pesos tal cual (no como centavos).
    if s.isdigit():
        try:
            return round(float(s), 2)   # "2750" -> 2750.00
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


def parse_product_cards_enriched(
    html: str,
    dep_id: int,
    cat_id: Optional[int] = None,
    dep_name: Optional[str] = None,
    cat_name: Optional[str] = None,
    scat_id: Optional[int] = None,
    scat_name: Optional[str] = None,
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
                ean = val.strip()
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
            "subsubcategoria_id": scat_id,
            "subsubcategoria_nombre": scat_name,
            "url": url,
            "imagen": img,
            "plu": plu,
            "precio_texto": precio_texto,
        })
    return [p for p in products if p["titulo"] or (p["precio_oferta"] is not None or p["precio_lista"] is not None)]


def fetch_products_html(
    session: requests.Session,
    dep_id: int,
    cat_id: Optional[int] = None,
    scat_id: Optional[int] = None,
    params_extra: Optional[Dict[str, Any]] = None,
    save_debug: Optional[Path] = None,
) -> str:
    """
    Devuelve el HTML crudo de /web/productos.r?dep=...&cat=...&scat=...
    """
    params = {"dep": str(dep_id)}
    if cat_id is not None:
        params["cat"] = str(cat_id)
    if scat_id is not None:
        params["scat"] = str(scat_id)
    if params_extra:
        params.update(params_extra)
    r = session.get(PROD, params=params, timeout=40)
    r.raise_for_status()
    html = r.text
    if save_debug:
        save_debug.write_text(html, encoding="utf-8")
    return html


def parse_subsubcategorias_from_html(
    html: str,
    dep_id: int,
    cat_id: int,
) -> List[Dict[str, Any]]:
    """
    Busca dentro del HTML enlaces de sub-subcategoría: <div id="sCat" ...>
    con <a ... data-s="..."><span>Nombre</span></a>
    """
    soup = BeautifulSoup(html, "html.parser")
    scats: List[Dict[str, Any]] = []
    for a in soup.select("#sCat a[data-s], .scat a[data-s]"):
        s_id = a.get("data-s")
        if not s_id:
            continue
        try:
            s_id_int = int(s_id)
        except (TypeError, ValueError):
            continue
        name = a.get_text(strip=True) or str(s_id_int)
        scats.append({
            "dep_id": dep_id,
            "cat_id": cat_id,
            "id": s_id_int,
            "nombre": tidy_space(name),
        })
    # dedupe por id
    seen = set()
    uniq = []
    for s in scats:
        if s["id"] not in seen:
            seen.add(s["id"])
            uniq.append(s)
    return uniq


# ====== Helpers MySQL ======
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
    marca = _truncate((r.get("marca") or None), MAXLEN_MARCA)  # Pingüino no provee marca: quedará None
    fabricante = _truncate((r.get("fabricante") or None), MAXLEN_FABRICANTE)
    categoria = _truncate((r.get("categoria_nombre") or None), MAXLEN_CATEGORIA)
    subcategoria = _truncate((r.get("subcategoria_nombre") or None), MAXLEN_SUBCATEGORIA)

    # 1) EAN
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

    # 2) Reusar por (nombre, marca) o (nombre, marca IS NULL)
    if nombre:
        if marca:
            cur.execute(
                """SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1""",
                (nombre, marca or "")
            )
        else:
            cur.execute(
                """SELECT id FROM productos WHERE nombre=%s AND marca IS NULL LIMIT 1""",
                (nombre,)
            )
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

    # 3) Insert nuevo producto
    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (%s, NULLIF(%s,''), %s, %s, %s, %s)
    """, (ean, nombre, marca, fabricante, categoria, subcategoria))
    return cur.lastrowid


def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any]) -> int:
    # 1) Intentar usar PLU como SKU
    sku = (r.get("plu") or None)

    # 2) Si NO hay PLU, usar el nombre del producto como SKU (truncado a 80 chars)
    if not sku:
        titulo = (r.get("titulo") or "").strip()
        if titulo:
            sku = _truncate(titulo, MAXLEN_SKU_TIENDA)

    # 3) Usar el mismo valor como record_id de respaldo
    record_id = sku

    url = _truncate((r.get("url") or None), MAXLEN_URL)
    nombre_tienda = _truncate((r.get("titulo") or None), MAXLEN_NOMBRE_TIENDA)

    if sku:
        cur.execute("""
            INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              -- producto_id NO se actualiza
              record_id_tienda = COALESCE(VALUES(record_id_tienda), record_id_tienda),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    # Último recurso: sin SKU ni record_id, solo URL + nombre
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, %s)
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid


def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: datetime):
    precio_lista = _parse_price_num(r.get("precio_lista"))
    precio_oferta = _parse_price_num(r.get("precio_oferta"))
    tipo_oferta = _truncate((r.get("tipo_descuento") or None), MAXLEN_TIPO_OFERTA)
    # Guardamos IDs/nombres de cat/subcat/subsubcat como comentario auditable
    promo_comentarios = _truncate(
        f"cat_id={r.get('categoria_id')}; cat_nombre={r.get('categoria_nombre') or ''}; "
        f"subcat_id={r.get('subcategoria_id')}; subcat_nombre={r.get('subcategoria_nombre') or ''}; "
        f"subsubcat_id={r.get('subsubcategoria_id')}; subsubcat_nombre={r.get('subsubcategoria_nombre') or ''}",
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


# ===== Runner (scrape + ingest) =====

# Departamento LÁCTEOS
DEP_LACTEOS_ID = 4
DEP_LACTEOS_NOMBRE = "Lácteos"

# Solo estas categorías dentro de LÁCTEOS
CATS_LACTEOS = {
    10: "CREMA",
    9:  "DULCE DE LECHE",
    1:  "LECHES",
    4:  "MANTECA",
    5:  "MARGARINA",
    3:  "POSTRES",
    7:  "QUESOS",
    2:  "YOGURES",
}


def main():
    ap = argparse.ArgumentParser(description="Pingüino LÁCTEOS → MySQL / Excel / CSV")
    ap.add_argument("--out", default="Productos_Pinguino_Lacteos.xlsx", help="Archivo XLSX de salida (opcional)")
    ap.add_argument("--csv", default=None, help="CSV adicional (opcional)")
    ap.add_argument("--sleep", type=float, default=1.2, help="Espera (seg) entre requests")
    ap.add_argument("--only-ofertas", action="store_true", help="Solo ofertas globales (ofe=1)")
    ap.add_argument("--debug-html", action="store_true", help="Guardar HTML en ./_html")
    ap.add_argument("--no-mysql", action="store_true", help="No insertar en MySQL; solo archivos")
    args = ap.parse_args()

    s = new_session()

    rows: List[Dict[str, Any]] = []
    html_dir = Path("_html")
    if args.debug_html:
        html_dir.mkdir(exist_ok=True)

    if args.only_ofertas:
        # Comportamiento original: ofertas globales
        r = s.get(PROD, params={"ofe": "1"}, timeout=40)
        r.raise_for_status()
        if args.debug_html:
            (html_dir / "ofertas.html").write_text(r.text, encoding="utf-8")
        rows.extend(parse_product_cards_enriched(r.text, dep_id=999))
    else:
        dep_id = DEP_LACTEOS_ID
        dep_name = DEP_LACTEOS_NOMBRE

        for cat_id, cat_name in CATS_LACTEOS.items():
            # 1) Categoría base: /productos.r?dep=4&cat=X
            try:
                cat_debug = (html_dir / f"dep_{dep_id}_cat_{cat_id}.html") if args.debug_html else None
                html_cat = fetch_products_html(
                    s,
                    dep_id=dep_id,
                    cat_id=cat_id,
                    scat_id=None,
                    save_debug=cat_debug,
                )
                cat_prods = parse_product_cards_enriched(
                    html_cat,
                    dep_id=dep_id,
                    cat_id=cat_id,
                    dep_name=dep_name,
                    cat_name=cat_name,
                    scat_id=None,
                    scat_name=None,
                )
                print(f"[dep {dep_id} ({dep_name}) cat {cat_id} ({cat_name})] productos: {len(cat_prods)} (cat)")
                rows.extend(cat_prods)

                # 2) Buscar sub-subcategorías en ese HTML (sCat)
                scats = parse_subsubcategorias_from_html(html_cat, dep_id, cat_id)
                if scats:
                    print(f"  ↳ sub-subcategorías encontradas en cat {cat_id}: {[s['id'] for s in scats]}")
                for ssub in scats:
                    s_id = ssub["id"]
                    s_name = ssub["nombre"]
                    try:
                        scat_debug = (html_dir / f"dep_{dep_id}_cat_{cat_id}_scat_{s_id}.html") if args.debug_html else None
                        html_scat = fetch_products_html(
                            s,
                            dep_id=dep_id,
                            cat_id=cat_id,
                            scat_id=s_id,
                            save_debug=scat_debug,
                        )
                        scat_prods = parse_product_cards_enriched(
                            html_scat,
                            dep_id=dep_id,
                            cat_id=cat_id,
                            dep_name=dep_name,
                            cat_name=cat_name,
                            scat_id=s_id,
                            scat_name=s_name,
                        )
                        print(f"    [dep {dep_id} cat {cat_id} scat {s_id} ({s_name})] productos: {len(scat_prods)}")
                        rows.extend(scat_prods)
                        time.sleep(args.sleep)
                    except requests.RequestException as e:
                        print(f"[dep {dep_id} cat {cat_id} scat {s_id}] error: {e}")
                        continue

                time.sleep(args.sleep)

            except requests.RequestException as e:
                print(f"[dep {dep_id} cat {cat_id}] error: {e}")
                continue

    if not rows:
        print("No se extrajo ningún producto. Revisa cookies/sucursal o ajusta selectores.")
        return

    # ===== DataFrame + dedupe =====
    df = pd.DataFrame(rows)
    cols = [
        "ean", "titulo", "precio_lista", "precio_oferta", "tipo_descuento",
        "categoria_id", "categoria_nombre",
        "subcategoria_id", "subcategoria_nombre",
        "subsubcategoria_id", "subsubcategoria_nombre",
        "url", "imagen", "plu", "precio_texto",
    ]
    df = df.reindex(columns=cols)

    print(f"Filas brutas antes de dedupe: {len(df)}")

    # DEDUPE:
    # - Si hay PLU: una fila por PLU (producto único).
    # - Sin PLU: una fila por (ean, titulo).
    if "plu" in df.columns:
        df["plu_norm"] = df["plu"].replace("", np.nan)
        mask_plu = df["plu_norm"].notna()

        # Con PLU: dedupe por PLU
        df_plu = df[mask_plu].copy()
        df_plu = df_plu.drop_duplicates(subset=["plu_norm"], keep="first")

        # Sin PLU: dedupe por (ean, titulo)
        df_no_plu = df[~mask_plu].copy()
        df_no_plu = df_no_plu.drop_duplicates(subset=["ean", "titulo"], keep="first")

        df = pd.concat([df_plu, df_no_plu], ignore_index=True)
        df = df.drop(columns=["plu_norm"])
    else:
        df = df.drop_duplicates(subset=["ean", "titulo"], keep="first")

    print(f"Filas después de dedupe: {len(df)}")

    # ===== Ingesta MySQL (vía túnel SSH) =====
    if not args.no_mysql:
        conn, cur = None, None
        try:
            conn, cur = open_db()
            tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
            capturado_en = datetime.now()

            inserted = 0
            for idx, r in df.iterrows():
                rec = r.to_dict()
                while True:
                    try:
                        producto_id = find_or_create_producto(cur, rec)
                        pt_id = upsert_producto_tienda(cur, tienda_id, producto_id, rec)
                        insert_historico(cur, tienda_id, pt_id, rec, capturado_en)

                        inserted += 1
                        if inserted % 50 == 0:
                            conn.commit()
                        break

                    except mysql_errors.OperationalError as e:
                        # 2006: MySQL server has gone away
                        # 2013: Lost connection to MySQL server during query
                        if getattr(e, "errno", None) in (2006, 2013):
                            print(f"[fila {idx}] Conexión perdida ({e.errno}). Reintentando conexión...")
                            try:
                                if cur:
                                    cur.close()
                            except Exception:
                                pass
                            try:
                                if conn:
                                    conn.close()
                            except Exception:
                                pass
                            conn, cur = open_db()
                            tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
                            continue
                        else:
                            raise

            conn.commit()
            print(f"✅ MySQL: {inserted} filas de histórico insertadas/actualizadas ({TIENDA_NOMBRE} / Lácteos).")

        except Exception as e:
            try:
                if conn and conn.is_connected():
                    conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass
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


if __name__ == "__main__":
    main()
