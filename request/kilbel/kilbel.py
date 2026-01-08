#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kilbel (kilbelonline.com) – Almacén n1_1 (modo súper respetuoso)

- Recorre /almacen/n1_1/pag/1/, /2/, ... hasta que no haya productos.
- Extrae desde el listado y luego completa en el detalle.
- Respeta robots.txt (Crawl-delay), limita RPS, usa jitter, siestas largas, y backoff.
- Ventana horaria opcional (solo ejecuta entre HH:MM y HH:MM).
- Auto-guardado periódico y reanudación desde XLSX previo.

Dependencias: requests, bs4, lxml, pandas
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

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import urllib.robotparser as robotparser

# ================== Config sitio ==================
BASE = "https://www.kilbelonline.com"
LISTING_FMT = "/almacen/n1_1/pag/{page}/"

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
RE_PAGE_QUERY = re.compile(r"([?&])page=\d+", re.I)

# ================== Utilidades ==================
def clean(text: str) -> str:
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
    """Devuelve True si hora local actual está dentro de [start, end] (HH:MM)."""
    if not start and not end:
        return True
    def to_time(hhmm: str) -> dtime:
        hh, mm = hhmm.split(":")
        return dtime(int(hh), int(mm))
    now = datetime.now().time()
    if start and end:
        t1, t2 = to_time(start), to_time(end)
        # maneja ventana que cruza medianoche (ej 23:00–04:00)
        if t1 <= t2:
            return t1 <= now <= t2
        else:
            return now >= t1 or now <= t2
    if start:
        return datetime.now().time() >= to_time(start)
    if end:
        return datetime.now().time() <= to_time(end)
    return True

def respect_robots_crawl_delay() -> Optional[float]:
    rp = robotparser.RobotFileParser()
    rp.set_url(urljoin(BASE, "/robots.txt"))
    try:
        rp.read()
        # Nota: algunos robots.txt no definen crawl-delay para nuestro UA;
        # robotparser acepta un UA string. Usamos el primero.
        return rp.crawl_delay(USER_AGENTS[0])  # puede devolver None
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
    """
    GET con rotación de UA/idioma y backoff adaptativo.
    adaptive_state: {'sleep_mult': float, 'max_sleep_mult': float, 'errors_row': int}
    """
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
            # Penaliza velocidad ante señales de rate-limit o sobrecarga
            wait = base_sleep * adaptive_state["sleep_mult"] + random.uniform(0, jitter)
            sys.stderr.write(f"[WARN] {url} -> {r.status_code}. Backoff {wait:.2f}s (try {attempt}/{tries})\n")
            time.sleep(wait)
            adaptive_state["sleep_mult"] = min(adaptive_state["sleep_mult"] * 2.0, adaptive_state["max_sleep_mult"])
            adaptive_state["errors_row"] += 1
            continue

        # éxito: relajamos el multiplicador poco a poco
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
        nombre_list = clean(a.get_text()) if a else None
        href = a.get("href") if a else None
        url_detalle = urljoin(BASE, href) if href else None


        img = prod.select_one(".ant_imagen img")
        img_url = img.get("data-src") or (img.get("src") if img else None)

        sku_input = prod.select_one(f"input#id_item_{codigo_interno}") if codigo_interno else None
        sku_tienda = sku_input.get("value") if sku_input else None

        precio_lista_list = None
        precio_oferta_list = None
        el_prev = prod.select_one(".precio_complemento .precio.anterior")
        if el_prev: precio_lista_list = parse_price(el_prev.get_text())

        el_act = prod.select_one(".precio_complemento .precio.aux1")
        if el_act: precio_oferta_list = parse_price(el_act.get_text())

        promo = None
        promo_span = prod.select_one("span.promocion")
        if promo_span:
            mm = re.search(r"promocion(\d+)-off", " ".join(promo_span.get("class", [])))
            if mm: promo = f"{mm.group(1)}% OFF"

        precio_x_kg_list = None
        sin_imp_list = None
        for cod in prod.select(".precio_complemento .codigo"):
            txt = cod.get_text(" ", strip=True)
            if "Precio por" in txt:
                precio_x_kg_list = parse_price(txt)
            if "imp" in txt.lower():
                sin_imp_list = parse_price(txt)

        print(f"    [{idx:03}] LISTADO  CODINT={codigo_interno}  SKU_TIENDA={sku_tienda}  NOMBRE='{nombre_list}'  URL={url_detalle}")
        print(f"           PRECIOS listado -> lista={precio_lista_list}  oferta={precio_oferta_list}  promo={promo}  porKg={precio_x_kg_list}  sinImp={sin_imp_list}")

        productos.append({
            "CodigoInterno_list": codigo_interno,
            "NombreProducto_list": nombre_list,
            "URL": url_detalle,
            "Imagen": img_url,
            "SKU_Tienda": sku_tienda,
            "RecordId_Tienda": sku_tienda,
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
    if h1: res["NombreProducto"] = clean(h1.get_text())

    cod_box = soup.find(string=re.compile(r"COD\.\s*\d+"))
    if cod_box:
        m = re.search(r"COD\.\s*(\d+)", cod_box)
        if m: res["CodigoInterno_det"] = m.group(1)

    prev = soup.select_one("#detalle_producto .precio.anterior")
    if prev: res["PrecioLista"] = parse_price(prev.get_text())

    act = soup.select_one("#detalle_producto .precio.aux1")
    if act: res["PrecioOferta"] = parse_price(act.get_text())

    for div in soup.select("#detalle_producto .codigo"):
        t = div.get_text(" ", strip=True)
        if "Precio por" in t: res["PrecioPorKg"] = parse_price(t)
        if "sin impuestos" in t.lower() or "imp.nac" in t.lower(): res["PrecioSinImpuestos"] = parse_price(t)

    onclick_node = soup.find(attrs={"onclick": re.compile(r"agregarLista_dataLayerPush")})
    if onclick_node:
        on = onclick_node.get("onclick", "")
        mm = re.search(r"agregarLista_dataLayerPush\('([^']+)'", on)
        if mm:
            ruta = clean(mm.group(1).replace("&gt;", ">"))
            partes = [clean(p) for p in ruta.split(">") if p.strip()]
            if partes:
                res["Categoria"] = partes[0] if len(partes) > 0 else None
                res["Subcategoria"] = partes[1] if len(partes) > 1 else None
                res["Subsubcategoria"] = partes[2] if len(partes) > 2 else None

    m_ean = re.search(r"\b(\d{13})\b", soup.get_text(" ", strip=True))
    if m_ean: res["EAN"] = m_ean.group(1)

    return res

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

    # Reanudación
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

    for page in range(start_page, max_pages + 1):
        if not now_in_window(window_start, window_end):
            print(f"⏳ Fuera de ventana horaria ({window_start or '-'}–{window_end or '-'}) — pausa 60s")
            time.sleep(60)
            continue

        url_list = urljoin(BASE, LISTING_FMT.format(page=page))
        print(f"\n=== Página {page} -> {url_list}")

        # Respeto de ritmo
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
                print("    - Card sin URL de detalle, salto.")
                continue
            if url in vistos:
                print(f"    - Ya visitado: {url}")
                continue

            # Ritmo por item
            base_sleep = random.uniform(*sleep_item)
            polite_sleep(base_sleep, jitter=0.4, rps=rps, crawl_delay=crawl_delay)

            d = parse_detail(session, url, timeouts, base_sleep=0.5, jitter=0.7, adaptive_state=adaptive_state)
            if all(v is None for v in d.values()):
                consecutive_errors += 1
            else:
                consecutive_errors = 0

            print(f"           DETALLE -> COD={d.get('CodigoInterno_det')}  NOMBRE='{d.get('NombreProducto')}'")
            print(f"                      CATEGORÍA='{d.get('Categoria')}'  SUBCAT='{d.get('Subcategoria')}'  SUBSUB='{d.get('Subsubcategoria')}'")
            print(f"                      PRECIOS -> lista={d.get('PrecioLista')}  oferta={d.get('PrecioOferta')}  porKg={d.get('PrecioPorKg')}  sinImp={d.get('PrecioSinImpuestos')}  EAN={d.get('EAN')}")

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
            resultados.append(merged)
            vistos.add(url)
            total_items += 1

            # Siesta larga por items
            if long_nap_every_items and total_items % long_nap_every_items == 0:
                nap = long_nap_seconds + random.uniform(0, 2)
                sys.stderr.write(f"[INFO] Siesta larga por items: {nap:.1f}s\n")
                time.sleep(nap)

            # Auto-save
            if autosave_every and total_items % autosave_every == 0:
                try:
                    pd.DataFrame(resultados).to_excel(outfile, index=False)
                    sys.stderr.write(f"[INFO] Autosave -> {outfile} ({len(resultados)} filas)\n")
                except Exception as e:
                    sys.stderr.write(f"[WARN] Autosave falló: {e}\n")

            # Presupuesto de errores consecutivos
            if max_consecutive_errors and consecutive_errors >= max_consecutive_errors:
                sys.stderr.write(f"[ERROR] Demasiados errores consecutivos ({consecutive_errors}). Abortando para respetar el sitio.\n")
                break

        # Siesta larga por páginas
        if long_nap_every_pages and page % long_nap_every_pages == 0:
            nap = long_nap_seconds + random.uniform(0, 2)
            sys.stderr.write(f"[INFO] Siesta larga por páginas: {nap:.1f}s\n")
            time.sleep(nap)

        # Sleep entre páginas
        time.sleep(random.uniform(*sleep_page))

        if max_consecutive_errors and consecutive_errors >= max_consecutive_errors:
            break

    if not resultados:
        print("\n⚠ No se obtuvieron productos. No se generará Excel.")
        return

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
    ap = argparse.ArgumentParser(description="Scraper Kilbel Almacén n1_1 (modo respetuoso).")
    ap.add_argument("--max-pages", type=int, default=300)
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--outfile", type=str, default="kilbel_almacen_n1_1.xlsx")
    ap.add_argument("--csv", type=str, default=None)

    # ritmo y pausas
    ap.add_argument("--rps", type=float, default=0.33, help="Requests por segundo (0 = sin tope).")
    ap.add_argument("--sleep-item-min", type=float, default=0.5)
    ap.add_argument("--sleep-item-max", type=float, default=1.2)
    ap.add_argument("--sleep-page-min", type=float, default=1.0)
    ap.add_argument("--sleep-page-max", type=float, default=2.0)
    ap.add_argument("--long-nap-every-items", type=int, default=40)
    ap.add_argument("--long-nap-every-pages", type=int, default=8)
    ap.add_argument("--long-nap-seconds", type=float, default=10.0)

    # operación
    ap.add_argument("--proxy", type=str, default=None, help="socks5://user:pass@host:1080 o http://host:puerto")
    ap.add_argument("--resume", type=str, default=None, help="xlsx previo para reanudar (lee columna URL)")
    ap.add_argument("--autosave-every", type=int, default=50, help="Guardar cada N filas")

    # ventana horaria y presupuesto de errores
    ap.add_argument("--window-start", type=str, default=None, help="HH:MM local (ej 01:00)")
    ap.add_argument("--window-end", type=str, default=None, help="HH:MM local (ej 06:00)")
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
