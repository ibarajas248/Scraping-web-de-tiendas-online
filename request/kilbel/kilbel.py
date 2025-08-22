#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kilbel (kilbelonline.com) – Scraper mixto:
  1) Recorre categorías (n1_/n2_/n3_) y pagina
  2) (Opcional) Autocompletado a-z0-9 para traer coincidencias extra
  3) Visita detalle para extraer COD./SKU tienda y datos adicionales
  4) Exporta CSV y XLSX con esquema estándar

Requisitos:
  pip install requests beautifulsoup4 lxml pandas tenacity

Uso:
  python kilbel_scraper.py --modo ambos        # categorías + autocompletar
  python kilbel_scraper.py --modo categorias   # solo categorías
  python kilbel_scraper.py --modo auto         # solo autocompletar
"""

import re
import time
import json
import string
import argparse
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE = "https://www.kilbelonline.com/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

AUTO_URL = "https://www.kilbelonline.com/paginas/buscador_autocompletar.php?term={term}"
SLEEP = (0.4, 0.9)  # (min, max) segundos entre requests
TIMEOUT = 25

# ---------- Helpers de red ----------
class SoftHTTPError(Exception):
    pass

def _sleep():
    import random
    time.sleep(random.uniform(*SLEEP))

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.max_redirects = 5
    return s

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((requests.RequestException, SoftHTTPError)),
)
def GET(session: requests.Session, url: str, **kwargs) -> requests.Response:
    r = session.get(url, timeout=TIMEOUT, **kwargs)
    if r.status_code >= 500:
        raise SoftHTTPError(f"HTTP {r.status_code} {url}")
    return r

# ---------- Modelo ----------
@dataclass
class Item:
    ean: Optional[str]
    codigo_interno: Optional[str]  # COD./SKU tienda
    nombre: Optional[str]
    categoria: Optional[str]
    subcategoria: Optional[str]
    marca: Optional[str]
    fabricante: Optional[str]
    precio_lista: Optional[float]
    precio_oferta: Optional[float]
    tipo_oferta: Optional[str]
    url: Optional[str]

def num(x: Optional[str]) -> Optional[float]:
    if not x:
        return None
    try:
        # Remueve símbolos comunes
        x = re.sub(r"[^\d,.\-]", "", x)
        # Normaliza coma decimal si aplica
        if x.count(",") == 1 and x.count(".") == 0:
            x = x.replace(",", ".")
        # Si hay separadores de miles mezclados, deja el último como decimal
        if x.count(".") > 1:
            x = x.replace(".", "")
        return float(x)
    except Exception:
        return None

# ---------- Descubrimiento de categorías ----------
def is_category_href(href: str) -> bool:
    # Kilbel usa rutas como /perfumeria/n1_3/, /bebidas/n2_123/, /gaseosas/n3_456/
    return bool(re.search(r"/n[123]_\d+/?$", href))

def discover_categories(session: requests.Session) -> List[str]:
    r = GET(session, BASE)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    cats: Set[str] = set()

    # Busca todos los enlaces que parezcan categorías
    for a in soup.select("a[href]"):
        href = a["href"].strip()
        if href.startswith("http"):
            url = href
        else:
            url = urljoin(BASE, href)

        path = urlparse(url).path
        if is_category_href(path):
            cats.add(url)

    # También intenta descubrir subniveles desde cada n1_
    new_found = True
    visited = set()
    while new_found:
        new_found = False
        for url in list(cats):
            if url in visited:
                continue
            visited.add(url)
            try:
                _sleep()
                r = GET(session, url)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "lxml")
                for a in soup.select("a[href]"):
                    href = a["href"].strip()
                    u = urljoin(BASE, href)
                    if is_category_href(urlparse(u).path) and u not in cats:
                        cats.add(u)
                        new_found = True
            except Exception:
                pass

    return sorted(cats)

# ---------- Listado: productos por categoría (con paginación) ----------
def extract_product_links_from_listing(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []

    # 1) tarjetas de producto típicas
    for a in soup.select("a[href].producto, .product a[href], .producto a[href]"):
        u = a.get("href", "").strip()
        if u:
            links.append(u)

    # 2) fallback: cualquier enlace con /producto/ en la ruta
    for a in soup.select("a[href]"):
        u = a["href"].strip()
        if "/producto/" in u or "/productos/" in u:
            links.append(u)

    # normaliza a URLs absolutas y filtra duplicados
    norm = []
    seen = set()
    for u in links:
        absu = urljoin(BASE, u)
        if absu not in seen:
            seen.add(absu)
            norm.append(absu)
    return norm

def find_next_page(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    # intenta localizar botón/enlace de siguiente
    candidates = soup.select("a[href].siguiente, a[href].next, a[href*='?pagina='], a[href*='&pagina=']")
    for a in candidates:
        u = urljoin(current_url, a.get("href", ""))
        if u and u != current_url:
            return u
    # fallback: ninguna paginación visible
    return None

def crawl_listing(session: requests.Session, category_url: str) -> List[str]:
    urls: List[str] = []
    seen_pages = set()
    url = category_url
    page = 0

    for _ in range(200):  # cap de seguridad
        if url in seen_pages:
            break
        seen_pages.add(url)
        _sleep()
        r = GET(session, url)
        if r.status_code != 200:
            break
        page += 1
        product_links = extract_product_links_from_listing(r.text)
        print(f"   📄 Página {page}: {len(product_links)} links encontrados")
        urls.extend(product_links)
        nxt = find_next_page(r.text, url)
        if not nxt:
            break
        url = nxt

    # único
    return sorted(set(urls))
# ---------- Detalle: extracción por producto ----------
def parse_detail(html: str, url: str, cat_hint: Tuple[str, str]) -> Item:
    soup = BeautifulSoup(html, "lxml")

    # nombre
    nombre = None
    for sel in ["h1", ".nombre-producto", ".product-name", "h1.titulo", ".detalle-producto h1"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            nombre = el.get_text(strip=True)
            break

    # marca (si viene rotulada)
    marca = None
    for lab in soup.select("li, .caracteristica, .atributo, .spec li"):
        txt = lab.get_text(" ", strip=True).lower()
        if "marca" in txt:
            # intenta extraer valor después de ':'
            m = re.search(r"marca[:\s]+(.+)", txt)
            if m:
                marca = m.group(1).strip()
            else:
                # último token
                parts = txt.split()
                if parts:
                    marca = parts[-1].strip()
            break

    # COD./SKU tienda (suele aparecer como "COD." o "Código:")
    codigo_interno = None
    candidates = soup.find_all(string=re.compile(r"(cod\.|código|sku)", re.I))
    for t in candidates:
        frag = t.strip()
        # Busca valor cercano
        parent_text = t.parent.get_text(" ", strip=True) if hasattr(t, "parent") else frag
        m = re.search(r"(?:cod\.|código|sku)[\s:]+([A-Z0-9\-\._/]+)", parent_text, flags=re.I)
        if m:
            codigo_interno = m.group(1).strip()
            break

    # precios: intenta dos niveles (lista/oferta)
    precio_lista = None
    precio_oferta = None
    tipo_oferta = None

    # selectores habituales
    price_selectors = [
        (".precio-oferta, .price.special, .price-sale, .oferta", "oferta"),
        (".precio, .price, .price-regular, .precio-lista", "lista"),
    ]
    for sel, typ in price_selectors:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            value = num(el.get_text(" ", strip=True))
            if value is not None:
                if typ == "oferta" and precio_oferta is None:
                    precio_oferta = value
                elif typ == "lista" and precio_lista is None:
                    precio_lista = value

    # si solo hay un precio, asúmelo como lista
    if precio_lista is None and precio_oferta is not None:
        precio_lista = precio_oferta
    if precio_oferta and precio_lista and precio_oferta < precio_lista:
        tipo_oferta = "descuento"
    else:
        tipo_oferta = None

    ean = None  # Kilbel normalmente no lo expone
    fabricante = None  # raramente aparece
    categoria, subcategoria = cat_hint

    return Item(
        ean=ean,
        codigo_interno=codigo_interno,
        nombre=nombre,
        categoria=categoria,
        subcategoria=subcategoria,
        marca=marca,
        fabricante=fabricante,
        precio_lista=precio_lista,
        precio_oferta=precio_oferta,
        tipo_oferta=tipo_oferta,
        url=url,
    )

def crawl_detail(session: requests.Session, url: str, cat_hint: Tuple[str, str]) -> Optional[Item]:
    try:
        _sleep()
        r = GET(session, url)
        if r.status_code != 200:
            print(f"   ⚠️ Error HTTP {r.status_code} en {url}")
            return None
        item = parse_detail(r.text, url, cat_hint)
        if item and item.nombre:
            print(f"      ✔ Producto: {item.nombre} | SKU: {item.codigo_interno or '-'}")
        else:
            print(f"      ✔ Producto sin nombre → {url}")
        return item
    except Exception as e:
        print(f"   ⚠️ Error detalle {url}: {e}")
        return None

# ---------- Mapeo categoría/subcategoría desde URL ----------
def parse_cat_from_url(category_url: str) -> Tuple[str, str]:
    """
    Intenta deducir (categoria, subcategoria) desde rutas tipo:
      /perfumeria/n1_3/
      /bebidas/gaseosas/n3_123/
    """
    path = urlparse(category_url).path.strip("/").split("/")
    # heurística sencilla:
    if len(path) >= 2 and re.search(r"n[123]_\d+", path[-1]):
        cat = path[0].replace("-", " ").title()
        sub = " ".join(path[1:-1]).replace("-", " ").title() if len(path) > 2 else None
        return (cat, sub)
    return (None, None)

# ---------- Autocompletado (diccionario) ----------
def autocomplete_sweep(session: requests.Session) -> Dict[str, Dict]:
    results: Dict[str, Dict] = {}
    alphabet = string.ascii_lowercase + string.digits

    for ch in alphabet:
        _sleep()
        url = AUTO_URL.format(term=ch)
        try:
            r = GET(session, url)
            if r.status_code != 200:
                continue
            data = r.json()
        except Exception:
            continue

        for row in data:
            # cada fila suele traer: id, label, value, url, precio, etc.
            pid = str(row.get("id") or row.get("value") or row.get("label") or row.get("url") or "")
            if not pid:
                continue
            results[pid] = row

    return results

# ---------- Orquestación ----------
def run(mode: str = "ambos", out_prefix: str = "kilbel"):
    session = make_session()

    items: List[Item] = []
    seen_keys: Set[str] = set()  # dedupe por url o por codigo_interno

    if mode in ("categorias", "ambos"):
        print("⛓ Descubriendo categorías…")
        cats = discover_categories(session)
        print(f"→ {len(cats)} categorías encontradas")

        for i, cat in enumerate(cats, 1):
            print(f"[{i}/{len(cats)}] {cat}")
            cat_hint = parse_cat_from_url(cat)
            product_links = crawl_listing(session, cat)
            print(f"   • {len(product_links)} productos en total en esta categoría")
            for pu in product_links:
                key = pu
                if key in seen_keys:
                    continue
                it = crawl_detail(session, pu, cat_hint)
                if it:
                    # dedupe por codigo_interno si existe
                    dedupe_key = it.codigo_interno or it.url
                    if dedupe_key in seen_keys:
                        continue
                    seen_keys.add(dedupe_key)
                    items.append(it)

    if mode in ("auto", "ambos"):
        print("🔎 Autocompletado a-z0-9…")
        auto = autocomplete_sweep(session)
        print(f"→ {len(auto)} entradas en autocompletar")
        # mostrar algunas
        for i, row in enumerate(auto.values()):
            if i < 5:
                print(f"   • {row.get('label') or row.get('nombre') or row.get('value')}")
        # Normaliza autocompletar a Item (mínimos campos)
        for row in auto.values():
            pu = row.get("url")
            nombre = row.get("label") or row.get("nombre") or row.get("value")
            precio_txt = row.get("precio") or row.get("price") or row.get("precio_oferta") or ""
            precio = num(str(precio_txt))
            key = row.get("id") or pu
            if not key:
                continue
            if key in seen_keys or (pu and pu in seen_keys):
                continue
            seen_keys.add(str(key))
            items.append(Item(
                ean=None,
                codigo_interno=None,
                nombre=nombre,
                categoria=None,
                subcategoria=None,
                marca=None,
                fabricante=None,
                precio_lista=precio,
                precio_oferta=None,
                tipo_oferta=None,
                url=urljoin(BASE, pu) if pu else None
            ))

    # ---- Exportación ----
    rows = [asdict(x) for x in items]
    df = pd.DataFrame(rows, columns=[
        "ean", "codigo_interno", "nombre", "categoria", "subcategoria",
        "marca", "fabricante", "precio_lista", "precio_oferta", "tipo_oferta", "url"
    ])

    if "codigo_interno" in df.columns:
        before = len(df)
        df = df.sort_values(by=["codigo_interno", "url"], na_position="last") \
               .drop_duplicates(subset=["codigo_interno", "url"], keep="first")
        print(f"🧹 Dedupe: {before} → {len(df)}")

    csv_path = f"{out_prefix}.csv"
    xlsx_path = f"{out_prefix}.xlsx"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as wb:
        df.to_excel(wb, index=False, sheet_name="kilbel")
    print(f"✅ Exportado: {csv_path} | {xlsx_path}")
    return df

# ---------- CLI ----------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--modo", choices=["categorias", "auto", "ambos"], default="ambos", help="Fuente de datos")
    ap.add_argument("--out", default="kilbel", help="Prefijo de salida (sin extensión)")
    args = ap.parse_args()
    run(mode=args.modo, out_prefix=args.out)
