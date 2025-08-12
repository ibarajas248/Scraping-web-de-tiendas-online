#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== Config =====================
CATEGORIES = [
    "https://www.lagallega.com.ar/productosnl.asp?nl=03000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=05000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=07000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=13000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=15000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=09000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=06000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=04000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=02000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=19000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=11000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=08000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=10000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=16000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=18000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=17000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=14000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=21000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=20000000",
    "https://www.lagallega.com.ar/productosnl.asp?nl=12000000",
]

OUT_XLSX = "lagallega.xlsx"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
TIMEOUT = 20
RETRIES = 3
SLEEP_PAGE = 0.30
MAX_EMPTY_PAGES = 2
MAX_PAGES = 200           # salvaguarda dura por categoría
PAGE_SIZE = 50            # opcional: lo fija la web; ayuda a respuestas consistentes

COLS_FINAL = [
    "EAN","Código Interno","Nombre Producto","Categoría","Subcategoría","Marca",
    "Fabricante","Precio de Lista","Precio de Oferta","Tipo de Oferta","URL"
]

# ===================== HTTP session =====================
def build_session():
    s = requests.Session()
    retry = Retry(total=RETRIES, backoff_factor=0.5,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET"], raise_on_status=False)
    ad = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update(HEADERS)

    # Esta cookie suele definir el tamaño de página en el sitio
    try:
        s.cookies.set("cantP", str(PAGE_SIZE), domain="www.lagallega.com.ar", path="/")
    except Exception:
        pass
    return s

# ===================== Helpers =====================
def parse_price_ar(texto: str) -> float:
    """'$1.417,00' → 1417.00"""
    if not texto: return 0.0
    t = (texto.replace("$","").replace(".","").replace("\xa0","").strip()
               .replace(",", "."))
    try: return float(t)
    except Exception: return 0.0

def extract_ean_from_alt(alt_text: str) -> str:
    # '0763571722411 - aceite...' → 0763571722411
    if not alt_text: return ""
    m = re.match(r"(\d{8,14})\s*-\s*", alt_text.strip())
    return m.group(1) if m else ""

def list_products_on_page(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for li in soup.select("li.cuadProd"):
        a = li.select_one(".FotoProd a[href]")
        if not a:
            continue
        href = urljoin(base_url, a["href"])
        img = li.select_one(".FotoProd img[alt]")
        ean_hint = extract_ean_from_alt(img.get("alt","")) if img else ""
        nombre_el = li.select_one(".InfoProd .desc")
        precio_el = li.select_one(".InfoProd .precio .izq")
        nombre = nombre_el.get_text(strip=True) if nombre_el else ""
        precio = parse_price_ar(precio_el.get_text(strip=True) if precio_el else "")
        out.append({"detail_url": href, "ean_hint": ean_hint,
                    "nombre_list": nombre, "precio_list": precio})
    return out

def parse_detail(html, detail_url, ean_hint=""):
    soup = BeautifulSoup(html, "html.parser")
    # Código interno Pr desde la URL de detalle
    pr = ""
    try:
        q = parse_qs(urlparse(detail_url).query)
        pr = (q.get("Pr") or q.get("pr") or [""])[0]
    except Exception:
        pass

    tile = soup.select_one("#ContainerDesc .DetallIzq .tile")
    ean = extract_ean_from_alt(tile.get("alt","")) if tile else ""
    if not ean: ean = ean_hint

    nombre_el = soup.select_one(".DetallDer .DetallDesc > b")
    marca_el  = soup.select_one(".DetallDer .DetallMarc")
    precio_el = soup.select_one(".DetallDer .DetallPrec .izq")

    nombre = nombre_el.get_text(strip=True) if nombre_el else ""
    marca  = marca_el.get_text(strip=True) if marca_el else ""
    precio = parse_price_ar(precio_el.get_text(strip=True) if precio_el else "")

    return {
        "EAN": ean or "",
        "Código Interno": pr or "",
        "Nombre Producto": nombre or "",
        "Marca": marca or "",
        "Precio": precio,
        "URL": detail_url,
    }

def set_query(url: str, **params) -> str:
    """Devuelve la URL con los params de query actualizados (pg, nl, etc.)."""
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in params.items():
        q[k] = [str(v)]
    new_q = urlencode({k: v[0] if isinstance(v, list) else v for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def scrape_one_category(url: str, session: requests.Session):
    base = "{u.scheme}://{u.netloc}/".format(u=urlparse(url))
    try:
        cat_code = parse_qs(urlparse(url).query).get("nl", [""])[0]
    except Exception:
        cat_code = ""

    page = 1
    empty_streak = 0
    rows = []

    seen_page_signatures = set()
    seen_detail_urls = set()

    while True:
        if page > MAX_PAGES:
            print(f"⛔️ Corte por MAX_PAGES en {cat_code} (>{MAX_PAGES})")
            break

        # Iteración correcta por páginas: ?pg=1&nl=XXXX
        page_url = set_query(url, pg=page)

        r = session.get(page_url, timeout=TIMEOUT)
        if r.status_code != 200:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES: break
            page += 1; continue

        items = list_products_on_page(r.text, base)
        if not items:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES: break
            page += 1; continue

        # Firma de página para detectar repetición
        page_signature = tuple(it["detail_url"] for it in items)
        if page_signature in seen_page_signatures:
            print(f"🔁 Página repetida detectada en {cat_code} (pg={page}). Corto paginación.")
            break
        seen_page_signatures.add(page_signature)

        empty_streak = 0
        print(f"📄 {cat_code} — pg {page}: {len(items)} productos (URL: {page_url})")

        for it in items:
            durl = it["detail_url"]
            if durl in seen_detail_urls:
                continue
            seen_detail_urls.add(durl)

            rd = session.get(durl, timeout=TIMEOUT)
            if rd.status_code != 200:
                continue
            det = parse_detail(rd.text, durl, ean_hint=it.get("ean_hint",""))

            # fallbacks
            if not det["Nombre Producto"] and it.get("nombre_list"):
                det["Nombre Producto"] = it["nombre_list"]
            if det["Precio"] == 0 and it.get("precio_list"):
                det["Precio"] = it["precio_list"]

            # mapear al formato final
            row = {
                "EAN": det["EAN"],
                "Código Interno": det["Código Interno"],
                "Nombre Producto": det["Nombre Producto"],
                "Categoría": cat_code,   # solo código disponible
                "Subcategoría": "",
                "Marca": det["Marca"],
                "Fabricante": "",
                "Precio de Lista": det["Precio"],
                "Precio de Oferta": det["Precio"],
                "Tipo de Oferta": "Precio regular",
                "URL": det["URL"],
            }

            # imprimir lo que va encontrando
            print(f"  🛒 {row['EAN']} | {row['Nombre Producto']} | ${row['Precio de Lista']:.2f} | {row['URL']}")
            rows.append(row)

        page += 1
        time.sleep(SLEEP_PAGE)

    return rows

# ===================== Runner =====================
def unique_in_order(seq):
    seen = set(); out = []
    for s in seq:
        if s not in seen:
            out.append(s); seen.add(s)
    return out

def main():
    session = build_session()
    urls = unique_in_order(CATEGORIES)  # quita repetidas manteniendo orden

    all_rows = []
    for u in urls:
        try:
            all_rows.extend(scrape_one_category(u, session))
        except Exception as e:
            print(f"⚠️ Error en categoría {u}: {e}")

    if not all_rows:
        print("⚠️ Sin datos.")
        return

    df = pd.DataFrame(all_rows)

    # Dedupe priorizado: EAN → Código Interno → URL
    df["EAN"] = df["EAN"].astype("string")
    df["_k"] = df["EAN"].fillna("").str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "Código Interno"].fillna("").astype(str).str.strip()
    m = df["_k"] == ""
    df.loc[m, "_k"] = df.loc[m, "URL"].fillna("").astype(str).str.strip()
    before = len(df)
    df = df.drop_duplicates(subset=["_k"]).drop(columns=["_k"])
    print(f"🧹 Dedupe: -{before - len(df)} duplicados → {len(df)} únicos")

    # Asegurar columnas/orden
    for c in COLS_FINAL:
        if c not in df.columns: df[c] = pd.NA
    df = df[COLS_FINAL]

    # Excel: desactivar hipervínculos automáticos (evita warning de límite)
    with pd.ExcelWriter(
        OUT_XLSX,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}}
    ) as writer:
        df.to_excel(writer, index=False, sheet_name="productos")
        wb = writer.book; ws = writer.sheets["productos"]
        money = wb.add_format({"num_format":"0.00"})
        text  = wb.add_format({"num_format":"@"})
        col = {n:i for i,n in enumerate(COLS_FINAL)}
        ws.set_column(col["EAN"], col["EAN"], 18, text)
        ws.set_column(col["Nombre Producto"], col["Nombre Producto"], 52)
        for c in ["Categoría","Subcategoría","Marca","Fabricante"]:
            ws.set_column(col[c], col[c], 20)
        ws.set_column(col["Precio de Lista"], col["Precio de Lista"], 14, money)
        ws.set_column(col["Precio de Oferta"], col["Precio de Oferta"], 14, money)
        ws.set_column(col["URL"], col["URL"], 46)

    print(f"💾 Guardado: {OUT_XLSX} ({len(df)} filas)")

if __name__ == "__main__":
    main()
