#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re, time, argparse, unicodedata, random
from typing import List, Dict, Any, Optional, Set, Callable
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from parsel import Selector   # pip install parsel lxml
import pandas as pd

BASE = "https://www.darentucasa.com.ar"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

def norm(s):
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c)).lower().strip()

def session_new():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-AR,es;q=0.9",
        "Referer": f"{BASE}/",
        "DNT": "1",
    })
    retry = Retry(total=6, backoff_factor=0.6, status_forcelist=[429,500,502,503,504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    try:
        s.get(f"{BASE}/Login.asp", timeout=20)
    except requests.RequestException:
        pass
    return s

def get_html(s: requests.Session, url: str) -> str:
    r = s.get(url, timeout=40)
    if r.status_code == 403:
        # re-siembra cookies una vez
        s.get(f"{BASE}/Login.asp", timeout=20)
        r = s.get(url, timeout=40)
    r.raise_for_status()
    return r.text

def add_qs(url: str, params: Dict[str, str]) -> str:
    p = urlparse(url)
    q = parse_qs(p.query, keep_blank_values=True)
    q.update({k:[str(v)] for k,v in params.items()})
    return urlunparse(p._replace(query=urlencode(q, doseq=True)))

def discover_categories(s: requests.Session) -> Dict[str,str]:
    """devuelve {texto_visible_normalizado: url_absoluta}"""
    cat = {}
    for path in ["/", "/Articulos.asp", "/Ofertas.asp", "/Categorias.asp", "/Rubros.asp"]:
        try:
            sel = Selector(get_html(s, urljoin(BASE, path)))
        except requests.RequestException:
            continue
        for a in sel.css("a[href]"):
            text = norm(a.xpath("normalize-space(string())").get())
            href = a.attrib.get("href")
            if not href or not text:
                continue
            # heurística: palabras típicas de categorías
            if any(k in text for k in ["almacen","bebidas","lacteos","limpieza","perfumeria","frescos","congelados"]):
                cat[text] = urljoin(BASE, href)
    return cat

def parse_listing(html: str) -> List[Dict[str,Any]]:
    sel = Selector(html)
    items = []
    for li in sel.css("li.cuadProd"):
        # PLU en onclick de la imagen
        onclick = li.css("div.FotoProd img::attr(onclick)").get("") or ""
        m = re.search(r"'(\d+)'", onclick)
        plu = m.group(1) if m else None
        titulo = li.css("div.desc::text").get(default="").strip()
        price_txt = li.css("div.precio div.izq ::text").getall()
        price_txt = " ".join([t.strip() for t in price_txt if t.strip()])
        precio = None
        m2 = re.search(r"([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})|[0-9]+(?:\.[0-9]{1,2})?)", price_txt.replace("\xa0"," "))
        if m2:
            num = m2.group(1).replace(".","").replace(" ", "").replace(",", ".")
            try: precio = float(num)
            except: pass
        oferta = bool(li.css("div.OferProd"))
        img = li.css("div.FotoProd img::attr(src)").get()
        if img and not img.startswith("http"):
            img = urljoin(BASE, img)
        items.append({
            "plu": plu, "titulo": titulo, "precio": precio,
            "precio_texto": price_txt, "oferta": oferta, "imagen": img
        })
    return items

def paginate_listing(s: requests.Session, url: str, max_pages=40) -> List[Dict[str,Any]]:
    all_items, seen: Set[str] = [], set()
    # 1) página base
    html = get_html(s, url); batch = parse_listing(html)
    for it in batch:
        key = f"{it.get('plu')}|{it.get('titulo')}"
        if key in seen: continue
        seen.add(key); all_items.append(it)
    # 2) enlaces “Siguiente”/numéricos
    sel = Selector(html)
    next_hrefs = [a.attrib["href"] for a in sel.css("a[href]")
                  if norm(a.xpath("normalize-space(string())").get()) in {"siguiente", "next"}]
    page_nums = [a.attrib["href"] for a in sel.css("a[href]") if re.search(r"[\?&](pag|pagina|page|p)=\d+", a.attrib["href"])]
    candidates = [urljoin(url, h) for h in (next_hrefs + page_nums)]
    # 3) si no hay enlaces, prueba parámetros comunes
    if not candidates:
        for param in ["pag","pagina","page","p"]:
            for i in range(2, max_pages+1):
                candidates.append(add_qs(url, {param: str(i)}))
    # 4) recorre candidatos hasta que no sume
    empty = 0
    for href in candidates[:max_pages*2]:
        try:
            html = get_html(s, href)
        except requests.RequestException:
            continue
        new = 0
        for it in parse_listing(html):
            key = f"{it.get('plu')}|{it.get('titulo')}"
            if key not in seen:
                seen.add(key); all_items.append(it); new += 1
        empty = empty + 1 if new == 0 else 0
        if empty >= 2: break
        time.sleep(random.uniform(0.3, 0.9))
    return all_items

# ----- Plug-in de detalle (ejemplo/cascarón) -----
def detalle_dar(session: requests.Session, plu: str) -> Dict[str,Any]:
    """
    Implementar según lo que observes en la red:
      - a veces POST a AjaxDetalle.asp con {plu: ####}
      - o GET a Detalle.asp?plu=####
    Este es un placeholder seguro.
    """
    return {}  # {'ean': '779…', 'sku': '…', 'fabricante': '…'}

DETAILERS: Dict[str, Callable[[requests.Session,str], Dict[str,Any]]] = {
    "darentucasa": detalle_dar,
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--categoria", default="Almacén")
    ap.add_argument("--max-pages", type=int, default=40)
    ap.add_argument("--with-detail", action="store_true")
    ap.add_argument("--out", default="DAR_full.xlsx")
    args = ap.parse_args()

    s = session_new()
    # 1) categorías
    cats = discover_categories(s)
    if args.categoria.lower() != "all":
        # pick la categoría más parecida
        key = max(cats.keys(), key=lambda k: (norm(args.categoria) in k, len(k))) if cats else None
        urls = [cats[key]] if key else []
    else:
        urls = list(cats.values())

    if not urls:
        print("No se hallaron enlaces de categoría. Revisa cookies/sesión.")
        return

    rows = []
    for cu in urls:
        items = paginate_listing(s, cu, max_pages=args.max_pages)
        for it in items:
            row = it.copy()
            if args.with-detail and it.get("plu"):
                row.update(DETAILERS["darentucasa"](s, it["plu"]))
            rows.append(row)

    df = pd.DataFrame(rows).drop_duplicates(subset=["plu","titulo"])
    df.to_excel(args.out, index=False)
    print(f"OK -> {len(df)} productos en {args.out}")

if __name__ == "__main__":
    main()
