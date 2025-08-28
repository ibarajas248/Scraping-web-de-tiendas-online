#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import argparse
from typing import List, Dict, Optional
import sys

import requests
from lxml import html
import pandas as pd

SITEMAP_URL = "https://www.kilbelonline.com/sitemap.xml"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}
RE_PRODUCT_URL = re.compile(r"https://www\.kilbelonline\.com/[^\s<>]*?/art_(\d+)/")

def fetch_sitemap() -> List[str]:
    r = requests.get(SITEMAP_URL, headers=HEADERS, timeout=25)
    r.raise_for_status()
    content = r.text
    ids = RE_PRODUCT_URL.findall(content)
    urls = []
    for art_id in ids:
        m = re.search(
            fr"https://www\.kilbelonline\.com/[^\s<>]*art_{re.escape(art_id)}/", content
        )
        if m:
            urls.append(m.group(0))
    # dedup preservando orden
    seen, uniq = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def parse_product_page(url: str) -> Optional[Dict[str, any]]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
    except Exception as e:
        print(f"[WARN] {url} -> fallo descarga: {e}")
        return None
    t = r.text
    tree = html.fromstring(t)

    title = (tree.xpath("//h1/text()") or [None])[0]
    title = title.strip() if title else None

    code_match = re.search(r"COD\.\s*(\d+)", t)
    code = code_match.group(1) if code_match else None

    price_match = re.search(r"\$[\s]*([\d\.\,]+)", t)
    price = price_match.group(1).strip() if price_match else None

    per_unit_match = re.search(r"Precio por.*?\$[\s]*([\d\.\,]+)", t)
    price_per_unit = per_unit_match.group(1).strip() if per_unit_match else None

    no_tax_match = re.search(r"Precio sin impuestos.*?\$[\s]*([\d\.\,]+)", t)
    price_no_tax = no_tax_match.group(1).strip() if no_tax_match else None

    stock = 0 if "Sin Stock" in t else 1

    breadcrumb = [b.strip() for b in tree.xpath("//nav//a/text()") if b.strip() and b.lower() != "home"]
    category_path = " / ".join(breadcrumb) if breadcrumb else None

    img = tree.xpath("//img[contains(@src, '/web/images/')]/@src")
    image_url = img[0] if img else None

    return {
        "ArtId": url.split("art_")[-1].strip("/"),
        "URL": url,
        "Nombre": title,
        "Codigo": code,
        "Categoria": category_path,
        "Precio": price,
        "PrecioPorUnidad": price_per_unit,
        "PrecioSinImpuestos": price_no_tax,
        "Stock": stock,
        "Imagen": image_url,
    }

def crawl_kilbel(max_products: Optional[int] = None, sleep: float = 0.2) -> pd.DataFrame:
    product_urls = fetch_sitemap()
    if max_products:
        product_urls = product_urls[:max_products]

    total = len(product_urls)
    rows: List[Dict[str, any]] = []

    print(f"Productos a procesar: {total}")
    for idx, url in enumerate(product_urls, start=1):
        data = parse_product_page(url)
        if data:
            rows.append(data)
            # --- imprime lo que va encontrando ---
            nombre = (data.get("Nombre") or "").strip()
            precio = data.get("Precio") or "-"
            stock_txt = "OK" if data.get("Stock") else "SIN STOCK"
            print(f"[{idx}/{total}] {nombre} | ${precio} | {stock_txt} | {url}")
            sys.stdout.flush()
        else:
            print(f"[{idx}/{total}] (sin datos) {url}")
        time.sleep(sleep)
    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="Procesar solo N productos (debug)")
    ap.add_argument("--sleep", type=float, default=0.2, help="Pausa entre requests (segundos)")
    ap.add_argument("--outfile", default="kilbel_productos.xlsx", help="Salida XLSX")
    ap.add_argument("--csv", default=None, help="(opcional) Salida CSV adicional")
    args = ap.parse_args()

    df = crawl_kilbel(max_products=args.limit, sleep=args.sleep)
    print(f"\nTotal productos procesados: {len(df)}")
    df.to_excel(args.outfile, index=False)
    if args.csv:
        df.to_csv(args.csv, index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()
