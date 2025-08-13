#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from urllib.parse import urlparse, parse_qs, urljoin
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://www.dinoonline.com.ar"
START_URL = (
    "https://www.dinoonline.com.ar/super/categoria"
    "?_dyncharset=utf-8&Dy=1&Nty=1&minAutoSuggestInputLength=3"
    "&autoSuggestServiceUrl=%2Fassembler%3FassemblerContentCollection%3D%2Fcontent%2FShared%2FAuto-Suggest+Panels%26format%3Djson"
    "&searchUrl=%2Fsuper&containerClass=search_rubricator"
    "&defaultImage=%2Fimages%2Fno_image_auto_suggest.png&rightNowEnabled=false&Ntt="
)

def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
                  status_forcelist=(429, 500, 502, 503, 504))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def find_next_url_by_icon(soup: BeautifulSoup):
    caret = soup.select_one("a i.fa.fa-angle-right, a i.fa-angle-right")
    if caret and caret.parent and caret.parent.name == "a":
        return urljoin(BASE, caret.parent.get("href"))
    for a in soup.select("a[href]"):
        if a.select_one("i.fa-angle-right, i.fa.fa-angle-right"):
            return urljoin(BASE, a.get("href"))
    return None

def detect_nrpp_and_stable(current_url: str, soup: BeautifulSoup, items_on_page: int):
    cand = find_next_url_by_icon(soup)
    if cand:
        p = urlparse(cand)
        q = parse_qs(p.query, keep_blank_values=True)
        nrpp = None
        if "Nrpp" in q and q["Nrpp"]:
            try:
                nrpp = int(q["Nrpp"][0])
            except:
                pass
        stable_params = {k: v[0] for k, v in q.items()}
        stable_params.pop("No", None)
        base_path = f"{p.scheme}://{p.netloc}{p.path}"
        return base_path, nrpp, stable_params
    return None, None, None

if __name__ == "__main__":
    s = make_session()
    r = s.get(START_URL, timeout=20)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    boxes = soup.select("div.item.col-lg-3, div.item.col-md-3, div.item.col-sm-4, div.item.col-xs-6")
    items_on_page = len(boxes)

    base_path, nrpp, stable_params = detect_nrpp_and_stable(START_URL, soup, items_on_page)

    print("\n--- DETECCIÓN NRPP ---")
    if nrpp:
        print(f"✅ Encontrado Nrpp: {nrpp}")
        print(f"Base path: {base_path}")
        print(f"Parámetros estables: {stable_params}")
        print(f"Items detectados en página: {items_on_page}")
    else:
        print("❌ No se encontró Nrpp en la URL de paginación.")
        print("Tendrás que usar solo clics o paginación por 'siguiente'.")
