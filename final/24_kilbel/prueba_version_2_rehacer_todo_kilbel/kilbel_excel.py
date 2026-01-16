#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import random
import argparse
from urllib.parse import urljoin
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE = "https://www.kilbelonline.com"
START = "https://www.kilbelonline.com/lacteos/n1_994/pag/{page}/"


# -------------------------
# HTTP session robusta
# -------------------------
def build_session():
    s = requests.Session()
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=0.9,
        status_forcelist=[403, 408, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def clean_text(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def parse_price_to_str(raw: str) -> str:
    """
    "$ 1.690,00" -> "1690.00"
    "$ 1690"     -> "1690"
    """
    raw = clean_text(raw)
    if not raw:
        return ""
    raw = raw.replace("$", "").strip()
    raw = raw.replace(".", "")      # miles
    raw = raw.replace(",", ".")     # decimales
    raw = re.sub(r"[^0-9.]", "", raw)
    if not raw:
        return ""
    if raw.count(".") > 1:
        parts = raw.split(".")
        raw = parts[0] + "." + "".join(parts[1:])
    return raw


def extract_price_blocks(item) -> tuple[str, str]:
    """
    REGLA (exacta a tu pedido):
    - Si hay oferta:
        precio_base  = div.precio.anterior.codigo
        precio_oferta= (span o div) .precio.aux1  (el precio actual grande)
    - Si NO hay oferta:
        precio_base  = (span o div) .precio.aux1
        precio_oferta= ""

    NOTA: NO usamos 'Precio por 1 Lt/Kg' (div.codigo.aux1). Eso se ignora.
    """
    cont = item.select_one("div.precio_complemento")
    if not cont:
        return "", ""

    # Oferta / base anterior
    anterior = cont.select_one("div.precio.anterior.codigo") or cont.select_one("div.precio.anterior")
    anterior_txt = clean_text(anterior.get_text(" ", strip=True)) if anterior else ""

    # Precio actual principal (puede ser span o div)
    # En tu caso “sin oferta” es <span class="precio aux1">...</span>
    actual = cont.select_one("span.precio.aux1") or cont.select_one("div.precio.aux1")
    actual_txt = clean_text(actual.get_text(" ", strip=True)) if actual else ""

    if anterior_txt:
        # con oferta
        return anterior_txt, actual_txt
    else:
        # sin oferta
        return actual_txt, ""


def parse_item(item) -> dict:
    # link + imagen
    a_img = item.select_one("div.ant_imagen a[href]")
    url_producto_rel = a_img.get("href") if a_img else ""
    url_producto = urljoin(BASE, url_producto_rel) if url_producto_rel else ""

    img = a_img.select_one("img") if a_img else None
    url_imagen = ""
    if img:
        url_imagen = img.get("data-src") or img.get("src") or ""
        url_imagen = clean_text(url_imagen)

    # nombre
    a_nombre = item.select_one("div.col1_listado a[id^='btn_nombre_imetrics_']")
    nombre = clean_text(a_nombre.get_text(" ", strip=True)) if a_nombre else ""

    # sku: hidden input id_item_XXXX value
    sku_input = item.select_one("input[type='hidden'][id^='id_item_']")
    sku = clean_text(sku_input.get("value")) if sku_input else ""

    # id prod_XXXXX por si sirve
    prod_id = clean_text(item.get("id"))
    prod_num = ""
    m = re.search(r"prod_(\d+)", prod_id)
    if m:
        prod_num = m.group(1)

    # precios raw según tu regla
    precio_base_raw, precio_oferta_raw = extract_price_blocks(item)

    # normalizados
    precio_base = parse_price_to_str(precio_base_raw)
    precio_oferta = parse_price_to_str(precio_oferta_raw)

    return {
        "sku": sku,
        "prod_block_id": prod_id,
        "prod_block_num": prod_num,
        "nombre": nombre,
        "url_producto": url_producto,
        "url_imagen": url_imagen,
        "precio_base_raw": precio_base_raw,
        "precio_oferta_raw": precio_oferta_raw,
        "precio_base": precio_base,
        "precio_oferta": precio_oferta,
    }


def fetch_page_html(session: requests.Session, url: str, proxies=None, timeout: int = 35) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123 Safari/537.36"
        ),
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = session.get(url, headers=headers, timeout=timeout, proxies=proxies)
    r.raise_for_status()
    return r.text


def scrape_category(start_url_pattern: str, max_pages: int, sleep_min: float, sleep_max: float, proxies=None, debug_html: bool = False):
    session = build_session()

    rows = []
    seen_skus = set()
    empty_pages_in_row = 0

    for page in range(1, max_pages + 1):
        url = start_url_pattern.format(page=page)
        print(f"[+] Pag {page}: {url}")

        html = ""
        for attempt in range(1, 3):
            try:
                html = fetch_page_html(session, url, proxies=proxies)
                break
            except Exception as e:
                print(f"    [!] Error fetch (intento {attempt}/2): {e}")
                time.sleep(2.0 + attempt)

        if not html:
            print("    -> Sin HTML, corto.")
            break

        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("div.producto.item")
        if not items:
            empty_pages_in_row += 1
            print(f"    -> 0 items (vacía #{empty_pages_in_row})")
            if debug_html:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                fname = f"debug_kilbel_pag_{page}_{ts}.html"
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"    -> Guardé HTML debug: {fname}")
            if empty_pages_in_row >= 2:
                print("    -> Corto: 2 páginas seguidas sin items.")
                break
            continue

        empty_pages_in_row = 0

        new_count = 0
        missing_price = 0

        for it in items:
            data = parse_item(it)

            sku = data.get("sku", "")
            if sku and sku in seen_skus:
                continue
            if sku:
                seen_skus.add(sku)

            if not data.get("precio_base_raw"):
                missing_price += 1

            rows.append(data)
            new_count += 1

        print(f"    -> items: {len(items)} | nuevos: {new_count} | sin precio_base: {missing_price} | total: {len(rows)}")

        # Si de golpe faltan muchos, guarda HTML para inspección
        if debug_html and len(items) > 0:
            ratio_missing = missing_price / max(len(items), 1)
            if ratio_missing >= 0.30:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                fname = f"debug_kilbel_precios_pag_{page}_{ts}.html"
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"    [!] Muchos precios faltantes ({ratio_missing:.0%}). Guardé HTML: {fname}")

        if new_count == 0:
            print("    -> Corto: página sin nuevos (todo repetido).")
            break

        time.sleep(random.uniform(sleep_min, sleep_max))

    return pd.DataFrame(rows)


def build_proxies(args):
    if not args.proxy_host:
        return None
    auth = ""
    if args.proxy_user and args.proxy_pass:
        auth = f"{args.proxy_user}:{args.proxy_pass}@"
    proxy_url = f"http://{auth}{args.proxy_host}:{args.proxy_port}"
    return {"http": proxy_url, "https": proxy_url}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--out", type=str, default="kilbel_lacteos_n1_994.xlsx")
    ap.add_argument("--sleep-min", type=float, default=0.9)
    ap.add_argument("--sleep-max", type=float, default=1.8)
    ap.add_argument("--debug-html", action="store_true", help="Guarda HTML cuando detecta problemas")

    # proxy opcional
    ap.add_argument("--proxy-host", type=str, default="")
    ap.add_argument("--proxy-port", type=int, default=823)
    ap.add_argument("--proxy-user", type=str, default="")
    ap.add_argument("--proxy-pass", type=str, default="")

    args = ap.parse_args()
    proxies = build_proxies(args)

    df = scrape_category(
        start_url_pattern=START,
        max_pages=args.max_pages,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
        proxies=proxies,
        debug_html=args.debug_html,
    )

    cols = [
        "sku",
        "nombre",
        "url_producto",
        "url_imagen",
        "precio_base_raw",
        "precio_oferta_raw",
        "precio_base",
        "precio_oferta",
        "prod_block_id",
        "prod_block_num",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    df = df[cols].copy()

    # dedupe: cuando hay sku, dedupe por sku; si no hay sku, dejamos tal cual
    df_sku = df[df["sku"] != ""].drop_duplicates(subset=["sku"], keep="first")
    df_nosku = df[df["sku"] == ""]
    df = pd.concat([df_sku, df_nosku], ignore_index=True)

    print(f"[OK] Filas finales: {len(df)}")
    df.to_excel(args.out, index=False)
    print(f"[OK] Excel guardado en: {args.out}")


if __name__ == "__main__":
    main()
