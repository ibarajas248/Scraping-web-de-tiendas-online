#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Abastecedor (VTEX)
- Login
- Selección “Retiro en sucursal”
- Scrape Almacén paginado
- Inserta/actualiza en MySQL (tiendas/productos/producto_tienda/historico_precios)
- Guarda XLSX final

Flujo:
- Va a /almacen?page=1..N
- Para cada página:
    - toma TODOS los productos (links)
    - entra a cada uno
    - extrae: nombre, sku (Referencia), precio, url
- Inserta en DB en el momento
- Mini-commit por lote
"""

import os
import sys
import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd
import numpy as np
from playwright.sync_api import sync_playwright

import mysql.connector
from mysql.connector import errors as myerr, Error as MySQLError

# ============================================================
# Configuración
# ============================================================

BASE = "https://www.abastecedor.com.ar"
START_LISTING = f"{BASE}/almacen?page=1"

EMAIL = "mauro@factory-blue.com"
PASSWORD = "Compras2025"

HEADLESS = False  # para cron: True
OUT_XLSX = f"Abastecedor_Almacen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

# Identidad tienda (DB)
TIENDA_CODIGO = BASE
TIENDA_NOMBRE = "El Abastecedor (Argentina)"

# MySQL tuning / robustez
LOCK_ERRNOS = {1205, 1213}
ERRNO_OUT_OF_RANGE = 1264
COMMIT_EVERY = 120

MAXLEN_NOMBRE = 255
MAXLEN_URL = 512
MAXLEN_PROMO_COMENT = 255

# ============================================================
# Conexión DB (usa TU helper)
# ============================================================

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from base_datos import get_conn  # <- Debe retornar mysql.connector.connect(...)

# ============================================================
# Helpers texto / precio
# ============================================================

PRICE_RE = re.compile(r"([0-9\.\,]+)")

def clean(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

def _truncate(s: Optional[str], n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]

def parse_price_to_varchar(raw: Optional[str]) -> Optional[str]:
    """
    "$ 1.837,5" -> "1837.50"
    "$ 2.450"   -> "2450.00"
    """
    if not raw:
        return None
    s = str(raw).strip()
    m = PRICE_RE.search(s)
    if not m:
        return None
    v = m.group(1)

    # Normaliza separadores AR
    v = re.sub(r"[^\d\.,]", "", v)
    if "," in v and "." in v:
        v = v.replace(".", "").replace(",", ".")
    elif "," in v and "." not in v:
        v = v.replace(",", ".")

    try:
        f = float(v)
        return f"{f:.2f}"
    except Exception:
        return None

def safe_text(loc):
    """Devuelve texto seguro desde un locator de Playwright."""
    try:
        if loc.count() <= 0:
            return None
        txt = loc.first.inner_text().strip()
        return txt if txt else None
    except Exception:
        return None

# ============================================================
# SQL helpers (patrón robusto)
# ============================================================

def exec_retry(cur, sql, params=(), max_retries=5, base_sleep=0.4):
    attempt = 0
    while True:
        try:
            cur.execute(sql, params)
            return
        except myerr.DatabaseError as e:
            code = getattr(e, "errno", None)
            if code in LOCK_ERRNOS and attempt < max_retries:
                time.sleep(base_sleep * (2 ** attempt))
                attempt += 1
                continue
            raise

def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    exec_retry(cur,
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, _truncate(nombre, MAXLEN_NOMBRE))
    )
    exec_retry(cur, "SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, nombre: str, marca: Optional[str] = None) -> int:
    """
    Sin EAN. Dedupe:
    - si hay marca: (nombre, marca)
    - si no: nombre
    """
    nombre = _truncate(clean(nombre) or "", MAXLEN_NOMBRE)
    marca = _truncate(clean(marca) or "", MAXLEN_NOMBRE)

    if marca:
        exec_retry(cur,
            "SELECT id FROM productos WHERE nombre=%s AND IFNULL(marca,'')=%s LIMIT 1",
            (nombre, marca)
        )
        row = cur.fetchone()
        if row:
            return row[0]

    if nombre:
        exec_retry(cur,
            "SELECT id FROM productos WHERE nombre=%s LIMIT 1",
            (nombre,)
        )
        row = cur.fetchone()
        if row:
            pid = row[0]
            # si luego aparece marca, la completa
            if marca:
                exec_retry(cur,
                    "UPDATE productos SET marca=COALESCE(NULLIF(%s,''), marca) WHERE id=%s",
                    (marca, pid)
                )
            return pid

    exec_retry(cur, """
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULLIF(%s,''), NULL, NULL, NULL)
    """, (nombre, marca))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, sku: Optional[str], url: Optional[str], nombre_tienda: Optional[str]) -> int:
    sku = _truncate(clean(sku), MAXLEN_NOMBRE)
    url = _truncate(clean(url), MAXLEN_URL)
    nombre_tienda = _truncate(clean(nombre_tienda), MAXLEN_NOMBRE)

    # Key natural esperada: (tienda_id, sku_tienda) (o record_id si no hay sku)
    record_id = sku

    if sku:
        exec_retry(cur, """
            INSERT INTO producto_tienda
              (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
            VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))
            ON DUPLICATE KEY UPDATE
              id = LAST_INSERT_ID(id),
              producto_id = VALUES(producto_id),
              url_tienda = COALESCE(VALUES(url_tienda), url_tienda),
              nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
        """, (tienda_id, producto_id, sku, record_id, url, nombre_tienda))
        return cur.lastrowid

    # fallback raro (sin sku)
    exec_retry(cur, """
        INSERT INTO producto_tienda (tienda_id, producto_id, url_tienda, nombre_tienda)
        VALUES (%s, %s, NULLIF(%s,''), NULLIF(%s,''))
    """, (tienda_id, producto_id, url, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, capturado_en: datetime, precio_raw: Optional[str], page_url: Optional[str]):
    precio_lista = parse_price_to_varchar(precio_raw)
    # si no hay oferta, guardamos vigente en precio_oferta para no perder precio
    precio_oferta = precio_lista

    promo_com = []
    if page_url:
        promo_com.append(f"url={page_url}")
    promo_comentarios = _truncate(" | ".join(promo_com), MAXLEN_PROMO_COMENT) if promo_com else None

    sql = """
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = VALUES(promo_tipo),
          promo_texto_regular = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios = VALUES(promo_comentarios)
    """
    params = (
        tienda_id, producto_tienda_id, capturado_en,
        precio_lista, precio_oferta, None,
        None, None, None, promo_comentarios
    )
    try:
        exec_retry(cur, sql, params)
    except myerr.DatabaseError as e:
        if getattr(e, "errno", None) == ERRNO_OUT_OF_RANGE:
            params2 = (tienda_id, producto_tienda_id, capturado_en, None, None, None, None, None, None, promo_comentarios)
            exec_retry(cur, sql, params2)
        else:
            raise

# ============================================================
# Login + selección de entrega
# ============================================================

def login_and_choose_delivery(page):
    """Login VTEX + selección 'Retiro en sucursal'."""

    page.goto(BASE, wait_until="domcontentloaded", timeout=60000)

    page.wait_for_selector("div.vtex-login-2-x-contentFormVisible", timeout=20000)

    page.get_by_role("button", name="Entrar con e-mail y contraseña").click(timeout=15000)

    email_sel = (
        'div.vtex-login-2-x-inputContainerEmail '
        'input[placeholder="Ej.: ejemplo@mail.com"]'
    )
    email = page.locator(email_sel).first
    email.wait_for(state="visible", timeout=20000)
    email.click()
    page.keyboard.press("Control+A")
    page.keyboard.type(EMAIL, delay=35)

    if (email.input_value() or "").strip() != EMAIL:
        page.evaluate(
            """
            (v) => {
                const el =
                    document.querySelector(
                        'div.vtex-login-2-x-inputContainerEmail input[placeholder="Ej.: ejemplo@mail.com"]'
                    ) ||
                    [...document.querySelectorAll('input[placeholder="Ej.: ejemplo@mail.com"]')]
                        .find(e => e.offsetParent !== null);
                if (!el) return;
                el.focus();
                el.value = v;
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
            }
            """,
            EMAIL
        )

    pass_sel = (
        'div.vtex-login-2-x-inputContainerPassword '
        'input[placeholder="Ingrese su contraseña "]'
    )
    pwd = page.locator(pass_sel).first
    pwd.wait_for(state="visible", timeout=20000)
    pwd.click()
    page.keyboard.press("Control+A")
    page.keyboard.type(PASSWORD, delay=35)

    if (pwd.input_value() or "") != PASSWORD:
        page.evaluate(
            """
            (v) => {
                const el =
                    document.querySelector(
                        'div.vtex-login-2-x-inputContainerPassword input[placeholder="Ingrese su contraseña "]'
                    ) ||
                    [...document.querySelectorAll('input[type="password"]')]
                        .find(e => e.offsetParent !== null);
                if (!el) return;
                el.focus();
                el.value = v;
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change', {bubbles:true}));
            }
            """,
            PASSWORD
        )

    page.get_by_role("button", name="Entrar").click(timeout=20000)

    page.wait_for_selector(
        "div.elabastecedorar-redclover-theme-0-x-deliverySelectorOptions",
        timeout=60000
    )

    retiro_card = page.locator(
        "div.elabastecedorar-redclover-theme-0-x-deliverySelectorOption"
        ":has(h4:has-text('Retiro en sucursal'))"
    ).first

    retiro_card.wait_for(state="visible", timeout=20000)
    retiro_card.scroll_into_view_if_needed()
    page.wait_for_timeout(400)

    try:
        retiro_card.click(timeout=8000)
    except Exception:
        try:
            retiro_card.click(timeout=8000, force=True)
        except Exception:
            page.evaluate(
                """
                () => {
                    const cards = Array.from(
                        document.querySelectorAll(
                            'div.elabastecedorar-redclover-theme-0-x-deliverySelectorOption'
                        )
                    );
                    const card = cards.find(c =>
                        (c.innerText || '').includes('Retiro en sucursal')
                    );
                    if (!card) return false;
                    card.scrollIntoView({block:'center'});
                    card.click();
                    return true;
                }
                """
            )

    page.wait_for_timeout(700)

# ============================================================
# Listado
# ============================================================

def collect_product_links_from_listing(page):
    """Devuelve lista de URLs absolutas de productos desde el listado."""

    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(800)

    cards = page.locator(
        "div.vtex-search-result-3-x-galleryItem "
        "a.vtex-product-summary-2-x-clearLink"
    )

    if cards.count() == 0:
        cards = page.locator('a[href$="/p"], a[href*="/p?"]')

    urls = []
    for i in range(cards.count()):
        href = cards.nth(i).get_attribute("href")
        if not href:
            continue
        urls.append(href if href.startswith("http") else BASE.rstrip("/") + href)

    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)

    return uniq

# ============================================================
# Detalle producto
# ============================================================

def scrape_product_detail(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=60000)

    name = safe_text(
        page.locator(
            "h1.vtex-store-components-3-x-productNameContainer "
            "span.vtex-store-components-3-x-productBrand"
        )
    ) or safe_text(
        page.locator("h1 span.vtex-store-components-3-x-productBrand")
    ) or safe_text(
        page.locator("h1")
    )

    sku = safe_text(
        page.locator("span.vtex-product-identifier-0-x-product-identifier__value")
    ) or safe_text(
        page.locator("span:has-text('Referencia')")
            .locator("xpath=following-sibling::span")
            .first
    )

    price = safe_text(
        page.locator("span.vtex-product-price-1-x-sellingPriceValue")
    ) or safe_text(
        page.locator("span.vtex-store-components-3-x-sellingPriceValue")
    ) or safe_text(
        page.locator("span.vtex-product-price-1-x-sellingPrice")
    )

    return {
        "nombre": clean(name),
        "sku": clean(sku),
        "precio": clean(price),
        "url": url
    }

# ============================================================
# Main
# ============================================================

def main():
    rows: List[Dict[str, Any]] = []
    capturado_en = datetime.now()

    # --- DB ---
    conn = None
    cur = None
    tienda_id = None

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(buffered=True)

        # tuning de sesión
        try:
            exec_retry(cur, "SET SESSION innodb_lock_wait_timeout = 15")
            exec_retry(cur, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
        except Exception:
            pass

        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        conn.commit()
        print(f"🗄️ DB lista. tienda_id={tienda_id}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)
            context = browser.new_context()
            page = context.new_page()

            print("🔐 Login + selección de entrega…")
            login_and_choose_delivery(page)
            print("✅ Login OK")

            page_num = 1
            inserted = 0
            in_batch = 0

            while True:
                listing_url = f"{BASE}/almacen?page={page_num}"
                print(f"\n📄 Listado: {listing_url}")

                page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1200)

                product_urls = collect_product_links_from_listing(page)
                if not product_urls:
                    print("✅ No hay más productos. Fin.")
                    break

                print(f"🧾 Productos en página {page_num}: {len(product_urls)}")

                for idx, u in enumerate(product_urls, start=1):
                    try:
                        data = scrape_product_detail(page, u)
                        rows.append(data)

                        # ===== INSERT DB =====
                        # marca no la tenemos aquí; queda NULL
                        pid = find_or_create_producto(cur, nombre=data.get("nombre") or "", marca=None)
                        ptid = upsert_producto_tienda(
                            cur, tienda_id, pid,
                            sku=data.get("sku"),
                            url=data.get("url"),
                            nombre_tienda=data.get("nombre")
                        )
                        insert_historico(
                            cur, tienda_id, ptid, capturado_en,
                            precio_raw=data.get("precio"),
                            page_url=data.get("url")
                        )

                        inserted += 1
                        in_batch += 1

                        print(f" ✅ ({page_num}-{idx}/{len(product_urls)}) "
                              f"{data.get('sku') or '-'} | {data.get('precio') or '-'} | {data.get('nombre') or '-'}")

                        # mini-commit
                        if in_batch >= COMMIT_EVERY:
                            conn.commit()
                            print(f"💾 mini-commit +{in_batch} (acum {inserted})")
                            in_batch = 0

                    except MySQLError as e:
                        errno = getattr(e, "errno", None)
                        print(f" ❌ DB ({page_num}-{idx}) errno={errno}: {e}")
                        try:
                            conn.rollback()
                        except Exception:
                            pass

                    except Exception as e:
                        print(f" ❌ ({page_num}-{idx}/{len(product_urls)}) Error en {u}: {repr(e)}")

                    finally:
                        # volver al listado para seguir
                        try:
                            page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
                            page.wait_for_timeout(500)
                        except Exception:
                            pass

                page_num += 1

            # commit final
            if in_batch:
                conn.commit()
                print(f"💾 commit final +{in_batch} (total {inserted})")

            try:
                browser.close()
            except Exception:
                pass

        # Guardar XLSX
        df = pd.DataFrame(rows, columns=["nombre", "sku", "precio", "url"])
        df.to_excel(OUT_XLSX, index=False)
        print(f"\n📦 XLSX generado: {OUT_XLSX} | filas: {len(df)}")
        print(f"🏁 Finalizado. Histórico insertado/actualizado: {len(rows)} filas para {TIENDA_NOMBRE} ({capturado_en})")

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
