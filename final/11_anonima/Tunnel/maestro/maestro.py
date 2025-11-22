#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
La Anónima (Selecta) – Detalle por URL → Excel

- Lee un Excel con columnas:
    - 'URLs'
    - 'Cód.Barras'
- Para cada URL:
    - Descarga el HTML
    - Extrae datos desde la estructura de detalle de producto
    - Incluye una columna 'ean' = valor de 'Cód.Barras' de esa fila
- Guarda un nuevo Excel con toda la info.

Requisitos:
    pip install requests pandas beautifulsoup4 openpyxl
"""

import time
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, Any, Optional

# ==========================
# Configuración básica
# ==========================

INPUT_XLSX  = "1_la_anonima.xlsx"        # archivo de entrada
OUTPUT_XLSX = "1_la_anonima_detalle.xlsx"  # archivo de salida

TIMEOUT = 20          # segundos de timeout por request
SLEEP_BETWEEN = 0.7   # pausa entre requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
}


# ==========================
# Helpers de scraping
# ==========================

def fetch_soup(url: str) -> Optional[BeautifulSoup]:
    """Descarga una URL y devuelve BeautifulSoup, o None si falla."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] Al descargar {url}: {e}")
        return None


def clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def parse_precio_span(precio_div: Optional[BeautifulSoup]) -> str:
    """
    Parsea el precio visual:
      <div class="precio plus">
          <span>$ 2.350<span class="decimal">,00</span></span>
      </div>
    Devuelve algo como '2350.00'
    """
    if not precio_div:
        return ""

    span = precio_div.find("span")
    if not span:
        return ""

    txt = "".join(span.stripped_strings)
    # Ej: "$ 2.350,00" → "2350.00"
    txt = txt.replace("$", "").replace(".", "").replace(" ", "")
    # ahora debería ser "2350,00" o "2350"
    txt = txt.replace(",", ".")
    return txt.strip()


def parse_product(url: str, ean_hint: Any) -> Dict[str, Any]:
    """
    Parsea un producto de La Anónima dado su URL.
    ean_hint = valor de 'Cód.Barras' del Excel, se usa como 'ean'.
    """
    soup = fetch_soup(url)
    if soup is None:
        return {
            "url": url,
            "ean": ean_hint,
            "codigo_barras_input": ean_hint,
            "nombre": "",
            "marca": "",
            "codigo_interno": "",
            "categorias_raw": "",
            "categoria": "",
            "subcategoria": "",
            "precio_lista": "",
            "precio_oferta": "",
            "tipo_oferta": "",
            "precio_referencia": "",
            "precio_sin_impuestos": "",
            "descripcion": "",
        }

    # ---------- Nombre ----------
    nombre = ""
    h1 = soup.find("h1", class_="titulo")
    if h1:
        nombre = clean_text(h1.get_text())

    # ---------- Botón agregar (contiene muchos datos en data-*) ----------
    btn = soup.find("button", id="btnAgregarCarrito")
    marca = ""
    codigo_interno = ""
    categorias_raw = ""
    categoria = ""
    subcategoria = ""

    precio_lista = ""
    precio_oferta = ""
    tipo_oferta = ""

    if btn:
        marca = clean_text(btn.get("data-marca", ""))
        codigo_interno = clean_text(btn.get("data-codigo", ""))  # ej: 3004211
        categorias_raw = clean_text(btn.get("data-rutacategorias", ""))  # "Frescos  > Lácteos  > Leches"

        # Split de categorías
        if categorias_raw:
            partes = [p.strip() for p in categorias_raw.split(">") if p.strip()]
            if partes:
                categoria = partes[0]
                subcategoria = partes[-1]
            else:
                categoria = ""
                subcategoria = ""

        # Precios desde data-*
        precio_raw = btn.get("data-precio")           # actual
        precio_anterior_raw = btn.get("data-precio_anterior")  # puede ser igual
        precio_oferta_raw = btn.get("data-precio_oferta")

        # Normalizar a string simple (sin decimales)
        def norm_price(v):
            if v is None:
                return ""
            # vienen como "2350" => interpretamos como "2350"
            v = str(v).strip()
            return v

        precio_lista = norm_price(precio_anterior_raw or precio_raw)
        precio_oferta = norm_price(precio_oferta_raw or "")

        # Tipo de oferta: PLUS, 2x1, etc.
        tipo_oferta = ""
        # Buscar clases 'promocion...' en cucardas-top
        cucardas = soup.find("div", class_="cucardas-top")
        if cucardas:
            promo_span = cucardas.find("span", class_=re.compile("promocion"))
            if promo_span and promo_span.get("class"):
                # Ejemplo: ['position', 'icono', 'promocion-plus', 'promocion2x1']
                clases = promo_span.get("class")
                # nos quedamos con las que empiezan por 'promocion'
                promos = [c for c in clases if c.startswith("promocion")]
                tipo_oferta = ", ".join(promos)

        # Si no hay tipo_oferta, pero hay 'plus' en el div.precio
        precio_div = soup.find("div", class_=re.compile(r"\bprecio\b"))
        if not tipo_oferta and precio_div and "plus" in (precio_div.get("class") or []):
            tipo_oferta = "promocion-plus"

    # ---------- Precio visual (como se ve en pantalla) ----------
    precio_div_visual = soup.find("div", class_=re.compile(r"\bprecio\b"))
    precio_visual = parse_precio_span(precio_div_visual)

    # Si no tenemos lista/oferta desde data-*, usamos el visual como lista
    if not precio_lista and precio_visual:
        precio_lista = precio_visual

    # ---------- Precio de referencia (por litro, kg, etc.) ----------
    precio_referencia_div = soup.find("div", class_="leyenda-precio-referencia")
    precio_referencia = ""
    if precio_referencia_div:
        precio_referencia = clean_text(precio_referencia_div.get_text())

    # ---------- Precio sin impuestos ----------
    precio_sin_imp_div = soup.find("div", class_="impuestos-nacionales")
    precio_sin_impuestos = ""
    if precio_sin_imp_div:
        precio_sin_impuestos = clean_text(precio_sin_imp_div.get_text())

    # ---------- Descripción ----------
    desc_div = soup.find("div", id="contenedor-descripcion")
    descripcion = ""
    if desc_div:
        descripcion = clean_text(desc_div.get_text())

    # Construimos el dict final
    return {
        "url": url,
        "ean": ean_hint,                # lo que pediste: viene del Excel
        "codigo_barras_input": ean_hint,  # por si quieres ver el original
        "nombre": nombre,
        "marca": marca,
        "codigo_interno": codigo_interno,
        "categorias_raw": categorias_raw,
        "categoria": categoria,
        "subcategoria": subcategoria,
        "precio_lista": precio_lista,
        "precio_oferta": precio_oferta,
        "tipo_oferta": tipo_oferta,
        "precio_visual": precio_visual,
        "precio_referencia": precio_referencia,
        "precio_sin_impuestos": precio_sin_impuestos,
        "descripcion": descripcion,
    }


# ==========================
# MAIN
# ==========================

def main():
    # Leer Excel de entrada
    try:
        df_in = pd.read_excel(INPUT_XLSX)
    except FileNotFoundError:
        print(f"[ERROR] No se encontró el archivo: {INPUT_XLSX}")
        print("Asegúrate de ejecutarlo en la misma carpeta o usa una ruta absoluta.")
        return

    # Verificar columnas requeridas
    for col in ["URLs", "Cód.Barras"]:
        if col not in df_in.columns:
            print(f"[ERROR] No se encontró la columna '{col}' en el Excel.")
            print("Columnas disponibles:", list(df_in.columns))
            return

    filas = []
    for idx, row in df_in.iterrows():
        url = str(row["URLs"]).strip()
        cod_barras = row["Cód.Barras"]

        if not url or url.lower() == "nan":
            print(f"[AVISO] Fila {idx}: URL vacía, se salta.")
            continue

        print(f"[{idx+1}/{len(df_in)}] Procesando: {url}")
        info = parse_product(url, cod_barras)
        filas.append(info)

        time.sleep(SLEEP_BETWEEN)

    if not filas:
        print("[AVISO] No se obtuvo ninguna fila de salida.")
        return

    df_out = pd.DataFrame(filas)

    # Guardar Excel de salida
    df_out.to_excel(OUTPUT_XLSX, index=False)
    print(f"[OK] Archivo generado: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
