#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import html
from typing import List, Tuple
import requests
import pandas as pd

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None  # limpieza HTML caerá en regex si no está bs4

BASE = "https://ofertas.lacteoselpuente.com.ar"
ENDPOINT = "/productos/get/{id}"

# Rubros principales visibles en la página
RUBROS = {
    1: "Quesos Blandos",
    2: "Quesos semiduros",
    3: "Quesos duros",
    4: "Tablas y picadas",
    5: "Lacteos",
    6: "Dulces",
    7: "Marca propia",
    12: "Otros Productos",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

# RegEx de precio y detección de líneas con precio
RE_PRICE_CAPTURE = re.compile(r"\$\s*([0-9.\s]+,\d{2})")
RE_HAS_PRICE     = re.compile(r"\$\s*\d")

# Limpia HTML → texto plano
def clean_html_text(s: str) -> str:
    if not s:
        return ""
    # 1) si hay BeautifulSoup, úsalo (mejor)
    if BeautifulSoup is not None:
        s = BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    else:
        # 2) fallback en regex si no hay bs4
        s = re.sub(r"<[^>]+>", " ", s)
    # 3) des-escapar entidades HTML
    s = html.unescape(s)
    # 4) normalizar espacios y NBSP
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def fetch_text(rubro_id: int, session: requests.Session) -> str:
    """Descarga el texto plano (o HTML) del rubro/subrubro."""
    url = f"{BASE}{ENDPOINT.format(id=rubro_id)}"
    r = session.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text

def split_nombre_presentacion(desc: str) -> Tuple[str, str]:
    """
    Separa 'desc' en (nombre, presentacion).
    Reglas:
      - Si hay punto, lo anterior al primer '.' es el nombre.
      - Si no, cortar antes de ' x ' o ' por '.
      - Limpieza de prefijos tipo 'Valor ...' en la presentación.
    """
    original = clean_html_text(desc)

    if "." in original:
        nombre, resto = original.split(".", 1)
        nombre = nombre.strip()
        presentacion = resto.strip()
    else:
        m = re.search(r"\s(x|por)\s", original, flags=re.IGNORECASE)
        if m:
            nombre = original[:m.start()].strip()
            presentacion = original[m.start():].strip()
        else:
            nombre = original
            presentacion = ""

    # Limpiezas
    presentacion = re.sub(r"^\.*\s*", "", presentacion)
    presentacion = re.sub(r"^\b[Vv]alor\b[:\s]*", "", presentacion)
    presentacion = re.sub(r"\s{2,}", " ", presentacion).strip()

    if not nombre:
        nombre = original

    return nombre, presentacion

def parse_text_to_rows(rubro_nombre: str, text: str) -> List[Tuple[str, str, str, str, str]]:
    """
    Convierte el bloque de texto/HTML en filas:
      (categoria, subcategoria, nombre, presentacion, precio)
    - '##### Título' define subcategoría.
    - Si descripción y precio vienen en líneas separadas, se unen.
    """
    rows: List[Tuple[str, str, str, str, str]] = []
    subcat = rubro_nombre
    carry_desc: str | None = None

    # dividir por líneas y limpiar HTML en cada línea
    for raw in text.splitlines():
        line = clean_html_text(raw)
        if not line:
            continue

        # Titulares de subcategoría tipo "#####"
        if line.startswith("#"):
            sub = line.lstrip("#").strip().rstrip(".")
            if sub:
                subcat = sub
            continue

        if RE_HAS_PRICE.search(line):
            # Extraer precio
            m = RE_PRICE_CAPTURE.search(line)
            precio = m.group(1).strip() if m else ""

            # Descripción antes del '$'
            before = line.split("$", 1)[0].strip()
            if carry_desc and before and before.lower() not in {"$", ""}:
                desc = f"{carry_desc} {before}".strip()
            elif carry_desc:
                desc = carry_desc.strip()
            else:
                desc = before

            # nombre/presentación limpios (sin HTML)
            nombre, presentacion = split_nombre_presentacion(desc)

            rows.append(("lacteos", subcat, nombre, presentacion, precio))
            carry_desc = None
        else:
            # Acumular posible descripción (a veces el precio viene en la siguiente línea)
            carry_desc = f"{carry_desc} {line}".strip() if carry_desc else line

    return rows

def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    all_rows: List[Tuple[str, str, str, str, str]] = []
    for rid, rubro_nombre in RUBROS.items():
        try:
            txt = fetch_text(rid, session)
        except requests.HTTPError as e:
            print(f"[WARN] {rid} {rubro_nombre}: {e}")
            continue

        rows = parse_text_to_rows(rubro_nombre, txt)
        print(f"{rid:>3} {rubro_nombre:<20} -> {len(rows)} productos")
        all_rows.extend(rows)
        time.sleep(0.7)  # cortesía

    df = pd.DataFrame(
        all_rows,
        columns=["categoria", "subcategoria", "nombre", "presentacion", "precio"],
    )

    # pulidos finales
    df["nombre"] = df["nombre"].str.replace(r"\s{2,}", " ", regex=True).str.strip()
    df["presentacion"] = df["presentacion"].str.replace(r"\s{2,}", " ", regex=True).str.strip()

    # exportar
    out = "Listado_ElPuente.xlsx"
    df.to_excel(out, index=False)
    print(f"OK -> {out} ({len(df)} filas)")

if __name__ == "__main__":
    main()
