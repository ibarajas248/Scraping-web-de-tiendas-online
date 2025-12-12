#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

INPUT_FILE = "cordiez_aux_viernes.xlsx"
OUTPUT_FILE = "cordiez_precios_viernes.xlsx"


def limpiar_precio_ar(texto: str):
    """
    Convierte algo como '$ 4.619,00' o '$ 1.460' en float (4619.00 o 1460.00)

    Reglas:
      - '.' se usa como separador de miles → se elimina
      - ',' se usa como separador decimal → se reemplaza por '.'
    """
    if not texto:
        return None

    # Dejar solo dígitos, puntos y comas
    txt = re.sub(r"[^\d\.,]", "", texto)
    if not txt:
        return None

    # Si tiene parte decimal con coma
    if "," in txt:
        entero, frac = txt.split(",", 1)
        entero = entero.replace(".", "")  # quitar separadores de miles
        frac = re.sub(r"\D", "", frac)    # por si trae algo raro
        txt_norm = f"{entero}.{frac}"
    else:
        # Sin decimales: sólo quitar puntos de miles
        entero = txt.replace(".", "")
        txt_norm = entero

    try:
        return float(txt_norm)
    except ValueError:
        return None


def extraer_precios_desde_url(url: str):
    """
    Lógica específica Cordiez:

    Caso 1: SIN oferta
      PRECIO_LISTA = precio actual
      PRECIO_OFERTA = None
      TIPO_OFERTA   = None

    Caso 2: CON oferta
      PRECIO_LISTA  = precio de lista (tachado / productListPriceFrom)
      PRECIO_OFERTA = precio con descuento (productPriceFrom)
      TIPO_OFERTA   = porcentaje de descuento
    """
    if not isinstance(url, str) or not url.strip():
        return None, None, None

    try:
        resp = requests.get(url, timeout=20, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0 Safari/537.36"
        })
        if resp.status_code != 200:
            print(f"[WARN] {url} → status {resp.status_code}")
            return None, None, None

        html = resp.text

        # ==========================================================
        # 1) Intentar primero vía JSON de VTEX (forma actual Cordiez)
        #    vtex.events.addData({"productListPriceFrom":"6589",
        #                         "productPriceFrom":"4199", ...})
        # ==========================================================
        m_list = re.search(r'"productListPriceFrom":"?(\d+)"?', html)
        m_price = re.search(r'"productPriceFrom":"?(\d+)"?', html)

        if m_list and m_price:
            precio_base = float(m_list.group(1))   # 6589 -> 6589.0
            precio_oferta = float(m_price.group(1))  # 4199 -> 4199.0

            if precio_base > 0:
                if precio_base > precio_oferta:
                    # Hay oferta
                    tipo_oferta = round(
                        100 * (precio_base - precio_oferta) / precio_base, 2
                    )
                    return precio_base, precio_oferta, tipo_oferta
                else:
                    # Sin oferta (mismo precio o precio_oferta mayor/igual)
                    return precio_base, None, None

        # ==========================================================
        # 2) Fallback: usar skuJson_0 con precios formateados
        #    "listPriceFormated":"$ 6.589,00"
        #    "bestPriceFormated":"$ 4.199,00"
        # ==========================================================
        m_list_fmt = re.search(r'"listPriceFormated":"([^"]+)"', html)
        m_best_fmt = re.search(r'"bestPriceFormated":"([^"]+)"', html)

        if m_list_fmt and m_best_fmt:
            precio_base = limpiar_precio_ar(m_list_fmt.group(1))
            precio_oferta = limpiar_precio_ar(m_best_fmt.group(1))

            if precio_base is not None and precio_oferta is not None:
                if abs(precio_base - precio_oferta) < 0.01:
                    # Sin oferta visible
                    return precio_base, None, None
                else:
                    tipo_oferta = round(
                        100 * (precio_base - precio_oferta) / precio_base, 2
                    )
                    return precio_base, precio_oferta, tipo_oferta

        # ==========================================================
        # 3) Último fallback: tu HTML viejo (por si en algún lado sigue)
        # ==========================================================
        soup = BeautifulSoup(html, "lxml")

        # Buscar siempre el <p class="offer-price mb-1"> (precio grande)
        p_offer = soup.find("p", class_="offer-price mb-1")
        # Buscar el <span class="regular-price"> (precio tachado cuando hay oferta)
        span_regular = soup.find("span", class_="regular-price")

        # --- Caso 2: hay oferta (regular + offer) ---
        if span_regular and p_offer:
            texto_base = span_regular.get_text(strip=True)
            texto_oferta = p_offer.get_text(strip=True)

            precio_base = limpiar_precio_ar(texto_base)
            precio_oferta = limpiar_precio_ar(texto_oferta)

            if precio_base and precio_oferta and precio_base > 0:
                tipo_oferta = round(
                    100 * (precio_base - precio_oferta) / precio_base, 2
                )
            else:
                tipo_oferta = None

            return precio_base, precio_oferta, tipo_oferta

        # --- Caso 1: NO hay oferta (solo offer-price, pero es el precio base) ---
        if p_offer:
            texto_base = p_offer.get_text(strip=True)
            precio_base = limpiar_precio_ar(texto_base)
            return precio_base, None, None

        # Si no encontró nada, devolver Nones
        print(f"[WARN] {url} → no se encontraron precios en JSON ni HTML")
        return None, None, None

    except Exception as e:
        print(f"[ERROR] {url} → {e}")
        return None, None, None


def main():
    # Leer Excel
    df = pd.read_excel(INPUT_FILE)

    if "URLs" not in df.columns:
        raise ValueError(
            "No se encontró columna 'URLs'. "
            "Renombra tu columna o ajusta el nombre en el script."
        )

    # Asegurar columnas de salida (por si vienen vacías)
    for col in ["PRECIO_LISTA", "PRECIO_OFERTA", "TIPO_OFERTA"]:
        if col not in df.columns:
            df[col] = None

    precios_lista = []
    precios_oferta = []
    tipos_oferta = []

    total = len(df)
    for idx, row in df.iterrows():
        url = row["URLs"]
        print(f"[{idx+1}/{total}] Procesando: {url}")

        precio_base, precio_oferta, tipo_oferta = extraer_precios_desde_url(url)

        precios_lista.append(precio_base)
        precios_oferta.append(precio_oferta)
        tipos_oferta.append(tipo_oferta)

        # Pequeña pausa para no ser tan agresivos con el servidor
        time.sleep(1.0)

    # Actualizar DataFrame
    df["PRECIO_LISTA"] = precios_lista
    df["PRECIO_OFERTA"] = precios_oferta
    df["TIPO_OFERTA"] = tipos_oferta

    # Guardar a nuevo Excel
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Listo. Archivo exportado a: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
