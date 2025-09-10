#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper Supermercado Pingüino -> Excel

Este script descarga el listado de productos disponibles en la web del
supermercado Pingüino y los exporta a un archivo Excel. A diferencia
de versiones previas, se han aplicado varias mejoras solicitadas:

* El precio de lista y el precio de oferta ya no se gestionan por
  separado. El valor de ambos campos será idéntico, tomando como
  referencia el precio final encontrado para el producto. De este
  modo, no se distinguen ofertas de precios de lista.

* Los precios que no contienen separadores decimales se transforman
  automáticamente considerando que los dos últimos dígitos corresponden
  a los centavos. Por ejemplo, "12345" se interpreta como 123,45.
  También se aplican dos decimales en el texto de precio para una
  mejor legibilidad.

* Además de los identificadores numéricos, ahora se incluyen en el
  resultado los nombres de las categorías y subcategorías. El
  DataFrame exportado incorpora las columnas `categoria_id`,
  `categoria_nombre`, `subcategoria_id` y `subcategoria_nombre` para
  facilitar el análisis.

Notas generales:

* El sitio no expone una API JSON; se parsea directamente el HTML.
* Se requieren cookies de sucursal/ciudad para que aparezcan los
  productos. Las cookies se configuran automáticamente en la sesión.
* Los selectores CSS se han diseñado de manera tolerante; si el
  sitio cambia su estructura deberán ajustarse en consecuencia.
* Para depurar el parseo de productos se puede habilitar la opción
  `--debug-html` para guardar el HTML descargado de cada sección.
"""

import re
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Base de la web
BASE = "https://www.pinguino.com.ar"
# Rutas utilizadas para recuperar departamentos, categorías y productos
INDEX = f"{BASE}/web/index.r"
MENU_CAT = f"{BASE}/web/menuCat.r"
PROD = f"{BASE}/web/productos.r"

# Cabecera User-Agent que imitamos para reducir bloqueos
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

def new_session() -> requests.Session:
    """
    Crea una nueva sesión HTTP configurada para interactuar con el sitio.
    Añade cabeceras que imitan un navegador real, configura reintentos
    automáticos ante ciertos códigos de error y establece las cookies
    mínimas necesarias para que se muestren los productos.
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        # Algunos recursos se cargan vía AJAX; esta cabecera ayuda a evitar
        # respuestas vacías o redireccionamientos no deseados.
        "X-Requested-With": "XMLHttpRequest",
        "Referer": INDEX,
    })
    retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    # Cookies de ciudad y sucursal necesarias para visualizar productos
    s.cookies.set("ciudad", "1", domain="www.pinguino.com.ar", path="/")
    s.cookies.set("sucursal", "4", domain="www.pinguino.com.ar", path="/")
    # Precalentar sesión (ignora posibles fallos)
    try:
        s.get(INDEX, timeout=20)
    except requests.RequestException:
        pass
    return s


def tidy_space(txt: str) -> str:
    """Normaliza espacios en un texto eliminando saltos de línea y espacios duplicados."""
    return re.sub(r"\s+", " ", txt or "").strip()


def parse_price_value(val: Any) -> Optional[float]:
    """
    Convierte distintos formatos numéricos a un float con dos decimales.

    Este helper acepta entradas como:

    * "1.234,56" → 1234.56
    * "1,234.56" → 1234.56
    * "1234,56"   → 1234.56
    * "1234.56"   → 1234.56
    * "12345"     → 123.45  (dos últimos dígitos como centavos)

    Se eliminan los separadores de miles y se identifica correctamente
    el carácter decimal. Si no se encuentra ningún separador, se asumen
    centavos en los dos últimos dígitos. Si la conversión falla se
    devuelve None.
    """
    if val is None:
        return None
    s = str(val)
    if not s:
        return None
    # Quitar espacios y separadores finos
    s = s.strip().replace("\u202f", "").replace(" ", "")
    if not s:
        return None
    # Contar separadores
    comma_count = s.count(',')
    dot_count = s.count('.')
    # Si hay al menos un separador decimal
    if comma_count or dot_count:
        # Determinar cuál es el separador decimal y cuál el de miles
        dec_sep = None
        thou_sep = None
        if comma_count and dot_count:
            # Ambos presentes: el separador decimal suele ser el que aparece más a la derecha
            if s.rfind(',') > s.rfind('.'):
                dec_sep, thou_sep = ',', '.'
            else:
                dec_sep, thou_sep = '.', ','
        elif comma_count:
            # Solo comas: si hay una sola y la parte decimal tiene ≤2 dígitos, es decimal
            parts = s.split(',')
            if comma_count == 1 and len(parts[-1]) <= 2:
                dec_sep, thou_sep = ',', '.'
            else:
                # Múltiples comas: asumimos que la última es decimal
                dec_sep, thou_sep = ',', ','
        elif dot_count:
            # Solo puntos: mismo criterio que para comas
            parts = s.split('.')
            if dot_count == 1 and len(parts[-1]) <= 2:
                dec_sep, thou_sep = '.', ','
            else:
                dec_sep, thou_sep = '.', '.'
        # Normalizar el número: eliminar separadores de miles y sustituir dec_sep por '.'
        normalized = s
        if thou_sep and thou_sep != dec_sep:
            normalized = normalized.replace(thou_sep, '')
        # Al eliminar miles, puede quedar el separador decimal; reemplazarlo por '.'
        if dec_sep:
            normalized = normalized.replace(dec_sep, '.')
        # Eliminar cualquier otro separador repetido (caso dec_sep == thou_sep)
        # Por ejemplo: '1,234,56' donde tanto la coma es miles y decimal: eliminar todas menos la última
        if dec_sep and dec_sep == thou_sep:
            # Encontrar la última posición
            last = normalized.rfind('.')
            if last != -1:
                # Quitar todas las apariciones excepto la última
                normalized = normalized.replace('.', '')
                # Insertar el punto decimal en la posición original
                normalized = normalized[:last] + '.' + normalized[last:]
        try:
            return round(float(normalized), 2)
        except ValueError:
            return None
    # Si no hay separadores y todos son dígitos, asumir centavos
    if s.isdigit():
        if len(s) == 1:
            return round(float('0.0' + s), 2)
        if len(s) == 2:
            return round(float('0.' + s), 2)
        entero, dec = s[:-2], s[-2:]
        try:
            return round(float(f"{entero}.{dec}"), 2)
        except ValueError:
            return None
    return None


def parse_price(text: str) -> Optional[float]:
    """
    Extrae un número decimal de un texto que contenga un precio. Se
    admiten pesos con separador de miles y decimal (coma o punto). Cuando
    no hay separador decimal se interpreta que los dos últimos dígitos
    corresponden a los centavos (por ejemplo "12345" se interpreta como
    123,45). Si no se encuentra ningún valor numérico se devuelve None.
    """
    if not text:
        return None
    # Expresión para capturar importes con o sin símbolo de moneda
    money_re = re.compile(
        r"(?:\$|\bARS\b|\bAR\$?\b)?\s*([0-9]{1,3}(?:[.\s][0-9]{3})*(?:,[0-9]{1,2})|[0-9]+(?:\.[0-9]{1,2})?|[0-9]+)"
    )
    m = money_re.search(text.replace("\xa0", " "))
    if not m:
        return None
    num = m.group(1)
    # Intentar convertir el número capturado con el helper mejorado
    val = parse_price_value(num)
    if val is not None:
        return val
    # Fallback: limpieza básica (mantener compatibilidad con formatos poco comunes)
    cleaned = num.replace(" ", "").replace("\u202f", "")
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return round(float(cleaned), 2)
    except ValueError:
        return None


def get_departments_details(session: requests.Session) -> List[Dict[str, Any]]:
    """
    Extrae la lista de departamentos disponibles en la página de inicio.
    Cada departamento incluye su identificador (`id`) y su nombre (`nombre`).

    Devuelve una lista de diccionarios con las claves `id` y `nombre`.
    Si no se encuentran departamentos se devuelve una lista vacía. En caso
    de error de red se captura la excepción y se devuelve una lista vacía.
    """
    deps: List[Dict[str, Any]] = []
    try:
        r = session.get(INDEX, timeout=30)
        if not r.ok:
            return deps
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select(".dpto a[data-d]"):
            dep_id = a.get("data-d")
            try:
                dep_id_int = int(dep_id)
            except (TypeError, ValueError):
                continue
            name = tidy_space(a.get_text()) if a.get_text() else str(dep_id_int)
            deps.append({"id": dep_id_int, "nombre": name})
        # Eliminamos duplicados manteniendo orden
        seen: set[int] = set()
        uniq: List[Dict[str, Any]] = []
        for d in deps:
            if d["id"] not in seen:
                seen.add(d["id"])
                uniq.append(d)
        return uniq
    except requests.RequestException:
        return deps


def get_categories_details(session: requests.Session, dep_id: int) -> List[Dict[str, Any]]:
    """
    Para un departamento concreto, intenta obtener las categorías disponibles.
    Devuelve una lista de diccionarios con claves `id` y `nombre`.
    Si no se encuentra ninguna categoría se devuelve una lista vacía.
    En caso de error de red la excepción se captura y se devuelve una lista vacía.
    """
    cats: List[Dict[str, Any]] = []
    try:
        r = session.get(MENU_CAT, params={"dep": str(dep_id)}, timeout=30)
        if not r.ok:
            return cats
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a[data-c]"):
            cat_id = a.get("data-c")
            try:
                cat_id_int = int(cat_id)
            except (TypeError, ValueError):
                continue
            name = tidy_space(a.get_text()) if a.get_text() else str(cat_id_int)
            cats.append({"id": cat_id_int, "nombre": name})
        # Eliminar duplicados conservando orden
        seen: set[int] = set()
        uniq: List[Dict[str, Any]] = []
        for c in cats:
            if c["id"] not in seen:
                seen.add(c["id"])
                uniq.append(c)
        return uniq
    except requests.RequestException:
        return cats


def parse_product_cards_enriched(
    html: str,
    dep_id: int,
    cat_id: Optional[int] = None,
    dep_name: Optional[str] = None,
    cat_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Analiza el HTML de la grilla de productos y extrae una lista de
    diccionarios con información enriquecida por producto. La extracción
    incluye ean, título, precio final (sin distinguir entre lista y oferta),
    tipo de descuento (si puede inferirse), categoría y subcategoría (id y
    nombre), URL de detalle, imagen y código interno (PLU).

    * `dep_id` y `cat_id` indican respectivamente el identificador del
      departamento y de la subcategoría.
    * `dep_name` y `cat_name` son opcionales y corresponden a los nombres
      humanos del departamento y la subcategoría. Si se proporcionan se
      incluirán en los resultados, en caso contrario se dejarán en blanco.
    """
    soup = BeautifulSoup(html, "html.parser")
    # Seleccionar tarjetas de producto de forma robusta
    cards: List[BeautifulSoup] = list(soup.select('[id^="prod-"]'))
    if not cards:
        cards = soup.select(
            ".item-prod, .producto, .prod, .card, .item, .row .col-12"
        )
    # Añadir nodos que contengan data-pre para no perder productos
    for node in soup.select('[data-pre]'):
        if node not in cards:
            cards.append(node)
    products: List[Dict[str, Any]] = []
    for node in cards:
        # Identificador interno (PLU)
        plu: Optional[str] = None
        node_id = node.get("id")
        if node_id and node_id.startswith("prod-"):
            plu = node_id.split("-", 1)[-1].strip()
        # EAN o código de barras
        ean: Optional[str] = None
        for key in ["data-ean", "data-ean13", "data-barcode", "data-bar"]:
            val = node.get(key)
            if val:
                ean = val.strip()
                break
        # Lectura de precios desde atributos de datos
        data_prelista = node.get("data-prelista") or node.get("data-precio")
        data_preofe = node.get("data-preofe") or node.get("data-oferta")
        data_pre = node.get("data-pre")
        precio: Optional[float] = None
        # Se intenta extraer del atributo de oferta, luego del genérico y por último del de lista
        for raw_val in [data_preofe, data_pre, data_prelista]:
            p = parse_price_value(raw_val)
            if p is not None:
                precio = p
                break
        # En caso de no haber encontrado precio en atributos, explorar textos
        if precio is None:
            # Buscar valores numéricos en nodos identificados con clases de precio
            for sel in [
                '[class*="precio"]', '[class*="price"]',
                'span', 'div',
            ]:
                price_node = node.select_one(sel)
                if price_node:
                    candidate_price = parse_price(price_node.get_text(" "))
                    if candidate_price is not None:
                        precio = candidate_price
                        break
        # Como último recurso, explorar todo el texto de la tarjeta
        if precio is None:
            candidate_price = parse_price(node.get_text(" "))
            precio = candidate_price
        # Si hemos obtenido un valor, redondeamos a dos decimales
        if precio is not None:
            precio = round(float(precio), 2)
        # Construir el texto de precio
        if precio is not None:
            precio_texto = f"{precio:.2f}"
        else:
            precio_texto = ""
        # Imagen y candidatos a título
        img: Optional[str] = None
        title_candidates: List[str] = []
        data_img = node.get("data-img")
        if data_img:
            img = (
                data_img
                if data_img.startswith("http")
                else (BASE + data_img if data_img.startswith("/") else data_img)
            )
        img_node = node.select_one("img[src]")
        if not img and img_node:
            alt = img_node.get("alt")
            if alt:
                title_candidates.append(tidy_space(alt))
            src = img_node.get("src")
            if src:
                img = src if not src.startswith("/") else (BASE + src)
        # Títulos potenciales en atributos
        data_des = node.get("data-des") or node.get("data-name")
        if data_des:
            title_candidates.append(tidy_space(str(data_des)))
        for a_tag in node.select("a[title]"):
            t = a_tag.get("title")
            if t:
                title_candidates.append(tidy_space(t))
        for sel in [
            'h1', 'h2', 'h3', 'h4', 'h5',
            '[class*="tit"][class!="precio"]',
            '[class*="desc"]',
        ]:
            tag = node.select_one(sel)
            if tag:
                text = tidy_space(tag.get_text(strip=True))
                if text:
                    title_candidates.append(text)
        # Seleccionar un título válido descartando textos con precios o frases
        title: Optional[str] = None
        price_pattern = re.compile(r"\$\s*\d")
        for cand in title_candidates:
            if price_pattern.search(cand):
                continue
            lower = cand.lower()
            if "carrito" in lower or "agreg" in lower:
                continue
            title = cand
            break
        # Si no se encontró un título fiable, limpiar el texto completo
        if not title:
            raw_text = tidy_space(node.get_text(" "))
            if precio_texto:
                raw_text = raw_text.replace(precio_texto, "")
            # Eliminar importes y textos de acciones
            raw_text = re.sub(r"\$\s*[0-9]+(?:[.,][0-9]+)*(?:\s*[a-zA-Z]|)", "", raw_text)
            raw_text = re.sub(r"agregaste.*", "", raw_text, flags=re.IGNORECASE)
            raw_text = re.sub(r"agregar.*", "", raw_text, flags=re.IGNORECASE)
            raw_text = re.sub(r"\+\s*-", "", raw_text)
            cleaned = tidy_space(raw_text)
            if len(cleaned) > 180:
                cleaned = cleaned[:177] + "..."
            title = cleaned
        # URL de detalle
        url: Optional[str] = None
        data_href = node.get("data-href")
        if data_href:
            url = (
                data_href
                if data_href.startswith("http")
                else (BASE + data_href if data_href.startswith("/") else data_href)
            )
        if not url:
            for a_tag in node.select("a[href]"):
                href = a_tag.get("href")
                if not href:
                    continue
                href_l = href.lower()
                if href_l == "#" or "javascript" in href_l:
                    continue
                if any(tok in href_l for tok in ["agregar", "addcart", "accioncarrito", "ticket"]):
                    continue
                url = href if href.startswith("http") else (BASE + href if href.startswith("/") else href)
                break
        # Tipo de descuento: no distinguimos lista vs oferta, pero podemos inferir promociones
        tipo_descuento: Optional[str] = None
        if precio is not None:
            # Revisar si existen indicadores de promoción en el texto
            texto_inferior = node.get_text(" ").lower()
            if "x" in texto_inferior and "%" not in texto_inferior:
                m = re.search(r"(\d+)\s*x\s*(\d+)", texto_inferior)
                if m:
                    tipo_descuento = f"{m.group(1)}x{m.group(2)}"
            elif "%" in texto_inferior:
                m = re.search(r"(\d+)%", texto_inferior)
                if m:
                    tipo_descuento = f"{m.group(1)}%"
        products.append({
            "ean": ean,
            "titulo": title or "",
            # Como se solicitó, precio_lista y precio_oferta son idénticos
            "precio_lista": precio,
            "precio_oferta": precio,
            "tipo_descuento": tipo_descuento,
            "categoria_id": dep_id,
            "categoria_nombre": dep_name,
            "subcategoria_id": cat_id,
            "subcategoria_nombre": cat_name,
            "url": url,
            "imagen": img,
            "plu": plu,
            "precio_texto": precio_texto,
        })
    # Filtrar productos sin título ni precio
    return [p for p in products if p["titulo"] or (p["precio_oferta"] is not None or p["precio_lista"] is not None)]


def fetch_products_by_dep(
    session: requests.Session,
    dep_id: int,
    dep_name: Optional[str] = None,
    cat_id: Optional[int] = None,
    cat_name: Optional[str] = None,
    page: Optional[int] = None,
    save_debug: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """
    Solicita la lista de productos para un departamento específico y, opcionalmente,
    para una subcategoría concreta. Algunos sitios soportan paginación; se deja
    el parámetro `page` para posibles usos futuros. Devuelve una lista de
    diccionarios con la información de cada producto.

    * `dep_name` y `cat_name` pueden suministrarse para incluir los nombres
      de categoría y subcategoría en el resultado.
    """
    params = {"dep": str(dep_id)}
    if cat_id is not None:
        params["cat"] = str(cat_id)
    if page is not None:
        params["pag"] = str(page)
    r = session.get(PROD, params=params, timeout=40)
    r.raise_for_status()
    html = r.text
    if save_debug:
        save_debug.write_text(html, encoding="utf-8")
    # Utiliza la versión enriquecida del parser pasando los nombres de las categorías
    return parse_product_cards_enriched(html, dep_id, cat_id, dep_name, cat_name)


def main():
    """Función principal: gestiona argumentos, recorre categorías y exporta a Excel."""
    ap = argparse.ArgumentParser(description="Scraper de productos (Pingüino) → Excel")
    ap.add_argument("--out", default="Productos_Pinguino_abc.xlsx", help="Archivo XLSX de salida")
    ap.add_argument("--sleep", type=float, default=1.2, help="Espera (seg) entre deptos")
    ap.add_argument("--only-ofertas", action="store_true", help="Solo ofertas (ofe=1)")
    ap.add_argument("--debug-html", action="store_true", help="Guardar HTML por depto en ./_html")
    args = ap.parse_args()

    s = new_session()

    rows: List[Dict[str, Any]] = []
    html_dir = Path("_html")
    if args.debug_html:
        html_dir.mkdir(exist_ok=True)

    if args.only_ofertas:
        # Descarga productos en oferta globalmente
        r = s.get(PROD, params={"ofe": "1"}, timeout=40)
        r.raise_for_status()
        if args.debug_html:
            (html_dir / "ofertas.html").write_text(r.text, encoding="utf-8")
        # Se utiliza 999 como identificador genérico cuando no hay departamento concreto
        rows.extend(parse_product_cards_enriched(r.text, 999))
    else:
        # Obtener departamentos con nombres
        deps = get_departments_details(s)
        if not deps:
            print("No se encontraron departamentos; saliendo.")
            return
        for dep in deps:
            dep_id = dep["id"]
            dep_name = dep.get("nombre")
            try:
                # Productos del departamento sin filtros
                debug_path = (html_dir / f"dep_{dep_id}.html") if args.debug_html else None
                prods = fetch_products_by_dep(s, dep_id, dep_name=dep_name, save_debug=debug_path)
                print(f"[dep {dep_id}] productos: {len(prods)} (dep)")
                rows.extend(prods)
                # Productos de las subcategorías del departamento
                cats = get_categories_details(s, dep_id)
                for cat in cats:
                    cat_id = cat["id"]
                    cat_name = cat.get("nombre")
                    try:
                        cat_debug = (html_dir / f"dep_{dep_id}_cat_{cat_id}.html") if args.debug_html else None
                        cat_prods = fetch_products_by_dep(
                            s,
                            dep_id,
                            dep_name=dep_name,
                            cat_id=cat_id,
                            cat_name=cat_name,
                            save_debug=cat_debug,
                        )
                        print(f"[dep {dep_id} cat {cat_id}] productos: {len(cat_prods)}")
                        rows.extend(cat_prods)
                        time.sleep(args.sleep)
                    except requests.RequestException as e:
                        print(f"[dep {dep_id} cat {cat_id}] error: {e}")
                        continue
                time.sleep(args.sleep)
            except requests.RequestException as e:
                print(f"[dep {dep_id}] error: {e}")
                continue

    if not rows:
        print("No se extrajo ningún producto. Revisa cookies/sucursal o ajusta selectores.")
        return

    # Construir DataFrame
    df = pd.DataFrame(rows)
    # Columnas para el DataFrame en orden explícito
    cols = [
        "ean", "titulo", "precio_lista", "precio_oferta", "tipo_descuento",
        "categoria_id", "categoria_nombre", "subcategoria_id", "subcategoria_nombre",
        "url", "imagen", "plu", "precio_texto",
    ]
    df = df.reindex(columns=cols)
    # Elimina duplicados por EAN y título para evitar repeticiones
    df = df.drop_duplicates(subset=["ean", "titulo"], keep="first")
    out_path = Path(args.out)
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="Productos")
    print(f"✅ Exportado: {out_path.resolve()} (filas: {len(df)})")
    print("Si falta información, activa --debug-html y ajusta los selectores en parse_product_cards_enriched().")


if __name__ == "__main__":
    main()