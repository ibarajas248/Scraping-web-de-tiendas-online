#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
extract_pdf_products_cards_play_debug_structured_v2.py
------------------------------------------------------
Modo "play y ya" + DEBUG + extracción estructurada:

En cada tarjeta se intenta:
- Marca: línea mayormente MAYÚSCULAS (ej: COCINERO, FAVORITA, CONCO)
- Producto: el resto de líneas (incluye empaques tipo "Botella x 910 Gr.")
- Presentación: si hay una línea "corta" tipo "x 1.5 Lt." o "x 1 Kg." se guarda aparte,
  pero la DESCRIPCIÓN final siempre queda completa.

Salida Excel incluye:
- marca, producto, presentacion, codigo, descripcion (producto + presentacion)
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from PIL import Image

try:
    import cv2
except Exception as e:
    raise SystemExit("Falta opencv-python. Instala: pip install opencv-python") from e

try:
    import pytesseract
except Exception as e:
    raise SystemExit("Falta pytesseract. Instala: pip install pytesseract") from e

try:
    from pdf2image import convert_from_path
except Exception:
    convert_from_path = None


# =========================
# CONFIG (EDITA AQUÍ)
# =========================

PDF_FILENAME: Optional[str] = "INT-26.01-AL-30.01.pdf"
OUTPUT_XLSX: str = "productos_pdf.xlsx"

DPI: int = 350
OCR_LANG_CARD: str = "spa"  # prueba "spa+eng"

TESSERACT_CMD: Optional[str] = None  # r"C:\Program Files\Tesseract-OCR\tesseract.exe"
POPPLER_PATH: Optional[str] = None   # r"C:\poppler\Library\bin"

S_MIN: int = 80
V_MIN: int = 120
MIN_AREA: int = 700
MIN_W: int = 35
MIN_H: int = 18

TOP_PAD_RATIO: float = 0.10
INTER_PAD_RATIO: float = 0.01

MAX_PAGES: Optional[int] = None
PRINT_CARD_OCR_TEXT: bool = False


# =========================
# REGEX / HELPERS
# =========================

PRICE_ONLY_RE = re.compile(r"^\$?\s*\d{3,5}\s*$")

CODE_RE = re.compile(r"(?:c[oó]d(?:igo)?\.?\s*[:#]?\s*)([A-Za-z0-9\-]+)", re.IGNORECASE)

# unidades / tokens típicos
UNIT_RE = re.compile(r"\b(kg|k|g|gr|lt|l|ml|cc|un|u|uds|ud)\b", re.IGNORECASE)

# presentación típica "corta": "x 1.5 Lt.", "x 1 Kg.", "4 Un.", "70 Gr."
SHORT_PRESENTATION_RE = re.compile(
    r"^(?:x\s*)?\d+(?:[.,]\d+)?\s*(?:kg|k|g|gr|lt|l|ml|cc|un|u|uds|ud)\.?\s*$",
    re.IGNORECASE
)

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def to_float_price(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.replace(" ", "").replace("$", "")
    if "," in s:
        whole, dec = s.split(",", 1)
        whole = whole.replace(".", "")
        dec = re.sub(r"\D", "", dec)[:2].ljust(2, "0")
        num = f"{whole}.{dec}"
    else:
        num = s.replace(".", "")
    try:
        return float(num)
    except Exception:
        return None

def is_brand_line(line: str) -> bool:
    """
    Marca suele ser una sola palabra o 2, mayormente en MAYÚSCULAS, sin dígitos.
    Ej: COCINERO, FAVORITA, CONCO, SURRISAS
    """
    l = normalize_space(line)
    if not l or re.search(r"\d", l):
        return False
    # porcentaje de letras mayúsculas
    letters = re.findall(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", l)
    if not letters:
        return False
    upper_letters = [ch for ch in letters if ch.isupper()]
    ratio = len(upper_letters) / max(1, len(letters))

    # líneas muy largas no suelen ser marca
    if len(l) > 20:
        return False

    # si es MUY mayúscula, probablemente marca
    return ratio >= 0.80

def looks_like_short_presentation(line: str) -> bool:
    l = normalize_space(line)
    if not l:
        return False
    # ejemplo: "x 1.5 Lt." => quitamos "x" al inicio
    l2 = re.sub(r"^x\s*", "", l, flags=re.IGNORECASE).strip()
    # si es corta y encaja en patrón de unidad
    if len(l2) <= 18 and SHORT_PRESENTATION_RE.match(l2.replace(" ", "")) is None:
        # fallback: si tiene número+unidad y es corta, vale
        pass
    # mejor: regla robusta
    has_digit = bool(re.search(r"\d", l))
    has_unit = bool(UNIT_RE.search(l))
    return has_digit and has_unit and len(l) <= 22

def strip_price_tax_noise(lines: List[str]) -> List[str]:
    cleaned = []
    for l in lines:
        l = normalize_space(l)
        if not l:
            continue
        low = l.lower()
        if "s/imp" in low:
            continue
        if re.search(r"\$\s*\d{3,6}", l):
            continue
        cleaned.append(l)
    return cleaned

def extract_structured_fields_from_card_text(card_text: str) -> dict:
    """
    Devuelve:
      - marca
      - producto (sin marca, sin código)
      - presentacion (si se detecta una presentación corta)
      - codigo
      - descripcion = producto + presentacion (lo que quieres mostrar)
    """
    raw_lines = (card_text or "").splitlines()
    lines = strip_price_tax_noise(raw_lines)

    if not lines:
        return {"marca": "", "producto": "", "presentacion": "", "codigo": "", "descripcion": ""}

    # extraer código desde cualquier línea
    codigo = ""
    kept = []
    for l in lines:
        m = CODE_RE.search(l)
        if m and not codigo:
            codigo = m.group(1).strip()
            continue
        kept.append(l)
    lines = kept

    if not lines:
        return {"marca": "", "producto": "", "presentacion": "", "codigo": codigo, "descripcion": ""}

    # detectar marca: primera línea que parezca marca (idealmente la primera)
    marca = ""
    if is_brand_line(lines[0]):
        marca = lines[0]
        lines = lines[1:]
    else:
        # fallback: busca en primeras 2 líneas
        for i in range(min(2, len(lines))):
            if is_brand_line(lines[i]):
                marca = lines[i]
                lines = [l for j, l in enumerate(lines) if j != i]
                break

    # ahora lines contiene lo que debe ser producto + (posible) presentación + etc.
    # detectamos una presentación "corta" típica, pero OJO:
    # - si hay línea como "Botella x 910 Gr." NO la quitamos del producto
    # - solo quitamos presentaciones MUY cortas tipo "x 1.5 Lt."
    presentacion = ""
    if lines:
        # si la última es presentación corta, la tomamos
        if looks_like_short_presentation(lines[-1]) and len(lines[-1]) <= 22:
            presentacion = lines[-1]
            lines = lines[:-1]

    # producto = todo lo que queda (une varias líneas)
    producto = normalize_space(" ".join(lines)).strip()

    # limpiar si por OCR quedó marca pegada al inicio del producto
    if marca and producto.upper().startswith(marca.upper() + " "):
        producto = producto[len(marca) + 1 :].strip()

    descripcion = normalize_space((producto + " " + presentacion).strip())

    return {
        "marca": marca,
        "producto": producto,
        "presentacion": presentacion,
        "codigo": codigo,
        "descripcion": descripcion,
    }


@dataclass
class PriceBox:
    page: int
    x: int
    y: int
    w: int
    h: int
    price_text: str
    conf: float = 0.0

    @property
    def cx(self) -> float:
        return self.x + self.w / 2

    @property
    def cy(self) -> float:
        return self.y + self.h / 2


# =========================
# PDF -> IMÁGENES
# =========================

def images_from_pdf(pdf_path: Path, dpi: int, poppler_path: Optional[str]) -> List[Image.Image]:
    if convert_from_path is None:
        raise SystemExit("pdf2image no está disponible. Instala: pip install pdf2image (y Poppler).")
    kwargs = {"dpi": dpi}
    if poppler_path:
        kwargs["poppler_path"] = poppler_path

    print("[INFO] Renderizando PDF a imágenes...")
    t0 = time.time()
    imgs = convert_from_path(str(pdf_path), **kwargs)
    print(f"[INFO] Render completo: {len(imgs)} páginas en {time.time() - t0:.1f}s")
    return imgs


# =========================
# DETECTAR PRECIO ROJO
# =========================

def detect_red_price_boxes(page_bgr: np.ndarray,
                           s_min: int, v_min: int,
                           min_area: int, min_w: int, min_h: int) -> List[Tuple[int, int, int, int]]:
    hsv = cv2.cvtColor(page_bgr, cv2.COLOR_BGR2HSV)

    lower1 = np.array([0,   s_min, v_min], dtype=np.uint8)
    upper1 = np.array([10,  255,   255], dtype=np.uint8)
    lower2 = np.array([170, s_min, v_min], dtype=np.uint8)
    upper2 = np.array([180, 255,   255], dtype=np.uint8)

    mask = cv2.bitwise_or(
        cv2.inRange(hsv, lower1, upper1),
        cv2.inRange(hsv, lower2, upper2),
    )

    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5))
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)

    cnts, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    bboxes = []
    for c in cnts:
        x, y, w, h = cv2.boundingRect(c)
        area = w * h
        if area < min_area:
            continue
        if w < min_w or h < min_h:
            continue
        if h > 0 and (w / h) < 1.0:
            continue
        bboxes.append((x, y, w, h))
    return bboxes


def ocr_validate_price(page_bgr: np.ndarray, bbox: Tuple[int, int, int, int]) -> str:
    x, y, w, h = bbox
    pad = 3
    H, W = page_bgr.shape[:2]
    x0 = max(0, x - pad); y0 = max(0, y - pad)
    x1 = min(W, x + w + pad); y1 = min(H, y + h + pad)
    crop = page_bgr[y0:y1, x0:x1]

    crop = cv2.resize(crop, None, fx=2.8, fy=2.8, interpolation=cv2.INTER_CUBIC)
    config = "--psm 7 --oem 1 -c tessedit_char_whitelist=$0123456789"
    txt = pytesseract.image_to_string(crop, lang="eng", config=config)
    txt = txt.strip().replace(" ", "")
    return txt if PRICE_ONLY_RE.match(txt) else ""


# =========================
# GRILLA / RECORTE TARJETAS
# =========================

def cluster_1d(values: List[float], tol: float) -> List[List[float]]:
    clusters: List[List[float]] = []
    for v in values:
        if not clusters:
            clusters.append([v])
        elif abs(v - clusters[-1][-1]) <= tol:
            clusters[-1].append(v)
        else:
            clusters.append([v])
    return clusters


def build_grid_and_crops(page_bgr: np.ndarray, price_boxes: List[PriceBox],
                         top_pad_ratio: float, inter_pad_ratio: float) -> List[Tuple[PriceBox, Tuple[int, int, int, int], int, int]]:
    H, W = page_bgr.shape[:2]
    if not price_boxes:
        return []

    ys = sorted([pb.cy for pb in price_boxes])
    y_tol = max(25.0, H * 0.06)
    y_clusters = cluster_1d(ys, tol=y_tol)
    row_centers = [float(np.mean(c)) for c in y_clusters]

    rows: List[List[PriceBox]] = [[] for _ in row_centers]
    for pb in price_boxes:
        ridx = int(np.argmin([abs(pb.cy - rc) for rc in row_centers]))
        rows[ridx].append(pb)

    rows = sorted(rows, key=lambda r: min(p.y for p in r) if r else 1e9)
    row_min_y = [min(p.y for p in r) for r in rows]

    xs = sorted([pb.cx for pb in price_boxes])
    x_tol = max(35.0, W * 0.12)
    x_clusters = cluster_1d(xs, tol=x_tol)
    col_centers = [float(np.mean(c)) for c in x_clusters]
    col_centers.sort()

    col_bounds = [0]
    for a, b in zip(col_centers, col_centers[1:]):
        col_bounds.append(int((a + b) / 2))
    col_bounds.append(W)

    top_pad = int(H * top_pad_ratio)
    inter_pad = int(H * inter_pad_ratio)

    row_tops = []
    row_bottoms = []
    for i, my in enumerate(row_min_y):
        top = max(0, my - top_pad)
        if i < len(row_min_y) - 1:
            bottom = max(top + 10, row_min_y[i + 1] - inter_pad)
        else:
            bottom = H
        row_tops.append(top)
        row_bottoms.append(min(H, bottom))

    for i in range(1, len(row_tops)):
        if row_tops[i] < row_bottoms[i - 1]:
            row_tops[i] = row_bottoms[i - 1]

    results = []
    for pb in price_boxes:
        ridx = int(np.argmin([abs(pb.y - my) for my in row_min_y]))
        cidx = int(np.argmin([abs(pb.cx - cc) for cc in col_centers]))

        x0 = col_bounds[cidx]
        x1 = col_bounds[cidx + 1]
        y0 = row_tops[ridx]
        y1 = row_bottoms[ridx]
        y1 = min(H, y1 + int(H * 0.015))

        results.append((pb, (x0, y0, x1, y1), ridx, cidx))

    ded: Dict[Tuple[int, int, int], Tuple[PriceBox, Tuple[int, int, int, int], int, int]] = {}
    for pb, crop, r, c in results:
        key = (pb.page, r, c)
        area = pb.w * pb.h
        if key not in ded or area > (ded[key][0].w * ded[key][0].h):
            ded[key] = (pb, crop, r, c)

    return list(ded.values())


# =========================
# OCR TARJETA
# =========================

def ocr_card_text(page_bgr: np.ndarray, crop_bbox: Tuple[int, int, int, int], lang: str) -> str:
    x0, y0, x1, y1 = crop_bbox
    crop = page_bgr[y0:y1, x0:x1]
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    gray = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)
    thr = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    thr = cv2.resize(thr, None, fx=1.6, fy=1.6, interpolation=cv2.INTER_CUBIC)

    # psm 4 suele separar mejor líneas (si el 6 junta cosas)
    config = "--psm 4 --oem 1"
    return pytesseract.image_to_string(thr, lang=lang, config=config)


# =========================
# UTIL
# =========================

def find_pdf_in_folder(folder: Path) -> Path:
    pdfs = sorted(folder.glob("*.pdf"))
    if not pdfs:
        raise SystemExit(f"No encontré PDFs en: {folder}")
    pdfs = sorted(pdfs, key=lambda p: p.stat().st_mtime, reverse=True)
    return pdfs[0]


# =========================
# MAIN
# =========================

def main():
    here = Path(__file__).resolve().parent

    pdf_path = (here / PDF_FILENAME) if PDF_FILENAME else find_pdf_in_folder(here)
    if not pdf_path.exists():
        if PDF_FILENAME:
            print(f"[WARN] No existe {pdf_path.name}. Busco el primer PDF en la carpeta...")
        pdf_path = find_pdf_in_folder(here)

    if TESSERACT_CMD:
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

    print(f"[INFO] PDF: {pdf_path}")
    print(f"[INFO] DPI: {DPI}")
    print(f"[INFO] Output: {OUTPUT_XLSX}")

    pages = images_from_pdf(pdf_path, dpi=DPI, poppler_path=POPPLER_PATH)
    if MAX_PAGES:
        pages = pages[:MAX_PAGES]
        print(f"[INFO] DEBUG: procesando solo {MAX_PAGES} páginas")

    all_rows = []

    for page_num, pil_img in enumerate(pages, start=1):
        t_page = time.time()

        rgb = np.array(pil_img.convert("RGB"))
        bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)

        cand = detect_red_price_boxes(
            bgr,
            s_min=S_MIN, v_min=V_MIN,
            min_area=MIN_AREA, min_w=MIN_W, min_h=MIN_H
        )
        print(f"\n[PAGE {page_num}] candidatos rojos detectados: {len(cand)}")

        price_boxes: List[PriceBox] = []
        for bbox in cand:
            txt = ocr_validate_price(bgr, bbox)
            if not txt:
                continue
            x, y, w, h = bbox
            price_boxes.append(PriceBox(page=page_num, x=x, y=y, w=w, h=h, price_text=txt))

        print(f"[PAGE {page_num}] precios validados por OCR: {len(price_boxes)}")

        if not price_boxes:
            print(f"[PAGE {page_num}] (saltada) no hay tarjetas con precio rojo")
            continue

        grid_items = build_grid_and_crops(
            bgr, price_boxes,
            top_pad_ratio=TOP_PAD_RATIO,
            inter_pad_ratio=INTER_PAD_RATIO
        )
        print(f"[PAGE {page_num}] tarjetas (grid) generadas: {len(grid_items)}")

        for pb, crop_bbox, r, c in grid_items:
            card_txt = ocr_card_text(bgr, crop_bbox, lang=OCR_LANG_CARD)
            fields = extract_structured_fields_from_card_text(card_txt)

            marca = fields["marca"]
            producto = fields["producto"]
            presentacion = fields["presentacion"]
            codigo = fields["codigo"]
            descripcion = fields["descripcion"]  # <- ESTO es lo que quieres

            price_val = to_float_price(pb.price_text)
            x0, y0, x1, y1 = crop_bbox

            print(
                f"  - r{r} c{c} | {pb.price_text} ({price_val}) "
                f"| desc='{descripcion}' | marca='{marca}' | cod='{codigo}'"
            )
            if PRINT_CARD_OCR_TEXT:
                print("    OCR:", normalize_space(card_txt))

            if price_val is None:
                continue

            all_rows.append({
                "page": pb.page,
                "row": r,
                "col": c,
                "price_text": pb.price_text,
                "price_value": price_val,
                "marca": marca,
                "producto": producto,
                "presentacion": presentacion,
                "codigo": codigo,
                "descripcion": descripcion,  # <- usa esta columna
                "price_bbox": f"{pb.x},{pb.y},{pb.w},{pb.h}",
                "card_bbox": f"{x0},{y0},{x1-x0},{y1-y0}",
                "ocr_text": normalize_space(card_txt),
            })

        print(f"[PAGE {page_num}] tiempo: {time.time() - t_page:.1f}s")

    df = pd.DataFrame(all_rows)
    if df.empty:
        print("\n[WARN] No se detectaron tarjetas con precio rojo. Prueba: DPI=400 o baja S_MIN/V_MIN.")
        return

    df = df.drop_duplicates(subset=["page", "row", "col"]).sort_values(["page", "row", "col"])
    out_path = (Path.cwd() / OUTPUT_XLSX).resolve()
    df.to_excel(out_path, index=False)
    print(f"\n[OK] Guardado: {out_path}  (filas: {len(df)})")

    try:
        if os.name == "nt":
            os.startfile(str(out_path))  # noqa
    except Exception:
        pass


if __name__ == "__main__":
    main()
