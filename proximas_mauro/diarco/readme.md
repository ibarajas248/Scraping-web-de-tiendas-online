# Extraer SOLO productos con “diagramación tipo tarjeta” (precio grande en rojo)

Este script está pensado para folletos donde los productos aparecen como **tarjetas** en un **grid** como tu ejemplo:

- **Precio grande en rojo** (con $)
- Imagen del producto
- Nombre/descrición dentro de la misma celda

La idea es **ignorar** otros formatos del folleto (promos en banda, banners, combos, etc.) y **extraer únicamente** estas tarjetas.

---

## Archivos

- `extract_pdf_products_cards.py` → Script principal (PDF/Imágenes → detecta precios rojos → recorta tarjeta → OCR → Excel)

> Usa los mismos `requirements_scrape_pdf.txt` que te pasé antes:
- pdf2image
- pytesseract
- pillow
- opencv-python
- pandas
- openpyxl
- numpy

---

## Requisitos

### 1) Instalar dependencias Python
```bash
pip install -r requirements_scrape_pdf.txt
```

### 2) Instalar Tesseract OCR (obligatorio)
- Linux (Ubuntu/Debian):
  ```bash
  sudo apt-get install -y tesseract-ocr tesseract-ocr-spa
  ```
- macOS:
  ```bash
  brew install tesseract
  ```
- Windows:
  Instala Tesseract y luego usa `--tesseract` con la ruta al `.exe`.

### 3) Poppler (solo si lees PDF directo)
`pdf2image` necesita Poppler.
- Linux: `sudo apt-get install -y poppler-utils`
- macOS: `brew install poppler`
- Windows: instala Poppler y pásalo por `--poppler_path`.

Si no quieres Poppler: renderiza el PDF a imágenes y usa `--images_dir`.

---

## Uso

### A) Desde PDF
```bash
python extract_pdf_products_cards.py --pdf "INT-26.01-AL-30.01.pdf" --out productos_pdf.xlsx
```

Windows (sin PATH):
```bash
python extract_pdf_products_cards.py --pdf "INT-26.01-AL-30.01.pdf" ^
  --tesseract "C:\Program Files\Tesseract-OCR\tesseract.exe" ^
  --poppler_path "C:\poppler\Library\bin" ^
  --out productos_pdf.xlsx
```

### B) Desde imágenes renderizadas (sin Poppler)
```bash
python extract_pdf_products_cards.py --images_dir pages --out productos_pdf.xlsx
```

---

## Ajustes si no detecta bien

### 1) Si no encuentra precios rojos (0 filas)
Sube DPI y/o baja umbrales de rojo:
```bash
python extract_pdf_products_cards.py --pdf "folleto.pdf" --dpi 400 --s_min 60 --v_min 100
```

### 2) Si está agarrando logos rojos (falsos positivos)
Sube filtros:
```bash
python extract_pdf_products_cards.py --pdf "folleto.pdf" --min_area 900 --min_w 45 --min_h 20
```

### 3) Si la tarjeta recortada queda “cortada” (pierde texto)
Ajusta el recorte por fila:
- `--top_pad_ratio` (sube el recorte hacia arriba desde el precio)
- `--inter_pad_ratio` (corta antes de la siguiente fila)

Ejemplo:
```bash
python extract_pdf_products_cards.py --pdf "folleto.pdf" --top_pad_ratio 0.12 --inter_pad_ratio 0.008
```

---

## Salida Excel

Columnas:
- `page`, `row`, `col` → posición en la grilla
- `price_text`, `price_value`
- `price_bbox` → bbox del precio detectado
- `card_bbox` → bbox de la tarjeta recortada
- `product_name` → nombre/descr estimado (heurístico)
- `ocr_text` → texto OCR completo de la tarjeta (para auditar)

---

## Nota
Esto funciona muy bien cuando la diagramación es **consistente** en la página.

Si tu PDF alterna varios diseños, este script va a:
- **extraer** tarjetas con precio rojo
- **ignorar** el resto (que es lo que tú pediste)
