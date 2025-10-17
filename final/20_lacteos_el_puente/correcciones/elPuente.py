#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
El Puente (text-only) -> MySQL con huella/fingerprint como sku_tienda
Robusto contra 1205 Lock wait timeout: transacciones cortas, retry/backoff,
aislamiento READ COMMITTED, advisory lock y verificación de índices.

Requiere:
  pip install mysql-connector-python beautifulsoup4 pandas requests
  y tu base_datos.get_conn()
"""

import re, html, time, hashlib
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from mysql.connector import Error as MySQLError
import mysql.connector  # para errno
import sys, os

# añade la carpeta raíz (2 niveles más arriba) al sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
)
from base_datos import get_conn  # <-- tu conexión

# ================== Config scraping ==================
BASE = "https://ofertas.lacteoselpuente.com.ar"
ENDPOINT = "/productos/get/{id}"

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
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
}

RE_PRICE_CAPTURE = re.compile(r"\$\s*([0-9.\s]+,\d{2})")
RE_HAS_PRICE     = re.compile(r"\$\s*\d")

TIENDA_CODIGO = "elpuente"
TIENDA_NOMBRE = "Lácteos El Puente"

# ================== Helpers scraping ==================
def clean_html_text(s: str) -> str:
    if not s:
        return ""
    s = BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    s = html.unescape(s).replace("\xa0", " ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def fetch_text(rubro_id: int, session: requests.Session) -> str:
    url = f"{BASE}{ENDPOINT.format(id=rubro_id)}"
    r = session.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text

def split_nombre_presentacion(desc: str) -> Tuple[str, str]:
    original = clean_html_text(desc)
    if "." in original:
        nombre, resto = original.split(".", 1)
        nombre, presentacion = nombre.strip(), resto.strip()
    else:
        m = re.search(r"\s(x|por)\s", original, flags=re.IGNORECASE)
        if m:
            nombre = original[:m.start()].strip()
            presentacion = original[m.start():].strip()
        else:
            nombre, presentacion = original, ""
    presentacion = re.sub(r"^\.*\s*", "", presentacion)
    presentacion = re.sub(r"^\b[Vv]alor\b[:\s]*", "", presentacion)
    presentacion = re.sub(r"\s{2,}", " ", presentacion).strip()
    if not nombre:
        nombre = original
    return nombre, presentacion

def parse_text_to_rows(rubro_nombre: str, text: str) -> List[Tuple[str, str, str, str, str]]:
    rows = []
    subcat = rubro_nombre
    carry_desc: Optional[str] = None
    for raw in text.splitlines():
        line = clean_html_text(raw)
        if not line:
            continue
        if line.startswith("#"):
            sub = line.lstrip("#").strip().rstrip(".")
            if sub:
                subcat = sub
            continue
        if RE_HAS_PRICE.search(line):
            m = RE_PRICE_CAPTURE.search(line)
            precio = m.group(1).strip() if m else ""
            before = line.split("$", 1)[0].strip()
            desc = (f"{carry_desc} {before}".strip() if carry_desc else before) or (carry_desc or "")
            nombre, presentacion = split_nombre_presentacion(desc)
            rows.append(("lacteos", subcat, nombre, presentacion, precio))
            carry_desc = None
        else:
            carry_desc = f"{carry_desc} {line}".strip() if carry_desc else line
    return rows

def scrape_elpuente() -> List[Dict[str, Any]]:
    s = requests.Session(); s.headers.update(HEADERS)
    all_rows: List[Dict[str, Any]] = []
    for rid, rubro_nombre in RUBROS.items():
        try:
            txt = fetch_text(rid, s)
        except requests.HTTPError as e:
            print(f"[WARN] {rid} {rubro_nombre}: {e}")
            continue
        rows = parse_text_to_rows(rubro_nombre, txt)
        for (categoria, subcat, nombre, presentacion, precio) in rows:
            all_rows.append({
                "categoria": categoria,
                "subcategoria": subcat,
                "nombre": nombre,
                "presentacion": presentacion,
                "precio_text": precio,
            })
        time.sleep(0.5)
    return all_rows

# ================== Normalización / Fingerprint ==================
_norm_ws = re.compile(r"\s+")
_nonword = re.compile(r"[^a-z0-9]+")

def _norm(s: Optional[str]) -> str:
    if not s: return ""
    s = s.strip().lower()
    s = _norm_ws.sub(" ", s)
    s = (s.replace("á","a").replace("é","e").replace("í","i")
           .replace("ó","o").replace("ú","u").replace("ñ","n"))
    s = _nonword.sub(" ", s)
    s = _norm_ws.sub(" ", s).strip()
    return s

def make_fingerprint(nombre: str, presentacion: str, subcategoria: str) -> str:
    base = f"{_norm(nombre)}|{_norm(presentacion)}|{_norm(subcategoria)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:24]  # 24 hex

def parse_price(text: Optional[str]) -> Optional[float]:
    if not text: return None
    t = re.sub(r"[^\d,.\-]", "", text)
    if "," in t and "." in t: t = t.replace(".", "").replace(",", ".")
    elif "," in t: t = t.replace(",", ".")
    try: return float(t)
    except Exception: return None

def price_to_varchar(x: Any) -> Optional[str]:
    if x is None: return None
    try:
        v = float(x)
        if np.isnan(v): return None
        return f"{round(v, 2)}"
    except Exception:
        s = str(x).strip()
        return s or None

def clean_txt(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s or None

# ================== Helpers de recorte por longitud ==================
def _fetch_maxlen_map(cur) -> Dict[Tuple[str, str], Optional[int]]:
    """
    Lee information_schema para obtener CHARACTER_MAXIMUM_LENGTH
    de las columnas que vamos a escribir.
    """
    targets = [
        ("productos", "nombre"),
        ("productos", "categoria"),
        ("productos", "subcategoria"),
        ("producto_tienda", "nombre_tienda"),
        ("historico_precios", "promo_comentarios"),
    ]
    result: Dict[Tuple[str, str], Optional[int]] = {t: None for t in targets}
    for table, column in targets:
        cur.execute("""
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            LIMIT 1
        """, (table, column))
        row = cur.fetchone()
        result[(table, column)] = int(row[0]) if row and row[0] is not None else None
    return result

def _clip(s: Optional[str], maxlen: Optional[int]) -> Optional[str]:
    if s is None or maxlen is None:
        return s
    if len(s) <= maxlen:
        return s
    return s[:maxlen]

class FieldClipper:
    def __init__(self, maxlens: Dict[Tuple[str, str], Optional[int]]):
        self.maxlens = maxlens

    def clip_prod(self, nombre: Optional[str], categoria: Optional[str], subcat: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        return (
            _clip(clean_txt(nombre), self.maxlens.get(("productos", "nombre"))),
            _clip(clean_txt(categoria), self.maxlens.get(("productos", "categoria"))),
            _clip(clean_txt(subcat), self.maxlens.get(("productos", "subcategoria"))),
        )

    def clip_pt_nombre(self, nombre_tienda: Optional[str]) -> Optional[str]:
        return _clip(clean_txt(nombre_tienda), self.maxlens.get(("producto_tienda", "nombre_tienda")))

    def clip_hist_promo_coment(self, texto: Optional[str]) -> Optional[str]:
        return _clip(clean_txt(texto), self.maxlens.get(("historico_precios", "promo_comentarios")))

# ================== Upserts MySQL ==================
def upsert_tienda(cur, codigo: str, nombre: str) -> int:
    cur.execute(
        "INSERT INTO tiendas (codigo, nombre) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE nombre=VALUES(nombre)",
        (codigo, nombre)
    )
    cur.execute("SELECT id FROM tiendas WHERE codigo=%s LIMIT 1", (codigo,))
    return cur.fetchone()[0]

def find_or_create_producto(cur, r: Dict[str, Any], clipper: FieldClipper) -> int:
    """
    Sin EAN. Intentamos por (nombre) y actualizamos categoria/subcategoria.
    Aplica recorte por longitud real de columnas.
    """
    nombre_raw = r.get("nombre")
    categoria_raw = r.get("categoria")
    sub_raw = r.get("subcategoria")

    nombre, categoria, sub = clipper.clip_prod(nombre_raw, categoria_raw, sub_raw)

    if nombre:
        cur.execute("SELECT id FROM productos WHERE nombre=%s LIMIT 1", (nombre,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE productos SET
                  categoria = COALESCE(NULLIF(%s,''), categoria),
                  subcategoria = COALESCE(NULLIF(%s,''), subcategoria)
                WHERE id=%s
            """, (categoria or "", sub or "", pid))
            return pid

    cur.execute("""
        INSERT INTO productos (ean, nombre, marca, fabricante, categoria, subcategoria)
        VALUES (NULL, NULLIF(%s,''), NULL, NULL, NULLIF(%s,''), NULLIF(%s,''))
    """, (nombre or "", categoria or "", sub or ""))
    return cur.lastrowid

def upsert_producto_tienda(cur, tienda_id: int, producto_id: int, r: Dict[str, Any], clipper: FieldClipper) -> int:
    """
    Clave natural: (tienda_id, sku_tienda = fingerprint).
    Guarda nombre_tienda con recorte si excede.
    """
    sku = r["sku_tienda"]  # fingerprint
    nombre_tienda = clipper.clip_pt_nombre(r.get("nombre"))
    cur.execute("""
        INSERT INTO producto_tienda (tienda_id, producto_id, sku_tienda, record_id_tienda, url_tienda, nombre_tienda)
        VALUES (%s, %s, %s, NULL, NULL, %s)
        ON DUPLICATE KEY UPDATE
          id = LAST_INSERT_ID(id),
          producto_id = VALUES(producto_id),
          nombre_tienda = COALESCE(VALUES(nombre_tienda), nombre_tienda)
    """, (tienda_id, producto_id, sku, nombre_tienda))
    return cur.lastrowid

def insert_historico(cur, tienda_id: int, producto_tienda_id: int, r: Dict[str, Any], capturado_en: datetime, clipper: FieldClipper):
    precio = price_to_varchar(r.get("precio_float"))
    promo_coment = clipper.clip_hist_promo_coment(r.get("presentacion"))
    cur.execute("""
        INSERT INTO historico_precios
          (tienda_id, producto_tienda_id, capturado_en,
           precio_lista, precio_oferta, tipo_oferta,
           promo_tipo, promo_texto_regular, promo_texto_descuento, promo_comentarios)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          precio_lista = VALUES(precio_lista),
          precio_oferta = VALUES(precio_oferta),
          tipo_oferta = VALUES(tipo_oferta),
          promo_tipo = VALUES(promo_tipo),
          promo_texto_regular = VALUES(promo_texto_regular),
          promo_texto_descuento = VALUES(promo_texto_descuento),
          promo_comentarios = VALUES(promo_comentarios)
    """, (
        tienda_id, producto_tienda_id, capturado_en,
        precio, None, None,
        None, None, None,
        promo_coment
    ))

# ================== Índices recomendados (opcional) ==================
def ensure_indexes(cur):
    """
    Intenta crear índices clave. Ignora si ya existen.
    """
    stmts = [
        ("idx_productos_nombre",
         "CREATE INDEX idx_productos_nombre ON productos (nombre)"),
        ("idx_producto_tienda_tienda_sku",
         "CREATE INDEX idx_producto_tienda_tienda_sku ON producto_tienda (tienda_id, sku_tienda)"),
        ("idx_hp_tienda_pt_fecha",
         "CREATE INDEX idx_hp_tienda_pt_fecha ON historico_precios (tienda_id, producto_tienda_id, capturado_en)")
        # Si quieres UNIQUE para ON DUP en historico_precios, hazlo manualmente:
        # ("uq_hp_tienda_pt_fecha",
        #  "ALTER TABLE historico_precios ADD UNIQUE KEY uq_hp_tienda_pt_fecha (tienda_id, producto_tienda_id, capturado_en)")
    ]
    for name, sql in stmts:
        try:
            cur.execute(sql)
            print(f"[INFO] Creado índice {name}")
        except MySQLError as e:
            # 1061: duplicate key name / 1831: duplicate index
            if getattr(e, "errno", None) in (1061, 1831):
                pass
            else:
                print(f"[WARN] Índice {name}: {e}")

# ================== Runner ==================
def _setup_session(cur, lock_timeout_sec: int = 15):
    # Menos bloqueos de next-key
    cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    # Falla rápido al esperar locks para poder reintentar
    cur.execute(f"SET SESSION innodb_lock_wait_timeout = {int(lock_timeout_sec)}")

def _acquire_job_lock(cur, name: str, timeout_sec: int = 5) -> bool:
    cur.execute("SELECT GET_LOCK(%s, %s)", (name, timeout_sec))
    got = cur.fetchone()[0]
    return got == 1

def _release_job_lock(cur, name: str):
    try:
        cur.execute("SELECT RELEASE_LOCK(%s)", (name,))
    except Exception:
        pass

def main():
    print("[INFO] Descargando El Puente…")
    raw_rows = scrape_elpuente()
    if not raw_rows:
        print("[INFO] Sin filas."); return

    # Enriquecer con fingerprint y precio parseado
    rows: List[Dict[str, Any]] = []
    for r in raw_rows:
        nombre = r["nombre"]
        present = r.get("presentacion") or ""
        subcat = r.get("subcategoria") or ""
        sku = make_fingerprint(nombre, present, subcat)
        precio_f = parse_price(r.get("precio_text"))
        rows.append({
            **r,
            "sku_tienda": sku,
            "precio_float": precio_f,
        })

    capturado_en = datetime.now()
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        _setup_session(cur, lock_timeout_sec=15)
        conn.commit()

        # Lock cooperativo para evitar dobles ejecuciones simultáneas
        if not _acquire_job_lock(cur, "job_elpuente", timeout_sec=5):
            print("[INFO] Otro proceso está ejecutándose. Saliendo.")
            return

        # Índices (opcional, ignora si existen)
        ensure_indexes(cur)
        conn.commit()

        # Resuelve tienda fuera de bucle
        tienda_id = upsert_tienda(cur, TIENDA_CODIGO, TIENDA_NOMBRE)
        conn.commit()

        # Proceso con transacción corta por FILA + retry/backoff
        insertados = 0

        def _do_row(_cur, _r):
            producto_id = find_or_create_producto(_cur, _r, clipper)
            pt_id = upsert_producto_tienda(_cur, tienda_id, producto_id, _r, clipper)
            insert_historico(_cur, tienda_id, pt_id, _r, capturado_en, clipper)

        # cache longitudes (safe fuera del bucle pesado)
        maxlens = _fetch_maxlen_map(cur)
        clipper = FieldClipper(maxlens)

        for r in rows:
            # reintenta 1205 con backoff exponencial
            for intento in range(4):  # 0..3
                try:
                    # reconectar si hizo timeout/conexión rota
                    if not conn.is_connected():
                        conn = get_conn()
                        cur = conn.cursor()
                        _setup_session(cur, lock_timeout_sec=15)
                        conn.commit()

                    conn.start_transaction()
                    _do_row(cur, r)
                    conn.commit()
                    insertados += 1
                    break
                except MySQLError as e:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    # 1205 Lock wait timeout
                    if getattr(e, "errno", None) == 1205:
                        backoff = 0.5 * (2 ** intento)  # 0.5, 1.0, 2.0, 4.0
                        time.sleep(backoff)
                        # refresca la sesión por si quedó sucia
                        try:
                            _setup_session(cur, lock_timeout_sec=15)
                            conn.commit()
                        except Exception:
                            pass
                        continue
                    # Deadlock 1213: aplica el mismo tratamiento
                    if getattr(e, "errno", None) == 1213:
                        backoff = 0.5 * (2 ** intento)
                        time.sleep(backoff)
                        continue
                    # Otros errores: propaga
                    raise

        # libera lock
        _release_job_lock(cur, "job_elpuente")

        print(f"💾 Guardado en MySQL: {insertados} filas de histórico para {TIENDA_NOMBRE} ({capturado_en})")

    except MySQLError as e:
        print(f"❌ Error MySQL: {e}")
        try:
            if conn: conn.rollback()
        except Exception:
            pass
    finally:
        try:
            if cur:
                try:
                    _release_job_lock(cur, "job_elpuente")
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if conn: conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
