#!/usr/bin/env python3
# -*- coding: utf-8 -*-
def ean():

    import re
    import unicodedata
    from typing import List, Tuple, Optional
    import os
    import numpy as np
    import pandas as pd
    import streamlit as st
    from mysql.connector import Error as MySQLError
    from rapidfuzz import fuzz, process
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    from base_datos import get_conn  # host=localhost, port=3310, db=analisis_retail

    st.set_page_config(page_title="Vincular SKU /ID  a productos con EAN", layout="wide")

    # ==========================
    # Normalización y parsing
    # ==========================
    _STOP = {"de","la","el","los","las","con","para","por","sin","y","o","u","en","x","lt","l","ml","kg","gr","g","cc"}

    def _norm(s: Optional[str]) -> str:
        if not s:
            return ""
        s = str(s).lower().strip()
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        s = re.sub(r"[^a-z0-9 ]+", " ", s)
        toks = [t for t in s.split() if t and t not in _STOP]
        return " ".join(toks)

    def combo_string(nombre: str, marca: Optional[str]) -> str:
        m = _norm(marca) if marca else ""
        return (m + " " + _norm(nombre)).strip()

    _pack_re = re.compile(r"(?:^|\s)(?:x\s*|(\d+)\s*[xX])(\d+)(?=\D|$)")
    _units_re = re.compile(r"(?:^|\s)x\s*(\d+)(?=\D|$)")
    _size_re = re.compile(
        r"(\d+(?:[.,]\d+)?)\s*(ml|cc|l|lt|litro?s?|g|gr|gramos?|kg|kilos?)",
        re.I
    )

    def parse_size_pack(text: str) -> Tuple[Optional[float], Optional[float], int]:
        """
        Devuelve (ml_total, g_total, unidades). Convierte L→ml, kg→g.
        Si hay pack (xN) intenta multiplicar.
        """
        if not text:
            return None, None, 1
        t = text.lower()
        # unidades/packs
        units = 1
        m_u = _units_re.search(t)
        if m_u:
            try:
                units = int(m_u.group(1))
            except Exception:
                units = 1

        # tamaños
        ml_total = None
        g_total = None
        sizes = _size_re.findall(t)
        # Si hay varios tamaños, tomamos el mayor (p. ej. “pack x6 500 ml” → 500 ml * 6)
        best_ml = 0.0
        best_g = 0.0
        for num, unit in sizes:
            try:
                val = float(num.replace(",", "."))
            except Exception:
                continue
            unit = unit.lower()
            if unit in ("l","lt","litro","litros"):
                val_ml = val * 1000.0
                best_ml = max(best_ml, val_ml)
            elif unit in ("ml","cc"):
                val_ml = val * 1.0
                best_ml = max(best_ml, val_ml)
            elif unit in ("kg","kilo","kilos"):
                val_g = val * 1000.0
                best_g = max(best_g, val_g)
            elif unit in ("g","gr","gramo","gramos"):
                val_g = val * 1.0
                best_g = max(best_g, val_g)
        if best_ml > 0:
            ml_total = best_ml * units
        if best_g > 0:
            g_total = best_g * units
        return ml_total, g_total, units

    def size_bonus(base_size: Tuple[Optional[float], Optional[float], int],
                   dest_size: Tuple[Optional[float], Optional[float], int]) -> float:
        b_ml, b_g, _ = base_size
        d_ml, d_g, _ = dest_size
        # mismo tipo
        if b_ml and d_ml:
            diff = abs(b_ml - d_ml) / max(b_ml, d_ml)
            if diff <= 0.2: return 0.10
            if diff <= 0.35: return 0.05
            return -0.08
        if b_g and d_g:
            diff = abs(b_g - d_g) / max(b_g, d_g)
            if diff <= 0.2: return 0.10
            if diff <= 0.35: return 0.05
            return -0.08
        # tipos distintos (ml vs g) penaliza
        if (b_ml and d_g) or (b_g and d_ml):
            return -0.10
        return 0.0

    def brand_bonus(b_brand: str, d_brand: str) -> float:
        b = _norm(b_brand)
        d = _norm(d_brand)
        if not b or not d:
            return 0.0
        if b == d:
            return 0.08
        # marca incluida (ej submarca)
        if b in d or d in b:
            return 0.04
        return -0.03

    # ==========================
    # DB helpers
    # ==========================
    def run_select_df(sql: str, params: Tuple = ()) -> pd.DataFrame:
        conn = get_conn()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(sql, params)
            rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()
        finally:
            cur.close()
            conn.close()

    def run_exec(sql: str, params: Tuple = (), many: bool = False):
        conn = get_conn()
        conn.start_transaction()
        try:
            cur = conn.cursor()
            if many:
                cur.executemany(sql, params)
            else:
                cur.execute(sql, params)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    # ==========================
    # Queries
    # ==========================
    @st.cache_data(ttl=120)
    def get_filtros_base():
        tiendas = run_select_df("SELECT id, nombre FROM tiendas ORDER BY nombre")
        marcas = run_select_df("SELECT DISTINCT marca FROM productos WHERE marca IS NOT NULL AND marca<>'' ORDER BY marca")
        categorias = run_select_df("SELECT DISTINCT categoria FROM productos WHERE categoria IS NOT NULL AND categoria<>'' ORDER BY categoria")
        return tiendas, marcas, categorias

    @st.cache_data(ttl=60, show_spinner=False)
    def get_pt_sin_ean(tienda_id: Optional[int], marca: Optional[str], categoria: Optional[str],
                       q_base: Optional[str], limit: int = 2000) -> pd.DataFrame:
        sql = """
        SELECT
          pt.id            AS pt_id,
          pt.tienda_id,
          t.nombre         AS tienda,
          pt.sku_tienda,
          pt.nombre_tienda,
          p.id             AS producto_id,
          p.nombre         AS producto,
          p.marca,
          p.categoria,
          p.subcategoria
        FROM producto_tienda pt
        JOIN productos p   ON p.id = pt.producto_id
        JOIN tiendas   t   ON t.id = pt.tienda_id
        WHERE p.ean IS NULL
        """
        params: list = []
        if tienda_id:
            sql += " AND pt.tienda_id=%s"
            params.append(tienda_id)
        if marca:
            sql += " AND p.marca=%s"
            params.append(marca)
        if categoria:
            sql += " AND p.categoria=%s"
            params.append(categoria)
        if q_base and q_base.strip():
            like = f"%{q_base.strip()}%"
            sql += """
              AND (
                    p.nombre LIKE %s
                 OR pt.nombre_tienda LIKE %s
                 OR pt.sku_tienda LIKE %s
                 OR p.marca LIKE %s
              )
            """
            params.extend([like, like, like, like])
        sql += " ORDER BY t.nombre, pt.nombre_tienda LIMIT %s"
        params.append(int(limit))
        return run_select_df(sql, tuple(params))

    @st.cache_data(ttl=60, show_spinner=False)
    def get_resumen_productos_sin_ean() -> pd.DataFrame:
        sql = """
        SELECT
          p.id AS producto_id,
          p.nombre,
          p.marca,
          p.categoria,
          COUNT(pt.id) AS skus_asociados
        FROM productos p
        LEFT JOIN producto_tienda pt ON pt.producto_id = p.id
        WHERE p.ean IS NULL
        GROUP BY p.id, p.nombre, p.marca, p.categoria
        HAVING skus_asociados > 0
        ORDER BY skus_asociados DESC, p.marca, p.nombre
        """
        return run_select_df(sql)

    @st.cache_data(ttl=60, show_spinner=False)
    def buscar_destinos_con_ean(q: str, limit: int = 200) -> pd.DataFrame:
        q_like = f"%{q}%"
        sql = f"""
        SELECT
          p.id   AS destino_producto_id,
          p.ean,
          p.nombre,
          p.marca,
          p.categoria,
          p.subcategoria,
          COUNT(pt.id) AS vinculaciones
        FROM productos p
        LEFT JOIN producto_tienda pt ON pt.producto_id = p.id
        WHERE p.ean IS NOT NULL
          AND (
                p.ean = %s
             OR p.nombre LIKE %s
             OR p.marca  LIKE %s
             OR p.categoria LIKE %s
          )
        GROUP BY p.id, p.ean, p.nombre, p.marca, p.categoria, p.subcategoria
        ORDER BY
          CASE WHEN p.ean = %s THEN 0 ELSE 1 END,
          vinculaciones DESC, p.nombre
        LIMIT {int(limit)}
        """
        return run_select_df(sql, (q, q_like, q_like, q_like, q))

    @st.cache_data(ttl=120, show_spinner=False)
    def pool_destinos_con_ean_acotado(marcas: List[str], categorias: List[str], limit: int = 40000) -> pd.DataFrame:
        sql = """
        SELECT
          p.id   AS destino_producto_id,
          p.ean,
          p.nombre,
          p.marca,
          p.categoria,
          p.subcategoria
        FROM productos p
        WHERE p.ean IS NOT NULL
        """
        params: list = []
        conds = []
        if marcas:
            conds.append("p.marca IN (" + ",".join(["%s"] * len(marcas)) + ")")
            params.extend(marcas)
        if categorias:
            conds.append("p.categoria IN (" + ",".join(["%s"] * len(categorias)) + ")")
            params.extend(categorias)
        if conds:
            sql += " AND (" + " OR ".join(conds) + ")"
        sql += " LIMIT %s"
        params.append(int(limit))
        return run_select_df(sql, tuple(params))

    def reassign_producto_ids(pt_ids: List[int], new_producto_id: int) -> int:
        if not pt_ids:
            return 0
        placeholders = ",".join(["%s"] * len(pt_ids))
        sql = f"UPDATE producto_tienda SET producto_id=%s WHERE id IN ({placeholders})"
        params = tuple([new_producto_id] + pt_ids)
        conn = get_conn()
        conn.start_transaction()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            updated = cur.rowcount
            conn.commit()
            return updated
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def delete_orphan_products(product_ids: List[int]) -> int:
        if not product_ids:
            return 0
        placeholders = ",".join(["%s"] * len(product_ids))
        sql = f"""
        DELETE FROM productos
        WHERE id IN ({placeholders})
          AND ean IS NULL
          AND NOT EXISTS (SELECT 1 FROM producto_tienda pt WHERE pt.producto_id = productos.id)
        """
        conn = get_conn()
        conn.start_transaction()
        try:
            cur = conn.cursor()
            cur.execute(sql, tuple(product_ids))
            deleted = cur.rowcount
            conn.commit()
            return deleted
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    # ---------- Helpers para EAN + Excel ----------
    def _clean_ean_value(x: Optional[str]) -> Optional[str]:
        if x is None:
            return None
        s = str(x).strip()
        if not s or s.lower() in {"nan", "none"}:
            return None
        s = re.sub(r"\D+", "", s)  # solo dígitos
        if len(s) in {8, 12, 13, 14}:
            return s
        return None

    def _ean_checksum_ok(ean: str) -> Optional[bool]:
        if not ean or not ean.isdigit():
            return None
        n = len(ean)
        digits = list(map(int, ean))
        if n == 13:  # EAN-13
            s = sum(digits[i] * (1 if i % 2 == 0 else 3) for i in range(12))
            check = (10 - (s % 10)) % 10
            return check == digits[12]
        if n == 8:   # EAN-8
            s = sum(digits[i] * (3 if i % 2 == 0 else 1) for i in range(7))
            check = (10 - (s % 10)) % 10
            return check == digits[7]
        if n == 12:  # UPC-A -> evaluar como EAN-13 con 0 delante
            return _ean_checksum_ok("0" + ean)
        if n == 14:  # GTIN-14
            base = digits[:-1][::-1]
            s = sum(d * (3 if i % 2 == 0 else 1) for i, d in enumerate(base))
            check = (10 - (s % 10)) % 10
            return check == digits[-1]
        return None

    def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
        import io
        bio = io.BytesIO()
        try:
            # openpyxl para .xlsx
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_name)
        except Exception:
            # Fallback a xlsxwriter si openpyxl no está disponible
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_name)
        bio.seek(0)  # importante para devolver todos los bytes
        return bio.getvalue()

    def plantilla_asignar_ean_df() -> pd.DataFrame:
        """
        Columnas:
          - mode: 'pt_id' | 'producto_id' | 'sku'
          - pt_id: (int) si mode='pt_id'
          - producto_id: (int) si mode='producto_id'
          - sku_tienda: (str) si mode='sku'
          - tienda_id: (int, OPCIONAL) para desambiguar SKUs repetidos entre tiendas
          - ean: (str) EAN a asignar (8/12/13/14 dígitos)
          - override: 'si' para sobrescribir si ya tenía EAN; 'no' (default) respeta si hay EAN
          - nota: libre (no se usa)
        """
        return pd.DataFrame([
            {"mode": "pt_id", "pt_id": 123, "producto_id": None, "sku_tienda": None, "tienda_id": None,
             "ean": "7791234567890", "override": "no", "nota": "ejemplo por pt_id"},
            {"mode": "producto_id", "pt_id": None, "producto_id": 456, "sku_tienda": None, "tienda_id": None,
             "ean": "7790000111123", "override": "no", "nota": "ejemplo por producto_id"},
            {"mode": "sku", "pt_id": None, "producto_id": None, "sku_tienda": "ABC-9999", "tienda_id": 7,
             "ean": "084123456789", "override": "si", "nota": "ejemplo por sku+tienda"},
        ], columns=["mode", "pt_id", "producto_id", "sku_tienda", "tienda_id", "ean", "override", "nota"])

    def _fetch_producto_id_by_pt_id(pt_id: int) -> Optional[int]:
        df = run_select_df("SELECT producto_id FROM producto_tienda WHERE id=%s", (int(pt_id),))
        if df.empty:
            return None
        return int(df.iloc[0]["producto_id"])

    def _fetch_producto_id_candidates_by_sku(sku: str) -> pd.DataFrame:
        return run_select_df(
            "SELECT id AS pt_id, producto_id, tienda_id FROM producto_tienda WHERE sku_tienda=%s",
            (sku,)
        )

    def _fetch_producto_id_by_sku_tienda(sku: str, tienda_id: int) -> Optional[int]:
        df = run_select_df(
            "SELECT producto_id FROM producto_tienda WHERE sku_tienda=%s AND tienda_id=%s",
            (sku, int(tienda_id))
        )
        if df.empty:
            return None
        return int(df.iloc[0]["producto_id"])

    def _ean_in_use(ean: str) -> Optional[int]:
        df = run_select_df("SELECT id FROM productos WHERE ean=%s", (ean,))
        if df.empty:
            return None
        return int(df.iloc[0]["id"])

    def _resolve_producto_id(mode: str,
                             pt_id: Optional[int],
                             producto_id: Optional[int],
                             sku_tienda: Optional[str],
                             tienda_id: Optional[int]) -> Tuple[Optional[int], str]:
        """
        Devuelve (producto_id | None, detalle/motivo).
        Si hay múltiples candidatos por SKU sin tienda, devuelve (None, 'ambiguous: ...').
        """
        try:
            if mode == "producto_id" and producto_id:
                return int(producto_id), "resuelto por producto_id"

            if mode == "pt_id" and pt_id:
                pid = _fetch_producto_id_by_pt_id(int(pt_id))
                return (pid, "resuelto por pt_id") if pid else (None, f"pt_id {pt_id} inexistente")

            if mode == "sku" and sku_tienda:
                if tienda_id:
                    pid = _fetch_producto_id_by_sku_tienda(sku_tienda, int(tienda_id))
                    return (pid, f"resuelto por sku+tienda_id={tienda_id}") if pid else (None, f"sku '{sku_tienda}' no existe en tienda_id={tienda_id}")
                cands = _fetch_producto_id_candidates_by_sku(sku_tienda)
                if cands.empty:
                    return None, f"sku '{sku_tienda}' inexistente"
                uniq = cands["producto_id"].dropna().unique().tolist()
                if len(uniq) == 1:
                    return int(uniq[0]), f"resuelto por sku (único en {len(cands)} fila/s)"
                tiendas = sorted(cands["tienda_id"].dropna().unique().tolist())
                ej_pt = cands["pt_id"].head(5).tolist()
                return None, f"ambiguous: sku en múltiples tiendas {tiendas}; ej pt_id={ej_pt} (agregá tienda_id)"
        except Exception as ex:
            return None, f"error resolver producto_id: {ex}"
        return None, "mode/parametros insuficientes"

    def update_producto_ean(producto_id: int, ean: str, override: bool) -> Tuple[str, str]:
        """
        Devuelve (status, message):
          - 'updated'  : EAN asignado
          - 'skipped'  : ya tenía EAN y override=False
          - 'conflict' : EAN ya lo usa otro producto
          - 'notfound' : producto_id no existe
          - 'error'    : excepción
        """
        try:
            chk = run_select_df("SELECT id, ean FROM productos WHERE id=%s", (int(producto_id),))
            if chk.empty:
                return "notfound", f"producto_id {producto_id} no existe"

            current = str(chk.iloc[0]["ean"]) if pd.notna(chk.iloc[0]["ean"]) else None
            if not ean:
                return "error", "EAN vacío o inválido"

            # ¿Ese EAN ya lo usa otro producto?
            used_by = _ean_in_use(ean)
            if used_by and used_by != int(producto_id):
                return "conflict", f"EAN {ean} ya pertenece a producto_id {used_by}"

            if current and not override:
                if current == ean:
                    return "skipped", "mismo EAN ya presente"
                return "skipped", f"ya tenía EAN {current} (override=no)"

            # Ejecutar UPDATE (si override True actualiza, si no, actualiza solo si estaba NULL)
            sql = "UPDATE productos SET ean=%s WHERE id=%s" if override else "UPDATE productos SET ean=%s WHERE id=%s AND ean IS NULL"
            conn = get_conn()
            conn.start_transaction()
            try:
                cur = conn.cursor()
                cur.execute(sql, (ean, int(producto_id)))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
                conn.close()
            return "updated", f"producto_id {producto_id} → EAN {ean}"
        except Exception as ex:
            return "error", str(ex)

    def procesar_excel_asignar_ean(df_in: pd.DataFrame, dry_run: bool = False) -> pd.DataFrame:
        # Normaliza columnas esperadas (case-insensitive)
        cols = {c.lower().strip(): c for c in df_in.columns}
        need = {"mode", "pt_id", "producto_id", "sku_tienda", "tienda_id", "ean", "override"}
        for c in need:
            if c not in cols:
                df_in[c] = None

        out_rows = []
        for _, r in df_in.iterrows():
            mode = str(r.get(cols.get("mode", "mode")) or "").strip().lower()
            ean_raw = r.get(cols.get("ean", "ean"))
            override_raw = str(r.get(cols.get("override", "override")) or "").strip().lower()

            # limpiar ean y validar checksum
            ean = _clean_ean_value(ean_raw)
            checksum_ok = _ean_checksum_ok(ean) if ean else None

            # override
            override = override_raw in {"si", "sí", "true", "1", "y", "yes"}

            # ids/keys
            pt_val = r.get(cols.get("pt_id", "pt_id"))
            pid_val = r.get(cols.get("producto_id", "producto_id"))
            sku_val = r.get(cols.get("sku_tienda", "sku_tienda"))
            tienda_val = r.get(cols.get("tienda_id", "tienda_id"))

            pt_id = None
            producto_id = None
            tienda_id = None

            try:
                if pd.notna(pt_val) and str(pt_val).strip() != "":
                    pt_id = int(str(pt_val).strip())
            except Exception:
                pt_id = None

            try:
                if pd.notna(pid_val) and str(pid_val).strip() != "":
                    producto_id = int(str(pid_val).strip())
            except Exception:
                producto_id = None

            try:
                if pd.notna(tienda_val) and str(tienda_val).strip() != "":
                    tienda_id = int(str(tienda_val).strip())
            except Exception:
                tienda_id = None

            # Validaciones base
            if mode not in {"pt_id", "producto_id", "sku"}:
                out_rows.append({
                    "mode": mode, "pt_id": pt_id, "producto_id": producto_id, "sku_tienda": sku_val, "tienda_id": tienda_id,
                    "ean": ean_raw, "override": override, "checksum_ok": checksum_ok,
                    "status": "error", "message": "mode inválido (usa pt_id | producto_id | sku)"
                })
                continue

            if not ean:
                out_rows.append({
                    "mode": mode, "pt_id": pt_id, "producto_id": producto_id, "sku_tienda": sku_val, "tienda_id": tienda_id,
                    "ean": ean_raw, "override": override, "checksum_ok": checksum_ok,
                    "status": "error", "message": "EAN vacío/incorrecto (solo dígitos; 8/12/13/14)"
                })
                continue

            # Resolver producto_id
            resolved_pid, detail = _resolve_producto_id(mode, pt_id, producto_id, str(sku_val or "").strip(), tienda_id)

            if resolved_pid is None:
                status_tag = "ambiguous" if detail.startswith("ambiguous") else "notfound"
                out_rows.append({
                    "mode": mode, "pt_id": pt_id, "producto_id": producto_id, "sku_tienda": sku_val, "tienda_id": tienda_id,
                    "ean": ean, "override": override, "checksum_ok": checksum_ok,
                    "status": status_tag, "message": detail
                })
                continue

            if dry_run:
                out_rows.append({
                    "mode": mode, "pt_id": pt_id, "producto_id": int(resolved_pid),
                    "sku_tienda": sku_val, "tienda_id": tienda_id,
                    "ean": ean, "override": override, "checksum_ok": checksum_ok,
                    "status": "would_update", "message": f"[simulación] {detail}"
                })
                continue

            status, message = update_producto_ean(int(resolved_pid), ean, override)
            out_rows.append({
                "mode": mode, "pt_id": pt_id, "producto_id": int(resolved_pid),
                "sku_tienda": sku_val, "tienda_id": tienda_id,
                "ean": ean, "override": override, "checksum_ok": checksum_ok,
                "status": status, "message": f"{detail} | {message}"
            })

        return pd.DataFrame(out_rows)

    # ==========================
    # UI
    # ==========================
    st.title("Vincular SKUs a productos con EAN")

    with st.sidebar:
        st.header("Filtros")
        tiendas_df, marcas_df, categorias_df = get_filtros_base()

        tienda_opt = st.selectbox(
            "Tienda",
            options=[("","— Todas —")] + [(int(r.id), r.nombre) for _, r in tiendas_df.iterrows()],
            format_func=lambda x: x[1] if isinstance(x, tuple) else x
        )
        tienda_id = tienda_opt[0] if isinstance(tienda_opt, tuple) and tienda_opt[0] != "" else None

        marca = st.selectbox("Marca", options=[""] + list(marcas_df["marca"].astype(str)), index=0) or None
        categoria = st.selectbox("Categoría", options=[""] + list(categorias_df["categoria"].astype(str)), index=0) or None

        q_base = st.text_input("Buscar en base (nombre/SKU/marca)", placeholder="Ej: 'aceite', '7790...', o parte del nombre")
        base_limit = st.slider("Máx. filas base", 200, 10000, 2000, step=200)

        st.markdown("---")
        st.subheader("Destino (producto con EAN)")
        q = st.text_input("Buscar destino por EAN, nombre o marca", placeholder="Ej: 7791234567890 o 'aceite natura'")
        limit = st.slider("Máx. resultados destino", 50, 1000, 200, step=50)

    # --------------------------
    # Tab 1: Vincular por SKU
    # --------------------------
    st.subheader("1) Selecciona SKUs cuyo producto no tiene EAN")
    pt_df = get_pt_sin_ean(tienda_id, marca, categoria, q_base, limit=base_limit)

    if pt_df.empty:
        st.info("No hay SKUs pendientes con producto sin EAN según los filtros/búsqueda.")
    else:
        show_cols = ["pt_id", "tienda", "sku_tienda", "nombre_tienda", "marca", "categoria", "producto_id", "producto"]
        for c in show_cols:
            if c not in pt_df.columns:
                pt_df[c] = None
        st.dataframe(pt_df[show_cols], use_container_width=True, hide_index=True)

        selected_ids = st.multiselect("Selecciona uno o varios pt_id a vincular", options=list(pt_df["pt_id"]))
        orig_prod_ids = sorted(set(pt_df.loc[pt_df["pt_id"].isin(selected_ids), "producto_id"].tolist())) if selected_ids else []

        st.subheader("2) Elige el producto destino (con EAN)")
        destinos_df = buscar_destinos_con_ean(q.strip(), limit=limit) if q.strip() else pd.DataFrame()
        if not q.strip():
            st.info("Escribe arriba un término de búsqueda para listar posibles destinos con EAN.")
        elif destinos_df.empty:
            st.warning("No se encontraron productos con EAN para esa búsqueda.")
        else:
            st.dataframe(destinos_df, use_container_width=True, hide_index=True)
            destino_id = st.number_input("ID del producto destino (destino_producto_id)", min_value=1, step=1)

            colA, colB = st.columns(2)
            with colA:
                do_cleanup = st.checkbox("Eliminar productos huérfanos sin EAN después de vincular", value=True,
                                         help="Solo eliminará productos sin EAN que queden sin ningún SKU asociado.")
            with colB:
                st.caption("Recomendado: mantener el catálogo limpio y sin duplicados.")

            btn = st.button("🔗 Vincular seleccionados → producto destino", type="primary",
                            disabled=(not selected_ids or not destino_id))

            if btn:
                try:
                    updated = reassign_producto_ids(selected_ids, int(destino_id))
                    st.success(f"Vinculación completada: {updated} SKU(s) → producto_id={int(destino_id)}")
                    if do_cleanup and orig_prod_ids:
                        removed = delete_orphan_products(orig_prod_ids)
                        st.info(f"Productos huérfanos eliminados: {removed}")
                    get_pt_sin_ean.clear(); get_resumen_productos_sin_ean.clear()
                    st.rerun()
                except MySQLError as e:
                    st.error(f"Error MySQL: {e}")
                except Exception as e:
                    st.error(f"Ocurrió un error: {e}")

    # --------------------------
    # Carga por Excel: asignar EAN a productos
    # --------------------------
    st.markdown("---")
    st.subheader(" Carga por Excel: asignar EAN a productos (por pt_id / producto_id / sku)")

    colT1, colT2 = st.columns(2)

    with colT1:
        st.markdown("**Descargar plantilla**")
        tpl_df = plantilla_asignar_ean_df()

        # 👉 XLSX (usando helper robusto)
        xlsx_bytes = _df_to_excel_bytes(tpl_df, sheet_name="plantilla")
        st.download_button(
            "⬇️ Plantilla XLSX",
            xlsx_bytes,
            file_name="plantilla_asignar_ean.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="btn_tpl_xlsx"
        )

        # (opcional) CSV
        st.download_button(
            "⬇️ Plantilla CSV",
            tpl_df.to_csv(index=False).encode("utf-8"),
            file_name="plantilla_asignar_ean.csv",
            mime="text/csv",
            key="btn_tpl_csv"
        )

        st.caption("Tip: si el SKU existe en varias tiendas, agregá la columna 'tienda_id' para desambiguar.")

    with colT2:
        st.markdown("**Subir archivo con asignaciones:**")
        up = st.file_uploader("Acepta .xlsx, .xls o .csv", type=["xlsx", "xls", "csv"])

    if up is not None:
        # Detecta extensión
        ext = os.path.splitext(up.name)[1].lower()
        df_in = None
        try:
            if ext == ".csv":
                # Preserva strings (EAN, IDs, SKU) y prueba encoding
                try:
                    df_in = pd.read_csv(up, dtype=str, keep_default_na=False, encoding="utf-8")
                except UnicodeDecodeError:
                    up.seek(0)
                    df_in = pd.read_csv(up, dtype=str, keep_default_na=False, encoding="latin-1")
            elif ext in (".xlsx", ".xls"):
                # Engine según extensión (openpyxl para xlsx, xlrd para xls si está instalado)
                engine = "openpyxl" if ext == ".xlsx" else "xlrd"
                df_in = pd.read_excel(up, dtype=str, engine=engine)
            else:
                st.error("Formato no soportado.")
        except Exception as e:
            st.error(f"No pude leer el archivo: {e}")
            df_in = None

        if df_in is not None:
            # Limpieza leve: recorta espacios en strings
            for c in df_in.columns:
                if pd.api.types.is_string_dtype(df_in[c]):
                    df_in[c] = df_in[c].str.strip()

            if df_in.empty:
                st.info("El archivo está vacío.")
            else:
                st.caption(f"Filas leídas: **{len(df_in)}**")
                st.dataframe(df_in.head(50), use_container_width=True)

                # 👇 NUEVO: modo simulación
                dry_run = st.checkbox("Simular sin escribir en BD (recomendado)", value=True, key="dryrun_excel")

                if st.button("✅ Procesar asignaciones de EAN", key="btn_procesar_ean"):
                    try:
                        res = procesar_excel_asignar_ean(df_in, dry_run=dry_run)
                        if res.empty:
                            st.info("No hubo filas procesables.")
                        else:
                            updated   = int((res['status'] == 'updated').sum())
                            skipped   = int((res['status'] == 'skipped').sum())
                            conflict  = int((res['status'] == 'conflict').sum())
                            notfound  = int((res['status'] == 'notfound').sum())
                            ambiguous = int((res['status'] == 'ambiguous').sum())
                            would_upd = int((res['status'] == 'would_update').sum()) if 'would_update' in res['status'].unique() else 0
                            errors    = int((res['status'] == 'error').sum())

                            st.success(
                                f"Procesadas: {len(res)}. "
                                f"{'Para escribir=' + str(would_upd) + ' (simulación). ' if dry_run else ''}"
                                f"Actualizadas={updated}, "
                                f"Saltadas={skipped}, "
                                f"Conflictos={conflict}, "
                                f"Ambiguas={ambiguous}, "
                                f"No encontradas={notfound}, "
                                f"Errores={errors}"
                            )

                            st.dataframe(res, use_container_width=True)

                            # Descargas: CSV + XLSX
                            st.download_button(
                                "⬇️ Descargar resultados (CSV)",
                                res.to_csv(index=False).encode("utf-8"),
                                file_name="resultado_asignar_ean.csv",
                                mime="text/csv",
                                key="btn_res_csv"
                            )
                            st.download_button(
                                "⬇️ Descargar resultados (XLSX)",
                                _df_to_excel_bytes(res, sheet_name="resultado"),
                                file_name="resultado_asignar_ean.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="btn_res_xlsx"
                            )
                    except Exception as e:
                        st.error(f"Fallo al procesar: {e}")
