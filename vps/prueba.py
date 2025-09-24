# prueba.py
# Vista "prueba": último snapshot por (tienda, SKU) o TODAS las capturas en un rango de fechas
# (sin input de EAN, con filtros por tienda/categoría/subcategoría/marca/fabricante, texto libre y oferta)
# Cambios:
# - No ejecuta consulta por defecto: se requiere pulsar "Buscar".
# - Solo una tienda a la vez (selectbox).
# - Modo por defecto: "Solo último snapshot".
# - Índices: se intentan crear automáticamente (silencioso) al lanzar la consulta (una vez por sesión).
# - Tabla limitada para visualización; descargas traen el resultado completo.

import io, os, re, unicodedata
from typing import List, Dict, Tuple
from datetime import datetime, date, timedelta

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine
from sqlalchemy import text

# ==========================
# Helpers locales
# ==========================
EFFECTIVE_PRICE_EXPR = """
CASE
  WHEN h.precio_oferta IS NOT NULL
       AND h.precio_oferta > 0
       AND h.precio_lista IS NOT NULL
       AND h.precio_oferta < h.precio_lista
    THEN h.precio_oferta
  ELSE h.precio_lista
END
"""

def _normalize_col(s: str) -> str:
    s = s.strip().lower()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = s.replace("  ", " ").replace("_", " ").strip()
    return s

def df_to_excel_bytes_multi(df: pd.DataFrame, sheet_base="data", chunk_size=1_000_000) -> bytes:
    """
    Exporta a XLSX. Si supera ~1,048,576 filas por hoja, divide en varias hojas.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if len(df) <= 1_048_000:
            df.to_excel(writer, index=False, sheet_name=sheet_base)
        else:
            n = len(df)
            parts = (n + chunk_size - 1) // chunk_size
            for i in range(parts):
                start = i * chunk_size
                end = min((i+1)*chunk_size, n)
                sheet_name = f"{sheet_base}_{i+1}"
                df.iloc[start:end].to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def _read_df(engine: Engine, q: str, params: Dict | None = None) -> pd.DataFrame:
    params = params or {}
    with engine.connect() as conn:
        return pd.read_sql(text(q), conn, params=params)

# ==========================
# Índices sugeridos (silencioso)
# ==========================
@st.cache_data(ttl=3600, show_spinner=False)
def _current_schema(engine: Engine) -> str:
    q = "SELECT DATABASE() AS dbname"
    with engine.connect() as conn:
        row = conn.execute(text(q)).mappings().first()
        return row["dbname"]

def _index_exists(engine: Engine, schema: str, table: str, index_name: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.statistics
    WHERE table_schema = :schema AND table_name = :table AND index_name = :index_name
    LIMIT 1
    """
    with engine.connect() as conn:
        r = conn.execute(text(q), {"schema": schema, "table": table, "index_name": index_name}).first()
        return r is not None

def _create_index(engine: Engine, table: str, index_name: str, columns_sql: str):
    ddl = f"CREATE INDEX {index_name} ON {table} {columns_sql}"
    with engine.begin() as conn:
        conn.execute(text(ddl))

@st.cache_resource(show_spinner=False)
def _ensure_perf_indexes_once(engine: Engine) -> bool:
    """
    Intenta crear índices útiles una sola vez por proceso.
    Devuelve True si se crearon/ya existían, False si falló algo.
    """
    try:
        schema = _current_schema(engine)
        todo = [
            ("historico_precios", "idx_hp_tienda_prod_capturado", "(`tienda_id`,`producto_tienda_id`,`capturado_en`)"),
            ("historico_precios", "idx_hp_capturado", "(`capturado_en`)"),
            ("productos", "idx_prod_categoria", "(`categoria`)"),
            ("productos", "idx_prod_subcategoria", "(`subcategoria`)"),
            ("productos", "idx_prod_marca", "(`marca`)"),
            ("productos", "idx_prod_fabricante", "(`fabricante`)"),
            ("productos", "idx_prod_ean", "(`ean`)"),
            ("producto_tienda", "idx_pt_producto", "(`producto_id`)"),
            ("producto_tienda", "idx_pt_sku", "(`sku_tienda`)"),
        ]
        for table, idx, cols in todo:
            try:
                if not _index_exists(engine, schema, table, idx):
                    _create_index(engine, table, idx, cols)
            except Exception:
                # Ignorar errores por índice individual (no bloquea la consulta)
                pass
        return True
    except Exception:
        return False

# ==========================
# Data access cacheado
# ==========================
@st.cache_data(ttl=300, show_spinner=False)
def get_tiendas(_engine: Engine):
    q = "SELECT id, codigo, nombre FROM tiendas ORDER BY nombre"
    return _read_df(_engine, q)

@st.cache_data(ttl=300, show_spinner=False)
def get_categorias(_engine: Engine):
    q = "SELECT DISTINCT categoria FROM productos WHERE categoria IS NOT NULL AND categoria<>'' ORDER BY 1"
    return _read_df(_engine, q)

@st.cache_data(ttl=300, show_spinner=False)
def get_marcas(_engine: Engine):
    q = "SELECT DISTINCT marca FROM productos WHERE marca IS NOT NULL AND marca<>'' ORDER BY 1"
    return _read_df(_engine, q)

@st.cache_data(ttl=300, show_spinner=False)
def get_subcategorias(_engine: Engine, categorias: List[str]):
    if not categorias:
        return pd.DataFrame(columns=["subcategoria"])
    placeholders = ",".join([f":cat{i}" for i in range(len(categorias))])
    params = {f"cat{i}": v for i, v in enumerate(categorias)}
    q = f"""
    SELECT DISTINCT subcategoria
    FROM productos
    WHERE subcategoria IS NOT NULL AND subcategoria<>'' AND categoria IN ({placeholders})
    ORDER BY 1
    """
    return _read_df(_engine, q, params)

@st.cache_data(ttl=300, show_spinner=False)
def get_fabricantes(_engine: Engine):
    q = """
    SELECT DISTINCT fabricante
    FROM productos
    WHERE fabricante IS NOT NULL AND fabricante <> ''
    ORDER BY 1
    """
    return _read_df(_engine, q)

# ==========================
# WHERE dinámico (sin filtros por EAN)
# ==========================
def build_where_prueba(f: dict) -> Tuple[str, Dict]:
    where_parts = ["1=1"]
    params: Dict = {}

    if f["tiendas_sel_ids"]:
        ids = ",".join(str(int(i)) for i in f["tiendas_sel_ids"])
        where_parts.append(f"h.tienda_id IN ({ids})")

    if f["cats_sel"]:
        ph = ",".join([f":cat{i}" for i in range(len(f["cats_sel"]))])
        where_parts.append(f"p.categoria IN ({ph})")
        params.update({f"cat{i}": v for i, v in enumerate(f["cats_sel"])})

    if f["subs_sel"]:
        ph = ",".join([f":sub{i}" for i in range(len(f["subs_sel"]))])
        where_parts.append(f"p.subcategoria IN ({ph})")
        params.update({f"sub{i}": v for i, v in enumerate(f["subs_sel"])})

    if f["marcas_sel"]:
        ph = ",".join([f":marca{i}" for i in range(len(f["marcas_sel"]))])
        where_parts.append(f"p.marca IN ({ph})")
        params.update({f"marca{i}": v for i, v in enumerate(f["marcas_sel"])})

    if f["fabricantes_sel"]:
        ph = ",".join([f":fab{i}" for i in range(len(f["fabricantes_sel"]))])
        where_parts.append(f"p.fabricante IN ({ph})")
        params.update({f"fab{i}": v for i, v in enumerate(f["fabricantes_sel"])})

    # Búsqueda libre
    qtext = (f.get("qtext") or "").strip().lower()
    if qtext:
        where_parts.append("""
            LOWER(CONCAT_WS(' ',
                COALESCE(pt.nombre_tienda,''), COALESCE(p.nombre,''),
                COALESCE(p.marca,''), COALESCE(p.fabricante,''),
                COALESCE(p.ean,''), COALESCE(pt.sku_tienda,'')
            )) LIKE :qtext
        """)
        params["qtext"] = f"%{qtext}%"

    if f.get("solo_en_oferta"):
        where_parts.append("""
            (h.precio_oferta IS NOT NULL AND h.precio_oferta > 0
             AND h.precio_lista IS NOT NULL
             AND h.precio_oferta < h.precio_lista
             AND (h.tipo_oferta IS NULL OR h.tipo_oferta NOT LIKE '%Precio regular%'))
        """)

    # Rango de fechas
    if f.get("use_date_range"):
        where_parts.append("h.capturado_en >= :d_from AND h.capturado_en <= :d_to")
        params["d_from"] = f["d_from"]
        params["d_to"]   = f["d_to"]

    return " AND ".join(where_parts), params

# ==========================
# Consultas principales
# ==========================
@st.cache_data(ttl=300, show_spinner=False)
def get_last_snapshot(_engine: Engine, where_str: str, params: Dict, effective: bool, limit: int):
    """
    Devuelve el último snapshot por (tienda, producto_tienda) dentro de los últimos 2 días.
    """
    now = datetime.now()
    d_from = now - timedelta(days=2)

    where_no_dates = where_str.replace(
        "h.capturado_en >= :d_from AND h.capturado_en <= :d_to", "1=1"
    )
    where_aug = f"({where_no_dates}) AND h.capturado_en >= :__last2_from AND h.capturado_en <= :__last2_to"

    qparams = dict(params)
    qparams["__last2_from"] = d_from
    qparams["__last2_to"]   = now

    price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
    q = f"""
    WITH ult AS (
      SELECT
        h.tienda_id,
        h.producto_tienda_id,
        MAX(h.capturado_en) AS maxc
      FROM historico_precios h
      JOIN producto_tienda pt ON pt.id = h.producto_tienda_id
      JOIN productos p        ON p.id  = pt.producto_id
      WHERE {where_aug}
      GROUP BY h.tienda_id, h.producto_tienda_id
    )
    SELECT
      u.tienda_id                                     AS TIENDA_ID,
      u.producto_tienda_id                            AS PRODUCTO_TIENDA_ID,
      t.nombre                                        AS TIENDA,
      p.ean                                           AS EAN,
      COALESCE(pt.nombre_tienda, p.nombre)            AS PRODUCTO,
      p.categoria                                     AS CATEGORIA,
      p.subcategoria                                  AS SUBCATEGORIA,
      p.marca                                         AS MARCA,
      p.fabricante                                    AS FABRICANTE,
      h.precio_lista                                  AS PRECIO_LISTA,
      CASE
        WHEN h.precio_oferta IS NULL THEN NULL
        WHEN h.precio_lista  IS NULL THEN h.precio_oferta
        WHEN (h.tipo_oferta IS NULL AND h.precio_oferta = h.precio_lista OR h.tipo_oferta LIKE '%Precio regular%') THEN NULL
        ELSE h.precio_oferta
      END                                             AS PRECIO_OFERTA,
      {price_expr}                                    AS PRECIO_EFECTIVO,
      (h.precio_oferta IS NOT NULL AND h.precio_oferta>0 AND h.precio_lista IS NOT NULL AND h.precio_oferta < h.precio_lista) AS EN_OFERTA,
      h.tipo_oferta                                   AS TIPO_OFERTA,
      pt.sku_tienda                                   AS SKU_TIENDA,
      pt.record_id_tienda                             AS RECORD_ID,
      pt.url_tienda                                   AS URL,
      h.capturado_en                                  AS CAPTURADO_EN
    FROM ult u
    JOIN historico_precios h
      ON h.tienda_id = u.tienda_id
     AND h.producto_tienda_id = u.producto_tienda_id
     AND h.capturado_en = u.maxc
    JOIN producto_tienda pt ON pt.id = u.producto_tienda_id
    JOIN productos p        ON p.id  = pt.producto_id
    JOIN tiendas t          ON t.id  = u.tienda_id
    ORDER BY t.nombre, PRODUCTO
    LIMIT {int(limit)}
    """
    return _read_df(_engine, q, qparams)

@st.cache_data(ttl=300, show_spinner=False)
def get_snapshots_in_range(_engine: Engine, where_str: str, params: Dict, effective: bool, limit: int):
    """
    Devuelve TODAS las filas en el rango de fechas seleccionado (limitado por 'limit').
    """
    price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
    q = f"""
    SELECT
      h.tienda_id                                     AS TIENDA_ID,
      h.producto_tienda_id                            AS PRODUCTO_TIENDA_ID,
      t.nombre                                        AS TIENDA,
      p.ean                                           AS EAN,
      COALESCE(pt.nombre_tienda, p.nombre)            AS PRODUCTO,
      p.categoria                                     AS CATEGORIA,
      p.subcategoria                                  AS SUBCATEGORIA,
      p.marca                                         AS MARCA,
      p.fabricante                                    AS FABRICANTE,
      h.precio_lista                                  AS PRECIO_LISTA,
      CASE
        WHEN h.precio_oferta IS NULL THEN NULL
        WHEN h.precio_lista  IS NULL THEN h.precio_oferta
        WHEN (h.tipo_oferta IS NULL AND h.precio_oferta = h.precio_lista OR h.tipo_oferta LIKE '%Precio regular%') THEN NULL
        ELSE h.precio_oferta
      END                                             AS PRECIO_OFERTA,
      {price_expr}                                    AS PRECIO_EFECTIVO,
      (h.precio_oferta IS NOT NULL AND h.precio_oferta>0 AND h.precio_lista IS NOT NULL AND h.precio_oferta < h.precio_lista) AS EN_OFERTA,
      h.tipo_oferta                                   AS TIPO_OFERTA,
      pt.sku_tienda                                   AS SKU_TIENDA,
      pt.record_id_tienda                             AS RECORD_ID,
      pt.url_tienda                                   AS URL,
      h.capturado_en                                  AS CAPTURADO_EN
    FROM historico_precios h
    JOIN producto_tienda pt ON pt.id = h.producto_tienda_id
    JOIN productos p        ON p.id  = pt.producto_id
    JOIN tiendas t          ON t.id  = h.tienda_id
    WHERE {where_str}
    ORDER BY t.nombre, PRODUCTO, h.capturado_en
    LIMIT {int(limit)}
    """
    return _read_df(_engine, q, params)

@st.cache_data(ttl=300, show_spinner=False)
def reduce_to_last_per_sku(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce el rango a la última captura por (TIENDA_ID, PRODUCTO_TIENDA_ID) en memoria.
    """
    if df.empty:
        return df
    tmp = df.sort_values(["TIENDA_ID", "PRODUCTO_TIENDA_ID", "CAPTURADO_EN"]).groupby(
        ["TIENDA_ID", "PRODUCTO_TIENDA_ID"], as_index=False
    ).tail(1)
    return tmp.sort_values(["TIENDA", "PRODUCTO"])

# ==========================
# Sidebar + Vista
# ==========================
def sidebar_prueba(engine: Engine):
    st.sidebar.header("Filtros – Último snapshot o Rango de fechas")

    if "do_search" not in st.session_state:
        st.session_state["do_search"] = False

    # --- Formulario de filtros ---
    with st.sidebar.form("flt_form"):
        mode = st.radio(
            "Modo de consulta",
            options=["Solo último snapshot (recomendado)", "Rango de fechas (todas las capturas)"],
            index=0,
            help="En 'Rango' se traen todas las capturas entre fechas; en 'Solo último' se ignora el rango."
        )
        use_date_range = (mode == "Rango de fechas (todas las capturas)")

        # Fechas
        today = date.today()
        default_from = today - timedelta(days=7)
        d_from_date, d_to_date = default_from, today
        if use_date_range:
            rango = st.date_input(
                "Rango de fechas (incluyente)",
                value=(default_from, today),
                help="Se aplica solo en modo 'Rango de fechas'."
            )
            if isinstance(rango, (list, tuple)) and len(rango) == 2:
                d_from_date, d_to_date = rango

        d_from = datetime.combine(d_from_date, datetime.min.time())
        d_to   = datetime.combine(d_to_date, datetime.max.time())

        # Tienda (una sola)
        tiendas_df = get_tiendas(engine)
        opciones_tiendas = ["— Seleccionar —"] + tiendas_df["nombre"].tolist()
        tienda_nombre = st.selectbox("Tienda (una sola)", opciones_tiendas, index=0)
        if tienda_nombre != "— Seleccionar —":
            tienda_id = int(tiendas_df.loc[tiendas_df["nombre"] == tienda_nombre, "id"].iloc[0])
            tiendas_sel_ids = [tienda_id]
            tiendas_sel_nombres = [tienda_nombre]
        else:
            tiendas_sel_ids, tiendas_sel_nombres = [], []

        # Cat/Sub/Marca/Fabricante
        cats_df = get_categorias(engine)
        cats_sel = st.multiselect("Categorías", cats_df["categoria"].tolist(), default=[])

        subs_df = get_subcategorias(engine, cats_sel) if cats_sel else pd.DataFrame(columns=["subcategoria"])
        subs_sel = st.multiselect("Subcategorías", subs_df["subcategoria"].tolist(), default=[])

        marcas_df = get_marcas(engine)
        marcas_sel = st.multiselect("Marcas", marcas_df["marca"].tolist(), default=[])

        fabs_df = get_fabricantes(engine)
        fabricantes_sel = st.multiselect("Fabricantes", fabs_df["fabricante"].tolist(), default=[])

        # Texto libre
        qtext = st.text_input("Buscar texto (producto/marca/fabricante/EAN/SKU)", "")

        use_effective = st.toggle("Usar precio efectivo (oferta si válida)", value=True)
        solo_en_oferta = st.toggle("Solo productos en oferta válida", value=False)

        reduce_last_in_range = False
        if use_date_range:
            reduce_last_in_range = st.toggle(
                "Reducir a último por (tienda, SKU) dentro del rango",
                value=False,
                help="Tras traer el rango, limita a la última captura por SKU."
            )

        # Límites: visual vs descarga
        display_limit = st.number_input(
            "Máx. filas a mostrar (tabla)",
            min_value=5_000, max_value=500_000, value=100_000, step=5_000,
            help="Para rendimiento de la interfaz. No afecta a las descargas."
        )
        download_limit = st.number_input(
            "Límite de filas (descarga/consulta)",
            min_value=10_000, max_value=10_000_000, value=1_000_000, step=50_000,
            help="Límite para la consulta en memoria y para las descargas."
        )

        submitted = st.form_submit_button("🔎 Buscar", type="primary", use_container_width=True)
        if submitted:
            st.session_state["do_search"] = True

    return {
        "mode": mode,
        "use_date_range": use_date_range,
        "d_from": d_from,
        "d_to": d_to,
        "tiendas_sel_ids": tiendas_sel_ids,
        "tiendas_sel_nombres": tiendas_sel_nombres,
        "cats_sel": cats_sel,
        "subs_sel": subs_sel,
        "marcas_sel": marcas_sel,
        "fabricantes_sel": fabricantes_sel,
        "qtext": qtext,
        "use_effective": use_effective,
        "solo_en_oferta": solo_en_oferta,
        "reduce_last_in_range": reduce_last_in_range,
        "display_limit": int(display_limit),
        "download_limit": int(download_limit),
        "do_search": st.session_state["do_search"],
    }

def prueba(engine: Engine):
    st.markdown("##  Reporte rápido — Snapshot por tienda (último o rango)")
    st.caption("Primero selecciona filtros y pulsa **Buscar**. La tabla muestra hasta N filas por rendimiento; las descargas traen el resultado completo.")

    f = sidebar_prueba(engine)

    # No consultar hasta que haya búsqueda + tienda
    if not f["do_search"]:
        st.info("Selecciona **al menos una tienda** y pulsa **Buscar** para ejecutar la consulta.")
        return
    if not f["tiendas_sel_ids"]:
        st.warning("Debes seleccionar una tienda antes de buscar.")
        return

    # (Silencioso) Intento de creación de índices (solo una vez por sesión/proceso)
    _ensure_perf_indexes_once(engine)

    # WHERE y consulta (limit = download_limit)
    where_str, params = build_where_prueba(f)

    with st.spinner("Ejecutando consulta..."):
        if f["use_date_range"]:
            df_full = get_snapshots_in_range(engine, where_str, params, f["use_effective"], f["download_limit"])
            if f["reduce_last_in_range"]:
                df_full = reduce_to_last_per_sku(df_full)
        else:
            df_full = get_last_snapshot(engine, where_str, params, f["use_effective"], f["download_limit"])

    if df_full.empty:
        st.info("No hay datos con los filtros actuales.")
        return

    price_col = "PRECIO_EFECTIVO" if f["use_effective"] else "PRECIO_LISTA"

    # KPIs (sobre df completo)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Filas (resultado)", len(df_full))
    c2.metric("SKUs distintos", df_full["SKU_TIENDA"].nunique())
    c3.metric("Precio medio", f"${df_full[price_col].dropna().mean():.2f}" if df_full[price_col].notna().any() else "—")
    c4.metric("Share en oferta", f"{(df_full['EN_OFERTA'].mean()*100):.1f}%" if "EN_OFERTA" in df_full.columns and df_full["EN_OFERTA"].notna().any() else "—")

    # Filtro por precio en memoria
    df = df_full
    if df[price_col].notna().any():
        pmin = float(np.nanmin(df[price_col])); pmax = float(np.nanmax(df[price_col]))
        if np.isfinite(pmin) and np.isfinite(pmax) and pmin < pmax:
            pr1, pr2 = st.slider(
                "Filtrar por precio",
                min_value=float(np.floor(pmin)),
                max_value=float(np.ceil(pmax)),
                value=(float(np.floor(pmin)), float(np.ceil(pmax))),
                step=1.0
            )
            df = df[(df[price_col] >= pr1) & (df[price_col] <= pr2)]
        else:
            st.caption("Rango de precio único; se omite el filtro por rango.")
    else:
        st.caption("Sin valores de precio para aplicar filtro por rango.")

    # Orden y columnas
    st.subheader("Resultados")
    cols_default = [
        "TIENDA_ID","PRODUCTO_TIENDA_ID",
        "TIENDA","PRODUCTO","EAN","MARCA","FABRICANTE","CATEGORIA","SUBCATEGORIA",
        "PRECIO_LISTA","PRECIO_OFERTA","PRECIO_EFECTIVO","EN_OFERTA","SKU_TIENDA","URL","CAPTURADO_EN"
    ]
    columnas_visibles = st.multiselect(
        "Columnas visibles",
        df.columns.tolist(),
        default=[c for c in cols_default if c in df.columns]
    )

    ordenar_por = st.selectbox(
        "Ordenar por",
        ["TIENDA","PRODUCTO","MARCA","FABRICANTE","CATEGORIA","SUBCATEGORIA", price_col, "TIENDA_ID","PRODUCTO_TIENDA_ID","CAPTURADO_EN"]
    )
    asc = st.toggle("Ascendente", value=True)
    df = df.sort_values(ordenar_por, ascending=asc)

    # Mostrar solo hasta display_limit
    display_cut = min(f["display_limit"], len(df))
    df_view = df.iloc[:display_cut]
    st.dataframe(df_view[columnas_visibles], use_container_width=True, height=420)
    if len(df) > display_cut:
        st.caption(f"Mostrando {display_cut:,} de {len(df):,} filas por rendimiento. Descarga para obtener todo el resultado.")

    # Descargas completas (sobre df filtrado/ordenado, NO recortado)
    fname_base = "rango_snapshot" if f["use_date_range"] else "ultimo_snapshot"
    st.download_button(
        "⬇️ CSV (completo)",
        df[columnas_visibles].to_csv(index=False).encode("utf-8"),
        file_name=f"{fname_base}.csv",
        mime="text/csv"
    )

    try:
        xlsx_bytes = df_to_excel_bytes_multi(df[columnas_visibles], sheet_base="data", chunk_size=1_000_000)
        st.download_button(
            "⬇️ XLSX (completo)",
            xlsx_bytes,
            file_name=f"{fname_base}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.error(f"No se pudo generar XLSX: {e}. Usa CSV (completo) como alternativa.")
