# app.py
# Retail Analytics – Streamlit para esquema analisis_retail
# Ejecutar: streamlit run app.py

import io
import os
import re
import unicodedata
from datetime import datetime, timedelta
from typing import List, Tuple, Dict

import altair as alt
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text

# ---------- Config página ----------
st.set_page_config(page_title="Retail Analytics", layout="wide")
alt.data_transformers.disable_max_rows()

# ---------- Conexión MySQL fija ----------
DB_HOST = "localhost"     # conexión local
DB_PORT = 3310            # puerto 3310
DB_USER = "root"          # usuario MySQL
DB_PASS = ""              # contraseña MySQL (vacía)
DB_NAME = "analisis_retail"

@st.cache_resource(show_spinner=False)
def get_engine():
    url = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

# Helper central para ejecutar SQL con parámetros nombrados (:param)
def read_df(q: str, params: Dict | None = None) -> pd.DataFrame:
    params = params or {}
    with engine.connect() as conn:
        return pd.read_sql(text(q), conn, params=params)

# ---------- Helpers ----------
EFFECTIVE_PRICE_EXPR = """
CASE
  WHEN h.precio_oferta IS NOT NULL AND h.precio_oferta > 0
       AND (h.precio_lista IS NULL OR h.precio_oferta <= h.precio_lista)
    THEN h.precio_oferta
  ELSE h.precio_lista
END
"""

def _normalize_col(s: str) -> str:
    s = s.strip().lower()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = s.replace("  ", " ").replace("_", " ").strip()
    return s

def _clean_ean_value(x) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    s = re.sub(r"\D+", "", s)  # sólo dígitos, preserva ceros
    # Acepta longitudes típicas (ajusta si necesitas)
    if len(s) in {8, 12, 13, 14}:
        return s
    # Si prefieres estrictos, quita la línea de abajo
    return s if s else None

def to_list_str(s: str) -> List[str]:
    if not s:
        return []
    raw = [x.strip() for x in s.replace(";", ",").split(",") if x.strip()]
    cleaned = [_clean_ean_value(x) for x in raw]
    return [x for x in cleaned if x]

def read_eans_from_uploaded(file) -> List[str]:
    """Lee EANs de .txt, .csv, .xlsx, .xls. Detecta columnas como 'Cód.Barras'."""
    name = getattr(file, "name", "upload")
    ext = os.path.splitext(name)[1].lower()

    ean_aliases = {
        "ean", "ean13", "gtin",
        "cod barras", "cod. barras", "codigo barras", "codigo de barras", "código de barras",
        "cod.barras", "cód.barras", "cód. barras", "codbarras", "codigo ean", "código ean"
    }

    if ext in {".txt", ".csv"}:
        try:
            content = file.read().decode("utf-8", errors="ignore")
        except Exception:
            try:
                file.seek(0)
                content = file.read().decode("utf-8", errors="ignore")
            except Exception:
                return []
        return to_list_str(content)

    if ext in {".xlsx", ".xls"}:
        try:
            xls = pd.ExcelFile(file)
            for sh in xls.sheet_names:
                df = pd.read_excel(xls, sh)
                if df is None or df.empty:
                    continue
                colmap = {c: _normalize_col(str(c)) for c in df.columns}
                target_col = None
                # 1) match directo por alias
                for orig, norm in colmap.items():
                    norm2 = norm.replace(".", "").replace("-", " ").replace("  ", " ").strip()
                    if norm in ean_aliases or norm2 in ean_aliases:
                        target_col = orig
                        break
                # 2) fallback: cualquier "cod*barra*"
                if target_col is None:
                    for orig in df.columns:
                        n = _normalize_col(str(orig))
                        if n.startswith("cod") and "barra" in n:
                            target_col = orig
                            break
                if target_col is None:
                    continue
                vals = df[target_col].tolist()
                cleaned = [_clean_ean_value(v) for v in vals]
                cleaned = [v for v in cleaned if v]
                if cleaned:
                    return cleaned
        except Exception:
            return []

    return []

def df_to_excel_bytes(df: pd.DataFrame, sheet_name="data") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

# ---------- Caches de datos base ----------
@st.cache_data(ttl=300, show_spinner=False)
def get_minmax_dates():
    q = "SELECT DATE(MIN(capturado_en)) AS min_d, DATE(MAX(capturado_en)) AS max_d FROM historico_precios"
    return read_df(q)

@st.cache_data(ttl=300, show_spinner=False)
def get_tiendas():
    q = "SELECT id, codigo, nombre FROM tiendas ORDER BY nombre"
    return read_df(q)

@st.cache_data(ttl=300, show_spinner=False)
def get_categorias():
    q = "SELECT DISTINCT categoria FROM productos WHERE categoria IS NOT NULL AND categoria<>'' ORDER BY 1"
    return read_df(q)

@st.cache_data(ttl=300, show_spinner=False)
def get_marcas():
    q = "SELECT DISTINCT marca FROM productos WHERE marca IS NOT NULL AND marca<>'' ORDER BY 1"
    return read_df(q)

@st.cache_data(ttl=300, show_spinner=False)
def get_subcategorias(categorias: List[str]):
    if not categorias:
        return pd.DataFrame(columns=["subcategoria"])
    placeholders = ",".join([f":cat{i}" for i in range(len(categorias))])
    params = {f"cat{i}": v for i, v in enumerate(categorias)}
    q = f"""
    SELECT DISTINCT subcategoria
    FROM productos
    WHERE subcategoria IS NOT NULL AND subcategoria<>''
      AND categoria IN ({placeholders})
    ORDER BY 1
    """
    return read_df(q, params)

# ---------- Sidebar filtros ----------
st.sidebar.header("Filtros")

dm = get_minmax_dates()
if dm.empty or pd.isna(dm.loc[0, "min_d"]):
    st.sidebar.warning("No hay datos en historico_precios todavía.")
    st.stop()

min_d = pd.to_datetime(dm.loc[0, "min_d"]).date()
max_d = pd.to_datetime(dm.loc[0, "max_d"]).date()
default_start = max(min_d, max_d - timedelta(days=29))

date_range = st.sidebar.date_input(
    "Rango de fechas (capturado_en UTC)",
    value=(default_start, max_d),
    min_value=min_d, max_value=max_d
)
if isinstance(date_range, tuple):
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, date_range

tiendas_df = get_tiendas()
tiendas_sel_nombres = st.sidebar.multiselect(
    "Tiendas",
    tiendas_df["nombre"].tolist(),
    default=tiendas_df["nombre"].tolist()
)
tiendas_sel_ids = tiendas_df.loc[tiendas_df["nombre"].isin(tiendas_sel_nombres), "id"].astype(int).tolist()

cats_df = get_categorias()
cats_sel = st.sidebar.multiselect("Categorías", cats_df["categoria"].tolist(), default=[])

subs_df = get_subcategorias(cats_sel) if cats_sel else pd.DataFrame(columns=["subcategoria"])
subs_sel = st.sidebar.multiselect("Subcategorías", subs_df["subcategoria"].tolist(), default=[])

marcas_df = get_marcas()
marcas_sel = st.sidebar.multiselect("Marcas", marcas_df["marca"].tolist(), default=[])

# Carga de EANs: texto + archivo (txt/csv/xlsx/xls)
ean_input = st.sidebar.text_area("EANs (coma o salto de línea)", "")
ean_file = st.sidebar.file_uploader("Subir EANs (.txt, .csv, .xlsx, .xls)", type=["txt", "csv", "xlsx", "xls"])

ean_list = to_list_str(ean_input)
if ean_file is not None:
    extra = read_eans_from_uploaded(ean_file)
    if not extra:
        st.sidebar.error("No se pudieron leer EANs del archivo subido.")
    else:
        ean_list += extra

# Deduplicar preservando orden
_seen = set()
ean_list = [x for x in ean_list if not (x in _seen or _seen.add(x))]
st.sidebar.caption(f"EANs cargados: **{len(ean_list)}**")

use_effective = st.sidebar.toggle("Usar precio efectivo (oferta si válida)", value=True)
sample_limit = st.sidebar.number_input("Límite de filas para vistas tabulares", 1000, 200000, 5000, step=500)

# ---------- WHERE dinámico con parámetros nombrados ----------
def build_where(base_alias: str = "h") -> Tuple[str, Dict]:
    where_parts = [f"{base_alias}.capturado_en BETWEEN :start_dt AND :end_dt"]
    params: Dict = {
        "start_dt": datetime.combine(start_date, datetime.min.time()),
        "end_dt": datetime.combine(end_date, datetime.max.time()),
    }

    if tiendas_sel_ids:
        ids = ",".join(str(int(i)) for i in tiendas_sel_ids)
        where_parts.append(f"{base_alias}.tienda_id IN ({ids})")

    if cats_sel:
        placeholders = ",".join([f":cat{i}" for i in range(len(cats_sel))])
        where_parts.append(f"p.categoria IN ({placeholders})")
        params.update({f"cat{i}": v for i, v in enumerate(cats_sel)})

    if subs_sel:
        placeholders = ",".join([f":sub{i}" for i in range(len(subs_sel))])
        where_parts.append(f"p.subcategoria IN ({placeholders})")
        params.update({f"sub{i}": v for i, v in enumerate(subs_sel)})

    if marcas_sel:
        placeholders = ",".join([f":marca{i}" for i in range(len(marcas_sel))])
        where_parts.append(f"p.marca IN ({placeholders})")
        params.update({f"marca{i}": v for i, v in enumerate(marcas_sel)})

    if ean_list:
        placeholders = ",".join([f":ean{i}" for i in range(len(ean_list))])
        where_parts.append(f"p.ean IN ({placeholders})")
        params.update({f"ean{i}": v for i, v in enumerate(ean_list)})

    return " AND ".join(where_parts), params

# ---------- Panel ----------
st.markdown("##  Panel de Investigación de Mercado")

where_str, where_params = build_where("h")

# ---------- Datos diarios promedio ----------
@st.cache_data(ttl=300, show_spinner=False)
def get_daily_avg(where_str: str, params: dict, effective: bool):
    price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
    q = f"""
    WITH ult AS (
      SELECT DATE(h.capturado_en) AS d, h.tienda_id, h.producto_tienda_id, MAX(h.capturado_en) AS maxc
      FROM historico_precios h
      JOIN producto_tienda pt ON pt.id = h.producto_tienda_id
      JOIN productos p ON p.id = pt.producto_id
      WHERE {where_str}
      GROUP BY DATE(h.capturado_en), h.tienda_id, h.producto_tienda_id
    )
    SELECT u.d, t.nombre AS tienda, AVG({price_expr}) AS precio_promedio
    FROM ult u
    JOIN historico_precios h ON h.tienda_id = u.tienda_id AND h.producto_tienda_id = u.producto_tienda_id AND h.capturado_en = u.maxc
    JOIN tiendas t ON t.id = u.tienda_id
    JOIN producto_tienda pt ON pt.id = u.producto_tienda_id
    JOIN productos p ON p.id = pt.producto_id
    WHERE {where_str}
    GROUP BY u.d, t.nombre
    ORDER BY u.d, t.nombre
    """
    return read_df(q, params)

daily = get_daily_avg(where_str, where_params, use_effective)

# ---------- KPIs ----------
@st.cache_data(ttl=300, show_spinner=False)
def get_kpis(where_str: str, params: Dict):
    q = f"""
    WITH ult AS (
      SELECT DATE(h.capturado_en) AS d, h.tienda_id, h.producto_tienda_id, MAX(h.capturado_en) AS maxc
      FROM historico_precios h
      JOIN producto_tienda pt ON pt.id=h.producto_tienda_id
      JOIN productos p ON p.id=pt.producto_id
      WHERE {where_str}
      GROUP BY DATE(h.capturado_en), h.tienda_id, h.producto_tienda_id
    ),
    snap AS (
      SELECT u.d, u.tienda_id, u.producto_tienda_id,
             h.precio_lista, h.precio_oferta,
             {EFFECTIVE_PRICE_EXPR} AS precio_efectivo,
             (h.precio_oferta IS NOT NULL AND h.precio_oferta>0 AND (h.precio_lista IS NULL OR h.precio_oferta<=h.precio_lista)) AS en_oferta
      FROM ult u
      JOIN historico_precios h
        ON h.tienda_id=u.tienda_id AND h.producto_tienda_id=u.producto_tienda_id AND h.capturado_en=u.maxc
    )
    SELECT
      COUNT(*) AS observaciones,
      COUNT(DISTINCT producto_tienda_id) AS productos_distintos,
      AVG(precio_efectivo) AS precio_medio,
      AVG(CASE WHEN en_oferta THEN 1 ELSE 0 END) AS share_oferta,
      AVG(CASE WHEN en_oferta AND precio_lista>0 THEN (precio_lista - precio_oferta)/precio_lista END) AS descuento_promedio
    FROM snap
    """
    return read_df(q, params)

kpis = get_kpis(where_str, where_params)
c1, c2, c3, c4, c5 = st.columns(5)
if not kpis.empty:
    c1.metric("Observaciones", int(kpis.loc[0, "observaciones"]))
    c2.metric("Productos distintos", int(kpis.loc[0, "productos_distintos"]))
    c3.metric("Precio medio", f"${kpis.loc[0, 'precio_medio']:.2f}" if pd.notna(kpis.loc[0, "precio_medio"]) else "—")
    c4.metric("Share en oferta", f"{kpis.loc[0, 'share_oferta']*100:.1f}%" if pd.notna(kpis.loc[0, "share_oferta"]) else "—")
    c5.metric("Descuento promedio", f"{kpis.loc[0, 'descuento_promedio']*100:.1f}%" if pd.notna(kpis.loc[0, "descuento_promedio"]) else "—")
else:
    st.info("Sin datos para los filtros seleccionados.")

# ---------- Gráfico: Evolución de precio promedio ----------
st.subheader("📈 Evolución de precios promedio por tienda")
if daily.empty:
    st.warning("No hay datos diarios para graficar.")
else:
    daily["d"] = pd.to_datetime(daily["d"])
    chart = alt.Chart(daily).mark_line(point=True).encode(
        x=alt.X("d:T", title="Fecha (UTC)"),
        y=alt.Y("precio_promedio:Q", title="Precio promedio"),
        color=alt.Color("tienda:N", title="Tienda"),
        tooltip=["tienda", alt.Tooltip("d:T", title="Fecha"), alt.Tooltip("precio_promedio:Q", title="Precio")]
    ).properties(height=350)
    st.altair_chart(chart, use_container_width=True)




# ---------- Comparador de canastas ----------
st.subheader(" Comparador de canastas (solo ean coincidentes)")
st.caption("Carga EANs en la barra lateral (texto o archivo). Calcula precio total por tienda al último snapshot dentro del rango.")

@st.cache_data(ttl=300, show_spinner=False)
def get_basket(eans: List[str], where_str: str, params: Dict, effective: bool):
    if not eans:
        return pd.DataFrame()
    price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
    placeholders = ",".join([f":be{i}" for i in range(len(eans))])
    p = params.copy()
    p.update({f"be{i}": e for i, e in enumerate(eans)})

    q = f"""
    WITH ult AS (
      SELECT h.tienda_id, h.producto_tienda_id, MAX(h.capturado_en) AS maxc
      FROM historico_precios h
      JOIN producto_tienda pt ON pt.id=h.producto_tienda_id
      JOIN productos p ON p.id=pt.producto_id
      WHERE {where_str} AND p.ean IN ({placeholders})
      GROUP BY h.tienda_id, h.producto_tienda_id
    )
    SELECT t.nombre AS tienda, p.ean, COALESCE(pt.nombre_tienda, p.nombre) AS producto,
           {price_expr} AS precio
    FROM ult u
    JOIN historico_precios h ON h.tienda_id=u.tienda_id AND h.producto_tienda_id=u.producto_tienda_id AND h.capturado_en=u.maxc
    JOIN producto_tienda pt ON pt.id=u.producto_tienda_id
    JOIN productos p ON p.id=pt.producto_id
    JOIN tiendas t ON t.id=u.tienda_id
    """
    return read_df(q, p)
basket = get_basket(ean_list, where_str, where_params, use_effective)

if not ean_list:
    st.info("Agrega EANs en la barra lateral para comparar canastas.")
elif basket.empty:
    st.warning("No se encontraron esos EANs con los filtros actuales.")
else:
    # --- 1) Tiendas relevantes con datos
    tiendas_relevantes = sorted(set(tiendas_sel_nombres) & set(basket["tienda"].unique()))
    if not tiendas_relevantes:
        st.warning("No hay coincidencias en las tiendas seleccionadas.")
    else:
        # --- 2) EANs presentes en TODAS las tiendas relevantes
        presencia = (
            basket.loc[basket["tienda"].isin(tiendas_relevantes), ["tienda", "ean"]]
            .drop_duplicates()
        )
        conteo_por_ean = presencia.groupby("ean")["tienda"].nunique()
        eans_comunes = set(conteo_por_ean[conteo_por_ean == len(tiendas_relevantes)].index)

        if not eans_comunes:
            st.warning("No hay productos que estén presentes en todas las tiendas seleccionadas.")
        else:
            # --- 3) Filtrar solo a los EANs coincidentes
            basket_comun = basket[
                basket["ean"].isin(eans_comunes) & basket["tienda"].isin(tiendas_relevantes)
            ].copy()

            # --- 4) Totales por tienda
            totales = (
                basket_comun
                .groupby("tienda", as_index=False)
                .agg(total_canasta=("precio", "sum"))
            )

            # --- 5) UI
            st.caption(f"Productos coincidentes en todas las tiendas: **{len(eans_comunes)}**")
            a, b = st.columns([1, 1])

            with a:
                st.dataframe(
                    totales.sort_values("total_canasta"),
                    use_container_width=True
                )
                st.download_button(
                    "⬇️ Descargar totales CSV",
                    totales.to_csv(index=False).encode("utf-8"),
                    file_name="totales_canasta_coincidentes.csv",
                    mime="text/csv"
                )

            with b:
                st.altair_chart(
                    alt.Chart(totales).mark_bar().encode(
                        x=alt.X("tienda:N", sort="-y", title="Tienda"),
                        y=alt.Y("total_canasta:Q", title="Total canasta (coincidentes)"),
                        tooltip=[
                            "tienda",
                            alt.Tooltip("total_canasta:Q", title="Total")
                        ]
                    ).properties(height=300),
                    use_container_width=True
                )

            with st.expander("Detalle de canasta por tienda (solo coincidentes)"):
                st.dataframe(basket_comun, use_container_width=True)





# ---------- Tabla detalle ----------
st.subheader("Tabla Reporte")
@st.cache_data(ttl=300, show_spinner=False)
def get_detail(where_str: str, params: Dict, effective: bool, limit: int):
    price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
    q = f"""
    SELECT
      s.ean,
      s.producto,
      s.categoria,
      s.subcategoria,
       s.marca,
      DATE(s.capturado_en) AS d,
      t.nombre AS tienda,
      
      
      
      s.precio_lista, s.precio_oferta, s.precio_efectivo,
      s.tipo_oferta, s.promo_texto_regular, s.promo_texto_descuento,
      s.sku_tienda, s.record_id_tienda, s.url_tienda
    FROM (
      SELECT
        h.tienda_id,
        p.ean,
        COALESCE(pt.nombre_tienda, p.nombre) AS producto,
        p.marca, p.categoria, p.subcategoria,
        h.precio_lista, h.precio_oferta,
        {price_expr} AS precio_efectivo,
        h.tipo_oferta, h.promo_texto_regular, h.promo_texto_descuento,
        pt.sku_tienda, pt.record_id_tienda, pt.url_tienda,
        h.capturado_en,
        ROW_NUMBER() OVER (
          PARTITION BY h.tienda_id, p.ean
          ORDER BY h.capturado_en DESC
        ) AS rn
      FROM historico_precios h
      JOIN producto_tienda pt ON pt.id = h.producto_tienda_id
      JOIN productos p        ON p.id  = pt.producto_id
      WHERE {where_str}
    ) AS s
    JOIN tiendas t ON t.id = s.tienda_id
    WHERE s.rn = 1
    ORDER BY t.nombre, s.ean
    LIMIT {int(limit)}
    """
    return read_df(q, params)




detail = get_detail(where_str, where_params, use_effective, sample_limit)
st.dataframe(detail, use_container_width=True, height=350)
st.download_button("⬇️ Descargar detalle (CSV)", detail.to_csv(index=False).encode("utf-8"),
                   file_name="detalle_snapshots.csv", mime="text/csv")
st.download_button("⬇️ Descargar detalle (XLSX)", df_to_excel_bytes(detail),
                   file_name="detalle_snapshots.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

