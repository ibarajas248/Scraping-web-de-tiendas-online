# app.py
# Retail Analytics – Streamlit (conexión directa)
# Ejecutar en el VPS (conexión directa, sin túnel):
#   streamlit run app.py --server.address 127.0.0.1 --server.port 8090 --server.headless true

def iniciaReporte():
    import io
    import os
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

    # ---------- Credenciales MySQL en el VPS ----------
    DB_HOST = "localhost"
    DB_PORT = 3310
    DB_USER = "root"
    DB_PASS = ""
    DB_NAME = "scrap"

    # ---------- Conexión SQLAlchemy (directa, sin túnel) ----------
    @st.cache_resource(show_spinner=False)
    def get_engine():
        url = (
            f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}"
            f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4&ssl_disabled=false"
        )
        engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={"connection_timeout": 15},
        )
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
        except Exception as e:
            st.error(f"No pude abrir la conexión a MySQL. Revisa host/puerto/SSL. Detalle: {e}")
            raise
        return engine

    engine = get_engine()

    def read_df(q: str, params: Dict | None = None) -> pd.DataFrame:
        params = params or {}
        with engine.connect() as conn:
            return pd.read_sql(text(q), conn, params=params)

    # ---------- Helpers ----------
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

    def _clean_code_value(x) -> str | None:
        if x is None:
            return None
        try:
            if isinstance(x, (int, np.integer)):
                s = str(int(x)).strip()
                return s if s else None
            if isinstance(x, (float, np.floating)):
                if np.isnan(x):
                    return None
                if float(x).is_integer():
                    s = str(int(x)).strip()
                    return s if s else None
                s = str(x).strip()
                return s if s else None
        except Exception:
            pass

        s = str(x).strip()
        if not s or s.lower() in {"nan", "none"}:
            return None
        return s  # preserva guiones y formato

    def to_list_str(s: str) -> List[str]:
        if not s:
            return []
        s2 = s.replace("\n", ",").replace(";", ",")
        raw = [x.strip() for x in s2.split(",") if x.strip()]
        cleaned = [_clean_code_value(x) for x in raw]
        seen = set()
        out = []
        for v in cleaned:
            if v and v not in seen:
                seen.add(v)
                out.append(v)
        return out

    # -------- Aliases de columnas del MAESTRO (acepta variantes) --------
    MASTER_ALIAS = {
        "CATEGORIA": {"categoria", "categoría", "cat", "departamento", "department", "categoria principal"},
        "SUBCATEGORIA": {"subcategoria", "subcategoría", "subcat", "categoria", "category", "rubro"},
        "FABRICANTE": {"fabricante", "nombre proveedor", "proveedor", "producer", "manufacturer"},
        "MARCA": {"marca", "brand"},
        "PRODUCTO": {"descripcion", "descripción", "producto", "nombre producto"}
    }

    def _pick_col(df: pd.DataFrame, names: set[str]) -> str | None:
        names_norm = {_normalize_col(x) for x in names}
        for col in df.columns:
            if _normalize_col(str(col)) in names_norm:
                return col
        return None

    def read_eans_and_attrs_from_uploaded(file) -> Tuple[List[str], pd.DataFrame | None]:
        name = getattr(file, "name", "upload")
        ext = os.path.splitext(name)[1].lower()

        if ext in {".txt", ".csv"}:
            try:
                content = file.read().decode("utf-8", errors="ignore")
            except Exception:
                try:
                    file.seek(0)
                    content = file.read().decode("utf-8", errors="ignore")
                except Exception:
                    return [], None
            return to_list_str(content), None

        if ext not in {".xlsx", ".xls"}:
            return [], None

        ean_aliases = {
            "ean", "ean13", "gtin",
            "cod barras", "cod. barras", "codigo barras", "codigo de barras", "código de barras",
            "cod.barras", "cód.barras", "cód. barras", "codbarras", "codigo ean", "código ean"
        }

        try:
            xls = pd.ExcelFile(file)
            codes_all: List[str] = []
            rows_list: List[pd.DataFrame] = []

            for sh in xls.sheet_names:
                df = pd.read_excel(xls, sh)
                if df is None or df.empty:
                    continue

                ean_col = None
                for col in df.columns:
                    n = _normalize_col(str(col))
                    n2 = n.replace(".", "").replace("-", " ").replace("  ", " ").strip()
                    if n in ean_aliases or n2 in ean_aliases:
                        ean_col = col
                        break
                if ean_col is None:
                    for col in df.columns:
                        n = _normalize_col(str(col))
                        if n.startswith("cod") and "barra" in n:
                            ean_col = col
                            break
                if ean_col is None:
                    continue

                df2 = df.copy()
                df2["EAN"] = df2[ean_col].map(_clean_code_value)
                df2 = df2.dropna(subset=["EAN"])
                if df2.empty:
                    continue

                codes_all.extend(df2["EAN"].tolist())

                for out_col, aliases in MASTER_ALIAS.items():
                    col = _pick_col(df, aliases)
                    if col:
                        df2[out_col] = df[col]

                keep = [c for c in ["EAN", "CATEGORIA", "SUBCATEGORIA", "MARCA", "FABRICANTE", "PRODUCTO"] if c in df2.columns]
                rows_list.append(df2[keep])

            attrs_df = None
            if rows_list:
                attrs_df = pd.concat(rows_list, ignore_index=True)
                for c in ["CATEGORIA", "SUBCATEGORIA", "MARCA", "FABRICANTE"]:
                    if c in attrs_df.columns:
                        attrs_df[c] = attrs_df[c].astype(str).str.strip()
                attrs_df = attrs_df.drop_duplicates(subset=["EAN"], keep="first")

            seen = set()
            codes: List[str] = []
            for e in codes_all:
                if e not in seen:
                    seen.add(e)
                    codes.append(e)

            return codes, attrs_df
        except Exception:
            return [], None

    def df_to_excel_bytes(df: pd.DataFrame, sheet_name="data") -> bytes:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        return output.getvalue()

    # ---------- SQL helpers para performance ----------
    def _placeholders(prefix: str, n: int) -> str:
        return ",".join([f":{prefix}{i}" for i in range(n)])

    def _params_list(prefix: str, values: List[str]) -> Dict:
        return {f"{prefix}{i}": v for i, v in enumerate(values)}

    def match_cte_sql(ph: str) -> str:
        """
        CTE que devuelve (producto_id, ean_match) usando índices separados (sin OR).
        """
        return f"""
        WITH matches AS (
          SELECT p.id AS producto_id, p.ean AS ean_match
          FROM productos p
          WHERE p.ean IN ({ph})

          UNION ALL

          SELECT p.id AS producto_id, p.ean_auxiliar AS ean_match
          FROM productos p
          WHERE p.ean_auxiliar IN ({ph})
        ),
        m_dedup AS (
          SELECT producto_id, ean_match,
                 ROW_NUMBER() OVER (PARTITION BY producto_id ORDER BY ean_match) AS rn
          FROM matches
        ),
        m AS (
          SELECT producto_id, ean_match
          FROM m_dedup
          WHERE rn = 1
        )
        """

    # ---------- Caches de datos base ----------
    @st.cache_data(ttl=300, show_spinner=False)
    def get_minmax_dates():
        q = "SELECT DATE(MIN(capturado_en)) AS min_d, DATE(MAX(capturado_en)) AS max_d FROM historico_precios"
        return read_df(q)

    @st.cache_data(ttl=300, show_spinner=False)
    def get_tiendas():
        q = """
        SELECT id, codigo, nombre, ref_tienda, provincia, sucursal
        FROM tiendas
        ORDER BY nombre
        """
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

    # ---------- WHERE base (SIN ean/ean_aux; eso se mete con CTE m) ----------
    def build_where_from_filters(filters: dict, base_alias: str = "h") -> Tuple[str, Dict]:
        start_date = filters["start_date"]
        end_date = filters["end_date"]
        tiendas_sel_ids = filters["tiendas_sel_ids"]
        cats_sel = filters["cats_sel"]
        subs_sel = filters["subs_sel"]
        marcas_sel = filters["marcas_sel"]

        where_parts = [f"{base_alias}.capturado_en BETWEEN :start_dt AND :end_dt"]
        params: Dict = {
            "start_dt": datetime.combine(start_date, datetime.min.time()),
            "end_dt": datetime.combine(end_date, datetime.max.time()),
        }

        if tiendas_sel_ids:
            ids = ",".join(str(int(i)) for i in tiendas_sel_ids)
            where_parts.append(f"{base_alias}.tienda_id IN ({ids})")

        # OJO: para filtrar por cat/sub/marca necesitamos JOIN productos p (se hace en queries)
        if cats_sel:
            ph = ",".join([f":cat{i}" for i in range(len(cats_sel))])
            where_parts.append(f"p.categoria IN ({ph})")
            params.update({f"cat{i}": v for i, v in enumerate(cats_sel)})

        if subs_sel:
            ph = ",".join([f":sub{i}" for i in range(len(subs_sel))])
            where_parts.append(f"p.subcategoria IN ({ph})")
            params.update({f"sub{i}": v for i, v in enumerate(subs_sel)})

        if marcas_sel:
            ph = ",".join([f":marca{i}" for i in range(len(marcas_sel))])
            where_parts.append(f"p.marca IN ({ph})")
            params.update({f"marca{i}": v for i, v in enumerate(marcas_sel)})

        return " AND ".join(where_parts), params

    # ---------- Consultas reutilizables (cacheadas) ----------
    @st.cache_data(ttl=300, show_spinner=False)
    def get_daily_avg(where_str: str, params: dict, effective: bool, eans: List[str]):
        if not eans:
            return pd.DataFrame(columns=["d", "tienda", "precio_promedio"])

        price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
        ph = _placeholders("ean", len(eans))
        p = params.copy()
        p.update(_params_list("ean", eans))

        q = f"""
        {match_cte_sql(ph)}
        , ult AS (
          SELECT DATE(h.capturado_en) AS d, h.tienda_id, h.producto_tienda_id, MAX(h.capturado_en) AS maxc
          FROM historico_precios h
          JOIN producto_tienda pt ON pt.id = h.producto_tienda_id
          JOIN productos p ON p.id = pt.producto_id
          JOIN m ON m.producto_id = p.id
          WHERE {where_str}
          GROUP BY DATE(h.capturado_en), h.tienda_id, h.producto_tienda_id
        )
        SELECT u.d, t.nombre AS tienda, AVG({price_expr}) AS precio_promedio
        FROM ult u
        JOIN historico_precios h
          ON h.tienda_id = u.tienda_id
         AND h.producto_tienda_id = u.producto_tienda_id
         AND h.capturado_en = u.maxc
        JOIN tiendas t ON t.id = u.tienda_id
        JOIN producto_tienda pt ON pt.id = u.producto_tienda_id
        JOIN productos p ON p.id = pt.producto_id
        WHERE {where_str}
        GROUP BY u.d, t.nombre
        ORDER BY u.d, t.nombre
        """
        return read_df(q, p)

    @st.cache_data(ttl=300, show_spinner=False)
    def get_kpis(where_str: str, params: Dict, eans: List[str]):
        if not eans:
            return pd.DataFrame(columns=["observaciones", "productos_distintos", "precio_medio", "share_oferta", "descuento_promedio"])

        ph = _placeholders("ean", len(eans))
        p = params.copy()
        p.update(_params_list("ean", eans))

        q = f"""
        {match_cte_sql(ph)}
        , ult AS (
          SELECT DATE(h.capturado_en) AS d, h.tienda_id, h.producto_tienda_id, MAX(h.capturado_en) AS maxc
          FROM historico_precios h
          JOIN producto_tienda pt ON pt.id=h.producto_tienda_id
          JOIN productos p ON p.id=pt.producto_id
          JOIN m ON m.producto_id = p.id
          WHERE {where_str}
          GROUP BY DATE(h.capturado_en), h.tienda_id, h.producto_tienda_id
        ),
        snap AS (
          SELECT u.d, u.tienda_id, u.producto_tienda_id,
                 h.precio_lista, h.precio_oferta,
                 {EFFECTIVE_PRICE_EXPR} AS precio_efectivo,
                 (h.precio_oferta IS NOT NULL
                  AND h.precio_oferta>0
                  AND h.precio_lista IS NOT NULL
                  AND h.precio_oferta < h.precio_lista) AS en_oferta
          FROM ult u
          JOIN historico_precios h
            ON h.tienda_id=u.tienda_id
           AND h.producto_tienda_id=u.producto_tienda_id
           AND h.capturado_en=u.maxc
        )
        SELECT
          COUNT(*) AS observaciones,
          COUNT(DISTINCT producto_tienda_id) AS productos_distintos,
          AVG(precio_efectivo) AS precio_medio,
          AVG(CASE WHEN en_oferta THEN 1 ELSE 0 END) AS share_oferta,
          AVG(CASE WHEN en_oferta AND precio_lista>0 THEN (precio_lista - precio_oferta)/precio_lista END) AS descuento_promedio
        FROM snap
        """
        return read_df(q, p)

    # ✅ OPTIMIZADO: canasta SIN OR, primero matchea productos, luego busca último snapshot solo para esos productos
    @st.cache_data(ttl=300, show_spinner=False)
    def get_basket(eans: List[str], where_str: str, params: Dict, effective: bool):
        if not eans:
            return pd.DataFrame()

        price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
        ph = _placeholders("be", len(eans))
        p = params.copy()
        p.update(_params_list("be", eans))

        q = f"""
        {match_cte_sql(ph.replace(":ean", ":be"))}
        , ult AS (
          SELECT h.tienda_id, h.producto_tienda_id, MAX(h.capturado_en) AS maxc
          FROM historico_precios h
          JOIN producto_tienda pt ON pt.id = h.producto_tienda_id
          JOIN productos p ON p.id = pt.producto_id
          JOIN m ON m.producto_id = p.id
          WHERE {where_str}
          GROUP BY h.tienda_id, h.producto_tienda_id
        ),
        snap AS (
          SELECT
            h.tienda_id,
            m.ean_match,
            COALESCE(pt.nombre_tienda, p.nombre) AS producto,
            {price_expr} AS precio,
            h.capturado_en
          FROM ult u
          JOIN historico_precios h
            ON h.tienda_id = u.tienda_id
           AND h.producto_tienda_id = u.producto_tienda_id
           AND h.capturado_en = u.maxc
          JOIN producto_tienda pt ON pt.id = u.producto_tienda_id
          JOIN productos p ON p.id = pt.producto_id
          JOIN m ON m.producto_id = p.id
        ),
        dedup AS (
          SELECT s.*,
                 ROW_NUMBER() OVER(
                   PARTITION BY s.tienda_id, s.ean_match
                   ORDER BY s.capturado_en DESC, s.precio ASC
                 ) rn
          FROM snap s
        )
        SELECT t.nombre AS tienda, d.ean_match AS ean, d.producto, d.precio
        FROM dedup d
        JOIN tiendas t ON t.id = d.tienda_id
        WHERE d.rn = 1
        """
        return read_df(q, p)

    # ✅ OPTIMIZADO: detalle SIN OR (usa match CTE + join por producto_id)
    @st.cache_data(ttl=300, show_spinner=False)
    def get_detail(where_str: str, params: Dict, effective: bool, limit: int, ean_list_for_match: List[str]):
        if not ean_list_for_match:
            return pd.DataFrame()

        ph = _placeholders("ean", len(ean_list_for_match))
        p = params.copy()
        p.update(_params_list("ean", ean_list_for_match))

        q = f"""
        {match_cte_sql(ph)}
        SELECT
          m.ean_match AS EAN,
          pt.sku_tienda AS COD,
          COALESCE(pt.nombre_tienda, p.nombre) AS PRODUCTO,
          p.categoria AS CATEGORIA,
          p.subcategoria AS SUBCATEGORIA,
          p.fabricante AS FABRICANTE,
          p.marca AS MARCA,
          h.precio_lista AS PRECIO_LISTA,
          CASE
            WHEN h.precio_oferta IS NULL THEN NULL
            WHEN h.precio_lista  IS NULL THEN h.precio_oferta
            WHEN (h.tipo_oferta IS NULL AND h.precio_oferta = h.precio_lista OR h.tipo_oferta like '%Precio regular%') THEN NULL
            ELSE h.precio_oferta
          END AS PRECIO_OFERTA,
          CASE
            WHEN h.tipo_oferta LIKE '%Precio%regular%' THEN NULL
            ELSE h.tipo_oferta
          END AS TIPO_OFERTA,
          DATE(h.capturado_en) AS FECHA,
          t.ref_tienda AS ID_BANDERA,
          t.nombre AS BANDERA,
          pt.url_tienda AS URLs
        FROM m
        JOIN producto_tienda pt ON pt.producto_id = m.producto_id
        JOIN productos p        ON p.id  = m.producto_id
        JOIN historico_precios h ON h.producto_tienda_id = pt.id
        JOIN tiendas t          ON t.id  = h.tienda_id
        WHERE {where_str}
          AND h.precio_lista IS NOT NULL
          AND h.precio_lista <> 0
        ORDER BY t.nombre, EAN, h.capturado_en
        LIMIT {int(limit)}
        """
        return read_df(q, p)

    # ---------- Navegación "tabs" ----------
    VISTAS = ["Reporte", "jobs", "ean", "reporte rapido aux", "tiendas", "cargar maestro"]
    vista = st.radio("Secciones", VISTAS, horizontal=True, key="vista_actual")

    def sidebar_reporte():
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
            min_value=datetime(2000, 1, 1).date(),
            max_value=datetime(2100, 12, 31).date(),
            key="rango_fechas_v2"
        )

        if isinstance(date_range, tuple):
            start_date, end_date = date_range
        else:
            start_date, end_date = default_start, date_range

        tiendas_df = get_tiendas().copy()

        def _opts(series: pd.Series) -> list[str]:
            return sorted([x for x in series.fillna("").astype(str).str.strip().unique().tolist() if x])

        provincias_opts = _opts(tiendas_df["provincia"])
        provincias_sel  = st.sidebar.multiselect("Provincias", provincias_opts, default=[])
        df1 = tiendas_df if not provincias_sel else tiendas_df[tiendas_df["provincia"].isin(provincias_sel)]

        sucursales_opts = _opts(df1["sucursal"])
        sucursales_sel  = st.sidebar.multiselect("Sucursales", sucursales_opts, default=[])
        df2 = df1 if not sucursales_sel else df1[df1["sucursal"].isin(sucursales_sel)]

        refs_opts = _opts(df2["ref_tienda"])
        refs_sel  = st.sidebar.multiselect("Ref. tienda", refs_opts, default=[])
        df3 = df2 if not refs_sel else df2[df2["ref_tienda"].isin(refs_sel)]

        nombres_opts = df3["nombre"].tolist()
        tiendas_sel_nombres = st.sidebar.multiselect("Tiendas", nombres_opts, default=nombres_opts)

        tiendas_sel_ids = (
            df3.loc[df3["nombre"].isin(tiendas_sel_nombres), "id"]
            .astype(int)
            .tolist()
        )

        cats_df = get_categorias()
        cats_sel = st.sidebar.multiselect("Categorías", cats_df["categoria"].tolist(), default=[])

        subs_df = get_subcategorias(cats_sel) if cats_sel else pd.DataFrame(columns=["subcategoria"])
        subs_sel = st.sidebar.multiselect("Subcategorías", subs_df["subcategoria"].tolist(), default=[])

        marcas_df = get_marcas()
        marcas_sel = st.sidebar.multiselect("Marcas", marcas_df["marca"].tolist(), default=[])

        ean_input = st.sidebar.text_area("Códigos/EANs (coma, salto de línea)", "")
        ean_file = st.sidebar.file_uploader("Subir códigos (.txt, .csv, .xlsx, .xls)", type=["txt", "csv", "xlsx", "xls"])

        ean_list = to_list_str(ean_input)
        master_attrs_df = None

        if ean_file is not None:
            eans_from_file, attrs_df = read_eans_and_attrs_from_uploaded(ean_file)
            if not eans_from_file:
                st.sidebar.error("No se pudieron leer códigos del archivo subido.")
            else:
                ean_list += eans_from_file
                master_attrs_df = attrs_df

        seen = set()
        ean_list2 = []
        for x in ean_list:
            if x and x not in seen:
                seen.add(x)
                ean_list2.append(x)
        ean_list = ean_list2

        st.sidebar.caption(f"Códigos cargados: **{len(ean_list)}**")

        use_effective = st.sidebar.toggle("Usar precio efectivo (oferta si válida)", value=True)
        sample_limit = st.sidebar.number_input("Límite de filas para vistas tabulares", 1000, 200000, 5000, step=500)

        return {
            "start_date": start_date,
            "end_date": end_date,
            "tiendas_sel_ids": tiendas_sel_ids,
            "tiendas_sel_nombres": tiendas_sel_nombres,
            "cats_sel": cats_sel,
            "subs_sel": subs_sel,
            "marcas_sel": marcas_sel,
            "ean_list": ean_list,
            "use_effective": use_effective,
            "sample_limit": int(sample_limit),
            "master_attrs_df": master_attrs_df,
            "provincias_sel": provincias_sel,
            "sucursales_sel": sucursales_sel,
            "refs_sel": refs_sel,
        }

    # ---- Vistas ----
    def vista_reporte():
        f = sidebar_reporte()

        if not f["ean_list"]:
            st.markdown("##  Panel de Investigación de Mercado")
            st.warning("Para empezar, **cargar EAN/código** en la barra lateral.")
            return

        st.markdown("##  Panel de Investigación de Mercado")

        where_str, where_params = build_where_from_filters(f, "h")

        # ---------- KPIs ----------
        kpis = get_kpis(where_str, where_params, f["ean_list"])
        c1, c2, c3, c4, c5 = st.columns(5)
        if not kpis.empty:
            c1.metric("Observaciones", int(kpis.loc[0, "observaciones"]))
            c2.metric("Precio medio", f"${kpis.loc[0, 'precio_medio']:.2f}" if pd.notna(kpis.loc[0, "precio_medio"]) else "—")
        else:
            st.info("Sin datos para los filtros seleccionados.")

        # ---------- Gráfico ----------
        st.subheader("Evolución de precios promedio por tienda")
        daily = get_daily_avg(where_str, where_params, f["use_effective"], f["ean_list"])
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

        # ---------- Canasta ----------
        st.subheader("Comparador de canastas (solo códigos coincidentes)")
        basket = get_basket(f["ean_list"], where_str, where_params, f["use_effective"])

        if basket.empty:
            st.warning("No se encontraron esos códigos con los filtros actuales.")
        else:
            tiendas_sel_nombres = f["tiendas_sel_nombres"]
            tiendas_relevantes = sorted(set(tiendas_sel_nombres) & set(basket["tienda"].unique()))
            if not tiendas_relevantes:
                st.warning("No hay coincidencias en las tiendas seleccionadas.")
            else:
                presencia = basket.loc[basket["tienda"].isin(tiendas_relevantes), ["tienda", "ean"]].drop_duplicates()
                conteo_por_ean = presencia.groupby("ean")["tienda"].nunique()
                eans_comunes = set(conteo_por_ean[conteo_por_ean == len(tiendas_relevantes)].index)

                if not eans_comunes:
                    st.warning("No hay productos que estén presentes en todas las tiendas seleccionadas.")
                else:
                    basket_comun = basket[basket["ean"].isin(eans_comunes) & basket["tienda"].isin(tiendas_relevantes)].copy()
                    totales = basket_comun.groupby("tienda", as_index=False).agg(total_canasta=("precio", "sum"))

                    st.caption(f"Productos coincidentes en todas las tiendas: **{len(eans_comunes)}**")
                    a, b = st.columns([1, 1])

                    with a:
                        st.dataframe(totales.sort_values("total_canasta"), use_container_width=True)
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
                                tooltip=["tienda", alt.Tooltip("total_canasta:Q", title="Total")]
                            ).properties(height=300),
                            use_container_width=True
                        )

                    with st.expander("Detalle de canasta por tienda (solo coincidentes)"):
                        st.dataframe(basket_comun, use_container_width=True)

        # ---------- Tabla detalle ----------
        st.subheader("Tabla Reporte (todas las capturas en el rango)")
        detail = get_detail(where_str, where_params, f["use_effective"], f["sample_limit"], f["ean_list"])

        detail = detail.drop_duplicates(subset=["EAN", "BANDERA", "FECHA"], keep="last").reset_index(drop=True)

        # Maestro (opcional)
        m = f.get("master_attrs_df")
        if m is not None and isinstance(m, pd.DataFrame) and not m.empty:
            detail["EAN"] = detail["EAN"].map(_clean_code_value)
            m = m.copy()
            m["EAN"] = m["EAN"].map(_clean_code_value)
            m = m.dropna(subset=["EAN"]).drop_duplicates("EAN")

            master_cols = [c for c in ["CATEGORIA", "SUBCATEGORIA", "MARCA", "FABRICANTE", "PRODUCTO"] if c in m.columns]
            extras = detail.drop(columns=master_cols, errors="ignore")

            m = m[["EAN"] + master_cols].copy()
            m["__ord__"] = np.arange(len(m))

            detail = m.merge(extras, on="EAN", how="left").sort_values(["__ord__", "FECHA"]).drop(columns="__ord__")

        desired_prefix = ["COD", "EAN", "PRODUCTO", "CATEGORIA", "SUBCATEGORIA", "MARCA", "FABRICANTE"]
        visible = [c for c in desired_prefix if c in detail.columns]
        rest = [c for c in detail.columns if c not in visible]
        detail = detail[visible + rest].drop_duplicates(keep="first").reset_index(drop=True)

        st.dataframe(detail, use_container_width=True, height=350)
        st.download_button("⬇️ Descargar detalle (CSV)",
                           detail.to_csv(index=False).encode("utf-8"),
                           file_name="detalle_snapshots.csv", mime="text/csv")
        st.download_button("⬇️ Descargar detalle (XLSX)",
                           df_to_excel_bytes(detail),
                           file_name="detalle_snapshots.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def vistaCron():
        import cron_manager
        cron_manager.cron_manager()

    def vistaEan():
        import eanconfig
        eanconfig.ean()

    if vista == "Reporte":
        vista_reporte()
    elif vista == "jobs":
        vistaCron()
    elif vista == "ean":
        vistaEan()
    elif vista == "reporte rapido aux":
        import prueba
        prueba.prueba(engine)
    elif vista == "cargar maestro":
        import ftp
        ftp.ftpcarga()
    elif vista == "tiendas":
        import tiendas
        tiendas.tiendas(engine)
    else:
        import app_regiones
        app_regiones.regiones()


"""
✅ ÍNDICES RECOMENDADOS (ejecútalos en MySQL si no existen):

ALTER TABLE productos ADD INDEX idx_productos_ean (ean);
ALTER TABLE productos ADD INDEX idx_productos_ean_aux (ean_auxiliar);

ALTER TABLE producto_tienda ADD INDEX idx_pt_producto (producto_id);

-- historico: el más útil para tus joins por producto_tienda + max(capturado_en)
ALTER TABLE historico_precios
  ADD INDEX idx_hist_pt_tienda_capt (producto_tienda_id, tienda_id, capturado_en);

-- opcional (si filtras mucho por tienda + fecha)
ALTER TABLE historico_precios
  ADD INDEX idx_hist_tienda_capt (tienda_id, capturado_en);
"""
