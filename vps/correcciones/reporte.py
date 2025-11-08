# app.py
# Retail Analytics – Streamlit (conexión directa)
# Ejecutar en el VPS (conexión directa, sin túnel):
#   streamlit run app.py --server.address 127.0.0.1 --server.port 8090 --server.headless true



def iniciaReporte():
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

    # ---------- Credenciales MySQL en el VPS ----------
    DB_HOST = "127.0.0.1"
    DB_PORT = 3306
    DB_USER = "userscrap"
    DB_PASS = "UY8rMSGcHUunSsyJE4c7"
    DB_NAME = "scrap"

    # ---------- Conexión SQLAlchemy (directa, sin túnel) ----------
    @st.cache_resource(show_spinner=False)
    def get_engine():
        """
        Crea el engine de SQLAlchemy con conexión directa (sin túnel).
        """
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

        # 🔎 Autotest rápido para fallar temprano con mensaje claro
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
        except Exception as e:
            st.error(f"No pude abrir la conexión a MySQL. Revisa host/puerto/SSL. Detalle: {e}")
            raise
        return engine


    engine = get_engine()

    # Helper central para ejecutar SQL con parámetros nombrados (:param)
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

    def _clean_ean_value(x) -> str | None:
        if x is None:
            return None
        s = str(x).strip()
        if not s or s.lower() in {"nan", "none"}:
            return None
        s = re.sub(r"\D+", "", s)  # sólo dígitos, preserva ceros
        if len(s) in {8, 12, 13, 14}:
            return s
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

    # -------- Aliases de columnas del MAESTRO (acepta variantes) --------
    MASTER_ALIAS = {
        # Salida deseada -> alias aceptados en el Excel (se normalizan)
        "CATEGORIA": {"categoria", "categoría", "cat", "departamento", "department", "categoria principal"},
        "SUBCATEGORIA": {"subcategoria", "subcategoría", "subcat", "categoria", "category", "rubro"},
        # 👇 clave: aceptar 'Nombre Proveedor' para FABRICANTE
        "FABRICANTE": {"fabricante", "nombre proveedor", "proveedor", "producer", "manufacturer"},
        "MARCA": {"marca", "brand"},
        "PRODUCTO": {"descripcion", "descripción", "producto", "nombre producto"}
    }

    def _pick_col(df: pd.DataFrame, names: set[str]) -> str | None:
        """Devuelve el nombre real de la primera columna cuyo encabezado normalizado
        esté en 'names' (también normalizado)."""
        names_norm = {_normalize_col(x) for x in names}
        for col in df.columns:
            if _normalize_col(str(col)) in names_norm:
                return col
        return None

    # === Lee EANs y (si existen) atributos maestro del Excel ===
    def read_eans_and_attrs_from_uploaded(file) -> Tuple[List[str], pd.DataFrame | None]:
        """
        Devuelve (lista_de_eans, df_atributos) cuando el archivo es .xlsx/.xls.
        Para .txt/.csv, devuelve (lista_de_eans, None).
        df_atributos tiene columnas: EAN y, si están presentes, CATEGORIA, SUBCATEGORIA, FABRICANTE, MARCA.
        """
        name = getattr(file, "name", "upload")
        ext = os.path.splitext(name)[1].lower()

        # TXT / CSV -> sólo EANs
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

        # Excel -> EANs + posibles columnas maestro
        if ext not in {".xlsx", ".xls"}:
            return [], None

        ean_aliases = {
            "ean", "ean13", "gtin",
            "cod barras", "cod. barras", "codigo barras", "codigo de barras", "código de barras",
            "cod.barras", "cód.barras", "cód. barras", "codbarras", "codigo ean", "código ean"
        }

        try:
            xls = pd.ExcelFile(file)
            eans_all: List[str] = []
            rows_list: List[pd.DataFrame] = []

            for sh in xls.sheet_names:
                df = pd.read_excel(xls, sh)
                if df is None or df.empty:
                    continue

                # Detectar columna EAN (alias + fallback cod*barra*)
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
                df2["EAN"] = df2[ean_col].map(_clean_ean_value)
                df2 = df2.dropna(subset=["EAN"])
                if df2.empty:
                    continue

                eans_all.extend(df2["EAN"].tolist())

                # Posibles columnas del maestro (usando aliases)
                for out_col, aliases in MASTER_ALIAS.items():
                    col = _pick_col(df, aliases)
                    if col:
                        df2[out_col] = df[col]

                keep = [c for c in ["EAN", "CATEGORIA", "SUBCATEGORIA", "MARCA", "FABRICANTE", "PRODUCTO"] if c in df2.columns]

                rows_list.append(df2[keep])

            attrs_df = None
            if rows_list:
                attrs_df = pd.concat(rows_list, ignore_index=True)
                # normalizar strings
                for c in ["CATEGORIA", "SUBCATEGORIA", "MARCA", "FABRICANTE"]:
                    if c in attrs_df.columns:
                        attrs_df[c] = attrs_df[c].astype(str).str.strip()
                attrs_df = attrs_df.drop_duplicates(subset=["EAN"], keep="first")

            # deduplicar EANs preservando orden
            seen = set()
            eans: List[str] = []
            for e in eans_all:
                if e not in seen:
                    seen.add(e)
                    eans.append(e)

            return eans, attrs_df
        except Exception:
            return [], None


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

    # ---------- Consultas reutilizables (cacheadas) ----------
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
        return read_df(q, params)

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
        return read_df(q, params)

    @st.cache_data(ttl=300, show_spinner=False)
    def get_basket(eans: List[str], where_str: str, params: Dict, effective: bool):
        if not eans:
            return pd.DataFrame()
        price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
        placeholders = ",".join([f":be{i}" for i in range(len(eans))])
        p = params.copy()
        p.update({f"be{i}": e for i, e in enumerate(eans)})

        q = f"""
        -- 1) Último snapshot por (tienda, producto_tienda)
        WITH ult AS (
          SELECT h.tienda_id, h.producto_tienda_id, MAX(h.capturado_en) AS maxc
          FROM historico_precios h
          JOIN producto_tienda pt ON pt.id = h.producto_tienda_id
          JOIN productos p ON p.id = pt.producto_id
          WHERE {where_str} AND p.ean IN ({placeholders})
          GROUP BY h.tienda_id, h.producto_tienda_id
        ),
        -- 2) Tomamos precio/atributos de ese último snapshot
        snap AS (
          SELECT
            h.tienda_id,
            p.ean,
            COALESCE(pt.nombre_tienda, p.nombre) AS producto,
            {price_expr} AS precio,
            h.capturado_en
          FROM ult u
          JOIN historico_precios h
            ON h.tienda_id = u.tienda_id
           AND h.producto_tienda_id = u.producto_tienda_id
           AND h.capturado_en = u.maxc
          JOIN producto_tienda pt ON pt.id = u.producto_tienda_id
          JOIN productos p        ON p.id  = pt.producto_id
        ),
        -- 3) Unificar por (tienda, EAN): preferimos snapshot más reciente
        --    y, si hay varios en ese mismo momento, el menor precio
        dedup AS (
          SELECT s.*,
                 ROW_NUMBER() OVER(
                   PARTITION BY s.tienda_id, s.ean
                   ORDER BY s.capturado_en DESC, s.precio ASC
                 ) AS rn
          FROM snap s
        )
        SELECT t.nombre AS tienda, d.ean, d.producto, d.precio
        FROM dedup d
        JOIN tiendas t ON t.id = d.tienda_id
        WHERE d.rn = 1
        """
        return read_df(q, p)

    @st.cache_data(ttl=300, show_spinner=False)
    def get_detail(where_str: str, params: Dict, effective: bool, limit: int):
        price_expr = EFFECTIVE_PRICE_EXPR if effective else "h.precio_lista"
        q = f"""
        SELECT
          p.ean AS EAN,
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
        FROM historico_precios h
        JOIN producto_tienda pt ON pt.id = h.producto_tienda_id
        JOIN productos p        ON p.id  = pt.producto_id
        JOIN tiendas t          ON t.id  = h.tienda_id
        WHERE {where_str}
          AND h.precio_lista IS NOT NULL
          AND h.precio_lista <> 0
        ORDER BY t.nombre, p.ean, h.capturado_en
        LIMIT {int(limit)}
        """
        return read_df(q, params)

    # ======= FIN cambio =======


    # ---------- Navegación "tabs" con sidebars distintos ----------
    VISTAS = ["Reporte", "jobs", "ean", "reporte rapido aux","tiendas","cargar maestro"]
    vista = st.radio("Secciones", VISTAS, horizontal=True, key="vista_actual")

    # ---- Sidebars por vista ----
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
            min_value=min_d, max_value=max_d
        )
        if isinstance(date_range, tuple):
            start_date, end_date = date_range
        else:
            start_date, end_date = default_start, date_range

        # ===== Nuevos filtros por atributos de tienda =====
        tiendas_df = get_tiendas().copy()

        # Helpers de opciones limpias (sin nulos/espacios)
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

        # Selector de tiendas (filtrado por lo anterior)
        nombres_opts = df3["nombre"].tolist()
        tiendas_sel_nombres = st.sidebar.multiselect(
            "Tiendas",
            nombres_opts,
            default=nombres_opts
        )
        # Ids finales (intersección de todo)
        tiendas_sel_ids = (
            df3.loc[df3["nombre"].isin(tiendas_sel_nombres), "id"]
            .astype(int)
            .tolist()
        )

        # ===== Resto de filtros existentes =====
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
        master_attrs_df = None

        if ean_file is not None:
            eans_from_file, attrs_df = read_eans_and_attrs_from_uploaded(ean_file)
            if not eans_from_file:
                st.sidebar.error("No se pudieron leer EANs del archivo subido.")
            else:
                ean_list += eans_from_file
                master_attrs_df = attrs_df  # Maestro opcional desde Excel

        # Deduplicar preservando orden
        _seen = set()
        ean_list = [x for x in ean_list if not (x in _seen or _seen.add(x))]
        st.sidebar.caption(f"EANs cargados: **{len(ean_list)}**")

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

            # (opcionales por si luego los quieres usar)
            "provincias_sel": provincias_sel,
            "sucursales_sel": sucursales_sel,
            "refs_sel": refs_sel,
        }


    def sidebar_analisis():
        st.sidebar.header("Filtros – Análisis")
        ventana = st.sidebar.slider("Ventana (días) para tendencia", 7, 90, 30, step=1)
        metrica = st.sidebar.selectbox("Métrica", ["Precio efectivo", "Precio lista", "Descuento %"])
        top_n = st.sidebar.number_input("Top N categorías por variación", 3, 50, 10)
        return {"ventana": ventana, "metrica": metrica, "top_n": int(top_n)}

    def sidebar_resultados():
        st.sidebar.header("Exportar / Acciones")
        formato = st.sidebar.selectbox("Formato de exportación", ["CSV", "XLSX"])
        incluir_detalle = st.sidebar.checkbox("Incluir detalle por SKU", value=True)
        nota = st.sidebar.text_area("Notas para el informe", "")
        return {"formato": formato, "incluir_detalle": incluir_detalle, "nota": nota}

    # ---- WHERE dinámico desde filtros ----
    def build_where_from_filters(filters: dict, base_alias: str = "h") -> Tuple[str, Dict]:
        start_date = filters["start_date"]; end_date = filters["end_date"]
        tiendas_sel_ids = filters["tiendas_sel_ids"]
        cats_sel = filters["cats_sel"]; subs_sel = filters["subs_sel"]; marcas_sel = filters["marcas_sel"]; ean_list = filters["ean_list"]

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

    # ---- Vistas ----
    def vista_reporte():
        f = sidebar_reporte()

        # ⛔ Solo ejecuta cuando hay EANs. Si no, muestra mensaje y corta.
        if not f["ean_list"]:
            st.markdown("##  Panel de Investigación de Mercado")
            st.warning("Para empezar, **cargar EAN** en la barra lateral. "
                       "No se ejecutará ninguna consulta hasta que agregues al menos un EAN.")
            return

        # Variables que reusan tus consultas
        tiendas_sel_nombres = f["tiendas_sel_nombres"]
        use_effective = f["use_effective"]

        st.markdown("##  Panel de Investigación de Mercado")

        where_str, where_params = build_where_from_filters(f, "h")

        # ---------- KPIs ----------
        kpis = get_kpis(where_str, where_params)
        c1, c2, c3, c4, c5 = st.columns(5)
        if not kpis.empty:
            c1.metric("Observaciones", int(kpis.loc[0, "observaciones"]))
            c2.metric("Precio medio", f"${kpis.loc[0, 'precio_medio']:.2f}" if pd.notna(kpis.loc[0, "precio_medio"]) else "—")
        else:
            st.info("Sin datos para los filtros seleccionados.")

        # ---------- Gráfico: Evolución de precio promedio ----------
        st.subheader("Evolución de precios promedio por tienda")
        daily = get_daily_avg(where_str, where_params, use_effective)
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
        st.subheader(" Comparador de canastas (solo EAN coincidentes)")
        st.caption("Carga EANs en la barra lateral (texto o archivo). Calcula precio total por tienda al último snapshot dentro del rango.")
        basket = get_basket(f["ean_list"], where_str, where_params, use_effective)

        if basket.empty:
            st.warning("No se encontraron esos EANs con los filtros actuales.")
        else:
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
                    basket_comun = basket[
                        basket["ean"].isin(eans_comunes) & basket["tienda"].isin(tiendas_relevantes)
                    ].copy()

                    totales = (
                        basket_comun
                        .groupby("tienda", as_index=False)
                        .agg(total_canasta=("precio", "sum"))
                    )

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
                                tooltip=["tienda", alt.Tooltip("total_canasta:Q", title="Total")]
                            ).properties(height=300),
                            use_container_width=True
                        )

                    with st.expander("Detalle de canasta por tienda (solo coincidentes)"):
                        st.dataframe(basket_comun, use_container_width=True)

        # ---------- Tabla detalle ----------
        st.subheader("Tabla Reporte (todas las capturas en el rango)")
        detail = get_detail(where_str, where_params, use_effective, f["sample_limit"])

        # --- Dedup: 1 EAN por tienda por día (mantener el último snapshot del día) ---
        # La consulta SQL viene ordenada por h.capturado_en asc, así que 'keep="last"' conserva el más reciente de ese día.
        detail = detail.drop_duplicates(subset=["EAN", "BANDERA", "FECHA"], keep="last").reset_index(drop=True)

        # === Si hay maestro subido por Excel, usar SUS valores y SU orden ===
        m = f.get("master_attrs_df")
        if m is not None and isinstance(m, pd.DataFrame) and not m.empty:
            # Normalizar EAN en ambos lados
            detail["EAN"] = detail["EAN"].map(_clean_ean_value)
            m = m.copy()
            m["EAN"] = m["EAN"].map(_clean_ean_value)
            m = m.dropna(subset=["EAN"]).drop_duplicates("EAN")

            # Orden requerido: ... MARCA, FABRICANTE
            master_cols = [c for c in ["CATEGORIA", "SUBCATEGORIA", "MARCA", "FABRICANTE", "PRODUCTO"] if c in m.columns]

            # Extras del detalle (para no pisar las 4 columnas del maestro)
            extras = detail.drop(columns=master_cols, errors="ignore")

            # Mantener orden del Excel (y el orden de columnas deseado en maestro)
            m = m[["EAN"] + master_cols].copy()
            m["__ord__"] = np.arange(len(m))

            # Tomar m como base y completar con extras por EAN
            detail = m.merge(extras, on="EAN", how="left").sort_values(["__ord__", "FECHA"]).drop(columns="__ord__")

        # Reordenar columnas visibles: COD primero
        desired_prefix = ["COD", "EAN", "PRODUCTO", "CATEGORIA", "SUBCATEGORIA", "MARCA", "FABRICANTE"]
        visible = [c for c in desired_prefix if c in detail.columns]
        rest = [c for c in detail.columns if c not in visible]
        detail = detail[visible + rest]
        detail = detail.drop_duplicates(keep="first").reset_index(drop=True)

        st.dataframe(detail, use_container_width=True, height=350)
        st.download_button("⬇️ Descargar detalle (CSV)", detail.to_csv(index=False).encode("utf-8"),
                           file_name="detalle_snapshots.csv", mime="text/csv")
        st.download_button("⬇️ Descargar detalle (XLSX)", df_to_excel_bytes(detail),
                           file_name="detalle_snapshots.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def vista_analisis():
        f = sidebar_analisis()
        st.markdown("## 📊 Análisis")
        st.info("Sidebar independiente para Análisis. Agrega aquí tus gráficos/consultas avanzadas.")
        st.write(f"Ventana: **{f['ventana']}** días · Métrica: **{f['metrica']}** · Top N: **{f['top_n']}**")

    def vista_resultados():
        f = sidebar_resultados()
        st.markdown("## ✅ Resultados")
        st.write("Página de resultados/exportación con su propio sidebar.")
        st.write(f"Formato: **{f['formato']}**, Detalle por SKU: **{f['incluir_detalle']}**")
        if f["nota"]:
            st.caption(f"Notas: {f['nota']}")

    # ---- Router ----
    def vistaCron():
        import cron_manager
        cron_manager.cron_manager()

    def vista_regiones():
        import app_regiones
        app_regiones.regiones()

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
        vista_regiones()
