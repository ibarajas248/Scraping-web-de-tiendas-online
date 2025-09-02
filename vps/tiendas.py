# tiendas.py
# Editor de tiendas: modifica ref_tienda, provincia y sucursal

import pandas as pd
import streamlit as st
from sqlalchemy import text
from typing import Dict, Any

def _ensure_columns(engine):
    """Crea columnas si faltan (idempotente). No falla si ya existen."""
    try:
        with engine.begin() as conn:
            cols = [r[0] for r in conn.exec_driver_sql("SHOW COLUMNS FROM tiendas").fetchall()]
            alters = []
            if "ref_tienda" not in cols:
                alters.append("ADD COLUMN `ref_tienda` VARCHAR(80) NULL AFTER `nombre`")
            if "provincia" not in cols:
                alters.append("ADD COLUMN `provincia` VARCHAR(80) NULL AFTER `ref_tienda`")
            if "sucursal" not in cols:
                alters.append("ADD COLUMN `sucursal` VARCHAR(160) NULL AFTER `provincia`")
            if alters:
                conn.exec_driver_sql(f"ALTER TABLE `tiendas` {', '.join(alters)}")
    except Exception as e:
        st.warning(f"No pude asegurar columnas (permiso ALTER). Continúo. Detalle: {e}")

def _read_df(engine, q: str, params: Dict[str, Any] | None = None) -> pd.DataFrame:
    params = params or {}
    with engine.connect() as conn:
        return pd.read_sql(text(q), conn, params=params)

def tiendas(engine):
    st.markdown("##  Editor de Tiendas")
    st.caption("Edita **ref_tienda**, **provincia** y **sucursal**. Guardamos solo lo que cambie.")

    _ensure_columns(engine)

    # -------- Filtros --------
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        q = st.text_input("Buscar (código / nombre / provincia / sucursal)", "")
    with c2:
        solo_vacias = st.checkbox("Solo filas con campos vacíos", value=False)
    with c3:
        limit = st.number_input("Límite", min_value=10, max_value=10000, value=500, step=10)

    where = []
    params: Dict[str, Any] = {"lim": int(limit)}
    if q:
        where.append("(codigo LIKE :q OR nombre LIKE :q OR provincia LIKE :q OR sucursal LIKE :q)")
        params["q"] = f"%{q}%"
    if solo_vacias:
        where.append("(COALESCE(ref_tienda,'')='' OR COALESCE(provincia,'')='' OR COALESCE(sucursal,'')='')")

    # -------- Métricas / Contadores --------
    try:
        with engine.connect() as conn:
            total_bd = conn.exec_driver_sql("SELECT COUNT(*) FROM tiendas").scalar() or 0

            vacias_total = conn.exec_driver_sql("""
                SELECT COUNT(*) FROM tiendas
                WHERE COALESCE(ref_tienda,'')='' OR COALESCE(provincia,'')='' OR COALESCE(sucursal,'')=''
            """).scalar() or 0

            count_sql = "SELECT COUNT(*) FROM tiendas"
            if where:
                count_sql += " WHERE " + " AND ".join(where)
            total_filtrado = conn.execute(text(count_sql), params).scalar() or 0

        m1, m2, m3 = st.columns(3)
        m1.metric("Tiendas (filtro)", f"{total_filtrado:,}")
        m2.metric("Tiendas en BD", f"{total_bd:,}")
        #m3.metric("Con campos vacíos (BD)", f"{vacias_total:,}")
        st.caption("Mostrando una porción de los resultados según el límite configurado.")
    except Exception as e:
        st.warning(f"No pude calcular los contadores. Detalle: {e}")

    # -------- Consulta de datos (con LIMIT para la grilla) --------
    sql = """
    SELECT
      id,
      codigo,
      nombre,
      ref_tienda,
      provincia,
      sucursal
    FROM tiendas
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY nombre LIMIT :lim"

    df = _read_df(engine, sql, params)

    if df.empty:
        st.info("No hay filas para mostrar con los filtros actuales.")
        return

    # Editor: bloqueamos id/codigo/nombre; editables: ref_tienda/provincia/sucursal
    base = df.copy().set_index("id")
    edited = st.data_editor(
        base,
        num_rows="fixed",
        use_container_width=True,
        hide_index=False,
        column_config={
            "codigo": st.column_config.TextColumn("Código", disabled=True),
            "nombre": st.column_config.TextColumn("Nombre", disabled=True),
            "ref_tienda": st.column_config.TextColumn("Ref. tienda", help="Identificador en sistema externo"),
            "provincia": st.column_config.TextColumn("Provincia"),
            "sucursal": st.column_config.TextColumn("Sucursal / Local"),
        },
        key="editor_tiendas",
        height=420,
    )

    # Detectar cambios comparando contra df original por id
    edited_reset = edited.reset_index()            # id vuelve como columna
    merged = edited_reset.merge(df, on="id", how="left", suffixes=("_new", "_old"))

    mask_cambio = (
        (merged["ref_tienda_new"].fillna("") != merged["ref_tienda_old"].fillna("")) |
        (merged["provincia_new"].fillna("")  != merged["provincia_old"].fillna(""))  |
        (merged["sucursal_new"].fillna("")   != merged["sucursal_old"].fillna(""))
    )

    cambios = merged.loc[mask_cambio, ["id", "ref_tienda_new", "provincia_new", "sucursal_new"]].copy()

    st.caption(f"Filas modificadas pendientes de guardar: **{len(cambios)}**")

    col_left, col_right = st.columns([1, 3])
    with col_left:
        guardar = st.button("💾 Guardar cambios", type="primary", disabled=cambios.empty)

    if guardar and not cambios.empty:
        try:
            updates = cambios.to_dict(orient="records")
            with engine.begin() as conn:
                for row in updates:
                    conn.execute(
                        text("""
                            UPDATE tiendas
                               SET ref_tienda = :ref,
                                   provincia  = :prov,
                                   sucursal   = :suc
                             WHERE id = :id
                        """),
                        {
                            "ref": (row["ref_tienda_new"] or None),
                            "prov": (row["provincia_new"] or None),
                            "suc": (row["sucursal_new"] or None),
                            "id": int(row["id"]),
                        }
                    )
            st.success(f"✅ Guardado OK: {len(updates)} fila(s) actualizada(s).")
            try:
                st.cache_data.clear()  # refresca caches de otras vistas
            except Exception:
                pass
        except Exception as e:
            st.error(f"❌ Error al guardar: {e}")

    st.caption("Tip: Deja una celda vacía para guardar **NULL** en la base.")
