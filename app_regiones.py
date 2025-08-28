# app_regiones.py
# Asignador de regiones a tiendas (analisis_retail)
# Ejecutar: streamlit run app_regiones.py
def regiones():

    import pandas as pd
    import streamlit as st
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine

    st.set_page_config(page_title="Asignar Regiones a Tiendas", layout="wide")

    # ---------------- Sidebar: conexión ----------------
    st.sidebar.header("Conexión MySQL")

    host = st.sidebar.text_input("Host", value="localhost")
    port = st.sidebar.number_input("Puerto", value=3310, min_value=1, max_value=65535, step=1)
    user = st.sidebar.text_input("Usuario", value="root")
    password = st.sidebar.text_input("Password", value="", type="password")
    db = st.sidebar.text_input("Base de datos", value="analisis_retail")

    usar_mysqlconnector = st.sidebar.toggle("Usar mysql-connector en lugar de PyMySQL", value=False,
                                            help="Actívalo si tienes instalado mysql-connector-python")

    @st.cache_resource(show_spinner=False)
    def get_engine(h: str, p: int, u: str, pw: str, d: str, use_mysqlconnector: bool) -> Engine:
        # Cambia de driver según toggle
        driver = "mysqlconnector" if use_mysqlconnector else "pymysql"
        uri = f"mysql+{driver}://{u}:{pw}@{h}:{p}/{d}?charset=utf8mb4"
        return create_engine(uri, pool_pre_ping=True, pool_recycle=1800)

    engine = get_engine(host, port, user, password, db, usar_mysqlconnector)

    # --------- DDL ----------
    DDL_REGIONES = """
    CREATE TABLE IF NOT EXISTS `regiones` (
      `id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
      `nombre` VARCHAR(120) NOT NULL,
      PRIMARY KEY (`id`),
      UNIQUE KEY `uniq_region_nombre` (`nombre`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    DDL_TIENDA_REGION = """
    CREATE TABLE IF NOT EXISTS `tienda_region` (
      `tienda_id` SMALLINT(5) UNSIGNED NOT NULL,
      `region_id` SMALLINT UNSIGNED NOT NULL,
      PRIMARY KEY (`tienda_id`, `region_id`),
      CONSTRAINT `fk_tr_tienda`
        FOREIGN KEY (`tienda_id`) REFERENCES `tiendas`(`id`)
        ON DELETE CASCADE ON UPDATE CASCADE,
      CONSTRAINT `fk_tr_region`
        FOREIGN KEY (`region_id`) REFERENCES `regiones`(`id`)
        ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    DEFAULT_REGIONES = [
        "Region BUENOS AIRES",
        "Region CENTRO-LITORAL",
        "Region NOA",
        "Nacional",
    ]

    def ensure_schema():
        with engine.begin() as cx:
            cx.execute(text(DDL_REGIONES))
            cx.execute(text(DDL_TIENDA_REGION))

    def seed_regiones():
        with engine.begin() as cx:
            for nombre in DEFAULT_REGIONES:
                cx.execute(text("INSERT IGNORE INTO regiones(nombre) VALUES (:n)"), {"n": nombre})

    @st.cache_data(show_spinner=False)
    def load_tablas():
        tiendas = pd.read_sql(text("SELECT id, codigo, nombre FROM tiendas ORDER BY nombre"), engine)
        regiones = pd.read_sql(text("SELECT id, nombre FROM regiones ORDER BY nombre"), engine)
        mapa = pd.read_sql(
            text("""
            SELECT t.id AS tienda_id, t.nombre AS tienda, r.id AS region_id, r.nombre AS region
            FROM tienda_region tr
            JOIN tiendas t   ON t.id = tr.tienda_id
            JOIN regiones r  ON r.id = tr.region_id
            ORDER BY t.nombre, r.nombre
            """),
            engine,
        )
        return tiendas, regiones, mapa

    def invalidate_cache():
        load_tablas.clear()

    def guardar_asignaciones(tienda_id: int, regiones_ids_nuevas: set):
        # ✅ Usar text() para :tid
        actuales_df = pd.read_sql(
            text("SELECT region_id FROM tienda_region WHERE tienda_id = :tid"),
            engine, params={"tid": int(tienda_id)}
        )
        actuales = set(actuales_df["region_id"].astype(int).tolist())

        a_insertar = regiones_ids_nuevas - actuales
        a_borrar   = actuales - regiones_ids_nuevas

        with engine.begin() as cx:
            for rid in a_insertar:
                cx.execute(text("""
                    INSERT IGNORE INTO tienda_region (tienda_id, region_id)
                    VALUES (:tid, :rid)
                """), {"tid": int(tienda_id), "rid": int(rid)})

            for rid in a_borrar:
                cx.execute(text("""
                    DELETE FROM tienda_region
                    WHERE tienda_id = :tid AND region_id = :rid
                """), {"tid": int(tienda_id), "rid": int(rid)})

        invalidate_cache()

    # ---------------- UI principal ----------------
    st.title("🗺️ Asignar regiones a tiendas")

    colA, colB = st.columns([1, 2], gap="large")

    with colA:
        st.subheader("Inicialización")
        if st.button("✅ Crear tablas (si no existen)", use_container_width=True):
            ensure_schema()
            st.success("Tablas verificadas/creadas.")
        if st.button("🌱 Sembrar regiones por defecto", use_container_width=True):
            ensure_schema()
            seed_regiones()
            invalidate_cache()
            st.success("Regiones iniciales insertadas.")

        st.divider()
        st.subheader("Regiones")
        with st.form("nueva_region"):
            nuevo_nombre = st.text_input("Nueva región", placeholder="Ej.: Region CUYO")
            submitted = st.form_submit_button("➕ Agregar región")
            if submitted and nuevo_nombre.strip():
                ensure_schema()
                with engine.begin() as cx:
                    cx.execute(text("INSERT IGNORE INTO regiones(nombre) VALUES (:n)"), {"n": nuevo_nombre.strip()})
                invalidate_cache()
                st.success(f"Región '{nuevo_nombre.strip()}' agregada.")

    with colB:
        st.subheader("Asignar regiones por tienda")
        ensure_schema()
        tiendas, regiones, mapa = load_tablas()

        if tiendas.empty:
            st.info("No hay tiendas en la tabla `tiendas`. Carga tus tiendas primero.")
        else:
            tienda_sel = st.selectbox(
                "Selecciona una tienda",
                options=tiendas["id"],
                format_func=lambda i: tiendas.loc[tiendas["id"] == i, "nombre"].values[0],
            )

            # regiones disponibles
            opciones = {row["nombre"]: int(row["id"]) for _, row in regiones.iterrows()}

            # regiones ya asignadas a la tienda
            asignadas = set(
                mapa.loc[mapa["tienda_id"] == tienda_sel, "region_id"].astype(int).tolist()
            )
            preselect = [nombre for nombre, rid in opciones.items() if rid in asignadas]

            seleccion = st.multiselect(
                "Regiones asignadas",
                options=list(opciones.keys()),
                default=preselect,
                help="Selecciona las regiones que deben quedar asociadas a esta tienda.",
            )

            if st.button("💾 Guardar asignaciones", type="primary"):
                nuevas_ids = {opciones[n] for n in seleccion}
                guardar_asignaciones(int(tienda_sel), nuevas_ids)
                st.success("Asignaciones guardadas.")
                # recargar mapa para reflejar cambios
                _, _, mapa = load_tablas()

        st.divider()
        st.subheader("Vista actual tienda ↔ región")
        if mapa.empty:
            st.caption("Aún no hay relaciones en `tienda_region`.")
        else:
            st.dataframe(mapa, use_container_width=True, hide_index=True)

    # --------- Extra: Acciones masivas ----------
    with st.expander("⚙️ Acciones masivas (opcional)"):
        if 'tiendas' not in locals():
            ensure_schema()
            tiendas, regiones, mapa = load_tablas()

        tiendas_multi = st.multiselect(
            "Tiendas",
            options=tiendas["id"] if not tiendas.empty else [],
            format_func=lambda i: tiendas.loc[tiendas["id"] == i, "nombre"].values[0] if not tiendas.empty else str(i),
        )
        region_bulk = st.selectbox(
            "Región",
            options=regiones["id"] if not regiones.empty else [],
            format_func=lambda i: regiones.loc[regiones["id"] == i, "nombre"].values[0] if not regiones.empty else str(i),
        )
        c1, c2 = st.columns(2)
        with c1:
            if st.button("➕ Agregar región a tiendas seleccionadas"):
                with engine.begin() as cx:
                    for tid in tiendas_multi:
                        cx.execute(text("""
                            INSERT IGNORE INTO tienda_region (tienda_id, region_id)
                            VALUES (:tid, :rid)
                        """), {"tid": int(tid), "rid": int(region_bulk)})
                invalidate_cache()
                st.success("Región agregada a las tiendas seleccionadas.")
        with c2:
            if st.button("🗑️ Quitar región de tiendas seleccionadas"):
                with engine.begin() as cx:
                    for tid in tiendas_multi:
                        cx.execute(text("""
                            DELETE FROM tienda_region
                            WHERE tienda_id = :tid AND region_id = :rid
                        """), {"tid": int(tid), "rid": int(region_bulk)})
                invalidate_cache()
                st.success("Región quitada de las tiendas seleccionadas.")
