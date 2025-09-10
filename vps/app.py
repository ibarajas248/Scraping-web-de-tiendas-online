# app.py
# Login -> carga por ruta absoluta ./reporte.py e invoca iniciaReporte()

import os, sys, importlib.util
import streamlit as st

APP_USER = os.getenv("APP_USER", "scrap@intelligenceblue.com.ar")
APP_PASS = os.getenv("APP_PASS", "scraptiendas")

def show_login():
    st.title("Iniciar sesión")
    with st.form("login_form", clear_on_submit=False):
        u = st.text_input("Correo", placeholder=APP_USER)
        p = st.text_input("Contraseña", type="password")
        ok = st.form_submit_button("Entrar")
        if ok:
            if u.strip().lower() == APP_USER.lower() and p == APP_PASS:
                st.session_state["authed"] = True
                st.session_state["user"] = u.strip()
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos.")

def load_reporte_local():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "reporte.py")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No encontré reporte.py en {base_dir}")

    # Limpia caché si ya existía otro 'reporte'
    if "reporte" in sys.modules:
        del sys.modules["reporte"]

    spec = importlib.util.spec_from_file_location("reporte", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["reporte"] = mod
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod, path

def show_reporte():
    st.sidebar.write(f"Usuario: {st.session_state.get('user', APP_USER)}")
    if st.sidebar.button("Cerrar sesión"):
        st.session_state.clear()
        st.rerun()

    mod, loaded_from = load_reporte_local()
    st.sidebar.caption(f"Cargado desde: {loaded_from}")

    if not hasattr(mod, "iniciaReporte"):
        st.error("El 'reporte' cargado no tiene iniciaReporte(). Revisa colisiones de nombres.")
        st.stop()
    mod.iniciaReporte()

# Router
if "authed" not in st.session_state:
    st.session_state["authed"] = False

if not st.session_state["authed"]:
    show_login()
else:
    show_reporte()
