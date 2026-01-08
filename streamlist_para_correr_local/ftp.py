# archivo.py
# -*- coding: utf-8 -*-

import os
import io
import pandas as pd
import streamlit as st


def ftpcarga():
    # =================== Config básica ===================
    st.set_page_config(page_title="Actualizar maestro.xlsx", layout="centered")
    st.title("Actualizar maestro.xlsx")
    st.caption("Sube un Excel para reemplazar el maestro.xlsx en la carpeta del sitio.")

    # =================== Ruta relativa al archivo ===================
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    DEST_DIR = os.path.join(BASE_DIR, "scrap_tiendas", "vtex")
    DEST_FILENAME = "maestro.xlsx"
    DEST_PATH = os.path.join(DEST_DIR, DEST_FILENAME)

    st.subheader("Carpeta de destino (relativa)")
    st.code(f"BASE_DIR: {BASE_DIR}\nDEST_DIR: {DEST_DIR}\nDEST_PATH: {DEST_PATH}")
    st.caption("Asegúrate de que la carpeta 'scrap_tiendas/vtex' exista junto a este archivo.py y tenga permisos de escritura.")

    # =================== Carga del Excel ===================
    uploaded = st.file_uploader("Selecciona tu Excel (.xlsx o .xls)",
                                type=["xlsx", "xls"], accept_multiple_files=False)
    preview_rows = st.number_input("Vista previa (filas)", min_value=0, max_value=50, value=5, step=1)

    save_btn = st.button("Actualizar maestro.xlsx", type="primary", use_container_width=True)

    # =================== Helpers ===================
    def ensure_dir(path: str):
        os.makedirs(path, exist_ok=True)

    def read_preview(file_bytes: bytes, n_rows: int):
        if n_rows <= 0:
            return None
        try:
            excel = pd.ExcelFile(io.BytesIO(file_bytes))
            first_sheet = excel.sheet_names[0]
            df = excel.parse(first_sheet, nrows=n_rows)
            return df
        except Exception as e:
            st.info(f"No se pudo generar vista previa: {e}")
            return None

    # =================== Vista previa ===================
    if uploaded is not None and preview_rows > 0:
        prev = read_preview(uploaded.getvalue(), preview_rows)
        if prev is not None and not prev.empty:
            st.subheader("Vista previa")
            st.dataframe(prev, use_container_width=True)

    # =================== Guardado ===================
    if save_btn:
        if uploaded is None:
            st.error("Primero selecciona un archivo Excel.")
        else:
            try:
                ensure_dir(DEST_DIR)

                # Validar que realmente es un Excel
                try:
                    _ = pd.ExcelFile(io.BytesIO(uploaded.getvalue()))
                except Exception as e:
                    st.error(f"El archivo no parece ser un Excel válido (.xlsx/.xls). Detalle: {e}")
                    st.stop()

                # Escribir binario (sobrescribe directamente)
                with open(DEST_PATH, "wb") as f:
                    f.write(uploaded.getvalue())

                # Confirmar que se guardó correctamente
                ok = os.path.exists(DEST_PATH) and os.path.getsize(DEST_PATH) > 0
                if ok:
                    st.success("✅ maestro.xlsx actualizado correctamente.")
                    st.write(f"**Guardado en:** `{DEST_PATH}`")
                    st.toast("Archivo actualizado", icon="✅")
                else:
                    st.error("El archivo no apareció o pesa 0 bytes. Revisa permisos y ruta.")

            except PermissionError:
                st.error(
                    "Permiso denegado al escribir en la carpeta de destino. "
                    "Revisa permisos/propietario de 'scrap_tiendas/vtex' y que el usuario que ejecuta Streamlit tenga escritura."
                )
            except FileNotFoundError:
                st.error("Ruta de destino no encontrada. Verifica que 'scrap_tiendas/vtex' exista junto a este archivo.")
            except Exception as e:
                st.error(f"Ocurrió un error guardando el archivo: {e}")


if __name__ == "__main__":
    ftpcarga()
