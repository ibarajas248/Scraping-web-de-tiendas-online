# app.py
import streamlit as st
import pandas as pd
from scraper import scrap_coto
from scraper import scrap_dia


st.title("🛒 Scraper Cotodigital")
query = st.text_input("🔎 Buscar productos por palabra clave:")

if st.button("Buscar"):
    if query.strip() == "":
        st.warning("Por favor, ingresa una palabra clave.")
    else:
        productos_data = []  # Inicializamos la lista aquí

        with st.spinner("⏳ Buscando productos..."):
            scrap_coto(query, productos_data)  # Se modifica por referencia
            #scrap_dia(query, productos_data)

        if productos_data:
            df = pd.DataFrame(productos_data)
            df.to_excel("productos_cotodigital.xlsx", index=False)
            st.success(f"✅ Se encontraron {len(df)} productos.")
            st.dataframe(df)
            with open("productos_cotodigital.xlsx", "rb") as f:
                st.download_button(
                    label="📥 Descargar Excel",
                    data=f,
                    file_name="productos_cotodigital.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.info("No se encontraron productos.")
