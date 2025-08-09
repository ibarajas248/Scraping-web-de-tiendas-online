import pandas as pd

csv_path = "carrefour_all_products.csv"
excel_path = "../../reportes_excel/carrefour.xlsx"

# Leer CSV como texto, evitando que las URLs se interpreten como links
df = pd.read_csv(csv_path, sep=None, engine="python", dtype=str)

# Guardar en Excel desactivando la detección de URLs
with pd.ExcelWriter(excel_path, engine="xlsxwriter", options={"strings_to_urls": False}) as writer:
    df.to_excel(writer, index=False)

print(f"💾 Archivo guardado sin hipervínculos en: {excel_path}")
