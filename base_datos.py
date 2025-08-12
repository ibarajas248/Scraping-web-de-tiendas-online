# base_datos.py
from mysql.connector import connect

def get_conn():
    """
    Devuelve una conexión a MySQL en localhost.
    Ajusta usuario, contraseña y base de datos a tu entorno.
    """
    return connect(
        host="localhost",   # conexión local
        port=3310,          # 🔹 puerto cambiado a 3310
        user="root",        # usuario MySQL
        password="",        # contraseña MySQL
        database="analisis_retail"  # nombre de la base
    )
