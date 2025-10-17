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



'''

# base_datos.py
from mysql.connector import connect
from sshtunnel import SSHTunnelForwarder

# 🔹 Datos de conexión SSH
SSH_HOST = "scrap.intelligenceblue.com.ar"
SSH_USER = "scrap-ssh"
SSH_PASS = "gLqqVHswm42QjbdvitJ0"

# 🔹 Datos de la base de datos MySQL
DB_HOST = "127.0.0.1"
DB_USER = "userscrap"
DB_PASS = "UY8rMSGcHUunSsyJE4c7"
DB_NAME = "scrap"


def get_conn():
    """
    Devuelve una conexión a MySQL a través de un túnel SSH.
    """
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, 22),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASS,
        remote_bind_address=(DB_HOST, 3306)
    )
    tunnel.start()

    conn = connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,  # puerto asignado local del túnel
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

    # devolvemos ambos para poder cerrar luego
    return conn, tunnel

'''