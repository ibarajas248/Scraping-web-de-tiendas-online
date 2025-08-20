import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1",   # tu PC
    port=3307,          # el túnel redirige al 3306 del server
    user="userscrap",
    password="UY8rMSGcHUunSsyJE4c7",
    database="scrap"
)
print("OK conectado")
