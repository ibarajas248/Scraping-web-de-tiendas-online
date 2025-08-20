from base_datos import get_conn

def test_connection():
    try:
        conn, tunnel = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT NOW();")
        result = cur.fetchone()
        print("✅ Conectado. Fecha/hora en el servidor:", result[0])

        cur.close()
        conn.close()
        tunnel.close()
        print("🔒 Conexión cerrada correctamente.")
    except Exception as e:
        print("❌ Error al conectar:", e)

if __name__ == "__main__":
    test_connection()
