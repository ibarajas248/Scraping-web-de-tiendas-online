import os
from flask import Flask
from dotenv import load_dotenv

# Cargar variables de entorno desde .env si existe
load_dotenv()

app = Flask(__name__)

@app.route("/")
def home():
    return "<h1>Scrap prueba </h1>"

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8090))
    app.run(host="0.0.0.0", port=port, debug=True)
