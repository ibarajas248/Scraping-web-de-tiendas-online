# Create a README file summarizing the Streamlit deployment steps as performed in the conversation.
readme = r"""# Deploy de Streamlit — scrap.intelligenceblue.com.ar

Este documento describe **cómo se instaló y dejó funcionando** la app de Streamlit en el VPS, detrás de Nginx como **proxy inverso** y con servicio `systemd`.

> **Dominio:** `scrap.intelligenceblue.com.ar`  
> **Ruta del proyecto:** `/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar`  
> **Puerto backend (interno):** `127.0.0.1:8090`  
> **Servicio systemd:** `streamlit-scrap`  
> **SO:** Ubuntu 24.04 (noble) con Nginx


---

## 1) Código y entorno virtual (venv)

Se dejó el código en la ruta del proyecto y se creó un **venv** local:

```bash
cd /home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar

# venv limpio
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip

# dependencias usadas por la app
./.venv/bin/pip install "mysql-connector-python>=9.0" "SQLAlchemy>=2.0" pandas numpy altair openpyxl
# (opcional, solo para correr desde tu PC con túnel)
# ./.venv/bin/pip install sshtunnel
