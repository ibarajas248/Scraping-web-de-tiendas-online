# Deploy completo de Streamlit en VPS (Nginx + systemd) — `scrap.intelligenceblue.com.ar`

Este README reúne **todo en un solo archivo**: pasos de instalación, servicio `systemd`, proxy inverso en Nginx, HTTPS con Let’s Encrypt, verificación, operación, solución de problemas y notas de seguridad.

> **Dominio:** `scrap.intelligenceblue.com.ar`  
> **Ruta del proyecto:** `/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar`  
> **Backend (interno):** `127.0.0.1:8090`  
> **Servicio:** `streamlit-scrap`  
> **SO:** Ubuntu 24.04 (noble) con Nginx

---

## 0) Resumen del objetivo

- La app de Streamlit corre en **loopback** (`127.0.0.1:8090`) para no exponer puertos.
- **Nginx** hace de **proxy inverso** y sirve el dominio (HTTP/HTTPS).
- **Certbot** configura **HTTPS** automáticamente.
- **systemd** mantiene la app viva y arrancando con el sistema.

---

## 1) Código & entorno virtual (venv)

Ubica el código en la ruta del proyecto y crea un **venv** local con dependencias.

```bash
cd /home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar

# venv (aislado de sistema)
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip

# dependencias usadas por la app
./.venv/bin/pip install "mysql-connector-python>=9.0" "SQLAlchemy>=2.0" pandas numpy altair openpyxl

# (opcional, solo si ejecutarás en tu PC con túnel SSH)
# ./.venv/bin/pip install sshtunnel
```

Archivo mínimo de prueba (para validar arranque rápido): `app.py`
```python
import streamlit as st
st.set_page_config(page_title="App de prueba", page_icon="👋")
st.markdown("# ¡Bienvenido! 👋")
st.write("Esta es una app mínima hecha con Streamlit.")
```

> La app de producción conecta a MySQL mediante SQLAlchemy/MySQL Connector. **En el VPS** se ejecuta **sin túnel SSH**, por lo que se recomienda forzar `USE_SSH_TUNNEL=0` (ver §3).

---

## 2) (Opcional) Config de Streamlit

Archivo `.streamlit/config.toml` en la raíz del proyecto:

```toml
[server]
headless = true
address = "127.0.0.1"
port = 8090
enableCORS = false
enableXsrfProtection = false
```

---

## 3) Servicio `systemd` (arranque automático)

Servicio **`streamlit-scrap`** ejecutando en loopback (8090). En la instalación se dejó corriendo como **root** por simplicidad (ver §9 para endurecer seguridad).

**Archivo:** `/etc/systemd/system/streamlit-scrap.service`
```ini
[Unit]
Description=Streamlit scrap.intelligenceblue.com.ar
After=network.target

[Service]
User=root
WorkingDirectory=/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar
ExecStart=/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar/.venv/bin/python -m streamlit run /home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar/app.py --server.address 127.0.0.1 --server.port 8090 --server.headless true
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1
Environment=USE_SSH_TUNNEL=0  # Importante: en VPS conectar directo a MySQL

[Install]
WantedBy=multi-user.target
```

Activación y verificación:
```bash
systemctl daemon-reload
systemctl enable --now streamlit-scrap
systemctl status streamlit-scrap --no-pager

# backend vivo (debe responder 200/302/400)
curl -I http://127.0.0.1:8090
```

---

## 4) Nginx como **proxy inverso** (HTTP)

En este entorno, Nginx carga archivos `sites-enabled/*.conf`. Por eso el vhost se dejó con extensión **`.conf`** y symlink correspondiente.

**Archivo:** `/etc/nginx/sites-available/scrap.intelligenceblue.com.ar.conf`
```nginx
server {
    listen 80;
    server_name scrap.intelligenceblue.com.ar;

    # Endpoint de diagnóstico (opcional)
    location = /nginxtest {
        add_header Content-Type text/plain;
        return 200 "nginx ok\n";
    }

    # Proxy a Streamlit en loopback
    location / {
        proxy_pass http://127.0.0.1:8090;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket (Streamlit lo usa)
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_read_timeout 86400;
    }
}
```

**Enlace activo:**
```bash
# symlink en sites-enabled
ln -s /etc/nginx/sites-available/scrap.intelligenceblue.com.ar.conf \
      /etc/nginx/sites-enabled/scrap.intelligenceblue.com.ar.conf

# quitar posibles duplicados antiguos (sin .conf)
rm -f /etc/nginx/sites-enabled/scrap.intelligenceblue.com.ar 2>/dev/null || true

# probar y recargar
nginx -t && systemctl reload nginx
```

**Pruebas locales (desde el server):**
```bash
# vhost correcto
curl -sI -H 'Host: scrap.intelligenceblue.com.ar' http://127.0.0.1/nginxtest

# proxy al backend
curl -I -H 'Host: scrap.intelligenceblue.com.ar' http://127.0.0.1/
```

---

## 5) HTTPS con Let’s Encrypt (Certbot)

Certbot añade el bloque `listen 443 ssl;` y la redirección **HTTP→HTTPS**.

```bash
apt-get update -y
apt-get install -y certbot python3-certbot-nginx
certbot --nginx -d scrap.intelligenceblue.com.ar --redirect
```

**Certificados:**
```
/etc/letsencrypt/live/scrap.intelligenceblue.com.ar/fullchain.pem
/etc/letsencrypt/live/scrap.intelligenceblue.com.ar/privkey.pem
```

**Comprobación:**
```bash
curl -Ik https://scrap.intelligenceblue.com.ar
```

**Renovación automática:**
```bash
systemctl list-timers | grep certbot
```

---

## 6) Verificación rápida (checklist)

```bash
# Backend vivo
curl -I http://127.0.0.1:8090

# Vhost HTTP (host header correcto)
curl -sI -H 'Host: scrap.intelligenceblue.com.ar' http://127.0.0.1/nginxtest

# Público (HTTPS)
curl -Ik https://scrap.intelligenceblue.com.ar
```

Las tres deben devolver `200/301/302` para considerar OK el deploy.

---

## 7) Operación

**Servicio**
```bash
systemctl status streamlit-scrap --no-pager
systemctl restart streamlit-scrap
```

**Logs**
```bash
# backend (según distro)
journalctl -u streamlit-scrap -n 100 --no-pager   # si journalctl está habilitado
tail -n 200 /var/log/syslog | sed -n '/streamlit/p'

# Nginx
tail -n 100 /var/log/nginx/error.log
tail -n 100 /var/log/nginx/access.log
```

**Nginx**
```bash
nginx -t && systemctl reload nginx
```

**Actualizar dependencias**
```bash
cd /home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar
./.venv/bin/pip install -U streamlit mysql-connector-python SQLAlchemy pandas numpy altair openpyxl
systemctl restart streamlit-scrap
```

---

## 8) Solución de problemas

- **Pantalla en blanco**: en el VPS, usa `USE_SSH_TUNNEL=0`. Si el código entra en modo túnel y falta `sshtunnel`, puede ejecutar `st.stop()` temprano.
- **`ModuleNotFoundError: No module named 'mysql'`**: instalar driver `mysql-connector-python` en el **venv**.
- **`No module named 'sqlalchemy'`**: instalar `SQLAlchemy` en el **venv**.
- **`Empty reply from server` en HTTP**: el vhost no se estaba cargando. En este entorno, usar `sites-enabled/*.conf` (con extensión `.conf`) y probar con `curl -H 'Host: scrap...' http://127.0.0.1/nginxtest`.
- **HTTPS falla / SNI**: reemitir con `certbot --nginx -d dominio --redirect` y verificar que el `server_name` coincida.
- **Puerto 8090 ocupado**: cambiar puerto en `ExecStart` y en `proxy_pass` (p. ej. 8091) y recargar `systemd` + Nginx.

---

## 9) Seguridad recomendada (migrar a usuario no root)

Cuando todo esté estable, correr la app con el usuario del sitio:

```bash
# cambiar ownership del proyecto
chown -R intelligenceblue-scrap:intelligenceblue-scrap /home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar

# editar el servicio: User=intelligenceblue-scrap
nano /etc/systemd/system/streamlit-scrap.service
systemctl daemon-reload
systemctl restart streamlit-scrap
```

> También puedes mover credenciales a **variables de entorno** o `.env` (fuera del repo) y leerlas en `app.py`.

---

## 10) Anexo — Estructura de archivos resultante (resumen)

```
/home/intelligenceblue-scrap/htdocs/scrap.intelligenceblue.com.ar/
├── app.py
├── .streamlit/
│   └── config.toml              # opcional
├── .venv/                       # entorno virtual
└── (otros archivos de la app...)

/etc/systemd/system/
└── streamlit-scrap.service      # servicio systemd

/etc/nginx/sites-available/
└── scrap.intelligenceblue.com.ar.conf

/etc/nginx/sites-enabled/
└── scrap.intelligenceblue.com.ar.conf -> ../sites-available/scrap.intelligenceblue.com.ar.conf
```

---

## 11) Notas finales

- El endpoint `/nginxtest` se puede eliminar cuando todo funcione; fue útil para verificar que el vhost correcto respondía.
- Evita exponer el puerto 8090 a internet; usa siempre Nginx como proxy inverso con HTTPS.
- Mantén `USE_SSH_TUNNEL=0` en el VPS. El túnel SSH es útil solo si ejecutas **desde tu equipo local** hacia MySQL del VPS.

**Fin.**
