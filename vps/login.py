# login.py
# Ejecutar: streamlit run login.py --server.address=0.0.0.0 --server.port=8090

import os, json, time, smtplib, hashlib, hmac, base64, importlib, runpy
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, Any, Optional
import streamlit as st

APP_TITLE = "intelligenceblue"
BASE_DIR = Path(__file__).resolve().parent
REPORT_FILE = BASE_DIR / "reporte.py"   # <- mismo nivel
APP_FILE    = BASE_DIR / "app.py"       # <- fallback si no hay reporte.py

AUTH_FILE  = BASE_DIR / "auth_users.json"
RESET_FILE = BASE_DIR / "password_reset.json"

DEFAULT_USERNAME = "scraptiendas"
DEFAULT_EMAIL    = "scrap@intelligenceblue.com.ar"
DEFAULT_PASSWORD = "intelligenceblue"

SMTP_HOST = "mail.intelligenceblue.com.ar"
SMTP_PORT_SSL = 465
SMTP_USER = "scrap@intelligenceblue.com.ar"

def _safe_get_secret(key: str):
    try:
        if (BASE_DIR / ".streamlit" / "secrets.toml").exists() or (Path.home() / ".streamlit" / "secrets.toml").exists():
            return st.secrets.get(key)
    except Exception:
        pass
    return None

SMTP_PASS = _safe_get_secret("SMTP_PASSWORD") or os.getenv("SMTP_PASSWORD") or "0DP$eRS(FPBl)"

# ---------- seguridad ----------
def _pbkdf2_hash(password: str, salt: Optional[bytes] = None, iterations: int = 200_000) -> Dict[str, str]:
    import os, hashlib, base64
    if salt is None:
        salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return {"salt": base64.b64encode(salt).decode(),
            "hash": base64.b64encode(dk).decode(),
            "iterations": str(iterations)}

def _verify_password(password: str, stored: Dict[str, str]) -> bool:
    import base64, hashlib, hmac
    salt = base64.b64decode(stored["salt"].encode())
    iterations = int(stored.get("iterations", "200000"))
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(base64.b64encode(dk).decode(), stored["hash"])

# ---------- storage ----------
def _load_auth() -> Dict[str, Any]:
    if AUTH_FILE.exists():
        return json.loads(AUTH_FILE.read_text(encoding="utf-8"))
    data = {"users": {DEFAULT_USERNAME: {"email": DEFAULT_EMAIL, **_pbkdf2_hash(DEFAULT_PASSWORD)}}}
    AUTH_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return data

def _save_auth(data: Dict[str, Any]) -> None:
    AUTH_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def _load_reset() -> Dict[str, Any]:
    if RESET_FILE.exists():
        return json.loads(RESET_FILE.read_text(encoding="utf-8"))
    return {}

def _save_reset(data: Dict[str, Any]) -> None:
    RESET_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

# ---------- email ----------
def _send_reset_email(to_email: str, code: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = "Código de recuperación de contraseña"
    msg["From"] = SMTP_USER
    msg["To"] = to_email
    msg.set_content(f"""Hola,

Se solicitó un código de recuperación para tu acceso.

Código: {code}

El código expira en 10 minutos.
Si no fuiste tú, ignora este mensaje.

Saludos,
Sistema Retail Analytics
Fecha/Hora: {datetime.now():%Y-%m-%d %H:%M:%S}
""")
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT_SSL) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

# ---------- lógica auth ----------
def do_login(username: str, password: str) -> bool:
    data = _load_auth()
    user = data["users"].get(username)
    return _verify_password(password, user) if user else False

def start_reset_flow(username: str) -> Optional[str]:
    auth = _load_auth()
    user = auth["users"].get(username)
    if not user:
        return None
    email = user["email"]
    code = f"{int.from_bytes(os.urandom(3), 'big') % 1_000_000:06d}"
    expires = int(time.time()) + 600
    reset_data = _load_reset()
    reset_data[username] = {"code": code, "expires": expires}
    _save_reset(reset_data)
    _send_reset_email(email, code)
    return email

def confirm_reset(username: str, code: str, new_password: str) -> bool:
    reset_data = _load_reset()
    item = reset_data.get(username)
    if not item or int(time.time()) > int(item["expires"]) or code.strip() != item["code"]:
        return False
    auth = _load_auth()
    if username not in auth["users"]:
        return False
    auth["users"][username].update(_pbkdf2_hash(new_password))
    _save_auth(auth)
    reset_data.pop(username, None)
    _save_reset(reset_data)
    return True

# ---------- target & router sin estado ----------
def _target_script() -> Path:
    return REPORT_FILE if REPORT_FILE.exists() else APP_FILE

def _get_qp():
    try:
        return st.query_params  # versiones nuevas
    except Exception:
        return st.experimental_get_query_params()

def _set_qp(**kwargs):
    try:
        st.query_params.update(kwargs)  # nuevas
    except Exception:
        st.experimental_set_query_params(**kwargs)

def _render_target_and_stop():
    script = _target_script()
    # Ejecutamos el script como si fuera principal (mismo proceso) y cortamos
    runpy.run_path(str(script), run_name="__main__")
    st.stop()

# Si la URL dice view=reporte -> cargar reporte.py y cortar (sin mostrar login)
qp = _get_qp()
view = (qp.get("view") or qp.get("view", [""]))[0] if isinstance(qp, dict) else (qp.get("view") or "")
if view == "reporte":
    _render_target_and_stop()

# ---------- UI ----------
st.set_page_config(page_title=APP_TITLE, layout="centered")
root = st.empty()
with root.container():
    st.title(APP_TITLE)

    tab_login, tab_reset = st.tabs(["Iniciar sesión", "¿Olvidaste tu contraseña?"])

    with tab_login:
        st.subheader("Acceso")
        with st.form("login_form", clear_on_submit=False):
            u = st.text_input("Usuario", value="", placeholder="scraptiendas")
            p = st.text_input("Contraseña", value="", type="password", placeholder="••••••••••")
            submit = st.form_submit_button("Entrar", use_container_width=True)

        if submit:
            if do_login(u.strip(), p):
                st.success("Autenticación correcta. Abriendo reporte…")
                # 1) Ponemos un query param para que en el próximo run cargue reporte.py
                #_set_qp(view="reporte")
                #st.rerun()
                import reporte

                reporte.inicio_reporte()
            else:
                st.error("Usuario o contraseña inválidos.")

    with tab_reset:
        st.subheader("Recuperar contraseña")
        st.caption("Se enviará un **código de 6 dígitos** al correo registrado del usuario.")
        with st.form("reset_request"):
            ur = st.text_input("Usuario", value="", placeholder="scraptiendas")
            req = st.form_submit_button("Enviar código", use_container_width=True)
        if req:
            email = start_reset_flow(ur.strip())
            st.success(f"Código enviado a {email}. Revisa tu bandeja.") if email else st.error("Usuario no encontrado.")

        with st.form("reset_confirm"):
            uc = st.text_input("Usuario (otra vez)", value="", placeholder="scraptiendas")
            code = st.text_input("Código recibido", value="", max_chars=6, placeholder="000000")
            new1 = st.text_input("Nueva contraseña", value="", type="password")
            new2 = st.text_input("Repite la nueva contraseña", value="", type="password")
            conf = st.form_submit_button("Actualizar contraseña", use_container_width=True)
        if conf:
            if new1 != new2:
                st.warning("Las contraseñas no coinciden.")
            elif len(new1) < 6:
                st.warning("La nueva contraseña debe tener al menos 6 caracteres.")
            else:
                ok = confirm_reset(uc.strip(), code.strip(), new1)
                st.success("Contraseña actualizada. Ya puedes iniciar sesión.") if ok else st.error("Código inválido/expirado o usuario no existe.")
