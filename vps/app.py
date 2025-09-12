# app.py
# Login -> carga por ruta absoluta ./reporte.py e invoca iniciaReporte()
# + Recuperación de contraseña por email (código temporal 6 dígitos)

import os, sys, importlib.util, secrets, ssl, smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import streamlit as st

# =========================
# Config (puedes sobreescribir por variables de entorno)
# =========================
APP_USER = os.getenv("APP_USER", "scrap@intelligenceblue.com.ar")
APP_PASS = os.getenv("APP_PASS", "scraptiendas")  # Solo usado para login normal

# SMTP principal (tu hosting)
SMTP_HOST = os.getenv("SMTP_HOST", "mail.intelligenceblue.com.ar")
SMTP_USER = os.getenv("SMTP_USER", "scrap@intelligenceblue.com.ar")
SMTP_PASS = os.getenv("SMTP_PASS", "0DP$eRS(FPBl")
SMTP_FROM = os.getenv("SMTP_FROM", "scrap@intelligenceblue.com.ar")

# Puertos y debug
SMTP_SSL_PORT = int(os.getenv("SMTP_SSL_PORT", "465"))      # SSL implícito
SMTP_STARTTLS_PORT = int(os.getenv("SMTP_STARTTLS_PORT", "587"))
SMTP_DEBUG = int(os.getenv("SMTP_DEBUG", "0"))              # 1 para ver handshake en logs

# Código de verificación: tiempo de vida (min)
RESET_CODE_TTL_MIN = int(os.getenv("RESET_CODE_TTL_MIN", "10"))

# =========================
# SMTP helpers
# =========================
def _smtp_send_ssl_465(msg_str: str, to_email: str):
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_SSL_PORT, timeout=20, context=ctx) as s:
        if SMTP_DEBUG:
            s.set_debuglevel(1)
        s.ehlo()
        # algunos servidores requieren ehlo antes de login
        features = s.esmtp_features or {}
        if "auth" not in features:
            raise smtplib.SMTPException("AUTH not advertised on SSL 465")
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_FROM, [to_email], msg_str)

def _smtp_send_starttls_587(msg_str: str, to_email: str):
    with smtplib.SMTP(SMTP_HOST, SMTP_STARTTLS_PORT, timeout=20) as s:
        if SMTP_DEBUG:
            s.set_debuglevel(1)
        s.ehlo()
        s.starttls(context=ssl.create_default_context())
        s.ehlo()  # volver a anunciar features tras TLS
        features = s.esmtp_features or {}
        if "auth" not in features:
            raise smtplib.SMTPException("AUTH not advertised after STARTTLS 587")
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_FROM, [to_email], msg_str)

def send_reset_code(to_email: str, code: str) -> None:
    """Envía el código de recuperación por SMTP.
    1) Intenta SSL 465 (recomendado por el hosting).
    2) Si no anuncia AUTH, hace fallback a STARTTLS 587.
    """
    subject = "Código de verificación – Retail Analytics"
    text_body = f"Tu código de verificación es: {code}\n\nVence en {RESET_CODE_TTL_MIN} minutos."
    html_body = f"""
    <html>
      <body>
        <p>Hola,<br><br>
           Tu <b>código de verificación</b> es:
           <span style="font-size:18px;font-weight:bold;">{code}</span><br>
           Vence en {RESET_CODE_TTL_MIN} minutos.<br><br>
           — Sistema Retail Analytics
        </p>
      </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to_email
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    msg_str = msg.as_string()

    # 1) Intento SSL 465
    try:
        _smtp_send_ssl_465(msg_str, to_email)
        return
    except Exception as e:
        if SMTP_DEBUG:
            print("Fallo en 465:", repr(e))

    # 2) Fallback STARTTLS 587
    _smtp_send_starttls_587(msg_str, to_email)

# =========================
# Utilidades de sesión
# =========================
def _gen_code(n_digits: int = 6) -> str:
    """Código 6 dígitos (zero-padded)."""
    return f"{secrets.randbelow(10**n_digits):0{n_digits}d}"

def _reset_state_clear():
    for k in ("reset_email", "reset_code", "reset_expires", "reset_sent_at"):
        st.session_state.pop(k, None)

# =========================
# UI: Login + Recuperación
# =========================
def show_login():
    st.title("Iniciar sesión")
    with st.form("login_form", clear_on_submit=False):
        u = st.text_input("Correo", placeholder=APP_USER)
        p = st.text_input("Contraseña", type="password")
        col1, col2 = st.columns([1,1])
        with col1:
            ok = st.form_submit_button("Entrar")
        with col2:
            pass

        if ok:
            if u.strip().lower() == APP_USER.lower() and p == APP_PASS:
                st.session_state["authed"] = True
                st.session_state["user"] = u.strip()
                _reset_state_clear()
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos.")

    # -------- Recuperación de acceso (código por email) --------
    with st.expander("¿Olvidaste tu contraseña? Recuperar acceso"):
        # Paso 1: enviar código
        with st.form("reset_request_form", clear_on_submit=False):
            rec_email = st.text_input(
                "Correo de la cuenta",
                value=st.session_state.get("reset_email", APP_USER),
                help="Se enviará un código temporal a este correo."
            )
            send_btn = st.form_submit_button("Enviar código")
            if send_btn:
                email_norm = rec_email.strip().lower()
                if email_norm != APP_USER.lower():
                    st.error("Ese correo no corresponde a una cuenta válida.")
                else:
                    try:
                        code = _gen_code()
                        send_reset_code(email_norm, code)
                        st.session_state["reset_email"] = email_norm
                        st.session_state["reset_code"] = code
                        st.session_state["reset_expires"] = (
                            datetime.utcnow() + timedelta(minutes=RESET_CODE_TTL_MIN)
                        )
                        st.session_state["reset_sent_at"] = datetime.utcnow()
                        st.success("Código enviado. Revisa tu bandeja (y carpeta de spam).")
                    except Exception as e:
                        st.error(f"No pude enviar el código: {e}")

        # Paso 2: verificar código
        if "reset_code" in st.session_state:
            with st.form("reset_verify_form", clear_on_submit=False):
                code_input = st.text_input("Ingresa el código recibido")
                ver_btn = st.form_submit_button("Verificar y entrar")
                if ver_btn:
                    now = datetime.utcnow()
                    code_ok = code_input.strip() == st.session_state.get("reset_code", "")
                    not_expired = now <= st.session_state.get("reset_expires", now)
                    if code_ok and not_expired:
                        st.session_state["authed"] = True
                        st.session_state["user"] = st.session_state.get("reset_email", APP_USER)
                        _reset_state_clear()
                        st.success("Verificación correcta. Entrando…")
                        st.rerun()
                    else:
                        if not code_ok:
                            st.error("Código incorrecto.")
                        elif not not_expired:
                            st.error("El código expiró. Solicita uno nuevo.")

        # Reenviar código (limitado cada 60s)
        if st.session_state.get("reset_email"):
            last = st.session_state.get("reset_sent_at")
            can_resend = True
            if last and (datetime.utcnow() - last).total_seconds() < 60:
                can_resend = False
            colA, colB = st.columns([1,2])
            with colA:
                if st.button("Reenviar código", disabled=not can_resend):
                    try:
                        code = _gen_code()
                        send_reset_code(st.session_state["reset_email"], code)
                        st.session_state["reset_code"] = code
                        st.session_state["reset_expires"] = (
                            datetime.utcnow() + timedelta(minutes=RESET_CODE_TTL_MIN)
                        )
                        st.session_state["reset_sent_at"] = datetime.utcnow()
                        st.success("Código reenviado.")
                    except Exception as e:
                        st.error(f"No pude reenviar el código: {e}")
            with colB:
                st.caption("Puedes reenviar un nuevo código cada 60 segundos.")

# =========================
# Carga dinámica de reporte.py
# =========================
def load_reporte_local():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "reporte.py")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No encontré reporte.py en {base_dir}")

    # Limpia caché si ya existía otro 'reporte'
    if "reporte" in sys.modules:
        del sys.modules["reporte"]

    spec = importlib.util.spec_from_file_location("reporte", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["reporte"] = mod
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod, path

def show_reporte():
    st.sidebar.write(f"Usuario: {st.session_state.get('user', APP_USER)}")
    if st.sidebar.button("Cerrar sesión"):
        st.session_state.clear()
        st.rerun()

    mod, loaded_from = load_reporte_local()
    st.sidebar.caption(f"Cargado desde: {loaded_from}")

    if not hasattr(mod, "iniciaReporte"):
        st.error("El 'reporte' cargado no tiene iniciaReporte(). Revisa colisiones de nombres.")
        st.stop()
    mod.iniciaReporte()

# =========================
# Router
# =========================
if "authed" not in st.session_state:
    st.session_state["authed"] = False

if not st.session_state["authed"]:
    show_login()
else:
    show_reporte()
