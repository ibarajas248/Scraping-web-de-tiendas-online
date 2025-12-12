import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ================== CONFIG SMTP ==================
SMTP_HOST = "smtp.hostinger.com"
SMTP_PORT = 587

USERNAME = "soporte.correo@khushiconfecciones.com"
PASSWORD = "b~86W[JJj@F6"   # Recomendado: usar variable de entorno

DESTINATARIO = "ivanbarajashurtado@gmail.com"

# ================== CONTENIDO DEL CORREO ==================
asunto = "🔥 Khushi: Convierte tu taller en una máquina de producción perfecta"

html_content = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Khushi Confecciones</title>
    <style>
        body {
            margin: 0;
            padding: 0;
            background: #020617;
            font-family: Arial, Helvetica, sans-serif;
        }
        .wrapper {
            width: 100%;
            padding: 25px 10px;
            box-sizing: border-box;
        }
        .card {
            max-width: 700px;
            margin: 0 auto;
            background: linear-gradient(135deg, #0f172a 0%, #020617 40%, #111827 100%);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0px 10px 30px rgba(0,0,0,0.6);
            border: 1px solid rgba(148, 163, 184, 0.25);
        }
        .header {
            padding: 18px 28px 10px 28px;
            text-align: center;
            background: radial-gradient(circle at top, #22c55e 0, #0f172a 45%, #020617 100%);
        }
        .pill {
            display: inline-block;
            padding: 6px 14px;
            border-radius: 999px;
            font-size: 11px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #bbf7d0;
            background: rgba(15, 118, 110, 0.18);
            border: 1px solid rgba(16, 185, 129, 0.6);
        }
        .headline {
            margin-top: 16px;
            font-size: 27px;
            line-height: 1.25;
            color: #ecfeff;
            font-weight: 800;
        }
        .headline span {
            color: #4ade80;
        }
        .subheadline {
            margin-top: 10px;
            font-size: 14px;
            color: #e5e7eb;
            max-width: 520px;
            margin-left: auto;
            margin-right: auto;
        }
        .hero-img {
            margin-top: 16px;
            width: 100%;
            max-width: 640px;
            border-radius: 14px;
            overflow: hidden;
            border: 1px solid rgba(148,163,184,0.4);
        }
        .hero-img img {
            width: 100%;
            display: block;
        }
        .badge {
            display: inline-block;
            margin-top: 10px;
            padding: 6px 12px;
            font-size: 11px;
            color: #facc15;
            background: rgba(250, 204, 21, 0.1);
            border-radius: 999px;
            border: 1px solid rgba(250, 204, 21, 0.5);
        }

        .content {
            padding: 22px 26px 26px 26px;
            background: #020617;
        }
        .content h2 {
            font-size: 18px;
            color: #e5e7eb;
            margin-bottom: 6px;
        }
        .content p {
            font-size: 13px;
            color: #9ca3af;
            line-height: 1.7;
            margin: 0 0 8px 0;
        }
        .highlight {
            color: #4ade80;
            font-weight: bold;
        }

        .grid {
            margin-top: 18px;
            display: block;
        }
        .grid-row {
            display: table;
            width: 100%;
        }
        .grid-col {
            display: table-cell;
            vertical-align: top;
            padding: 6px 6px;
        }
        .feature-box {
            background: rgba(15,23,42,0.95);
            border-radius: 12px;
            padding: 12px 12px;
            border: 1px solid rgba(55,65,81,0.8);
        }
        .feature-title {
            font-size: 13px;
            color: #e5e7eb;
            font-weight: bold;
            margin-bottom: 4px;
        }
        .feature-icon {
            font-size: 16px;
            margin-right: 4px;
        }
        .feature-text {
            font-size: 12px;
            color: #9ca3af;
        }

        .before-after {
            margin-top: 18px;
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(55,65,81,0.8);
        }
        .before-after-header {
            background: #020617;
            color: #9ca3af;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            padding: 8px 10px;
            text-align: center;
        }
        .before-after-row {
            display: table;
            width: 100%;
            background: #020617;
        }
        .before-cell, .after-cell {
            display: table-cell;
            width: 50%;
            padding: 10px;
            font-size: 12px;
            vertical-align: top;
        }
        .before-cell {
            background: rgba(127,29,29,0.95);
            color: #fee2e2;
        }
        .after-cell {
            background: rgba(6,78,59,0.96);
            color: #d1fae5;
        }
        .before-title, .after-title {
            font-weight: bold;
            margin-bottom: 5px;
        }
        .before-item, .after-item {
            margin-bottom: 4px;
        }

        .cta-section {
            margin-top: 24px;
            text-align: center;
        }
        .btn-primary {
            display: inline-block;
            padding: 13px 26px;
            border-radius: 999px;
            text-decoration: none;
            font-size: 15px;
            font-weight: bold;
            color: #ecfdf5 !important;
            background: linear-gradient(135deg, #22c55e, #16a34a);
            box-shadow: 0px 10px 25px rgba(34,197,94,0.35);
        }
        .btn-primary:hover {
            opacity: 0.92;
        }
        .cta-note {
            margin-top: 8px;
            font-size: 11px;
            color: #9ca3af;
        }

        .social-proof {
            margin-top: 20px;
            background: #020617;
            border-radius: 12px;
            border: 1px solid rgba(55,65,81,0.8);
            padding: 10px 12px;
        }
        .social-proof-title {
            font-size: 12px;
            color: #e5e7eb;
            font-weight: bold;
            margin-bottom: 4px;
        }
        .social-proof-text {
            font-size: 11px;
            color: #9ca3af;
        }

        .footer {
            text-align: center;
            font-size: 10px;
            color: #6b7280;
            padding: 12px 10px 4px 10px;
        }

        @media (max-width: 640px) {
            .header {
                padding: 16px 16px 8px 16px;
            }
            .headline {
                font-size: 22px;
            }
            .content {
                padding: 18px 14px 22px 14px;
            }
            .grid-row {
                display: block;
            }
            .grid-col {
                display: block;
                width: 100%;
                padding: 5px 0;
            }
            .before-after-row {
                display: block;
            }
            .before-cell, .after-cell {
                display: block;
                width: 100%;
            }
            .btn-primary {
                width: 100%;
            }
        }
    </style>
</head>
<body>
    <div class="wrapper">
        <div class="card">

            <!-- HEADER / HERO -->
            <div class="header">
                <div class="pill">ATENCIÓN TALLERES DE CONFECCIÓN · EDICIÓN LIMITADA</div>

                <div class="headline">
                    Convierte tu producción en una <span>máquina de precisión</span> 🧵⚙️
                </div>

                <div class="subheadline">
                    Deja atrás el caos de las libretas, mensajes perdidos y números que no cuadran.
                    <b>Khushi</b> toma el control de tu taller y te muestra, en tiempo real, qué
                    está pasando con cada prenda.
                </div>

                <div class="hero-img">
                    <img src="https://images.pexels.com/photos/3738081/pexels-photo-3738081.jpeg?auto=compress&cs=tinysrgb&w=1200"
                         alt="Taller textil organizado">
                </div>

                <div class="badge">
                    ⭐ Beta exclusiva para talleres que quieren subir al siguiente nivel
                </div>
            </div>

            <!-- CONTENIDO PRINCIPAL -->
            <div class="content">

                <h2>Hola Iván, esto es lo que Khushi hace por tu taller 👇</h2>

                <p>
                    Imagina abrir tu celular y ver al instante:
                    <span class="highlight">cuántas piezas se cortaron hoy, cuántas faltan, qué insumo se está agotando</span>
                    y qué orden está en riesgo de atraso.
                </p>

                <p>
                    Khushi no es un sistema genérico. Es un <span class="highlight">cerebro digital</span> diseñado
                    específicamente para confección: cortes, tendidos, sobrantes, rendimiento por insumo, todo
                    conectado y listo para tomar decisiones.
                </p>

                <!-- GRID DE BENEFICIOS -->
                <div class="grid">
                    <div class="grid-row">
                        <div class="grid-col">
                            <div class="feature-box">
                                <div class="feature-title">
                                    <span class="feature-icon">📡</span> Producción en tiempo real
                                </div>
                                <div class="feature-text">
                                    Desde el área de corte hasta terminación: todos registran avances
                                    directamente en el sistema. Tú ves la película completa sin moverte
                                    de tu escritorio (o del celular).
                                </div>
                            </div>
                        </div>
                        <div class="grid-col">
                            <div class="feature-box">
                                <div class="feature-title">
                                    <span class="feature-icon">📊</span> Números que mandan, no que confunden
                                </div>
                                <div class="feature-text">
                                    Reportes claros de piezas cortadas, sobrantes, rendimiento por metro
                                    de tela e insumo. Descubre dónde estás perdiendo dinero y corrige a tiempo.
                                </div>
                            </div>
                        </div>
                        <div class="grid-col">
                            <div class="feature-box">
                                <div class="feature-title">
                                    <span class="feature-icon">⚙️</span> Flujo hecho a tu medida
                                </div>
                                <div class="feature-text">
                                    Khushi se adapta al lenguaje de tu taller: órdenes de corte,
                                    referencias, tallas, colores, tendidos, sobrantes y adicionales.
                                    No tienes que forzar tu proceso a un sistema rígido.
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- ANTES / DESPUÉS -->
                <div class="before-after">
                    <div class="before-after-header">
                        ANTES VS DESPUÉS DE KHUSHI
                    </div>
                    <div class="before-after-row">
                        <div class="before-cell">
                            <div class="before-title">Antes 😩</div>
                            <div class="before-item">· Órdenes anotadas en hojas que se pierden.</div>
                            <div class="before-item">· Nadie sabe exactamente cuántas piezas se cortaron.</div>
                            <div class="before-item">· Sobrantes sin control y tela que “desaparece”.</div>
                            <div class="before-item">· Atrasos que se descubren cuando ya es demasiado tarde.</div>
                        </div>
                        <div class="after-cell">
                            <div class="after-title">Con Khushi 😎</div>
                            <div class="after-item">· Órdenes, cortes e insumos digitalizados y conectados.</div>
                            <div class="after-item">· Dashboard con piezas planeadas vs cortadas en tiempo real.</div>
                            <div class="after-item">· Sobrantes medidos, convertidos en datos, no en misterio.</div>
                            <div class="after-item">· Alertas tempranas para actuar antes del retraso.</div>
                        </div>
                    </div>
                </div>

                <!-- CTA -->
                <div class="cta-section">
                    <a class="btn-primary"
                       href="https://wa.me/573001234567?text=Hola%2C%20quiero%20ver%20una%20demo%20de%20Khushi%20para%20mi%20taller%20de%20confecciones">
                        🚀 Quiero ver Khushi en acción (Demo por WhatsApp)
                    </a>
                    <div class="cta-note">
                        No es una presentación genérica: te mostramos el flujo completo usando ejemplos reales de producción textil.
                    </div>
                </div>

                <!-- PRUEBA SOCIAL / CIERRE -->
                <div class="social-proof">
                    <div class="social-proof-title">Lo que pasa cuando entras a Khushi 👇</div>
                    <div class="social-proof-text">
                        Talleres que usan sistemas como Khushi reportan:
                        <b>menos reprocesos, menos tela desperdiciada y más claridad</b>
                        en cada orden. La diferencia no es un “software bonito”, es
                        <span class="highlight">tomar el control de tu producción</span>.
                    </div>
                </div>
            </div>

            <!-- FOOTER -->
            <div class="footer">
                © 2025 Khushi Confecciones · Mensaje enviado automáticamente desde el sistema.<br>
                Si este correo no es relevante para ti, contáctanos para ajustar tus preferencias.
            </div>
        </div>
    </div>
</body>
</html>
"""

# ================== CREAR CORREO ==================
msg = MIMEMultipart()
msg["From"] = USERNAME
msg["To"] = DESTINATARIO
msg["Subject"] = asunto
msg.attach(MIMEText(html_content, "html", "utf-8"))

# ================== ENVÍO ==================
try:
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(USERNAME, PASSWORD)
    server.sendmail(USERNAME, DESTINATARIO, msg.as_string())
    server.quit()
    print("✅ Correo HIPER MEGA POTENTE enviado correctamente.")
except Exception as e:
    print("❌ Error al enviar el correo:")
    print(e)
