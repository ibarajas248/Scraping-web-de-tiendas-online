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
asunto = "🚨 Tu taller pierde dinero cada día (y Khushi puede detenerlo)"

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
            padding: 24px 10px;
            box-sizing: border-box;
        }
        .card {
            max-width: 720px;
            margin: 0 auto;
            background: radial-gradient(circle at top, #22c55e 0, #020617 45%, #020617 100%);
            border-radius: 20px;
            overflow: hidden;
            box-shadow: 0 18px 45px rgba(0,0,0,0.75);
            border: 1px solid rgba(148,163,184,0.4);
        }

        /* CABECERA / HERO */
        .header {
            padding: 22px 26px 14px 26px;
            text-align: center;
        }
        .tagline {
            display: inline-block;
            padding: 7px 14px;
            border-radius: 999px;
            font-size: 11px;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #bbf7d0;
            background: rgba(6,95,70,0.3);
            border: 1px solid rgba(34,197,94,0.7);
        }
        .headline {
            margin-top: 16px;
            font-size: 30px;
            line-height: 1.25;
            color: #ecfeff;
            font-weight: 900;
        }
        .headline span {
            color: #4ade80;
        }
        .subheadline {
            margin-top: 10px;
            font-size: 14px;
            color: #e5e7eb;
            max-width: 540px;
            margin-left: auto;
            margin-right: auto;
        }
        .hero-img {
            margin-top: 18px;
            width: 100%;
            max-width: 650px;
            border-radius: 14px;
            overflow: hidden;
            border: 1px solid rgba(148,163,184,0.45);
            margin-left: auto;
            margin-right: auto;
        }
        .hero-img img {
            display: block;
            width: 100%;
        }
        .micro-badge {
            margin-top: 12px;
            display: inline-block;
            padding: 6px 12px;
            font-size: 11px;
            border-radius: 999px;
            color: #fde68a;
            background: rgba(180,83,9,0.35);
            border: 1px solid rgba(234,179,8,0.8);
        }

        /* CONTENIDO */
        .content {
            background: #020617;
            padding: 22px 26px 26px 26px;
        }
        .content h2 {
            font-size: 18px;
            color: #e5e7eb;
            margin-bottom: 8px;
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
        .danger {
            color: #f97316;
            font-weight: bold;
        }

        /* BLOQUE DOLOR */
        .pain-box {
            margin-top: 14px;
            border-radius: 14px;
            background: rgba(127,29,29,0.96);
            padding: 14px 14px 12px 14px;
            border: 1px solid rgba(248,113,113,0.9);
        }
        .pain-title {
            font-size: 14px;
            font-weight: bold;
            color: #fee2e2;
            margin-bottom: 6px;
        }
        .pain-item {
            font-size: 12px;
            color: #fee2e2;
            margin-bottom: 4px;
        }

        /* BLOQUE BENEFICIOS */
        .benefits {
            margin-top: 18px;
        }
        .benefit-row {
            display: table;
            width: 100%;
        }
        .benefit-col {
            display: table-cell;
            vertical-align: top;
            padding: 5px 6px;
        }
        .benefit-box {
            background: rgba(15,23,42,0.98);
            border-radius: 12px;
            border: 1px solid rgba(55,65,81,0.9);
            padding: 12px 12px 10px 12px;
        }
        .benefit-title {
            font-size: 13px;
            color: #e5e7eb;
            font-weight: bold;
            margin-bottom: 4px;
        }
        .benefit-icon {
            font-size: 17px;
            margin-right: 6px;
        }
        .benefit-text {
            font-size: 12px;
            color: #9ca3af;
        }

        /* BLOQUE IMPACTO NÚMEROS */
        .impact {
            margin-top: 20px;
            border-radius: 14px;
            border: 1px solid rgba(55,65,81,0.8);
            background: radial-gradient(circle at top left, rgba(34,197,94,0.22) 0, #020617 60%);
            padding: 12px 14px;
        }
        .impact-title {
            font-size: 13px;
            font-weight: bold;
            color: #e5e7eb;
            margin-bottom: 6px;
        }
        .impact-grid {
            display: table;
            width: 100%;
        }
        .impact-col {
            display: table-cell;
            width: 33%;
            padding: 6px 4px;
            text-align: center;
        }
        .impact-number {
            font-size: 18px;
            font-weight: 900;
            color: #4ade80;
        }
        .impact-label {
            font-size: 11px;
            color: #9ca3af;
        }
        .impact-note {
            font-size: 10px;
            color: #6b7280;
            margin-top: 4px;
        }

        /* CTA */
        .cta-section {
            margin-top: 22px;
            text-align: center;
        }
        .btn-primary {
            display: inline-block;
            padding: 13px 28px;
            border-radius: 999px;
            text-decoration: none;
            font-size: 15px;
            font-weight: bold;
            background: linear-gradient(135deg, #22c55e, #16a34a);
            color: #ecfdf5 !important;
            box-shadow: 0 14px 35px rgba(34,197,94,0.55);
        }
        .btn-primary:hover {
            opacity: 0.93;
        }
        .cta-sub {
            margin-top: 10px;
            font-size: 11px;
            color: #9ca3af;
        }

        /* GARANTÍA */
        .guarantee {
            margin-top: 20px;
            border-radius: 14px;
            border: 1px dashed rgba(52,211,153,0.8);
            background: rgba(6,78,59,0.9);
            padding: 10px 14px;
        }
        .guarantee-title {
            font-size: 13px;
            font-weight: bold;
            color: #a7f3d0;
            margin-bottom: 4px;
        }
        .guarantee-text {
            font-size: 11px;
            color: #d1fae5;
        }

        /* CIERRE / PS */
        .ps {
            margin-top: 14px;
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
                padding: 18px 16px 10px 16px;
            }
            .headline {
                font-size: 24px;
            }
            .content {
                padding: 18px 14px 22px 14px;
            }
            .benefit-row {
                display: block;
            }
            .benefit-col {
                display: block;
                width: 100%;
                padding: 4px 0;
            }
            .impact-grid {
                display: block;
            }
            .impact-col {
                display: block;
                width: 100%;
                padding: 6px 0;
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

            <!-- HERO / CABECERA -->
            <div class="header">
                <div class="tagline">
                    TALLERES QUE QUIEREN PASAR DE “SOBREVIVIR” A “DOMINAR” LA PRODUCCIÓN
                </div>

                <div class="headline">
                    Cada día sin <span>control</span> es dinero que se te escapa entre las telas 💸
                </div>

                <div class="subheadline">
                    Si hoy no puedes responder en segundos cuántas piezas se cortaron, cuántas faltan
                    y cuánta tela se desperdició, tu taller está operando a ciegas. 
                    <b>Khushi</b> existe para que eso deje de pasar.
                </div>

                <div class="hero-img">
                    <img src="https://images.pexels.com/photos/3738081/pexels-photo-3738081.jpeg?auto=compress&cs=tinysrgb&w=1200"
                         alt="Taller textil profesional y organizado">
                </div>

                <div class="micro-badge">
                    🔐 Acceso limitado · Probando con talleres que quieren dar un salto serio
                </div>
            </div>

            <!-- CONTENIDO -->
            <div class="content">
                <h2>Hola Iván, si esto te suena… Khushi es para ti 👇</h2>

                <div class="pain-box">
                    <div class="pain-title">Dolores típicos de un taller sin sistema real de producción:</div>
                    <div class="pain-item">• Hojas sueltas, fotos de pizarras y libretas que “desaparecen”.</div>
                    <div class="pain-item">• Nadie sabe con exactitud cuántas piezas se cortaron por orden.</div>
                    <div class="pain-item">• Sobrantes sin control: rollos a medias, metros perdidos, costos invisibles.</div>
                    <div class="pain-item">• Los atrasos se detectan tarde: cuando el cliente ya está preguntando.</div>
                </div>

                <p>
                    <span class="danger">La verdad incómoda:</span> cada uno de esos puntos es 
                    <span class="highlight">dinero saliendo silenciosamente de tu taller</span>.
                    No es solo “desorden”, es rentabilidad que se escapa.
                </p>

                <p>
                    <b>Khushi</b> convierte todo ese caos en datos claros:
                    quién cortó qué, cuántas piezas salieron, cuántos sobrantes se generaron
                    y cómo se comporta cada orden, en tiempo real.
                </p>

                <!-- BENEFICIOS -->
                <div class="benefits">
                    <div class="benefit-row">
                        <div class="benefit-col">
                            <div class="benefit-box">
                                <div class="benefit-title">
                                    <span class="benefit-icon">📍</span> Un panel claro de tu producción
                                </div>
                                <div class="benefit-text">
                                    Visualiza por orden, referencia y color cuántas piezas 
                                    <b>deberían estar cortadas</b> vs cuántas se han cortado de verdad. 
                                    Sin correr detrás de nadie para preguntarle.
                                </div>
                            </div>
                        </div>
                        <div class="benefit-col">
                            <div class="benefit-box">
                                <div class="benefit-title">
                                    <span class="benefit-icon">🧮</span> Sobrantes bajo control
                                </div>
                                <div class="benefit-text">
                                    Registra sobrantes y adicionales por insumo y por tendido. 
                                    Transforma lo que antes era “pérdida difusa” en números 
                                    concretos que puedes reducir y optimizar.
                                </div>
                            </div>
                        </div>
                        <div class="benefit-col">
                            <div class="benefit-box">
                                <div class="benefit-title">
                                    <span class="benefit-icon">📲</span> Flujo desde el piso de planta
                                </div>
                                <div class="benefit-text">
                                    Los operarios registran directamente desde el celular 
                                    (corte, tendidos, piezas, sobrantes). 
                                    <span class="highlight">Tú ves el mapa completo</span> sin salir de tu oficina.
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- IMPACTO -->
                <div class="impact">
                    <div class="impact-title">¿Qué ocurre cuando tomas control de tus números?</div>
                    <div class="impact-grid">
                        <div class="impact-col">
                            <div class="impact-number">10–20%</div>
                            <div class="impact-label">Menos desperdicio de tela<br>(sobrantes controlados)</div>
                        </div>
                        <div class="impact-col">
                            <div class="impact-number">+ Precisión</div>
                            <div class="impact-label">Órdenes con trazabilidad real<br>de corte a despacho</div>
                        </div>
                        <div class="impact-col">
                            <div class="impact-number">0</div>
                            <div class="impact-label">Dependencia de hojas, fotos<br>y memoria de la gente</div>
                        </div>
                    </div>
                    <div class="impact-note">
                        No son promesas mágicas: son efectos naturales cuando tu taller 
                        deja de operar a ciegas y empieza a tomar decisiones con datos.
                    </div>
                </div>

                <!-- CTA -->
                <div class="cta-section">
                    <a class="btn-primary"
                       href="https://wa.me/573001234567?text=Hola%2C%20quiero%20ver%20una%20demo%20real%20de%20Khushi%20en%20un%20taller%20de%20confecciones">
                        🚀 Quiero ver Khushi funcionando en un taller como el mío
                    </a>
                    <div class="cta-sub">
                        En la demo no verás pantallas vacías: te mostramos el flujo completo con ejemplos 
                        reales de corte, tendidos, sobrantes y control de producción.
                    </div>
                </div>

                <!-- GARANTÍA -->
                <div class="guarantee">
                    <div class="guarantee-title">Sin humo, sin promesas vacías:</div>
                    <div class="guarantee-text">
                        La idea no es llenarte de software, sino ayudarte a ver con claridad 
                        <b>dónde se gana y dónde se pierde dinero en tu producción</b>.
                        Si después de la demo sientes que Khushi no aporta valor a tu taller, 
                        no pasa nada: agradecemos tu tiempo y ya.
                    </div>
                </div>

                <!-- PS -->
                <div class="ps">
                    <b>PD:</b> Cada mes que tu producción depende de libretas y memoria, 
                    estás aceptando un nivel de riesgo y desperdicio que ya no es necesario. 
                    Aunque no implementes nada aún, 
                    <span class="highlight">ver una demo de Khushi te va a abrir los ojos</span>
                    sobre el potencial real de tu taller.
                </div>
            </div>

            <!-- FOOTER -->
            <div class="footer">
                © 2025 Khushi Confecciones · Este mensaje fue enviado automáticamente desde el sistema.<br>
                Si no deseas recibir más correos de este tipo, respóndenos indicando "remover" y ajustaremos tus preferencias.
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
    print("✅ Correo ULTRA POTENTE enviado correctamente.")
except Exception as e:
    print("❌ Error al enviar el correo:")
    print(e)
