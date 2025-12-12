import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ================== CONFIGURACIÓN SMTP ==================
SMTP_HOST = "smtp.hostinger.com"
SMTP_PORT = 587

USERNAME = "soporte.correo@khushiconfecciones.com"
PASSWORD = "b~86W[JJj@F6"   # Recomendado: usar variable de entorno

DESTINATARIO = "ivanbarajashurtado@gmail.com"

# ================== CONTENIDO DEL CORREO ==================
asunto = "🚀 Khushi Confecciones – Controla tu producción como nunca antes"

html_content = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Khushi Confecciones</title>
    <style>
        body {
            background-color: #f3f4f6;
            font-family: Arial, Helvetica, sans-serif;
            margin: 0;
            padding: 20px;
        }
        .wrapper {
            max-width: 650px;
            margin: 0 auto;
        }
        .card {
            background-color: #ffffff;
            border-radius: 14px;
            overflow: hidden;
            box-shadow: 0px 6px 18px rgba(0,0,0,0.12);
        }
        .hero-img {
            width: 100%;
            display: block;
        }
        .content {
            padding: 25px 30px 30px 30px;
        }
        .logo {
            text-align: center;
            margin-bottom: 15px;
        }
        .logo img {
            max-width: 140px;
        }
        h1 {
            font-size: 24px;
            color: #111827;
            text-align: center;
            margin-top: 5px;
            margin-bottom: 10px;
        }
        h2 {
            font-size: 18px;
            color: #111827;
            margin-bottom: 10px;
        }
        p {
            font-size: 14px;
            color: #4b5563;
            line-height: 1.6;
            margin: 0 0 10px 0;
        }
        .highlight {
            color: #16a34a;
            font-weight: bold;
        }
        .benefits {
            margin-top: 15px;
            margin-bottom: 15px;
        }
        .benefit-box {
            background-color: #f9fafb;
            border-radius: 10px;
            padding: 10px 12px;
            margin-bottom: 8px;
            display: flex;
            align-items: flex-start;
            gap: 10px;
        }
        .benefit-icon {
            font-size: 18px;
        }
        .benefit-text {
            font-size: 13px;
            color: #374151;
        }
        .cta-container {
            text-align: center;
            margin-top: 20px;
        }
        .btn-primary {
            display: inline-block;
            background: linear-gradient(135deg, #22c55e, #16a34a);
            color: #ffffff !important;
            text-decoration: none;
            padding: 12px 22px;
            border-radius: 999px;
            font-size: 15px;
            font-weight: bold;
        }
        .btn-primary:hover {
            opacity: 0.9;
        }
        .secondary-note {
            font-size: 12px;
            color: #6b7280;
            text-align: center;
            margin-top: 10px;
        }
        .footer {
            text-align: center;
            font-size: 11px;
            color: #9ca3af;
            padding: 15px 10px 5px 10px;
        }
        .small-img-strip {
            text-align: center;
            margin-top: 18px;
        }
        .small-img-strip img {
            max-width: 30%;
            margin: 0 3px;
            border-radius: 10px;
        }
        @media (max-width: 600px) {
            .content {
                padding: 18px 16px 22px 16px;
            }
            h1 {
                font-size: 20px;
            }
            .btn-primary {
                width: 100%;
            }
            .small-img-strip img {
                max-width: 29%;
            }
        }
    </style>
</head>
<body>
    <div class="wrapper">
        <div class="card">

            <!-- Imagen principal / banner -->
            <img class="hero-img" src="https://images.pexels.com/photos/6311575/pexels-photo-6311575.jpeg?auto=compress&cs=tinysrgb&w=1200"
                 alt="Producción textil organizada">

            <div class="content">

                <!-- Logo -->
                <div class="logo">
                    <img src="https://images.pexels.com/photos/3738084/pexels-photo-3738084.jpeg?auto=compress&cs=tinysrgb&w=300"
                         alt="Logo Khushi">
                </div>

                <!-- Título principal -->
                <h1>Controla tu taller textil con <span class="highlight">Khushi</span> 🧵</h1>

                <p>Hola Iván,</p>

                <p>
                    Imagina tener el <b>estado real de tu producción</b> en la palma de tu mano:
                    qué se cortó, qué falta por terminar, cuántas piezas van atrasadas y qué insumos
                    estás usando en cada orden.
                </p>

                <p>
                    <span class="highlight">Khushi Confecciones</span> es un sistema pensado
                    específicamente para talleres textiles que quieren dejar atrás el desorden de
                    las hojas sueltas y los mensajes de WhatsApp.
                </p>

                <!-- Beneficios -->
                <div class="benefits">
                    <div class="benefit-box">
                        <div class="benefit-icon">✅</div>
                        <div class="benefit-text">
                            Visualiza el avance de cada orden de trabajo en tiempo real.
                        </div>
                    </div>
                    <div class="benefit-box">
                        <div class="benefit-icon">📊</div>
                        <div class="benefit-text">
                            Reportes claros de cortes, sobrantes y rendimiento por prenda y por insumo.
                        </div>
                    </div>
                    <div class="benefit-box">
                        <div class="benefit-icon">📱</div>
                        <div class="benefit-text">
                            Registra la producción directamente desde el celular, sin papeles ni confusión.
                        </div>
                    </div>
                </div>

                <!-- Tira de imágenes pequeñas (ejemplo de catálogo / taller) -->
                <div class="small-img-strip">
                    <img src="https://images.pexels.com/photos/3738081/pexels-photo-3738081.jpeg?auto=compress&cs=tinysrgb&w=600"
                         alt="Taller de confección">
                    <img src="https://images.pexels.com/photos/3738080/pexels-photo-3738080.jpeg?auto=compress&cs=tinysrgb&w=600"
                         alt="Detalles de costura">
                    <img src="https://images.pexels.com/photos/5324972/pexels-photo-5324972.jpeg?auto=compress&cs=tinysrgb&w=600"
                         alt="Producción textil">
                </div>

                <!-- Llamado a la acción -->
                <div class="cta-container">
                    <a class="btn-primary"
                       href="https://wa.me/573001234567?text=Hola%2C%20quiero%20agendar%20una%20demo%20de%20Khushi%20Confecciones">
                        Agenda una demo gratuita por WhatsApp
                    </a>
                </div>

                <p class="secondary-note">
                    En la demo te mostramos cómo Khushi se adapta al flujo real de tu taller:
                    cortes, confección, terminación y despacho.
                </p>
            </div>

            <!-- Footer -->
            <div class="footer">
                © 2025 Khushi Confecciones · Este es un mensaje automático, por favor no responder a este correo.<br>
                Si no deseas recibir más mensajes, contáctanos y actualizaremos tus preferencias.
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
    print("✅ Correo publicitario enviado correctamente.")
except Exception as e:
    print("❌ Error al enviar el correo:")
    print(e)
