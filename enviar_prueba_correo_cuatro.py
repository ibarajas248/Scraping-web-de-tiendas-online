#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import ssl
from datetime import datetime

import pandas as pd
import mysql.connector
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# ========= CONFIGURACIÓN BASE DE DATOS =========
DB_HOST = "srv1069.hstgr.io"
DB_NAME = "u506324710_Khushi_android"
DB_USER = "u506324710_David"
DB_PASS = "Tamedotothepa1$"

# ========= CONFIGURACIÓN CORREO =========
SMTP_SERVER = "smtp.hostinger.com"
SMTP_PORT = 587

EMAIL_USER = "soporte.correo@khushiconfecciones.com"
EMAIL_PASS = "b~86W[JJj@F6"

DESTINATARIO = "khushi.confecciones@gmail.com"

ASUNTO = "Reporte seguimiento_produccion_febrero"

# ========= HTML DEL CORREO (CON ESTILOS Y LOGO) =========

CUERPO_HTML = """
<!DOCTYPE html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <title>Reporte de Seguimiento de Producción</title>
  </head>
  <body style="margin:0; padding:0; background-color:#eceff3; font-family:Arial, Helvetica, sans-serif;">

    <!-- PREHEADER (TEXTO OCULTO EN LA BANDEJA) -->
    <div style="display:none; max-height:0; overflow:hidden; opacity:0; font-size:1px; color:#eceff3;">
      Reporte automático de seguimiento de producción generado desde el ERP de Khushi Confecciones.
    </div>

    <table width="100%" cellspacing="0" cellpadding="0" style="background-color:#eceff3; padding:28px 0;">
      <tr>
        <td align="center">

          <!-- CARD PRINCIPAL -->
          <table width="620" cellspacing="0" cellpadding="0" 
                 style="background-color:#ffffff; border-radius:14px; box-shadow:0 10px 30px rgba(0,0,0,0.10); overflow:hidden;">

            <!-- HEADER CON LOGO Y ETIQUETA -->
            <tr>
              <td style="padding:22px 28px 16px 28px; background:linear-gradient(135deg,#111111 0%,#333333 45%,#555555 100%);">
                <table width="100%" cellspacing="0" cellpadding="0">
                  <tr>
                    <td align="left" valign="middle">
                      <img src="https://khushiconfecciones.com/wp-content/uploads/2023/08/04-23-Logo-Kushi-Negro-Horizontal-e1692072152748.png"
                           alt="Khushi Confecciones"
                           style="max-width:220px; height:auto; display:block; background:#ffffff; padding:6px 10px; border-radius:6px;">
                    </td>
                    <td align="right" valign="middle" style="text-align:right;">
                      <span style="
                        display:inline-block;
                        padding:4px 10px;
                        font-size:11px;
                        letter-spacing:0.06em;
                        text-transform:uppercase;
                        color:#f4f4f4;
                        border-radius:999px;
                        border:1px solid rgba(255,255,255,0.35);
                        background:rgba(255,255,255,0.06);
                      ">
                        Reporte automático
                      </span>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- TÍTULO Y SALUDO -->
            <tr>
              <td style="padding:22px 32px 4px 32px;">
                <h2 style="
                  margin:0;
                  font-size:22px;
                  color:#111111;
                  font-weight:700;
                  letter-spacing:0.02em;
                ">
                  Seguimiento de Producción
                </h2>
                <p style="margin:10px 0 0 0; font-size:14px; color:#555555;">
                  Hola Khushi,
                </p>
              </td>
            </tr>

            <!-- DIVISOR SUTIL -->
            <tr>
              <td style="padding:8px 32px 4px 32px;">
                <div style="height:1px; width:100%; background:linear-gradient(to right,#f0f0f0, #e0e0e0, #f0f0f0);"></div>
              </td>
            </tr>

            <!-- CUERPO DEL MENSAJE -->
            <tr>
              <td style="padding:10px 32px 0 32px;">
                <p style="margin:0 0 10px 0; font-size:14px; color:#555555; line-height:1.7;">
                  El sistema ERP de <strong>Khushi Confecciones</strong> ha generado un nuevo
                  reporte de <strong>seguimiento de producción</strong>.
                  
            
                </p>

                
              </td>
            </tr>

            

            <!-- ESPACIO + BOTÓN CTA -->
            <tr>
              <td align="center" style="padding:22px 32px 26px 32px;">
                <a href="#"
                   style="
                     display:inline-block;
                     padding:12px 32px;
                     border-radius:999px;
                     text-decoration:none;
                     font-size:14px;
                     font-weight:700;
                     letter-spacing:0.06em;
                     text-transform:uppercase;
                     background:linear-gradient(135deg,#111111 0%,#444444 50%,#111111 100%);
                     color:#ffffff;
                     box-shadow:0 8px 18px rgba(0,0,0,0.25);
                   ">
                  Ver reporte adjunto
                </a>

                <p style="margin:10px 0 0 0; font-size:11px; color:#888888;">
                  Abre el archivo adjunto en tu equipo para explorar el detalle de la producción.
                </p>
              </td>
            </tr>

            <!-- FOOTER -->
            <tr>
              <td style="padding:14px 32px 18px 32px; background-color:#fafafa; border-top:1px solid #e6e6e6;">
                <p style="margin:0; font-size:11px; color:#999999;">
                  Khushi Confecciones · Módulo ERP de Producción
                </p>
                <p style="margin:5px 0 0 0; font-size:10px; color:#b0b0b0; line-height:1.5;">
                  Este correo ha sido generado automáticamente por el sistema. 
                  Si no esperabas este mensaje, puedes simplemente ignorarlo.
                </p>
              </td>
            </tr>

          </table>
          <!-- FIN CARD PRINCIPAL -->

        </td>
      </tr>
    </table>
  </body>
</html>
"""



def exportar_excel_desde_mysql():
    """Conecta a MySQL, ejecuta la consulta y exporta a Excel. Devuelve el nombre de archivo."""
    print("Conectando a la base de datos...")
    conn = mysql.connector.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        charset="utf8mb4",
        use_pure=True,
    )

    consulta = "SELECT * FROM seguimiento_produccion_febrero;"
    print("Ejecutando consulta:", consulta)

    df = pd.read_sql(consulta, conn)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"seguimiento_produccion_febrero_{timestamp}.xlsx"

    print(f"Exportando a Excel: {filename}")
    df.to_excel(filename, index=False)

    conn.close()
    print("Conexión a base de datos cerrada.")
    return filename


def enviar_correo_con_adjunto(ruta_archivo):
    """Envía un correo con el archivo Excel adjunto."""
    print("Preparando correo...")

    mensaje = MIMEMultipart()
    mensaje["From"] = EMAIL_USER
    mensaje["To"] = DESTINATARIO
    mensaje["Subject"] = ASUNTO

    # Cuerpo HTML
    mensaje.attach(MIMEText(CUERPO_HTML, "html", "utf-8"))

    # Adjuntar archivo
    with open(ruta_archivo, "rb") as f:
        parte = MIMEBase("application", "octet-stream")
        parte.set_payload(f.read())

    encoders.encode_base64(parte)
    parte.add_header(
        "Content-Disposition",
        f"attachment; filename={os.path.basename(ruta_archivo)}",
    )
    mensaje.attach(parte)

    # Enviar por SMTP con TLS
    print("Conectando al servidor SMTP...")
    contexto = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls(context=contexto)
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, DESTINATARIO, mensaje.as_string())

    print("Correo enviado correctamente a", DESTINATARIO)


def main():
    archivo_excel = exportar_excel_desde_mysql()
    enviar_correo_con_adjunto(archivo_excel)
    # Si quieres borrar el archivo local después de enviar:
    # os.remove(archivo_excel)


if __name__ == "__main__":
    main()
