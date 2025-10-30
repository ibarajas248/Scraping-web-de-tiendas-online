#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test de autenticación con proxy DataImpulse.
Sirve para verificar si el usuario/contraseña son válidos,
aunque la cuenta no tenga créditos disponibles.

Resultados posibles:
- 200 Connection established → Credenciales válidas
- 407 Proxy Authentication Required → Usuario o contraseña incorrectos
- 402 / 403 / 429 → Cuenta válida pero sin saldo o bloqueada
- Sin respuesta → Error de red o puerto bloqueado
"""

import os
import sys
import base64
import socket
import argparse

# ======== Configuración por defecto (usa tus valores reales) ========
DI_USER = os.getenv("DI_USER", "2cf8063dbace06f69df4")
DI_PASS = os.getenv("DI_PASS", "61425d26fb3c7287")
DI_HOST = os.getenv("DI_HOST", "gw.dataimpulse.com")
DI_PORT = int(os.getenv("DI_PORT", "823"))

# Destino simulado para el CONNECT
DEFAULT_TARGET_HOST = "api.myip.com"
DEFAULT_TARGET_PORT = 443


def connect_and_auth(proxy_host, proxy_port, user, password, target_host, target_port, timeout=10.0):
    """
    Envía manualmente un CONNECT con Proxy-Authorization y devuelve
    el código y la primera línea de respuesta del proxy.
    """
    # Codificar credenciales en Base64
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    connect_line = f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
    headers = (
        f"{connect_line}"
        f"Host: {target_host}:{target_port}\r\n"
        f"Proxy-Authorization: Basic {token}\r\n"
        f"User-Agent: auth-probe/1.0\r\n"
        f"Proxy-Connection: keep-alive\r\n\r\n"
    )

    with socket.create_connection((proxy_host, proxy_port), timeout=timeout) as sock:
        sock.sendall(headers.encode())
        sock.settimeout(timeout)
        data = sock.recv(4096)

    if not data:
        return (0, "No response from proxy")
    first_line = data.split(b"\r\n", 1)[0].decode(errors="replace").strip()
    parts = first_line.split()
    try:
        code = int(parts[1])
    except Exception:
        code = 0
    return code, first_line


def interpret_response(code: int):
    """
    Traduce el código HTTP del proxy en un mensaje legible.
    """
    if code == 200:
        return "✅ Credenciales válidas: proxy aceptó autenticación (200 Connection established)", 0
    elif code == 407:
        return "❌ Credenciales incorrectas o usuario inexistente (407 Proxy Authentication Required)", 1
    elif code in (402, 403, 429):
        return f"⚠️ Autenticación válida pero sin saldo o bloqueada ({code})", 2
    elif code == 0:
        return "⚠️ El proxy no respondió (posible red/puerto/firewall bloqueado)", 3
    else:
        return f"⚠️ Respuesta inesperada del proxy ({code})", 3


def main():
    parser = argparse.ArgumentParser(description="Prueba autenticación DataImpulse (aunque no haya créditos)")
    parser.add_argument("--user", default=DI_USER, help="Usuario del proxy")
    parser.add_argument("--passw", default=DI_PASS, help="Contraseña del proxy")
    parser.add_argument("--host", default=DI_HOST, help="Host del proxy (ej. gw.dataimpulse.com)")
    parser.add_argument("--port", default=DI_PORT, type=int, help="Puerto (ej. 823)")
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST, help="Host destino del CONNECT")
    parser.add_argument("--target-port", default=DEFAULT_TARGET_PORT, type=int, help="Puerto destino del CONNECT")
    parser.add_argument("--timeout", default=10.0, type=float, help="Tiempo máximo de espera (segundos)")
    args = parser.parse_args()

    print("==== Verificando autenticación DataImpulse ====")
    print(f"Proxy: {args.host}:{args.port}")
    print(f"User : {args.user}")
    print(f"Dest : {args.target_host}:{args.target_port}")
    print("===============================================")

    try:
        code, line = connect_and_auth(
            args.host, args.port, args.user, args.passw,
            args.target_host, args.target_port, args.timeout
        )
    except (socket.timeout, ConnectionRefusedError) as e:
        print(f"\n❌ Error de red: {e}")
        print("Revisa el puerto 823 o tu conexión a internet.")
        sys.exit(3)
    except OSError as e:
        print(f"\n❌ Error del sistema: {e}")
        sys.exit(3)

    print(f"\nRespuesta del proxy: {line}")
    msg, exit_code = interpret_response(code)
    print(msg)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
