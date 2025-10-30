#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import random
import string
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======== Vars por defecto (puedes exportarlas como env) ========
DI_USER_BASE = os.getenv("DI_USER", "2cf8063dbace06f69df4")
DI_PASS      = os.getenv("DI_PASS", "61425d26fb3c7287")
DI_HOST      = os.getenv("DI_HOST", "gw.dataimpulse.com")
DI_PORT      = int(os.getenv("DI_PORT", "823"))
DI_COUNTRY   = os.getenv("DI_COUNTRY", "ar")

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

TEST_URLS = [
    # Devuelven IP/país rápido (usamos varias por redundancia)
    ("https://api.myip.com", "json"),             # {"ip": "...", "country": "...", "cc": "..."}
    ("http://ip-api.com/json", "json"),           # {"query": "...", "country": "...", "countryCode": "..."}
    ("https://ifconfig.me/all.json", "json"),     # {"ip_addr": "...", ...}
    ("https://httpbin.org/ip", "json"),           # {"origin": "..."}
    ("http://example.com", "text"),               # smoke test
]

def rand_session(n=8):
    import string, random
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

def build_username(user_base: str, country: str = None, session_id: str = None) -> str:
    # Ajusta este formato si tu cuenta DI usa naming distinto
    parts = [user_base]
    if country:
        parts.append(f"country-{country.lower()}")
    if session_id:
        parts.append(f"session-{session_id}")
    return "-".join(parts)

def make_session(proxies=None) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"]
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    if proxies:
        s.proxies = proxies
    return s

def probe(s: requests.Session, url: str, kind: str, timeout=15):
    try:
        r = s.get(url, timeout=timeout)
        info = {"url": url, "status": r.status_code, "ok": r.ok, "kind": kind}
        # Cuerpo resumido / parseado
        if kind == "json":
            try:
                j = r.json()
            except Exception:
                j = {"_raw": (r.text[:200] if r.text else "")}
            info["data"] = j
        else:
            info["data"] = (r.text[:200] if r.text else "")
        # Encabezados útiles
        info["headers"] = dict(r.headers)
        return info
    except requests.exceptions.ProxyError as e:
        return {"url": url, "status": None, "ok": False, "error": f"ProxyError: {e}"}
    except requests.exceptions.ConnectTimeout:
        return {"url": url, "status": None, "ok": False, "error": "ConnectTimeout"}
    except requests.exceptions.ReadTimeout:
        return {"url": url, "status": None, "ok": False, "error": "ReadTimeout"}
    except requests.exceptions.SSLError as e:
        return {"url": url, "status": None, "ok": False, "error": f"SSLError: {e}"}
    except requests.RequestException as e:
        return {"url": url, "status": None, "ok": False, "error": f"RequestException: {e}"}

def guess_balance_status(results):
    """
    Heurística:
      - Si al menos una URL devuelve 200 → proxy OK (probable con créditos).
      - 407 Proxy Authentication Required → credenciales inválidas / sin saldo (según proveedor).
      - 403/429 con cuerpo del proxy → bloqueo / cuota agotada / rate-limit.
    """
    any_200 = any(r.get("ok") and r.get("status") == 200 for r in results)
    if any_200:
        return "OK: proxy respondió 200 en al menos una URL (probable con créditos).", 0

    # Recolectar códigos y errores
    statuses = [r.get("status") for r in results if r.get("status") is not None]
    errors = [r.get("error") for r in results if r.get("error")]

    if 407 in statuses:
        return "ERROR: 407 Proxy Authentication Required (credenciales inválidas o saldo agotado).", 2
    if 403 in statuses:
        return "POSIBLE BLOQUEO/CUOTA: 403 desde el proxy o destino (puede ser falta de créditos o bloqueo por destino).", 3
    if 429 in statuses:
        return "LÍMITE: 429 Too Many Requests (rate limit alcanzado).", 3
    if errors:
        return f"ERROR de red/proxy: {', '.join(set(errors))}", 4
    return "FALLO: no hubo 200 y no se detectó causa clara.", 4

def main():
    parser = argparse.ArgumentParser(description="Chequeo de créditos/funcionamiento de proxy DataImpulse")
    parser.add_argument("--country", default=DI_COUNTRY, help="Código de país (ej: ar, br, us)")
    parser.add_argument("--session", default=None, help="ID de sesión sticky (si no, se genera aleatoria)")
    parser.add_argument("--host", default=DI_HOST)
    parser.add_argument("--port", default=DI_PORT, type=int)
    parser.add_argument("--user", default=DI_USER_BASE)
    parser.add_argument("--passw", default=DI_PASS)
    parser.add_argument("--no-proxy", action="store_true", help="Probar sin proxy (debug)")
    args = parser.parse_args()

    if args.no_proxy:
        proxies = None
        print(">> Modo sin proxy (debug local).")
    else:
        session_id = args.session or rand_session()
        username = build_username(args.user, args.country, session_id)
        auth = f"{username}:{args.passw}@{args.host}:{args.port}"
        proxy_url = f"http://{auth}"
        proxies = {"http": proxy_url, "https": proxy_url}
        print("==== Config proxy DataImpulse ====")
        print(f"Host:Port  = {args.host}:{args.port}")
        print(f"Country    = {args.country}")
        print(f"Session    = {session_id}")
        print(f"Username   = {username}")
        print("==================================")

    s = make_session(proxies)

    results = []
    for url, kind in TEST_URLS:
        r = probe(s, url, kind)
        results.append(r)
        # Log compacto por línea
        status = r.get("status")
        mark = "OK" if r.get("ok") and status == 200 else "FAIL"
        print(f"[{mark}] {url}  status={status}  err={r.get('error')}")

    # Mostrar un resumen legible de IP/país si lo obtuvimos
    def show_ip_country(rlist):
        ip, country, cc = None, None, None
        for r in rlist:
            if not r.get("ok") or r.get("status") != 200:
                continue
            data = r.get("data", {})
            # api.myip.com
            if "ip" in data:
                ip = data.get("ip") or ip
                country = data.get("country") or country
                cc = data.get("cc") or cc
            # ip-api.com
            if "query" in data:
                ip = data.get("query") or ip
                country = data.get("country") or country
                cc = data.get("countryCode") or cc
            # ifconfig.me
            if "ip_addr" in data:
                ip = data.get("ip_addr") or ip
        if ip or country or cc:
            print("\n— Salida detectada —")
            if ip: print(f"IP pública: {ip}")
            if country: print(f"País: {country}")
            if cc: print(f"CC: {cc}")

    show_ip_country(results)

    msg, exit_code = guess_balance_status(results)
    print("\nResultado:", msg)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
