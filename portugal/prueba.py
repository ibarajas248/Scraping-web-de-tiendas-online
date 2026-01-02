import requests

USER = "2cf8063dbace06f69df4"        # o con __cr.ar
PASS = "61425d26fb3c7287"
HOST = "gw.dataimpulse.com"
PORT = 823

proxy = f"http://{USER}:{PASS}@{HOST}:{PORT}"

proxies = {
    "http": proxy,
    "https": proxy,
}

try:
    r = requests.get(
        "https://httpbin.org/ip",
        proxies=proxies,
        timeout=15
    )
    print("✅ Proxy ACTIVO")
    print("IP salida:", r.text)
except Exception as e:
    print("❌ Proxy NO operativo:", e)
