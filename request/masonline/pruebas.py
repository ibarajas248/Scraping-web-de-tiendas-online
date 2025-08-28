import requests
base = "https://www.pinguino.com.ar"
paths = [
    "/web/api", "/api/", "/ws/", "/webservices/",
    "/products.json", "/wp-json/", "/rest/default/V1/store/websites",
    "/web/buscar.r?format=json", "/web/productos.r?format=json"
]
for p in paths:
    url = base + p
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        print(f"{url} -> {r.status_code} | {r.headers.get('Content-Type','')} | {r.text[:120]!r}")
    except Exception as e:
        print(f"{url} -> error: {e}")
