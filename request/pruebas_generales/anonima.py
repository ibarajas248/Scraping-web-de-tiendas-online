import requests

cookies_str = "PEGA_AQUI_TUS_COOKIES"
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://supermercado.laanonimaonline.com/buscar?idListadoEspecial=352",
    "Cookie": cookies_str
}

payload = {
    "idListadoEspecial": "352",
    "pagina": "1",
    "orden": "relevancia",
    "filtros": ""
}

url = "https://supermercado.laanonimaonline.com/paginas/controlStockListados.php"

r = requests.post(url, headers=headers, data=payload)
print("Status:", r.status_code)
print(r.text[:500])  # ver inicio de respuesta
