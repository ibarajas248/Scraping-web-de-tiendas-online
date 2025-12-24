import requests
import json

CAPSOLVER_API_KEY = "CAP-D2D4BC1B86FD4F550ED83C329898264E02F0E2A7A81E1B079F64F7F11477C8FD"

URL = "https://api.capsolver.com/getBalance"

payload = {
    "clientKey": CAPSOLVER_API_KEY
}

response = requests.post(URL, json=payload, timeout=15)

print("Status code:", response.status_code)
print("Respuesta:")
print(json.dumps(response.json(), indent=2))
