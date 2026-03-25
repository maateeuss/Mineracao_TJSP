
import requests
import json

url = "https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search"
headers = {
    "Authorization": "APIKey cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw==",
    "Content-Type": "application/json"
}

# Exemplo de consulta (busca todos os documentos, limitado a 10)
payload = {
    "query": {
        "match_all": {}
    },
    "size": 10
}

response = requests.post(url, headers=headers, json=payload)

print("Status:", response.status_code)
data = response.json()  # Parse the JSON response
print("Resposta:")
print(data)

# Save to a JSON file
with open('datajud_response_sample.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Arquivo 'datajud_response.json' salvo com sucesso.")