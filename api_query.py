import requests
import json
import time
import os
from datetime import datetime

# ========== CONFIGURAÇÕES ==========
URL = "https://api-publica.datajud.cnj.jus.br/api_publica_tjsp"
HEADERS = {
    "Authorization": "APIKey cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw==",
    "Content-Type": "application/json"
}

# Filtros (os que funcionaram: nivelSigilo=0 e sistema.codigo=3)
FILTROS = [
    {"term": {"nivelSigilo": 0}},
    {"term": {"sistema.codigo": 3}},
    {
        "range": {
            "dataAjuizamento": {
                "gte": "2025-01-01",
                "lte": "2025-12-31"
            }
        }
    }
]

SIZE = 10000          # documentos por requisição (pode ser até 10000, mas 1000 é seguro)
SCROLL_TIMEOUT = "2m" # tempo de vida do scroll
SLEEP = 0.5          # pausa entre requisições

# ========== PREPARAR PASTA DE SAÍDA ==========
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = f"D:\\TJSP\\extracao_{timestamp}"
os.makedirs(output_dir, exist_ok=True)
print(f"Resultados serão salvos em: {output_dir}")

# Arquivos de controle
metadata_path = os.path.join(output_dir, "_metadata.json")
state_path = os.path.join(output_dir, "_state.json")

# ========== FUNÇÕES AUXILIARES ==========
def salvar_lote(batch_num, docs):
    filename = f"batch_{batch_num:04d}.json"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    return filepath

def salvar_metadata(total_estimado, total_extraido):
    meta = {
        "data_inicio": timestamp,
        "filtros": FILTROS,
        "size_por_requisicao": SIZE,
        "scroll_timeout": SCROLL_TIMEOUT,
        "total_estimado": total_estimado,
        "total_extraido": total_extraido
    }
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

def salvar_estado(scroll_id, batch_num):
    state = {
        "scroll_id": scroll_id,
        "ultimo_lote": batch_num
    }
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def carregar_estado():
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

# ========== 1. ESTIMAR TOTAL (opcional) ==========
print("Estimando total de documentos...")
payload_total = {"query": {"bool": {"filter": FILTROS}}, "size": 0}
resp_total = requests.post(f"{URL}/_search", headers=HEADERS, json=payload_total)
if resp_total.status_code == 200:
    total_estimado = resp_total.json()["hits"]["total"]["value"]
    print(f"Total estimado (pode ser aproximado): {total_estimado}")
else:
    total_estimado = None
    print("Erro ao estimar total:", resp_total.text)

# ========== 2. VERIFICAR SE HÁ ESTADO PARA CONTINUAR ==========
estado = carregar_estado()
if estado:
    print(f"Estado encontrado. Scroll ID: {estado['scroll_id']}, último lote: {estado['ultimo_lote']}")
    scroll_id = estado["scroll_id"]
    batch_num = estado["ultimo_lote"] + 1
    # Tentar recuperar o próximo lote a partir do scroll_id
    scroll_payload = {"scroll": SCROLL_TIMEOUT, "scroll_id": scroll_id}
    resp = requests.post(f"https://api-publica.datajud.cnj.jus.br/_search/scroll",
                         headers=HEADERS, json=scroll_payload)
    if resp.status_code == 200:
        data = resp.json()
        hits = data["hits"]["hits"]
        if not hits:
            print("Scroll expirado ou não há mais documentos. Reiniciando.")
            estado = None
        else:
            print("Continuando extração...")
            # Já temos o primeiro lote após a recuperação
            # Vamos tratá-lo no loop
    else:
        print("Falha ao recuperar scroll. Reiniciando.")
        estado = None

# ========== 3. INICIAR NOVO SCROLL SE NECESSÁRIO ==========
if not estado:
    print("Iniciando novo scroll...")
    query = {"bool": {"filter": FILTROS}}
    payload_scroll = {"query": query, "size": SIZE}
    resp = requests.post(f"{URL}/_search?scroll={SCROLL_TIMEOUT}",
                         headers=HEADERS, json=payload_scroll)
    if resp.status_code != 200:
        print("Erro ao iniciar scroll:", resp.text)
        exit(1)
    data = resp.json()
    scroll_id = data["_scroll_id"]
    hits = data["hits"]["hits"]
    batch_num = 1
    # Salvar estado inicial
    salvar_estado(scroll_id, batch_num-1)  # último lote salvo = 0

# ========== 4. LOOP DE EXTRAÇÃO ==========
total_extraido = 0
while hits:
    # Salvar lote atual
    filepath = salvar_lote(batch_num, hits)
    total_extraido += len(hits)
    print(f"Lote {batch_num}: {len(hits)} docs | Total acumulado: {total_extraido}")
    
    # Atualizar metadados
    salvar_metadata(total_estimado, total_extraido)
    
    # Preparar próximo lote via scroll
    scroll_payload = {"scroll": SCROLL_TIMEOUT, "scroll_id": scroll_id}
    resp = requests.post("https://api-publica.datajud.cnj.jus.br/_search/scroll",
                         headers=HEADERS, json=scroll_payload)
    if resp.status_code != 200:
        print("Erro ao obter próximo lote:", resp.text)
        break
    
    data = resp.json()
    scroll_id = data.get("_scroll_id")
    hits = data["hits"]["hits"]
    
    if hits:
        batch_num += 1
        salvar_estado(scroll_id, batch_num-1)  # atualiza estado após cada lote
    
    time.sleep(SLEEP)

print("Extração concluída!")

# ========== 5. LIMPEZA (opcional) ==========
# Remove o scroll_id do servidor
if scroll_id:
    requests.delete("https://api-publica.datajud.cnj.jus.br/_search/scroll",
                    headers=HEADERS, json={"scroll_id": scroll_id})

print(f"Total extraído: {total_extraido} documentos.")
print(f"Arquivos salvos em: {output_dir}")