from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import pandas as pd
from rapidfuzz import process, fuzz
import unicodedata
import urllib.parse
import time
from collections import defaultdict
from starlette.responses import JSONResponse

# Inicializa o aplicativo FastAPI
app = FastAPI()

# Define os parâmetros de rate limiting
RATE_LIMIT = 100  # Número máximo de requisições permitidas
TIME_WINDOW = 60  # Janela de tempo em segundos

# Dicionário para armazenar informações de requisições por IP
requests_counts = defaultdict(list)

# Middleware para rate limiting
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host
    current_time = time.time()

    if client_ip not in requests_counts:
        requests_counts[client_ip] = []

    window_start = current_time - TIME_WINDOW
    requests_counts[client_ip] = [timestamp for timestamp in requests_counts[client_ip] if timestamp > window_start]

    if len(requests_counts[client_ip]) >= RATE_LIMIT:
        return JSONResponse(
            {"detail": "Muitas requisições. Por favor, tente novamente mais tarde."},
            status_code=429
        )

    requests_counts[client_ip].append(current_time)
    response = await call_next(request)
    return response

# Carrega a tabela agregada em um DataFrame
tabela_agregada = pd.read_csv(r"C:\Users\fabricio.silva\Documents\simple project\arquivos_ultimos_3_dias\tabela_filtrada.csv")

# Função para normalizar textos
def normalizar_texto(texto):
    if isinstance(texto, str):
        texto = unicodedata.normalize('NFKD', texto)
        texto = ''.join([c for c in texto if not unicodedata.combining(c)])
        texto = texto.lower().strip()
        return texto
    return ''

# Aplica a normalização nos nomes dos municípios
tabela_agregada['municipio_normalizado'] = tabela_agregada['municipio'].apply(normalizar_texto)

# Cria um conjunto de municípios únicos normalizados
municipios_unicos = set(tabela_agregada['municipio_normalizado'].unique())

# Cria um dicionário que mapeia o município normalizado para os dados correspondentes
municipio_para_focos = {}
for municipio_normalizado, grupo in tabela_agregada.groupby('municipio_normalizado'):
    municipio_para_focos[municipio_normalizado] = grupo

# Define o modelo de entrada
class MunicipioRequest(BaseModel):
    municipio: str

# Dicionário de cache para resultados de alta similaridade
cache = {}

# Define o endpoint POST
@app.post("/focos")
async def get_focos(municipio_request: MunicipioRequest):
    municipio_input = normalizar_texto(municipio_request.municipio)

    # Verifica se o resultado está no cache
    if municipio_input in cache:
        return cache[municipio_input]

    # Usa rapidfuzz para encontrar correspondências parciais
    correspondencias = process.extract(
        municipio_input,
        municipios_unicos,
        scorer=fuzz.partial_ratio,
        limit=20,
        score_cutoff=75
    )

    if not correspondencias:
        raise HTTPException(status_code=404, detail="Nenhum município encontrado. Verifique a ortografia ou tente um nome diferente.")

    # Verifica se há alguma correspondência com score 100
    tem_score_100 = any(score == 100 for _, score, _ in correspondencias)

    if tem_score_100:
        # Filtra apenas as correspondências com score 100
        correspondencias = [c for c in correspondencias if c[1] == 100]
    else:
        # Se não houver score 100, mantém todas as correspondências com score >= 75
        correspondencias = [c for c in correspondencias if c[1] >= 75]

    # Ordena as correspondências para priorizar municípios que contêm a entrada do usuário
    def prioridade(municipio_encontrado):
        return 0 if municipio_input in municipio_encontrado else 1

    correspondencias.sort(key=lambda x: (prioridade(x[0]), -x[1]))

    resultados_focos = []
    municipios_encontrados = []
    municipios_adicionados = set()

    for municipio_encontrado, score, _ in correspondencias:
        if municipio_encontrado in municipios_adicionados:
            continue
        municipios_adicionados.add(municipio_encontrado)

        focos = municipio_para_focos.get(municipio_encontrado)

        if focos is not None and not focos.empty:
            municipio_original = focos['municipio'].iloc[0]
            municipios_encontrados.append(municipio_original)

            municipio_encoded = urllib.parse.quote(municipio_original)

            for _, row in focos.iterrows():
                lat = row['lat']
                lon = row['lon']
                data_hora_gmt = row['data_hora_gmt']

                google_maps_url = f"https://www.google.com/maps?q={lat},{lon}"
                google_maps_directions_url = f"https://www.google.com/maps/dir/{municipio_encoded}/{lat},{lon}"

                resultados_focos.append({
                    "municipio": municipio_original,
                    "lat": lat,
                    "lon": lon,
                    "data_hora_gmt": data_hora_gmt,
                    "google_maps_url": google_maps_url,
                    "google_maps_directions_url": google_maps_directions_url
                })
            print(municipio_original, score)
            # Apenas cacheia se todas as correspondências são score 100
            if tem_score_100 and score == 100:
                cache[municipio_input] = {"municipios_encontrados": municipios_encontrados, "focos": resultados_focos}
            elif not tem_score_100 and score >= 95:
                cache[municipio_input] = {"municipios_encontrados": municipios_encontrados, "focos": resultados_focos}

    return {"municipios_encontrados": municipios_encontrados, "focos": resultados_focos}


# uvicorn procurar_focos:app --reload
