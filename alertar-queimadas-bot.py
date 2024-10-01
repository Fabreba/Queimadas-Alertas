from pyrogram import Client, filters
import pandas as pd
from rapidfuzz import process, fuzz
import unicodedata
import urllib.parse

api_id = 27493038
api_hash = "71cea1f2a7f148b329e7ce5f3aa19c2f"
bot_token = "7475262429:AAGW-6LkyVqj0wXtub74Rk_8K8cUWgwVv5Y"
app = Client(
    "queimadas-alerta-bot",
    api_id=api_id, api_hash=api_hash,
    bot_token=bot_token
)

# Carrega a tabela agregada em um DataFrame
tabela_agregada = pd.read_csv(r"tabela_filtrada.csv")

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

# Dicionário de cache para resultados de alta similaridade
cache = {}

# Dicionário para armazenar o estado de cada usuário
user_states = {}

# Mensagem de boas-vindas ou ajuda
@app.on_message(filters.private & filters.command(["start", "help"]))
async def send_welcome(client, message):
    user_id = message.from_user.id
    welcome_text = ("Olá! Eu sou o bot de alerta de queimadas.\n"
                    "Por favor, digite o nome do seu município para verificar focos de queimada.")
    await client.send_message(user_id, welcome_text)

# Handler para mensagens de texto
@app.on_message(filters.private & ~filters.command(["sim", "não"]))
async def handle_municipio_input(client, message):
    user_id = message.from_user.id
    municipio_input_raw = message.text
    municipio_input = normalizar_texto(municipio_input_raw)

    # Usa rapidfuzz para encontrar correspondências parciais
    correspondencias = process.extract(
        municipio_input,
        municipios_unicos,
        scorer=fuzz.partial_ratio,
        limit=10,
        score_cutoff=75
    )

    if not correspondencias:
        await client.send_message(user_id, "Nenhum município encontrado. Verifique a ortografia ou tente um nome diferente.")
        return

    # Ordena as correspondências pela melhor similaridade
    correspondencias.sort(key=lambda x: -x[1])

    # Armazena o estado do usuário com a lista de municípios encontrados
    user_states[user_id] = {
        'municipios_sugeridos': correspondencias,
        'indice_atual': 0
    }

    await perguntar_municipio(client, user_id)

async def perguntar_municipio(client, user_id):
    estado = user_states.get(user_id)
    if estado is None:
        return

    indice = estado['indice_atual']
    municipios_sugeridos = estado['municipios_sugeridos']

    if indice >= len(municipios_sugeridos):
        await client.send_message(user_id, "Nenhum outro município encontrado.")
        del user_states[user_id]
        return

    municipio_encontrado = municipios_sugeridos[indice][0]
    municipio_original = next(iter(tabela_agregada[tabela_agregada['municipio_normalizado'] == municipio_encontrado]['municipio'].unique()))

    estado['municipio_selecionado'] = municipio_encontrado
    await client.send_message(user_id, f"Sua cidade é {municipio_original}?\nResponda com /sim ou /não.")

# Handler para respostas '/sim' e '/não'
@app.on_message(filters.private & filters.command(["sim", "nao"]))
async def handle_sim_nao(client, message):
    user_id = message.from_user.id
    resposta = message.text[1:].lower()  # Remove a barra inicial e converte para minúsculas

    estado = user_states.get(user_id)
    if estado is None:
        await client.send_message(user_id, "Por favor, digite o nome do seu município para começar.")
        return

    if resposta == 'sim':
        municipio_encontrado = estado['municipio_selecionado']
        municipio_input = municipio_encontrado

        focos = municipio_para_focos.get(municipio_input)

        if focos is None or focos.empty:
            await client.send_message(user_id, "Nenhum foco de queimada encontrado para este município.")
        else:
            resultados_focos = []

            municipio_original = focos['municipio'].iloc[0]
            municipio_encoded = urllib.parse.quote(municipio_original)

            for _, row in focos.iterrows():
                lat = row['lat']
                lon = row['lon']
                data_hora_gmt = row['data_hora_gmt']

                google_maps_url = f"https://www.google.com/maps?q={lat},{lon}"

                resultados_focos.append({
                    "municipio": municipio_original,
                    "lat": lat,
                    "lon": lon,
                    "data_hora_gmt": data_hora_gmt,
                    "google_maps_url": google_maps_url,
                })

            # Envia os resultados em mensagens separadas se necessário
            max_length = 4096
            message_parts = []
            current_message = ""

            for foco in resultados_focos:
                foco_text = (f"Município: {foco['municipio']}\n"
                             f"Data/Hora: {foco['data_hora_gmt']}\n"
                             f"Latitude: {foco['lat']}\n"
                             f"Longitude: {foco['lon']}\n"
                             f"Google Maps: {foco['google_maps_url']}\n\n")
                if len(current_message) + len(foco_text) > max_length:
                    message_parts.append(current_message)
                    current_message = foco_text
                else:
                    current_message += foco_text

            if current_message:
                message_parts.append(current_message)

            for part in message_parts:
                await client.send_message(user_id, part)

        del user_states[user_id]
    elif resposta == 'nao':
        # Passa para o próximo município sugerido
        estado['indice_atual'] += 1
        await perguntar_municipio(client, user_id)
    else:
        await client.send_message(user_id, "Resposta inválida. Por favor, responda com /sim ou /nao.")

app.run()
