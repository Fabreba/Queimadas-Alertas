from pyrogram import Client
from pyrogram.handlers import MessageHandler
import aiohttp  # Biblioteca HTTP assíncrona

api_id = 27493038
api_hash = "71cea1f2a7f148b329e7ce5f3aa19c2f"
bot_token = "7475262429:AAGW-6LkyVqj0wXtub74Rk_8K8cUWgwVv5Y"
app = Client(
    "queimadas-alerta-bot",
    api_id=api_id, api_hash=api_hash,
    bot_token=bot_token
)

user_states = {}  # Para rastrear o estado de cada usuário

async def my_function(client, message):
    user_id = message.from_user.id
    print("user_id", user_id)
    if user_id not in user_states:
        # Primeira mensagem do usuário, solicita o município
        user_states[user_id] = 'awaiting_municipio'
        await client.send_message(user_id, "Por favor, digite o seu município:")
    elif user_states[user_id] == 'awaiting_municipio':
        # Usuário enviou o município
        municipio_user = message.text
        # Faz a requisição POST assíncrona
        url = "http://127.0.0.1:8000/focos"  # Substitua SEU_SERVIDOR pelo IP ou hostname acessível
        payload = {'municipio': municipio_user}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    response.raise_for_status()
                    data = await response.json()
                    # Envia a resposta formatada de volta para o usuário
                    municipios_encontrados = data.get('municipios_encontrados', [])
                    focos = data.get('focos', [])
                    message_text = f"Municípios encontrados:\n"
                    for m in municipios_encontrados:
                        message_text += f"- {m}\n"
                    message_text += "\nFocos de queimada:\n"
                    if not focos:
                        message_text += "Nenhum foco de queimada encontrado."
                    else:
                        for foco in focos:
                            municipio = foco.get('municipio', 'N/A')
                            lat = foco.get('lat', 'N/A')
                            lon = foco.get('lon', 'N/A')
                            data_hora = foco.get('data_hora_gmt', 'N/A')
                            google_maps_url = foco.get('google_maps_url', '')
                            message_text += (f"Município: {municipio}\n"
                                             f"Data/Hora: {data_hora}\n"
                                             f"Latitude: {lat}\n"
                                             f"Longitude: {lon}\n"
                                             f"Google Maps: {google_maps_url}\n\n")
                    # Verifica se a mensagem excede o limite de caracteres do Telegram
                    max_length = 4096
                    message_text = message_text[:max_length-100]
                    print(message_text)
                    await client.send_message(user_id, message_text)
                
        except Exception as e:
            await client.send_message(user_id, f"Erro ao fazer a requisição: {e}")
        # Reseta o estado do usuário
        del user_states[user_id]
    else:
        # Qualquer outro estado, reinicia e começa novamente
        user_states[user_id] = 'awaiting_municipio'
        await client.send_message(user_id, "Por favor, digite o seu município:")

my_handler = MessageHandler(my_function)
app.add_handler(my_handler)

app.run()
# Retrying "messages.SendMessage" due to: Request timed out