import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime, timedelta
import pandas as pd

# URL do site
url = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil/"

# Função para obter as datas dos últimos 3 dias
def obter_ultimas_datas(num_dias=3):
    hoje = datetime.now()
    return [(hoje - timedelta(days=i)).strftime("%Y%m%d") for i in range(num_dias)]

# Obtém as datas dos últimos 3 dias
datas = obter_ultimas_datas()

# Faz a requisição GET para o site
try:
    response = requests.get(url)
    response.raise_for_status()  # Levanta um erro para códigos de status HTTP 4xx e 5xx
except requests.exceptions.HTTPError as errh:
    print(f"Erro HTTP: {errh}")
except requests.exceptions.ConnectionError as errc:
    print(f"Erro de Conexão: {errc}")
except requests.exceptions.Timeout as errt:
    print(f"Timeout: {errt}")
except requests.exceptions.RequestException as err:
    print(f"Erro: {err}")
else:
    soup = BeautifulSoup(response.content, 'html.parser')
    # Continue com o processamento do soup...

    # Encontra todas as divs com classe "row"
    rows = soup.find_all('div', class_='row')

    # Filtra as linhas que contêm as datas dos últimos 3 dias no nome do arquivo
    arquivos_para_baixar = []
    for row in rows:
        link = row.find('a')
        if link and link['href'].endswith('.csv'):
            for data in datas:
                if f"_{data}" in link['href']:
                    arquivos_para_baixar.append((data, link['href']))
                    break

    # Verifica se todos os arquivos dos últimos 3 dias estão disponíveis
    if len(arquivos_para_baixar) < len(datas):
        print("Nem todos os arquivos dos últimos 3 dias estão disponíveis. Verifique a página.")
    else:
        # Cria um diretório para salvar os arquivos
        diretorio = "arquivos_ultimos_3_dias"
        os.makedirs(diretorio, exist_ok=True)

        # Lista para armazenar DataFrames
        dataframes = []

        # Faz o download de cada arquivo e agrega os dados
        for data, arquivo in arquivos_para_baixar:
            full_url = url + arquivo
            file_name = os.path.basename(full_url)
            caminho_arquivo = os.path.join(diretorio, file_name)

            # Verifica se o arquivo já foi baixado
            if os.path.exists(caminho_arquivo):
                # Verifica se o arquivo não está vazio
                if os.path.getsize(caminho_arquivo) > 0:
                    print(f"Arquivo '{file_name}' já existe e não está vazio. Pulando download.")
                    df = pd.read_csv(caminho_arquivo)
                    df['data'] = data
                    dataframes.append(df)
                    continue
                else:
                    print(f"Arquivo '{file_name}' existe, mas está vazio. Baixando novamente.")

            # Faz o download do arquivo
            file_response = requests.get(full_url)
            
            # Salva o arquivo localmente
            with open(caminho_arquivo, 'wb') as file:
                file.write(file_response.content)
            
            print(f"Arquivo '{file_name}' baixado com sucesso.")

            # Lê o arquivo CSV em um DataFrame
            df = pd.read_csv(caminho_arquivo)
            
            # Adiciona uma coluna com a data para diferenciar as tabelas
            df['data'] = data
            
            # Adiciona o DataFrame à lista
            dataframes.append(df)

        # Concatena todos os DataFrames em um único DataFrame
        tabela_agregada = pd.concat(dataframes, ignore_index=True)

        # Remove linhas duplicadas
        tabela_agregada = tabela_agregada.drop_duplicates()

        # Converte a coluna 'data_hora_gmt' para datetime
        tabela_agregada['data_hora_gmt'] = pd.to_datetime(tabela_agregada['data_hora_gmt'])

        # Filtra registros com base na diferença de 2 horas
        registros_filtrados = []

        # Agrupa por município
        for municipio, grupo in tabela_agregada.groupby('municipio'):
            grupo = grupo.sort_values(by='data_hora_gmt', ascending=False)  # Ordena por data_hora_gmt mais recente
            mais_recente = grupo.iloc[0]  # Mantém o mais recente

            for i in range(1, len(grupo)):
                if (mais_recente['data_hora_gmt'] - grupo.iloc[i]['data_hora_gmt']).total_seconds() >= 7200:
                    registros_filtrados.append(mais_recente)  # Adiciona o mais recente
                    mais_recente = grupo.iloc[i]  # Atualiza para o próximo mais recente
                else:
                    continue  # Ignora se a diferença for menor que 2 horas

            registros_filtrados.append(mais_recente)  # Adiciona o último registro mais recente

        # Cria um DataFrame com os registros filtrados
        tabela_filtrada = pd.DataFrame(registros_filtrados)

        # Salva a tabela filtrada em um arquivo CSV
        tabela_filtrada.to_csv(os.path.join(diretorio, 'tabela_filtrada.csv'), index=False)

        print("Tabela filtrada salva como 'tabela_filtrada.csv'.")
