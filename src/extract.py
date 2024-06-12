#%%
import os
import urllib.request
import gzip
from pathlib import Path
import json
import unicodedata
from tqdm import tqdm
from datetime import datetime
#%%
def normalize_text(name):
    """Normaliza o texto para nomes de arquivos e diretórios.
        - Remove caracteres combinados (acentos, etc.)
        - Converte para letras minúsculas
        - Substitui espaços por sublinhados
        - Corta sublinhados à direita

    Argumentos:
        name (str): O texto a ser normalizado.

    Retorna:
        str: O texto normalizado.
    """

    normalized = unicodedata.normalize('NFKD', name)
    normalized_name = (
        ''.join([char for char in normalized if not unicodedata.combining(char)])
        .lower()
        .replace(" - ", " ")
        .replace(" ", "_")
    )
    if normalized_name.endswith('_'):
        normalized_name = normalized_name[:-1]
    return normalized_name

#%%
def download_and_extract(url, output_dir):
    """Baixa e extrai um arquivo compactado de um URL.

    Argumentos:
        url (str): O URL do arquivo para download.
        output_dir (str): O diretório onde o arquivo extraído será salvo.
    """

    local_path = Path(output_dir) / Path(url).name
    urllib.request.urlretrieve(url, local_path)

    with gzip.open(local_path, 'rb') as f_in:
        with open(local_path.with_suffix(''), 'wb') as f_out:
            f_out.write(f_in.read())

    os.remove(local_path)

#%%
def fetch_and_process(api_url, base_output_dir, metadata_file):
    """Busca dados de uma API, processa-os e baixa arquivos relevantes.

    Argumentos:
        api_url (str): O URL da API da qual buscar dados.
        base_output_dir (str): O diretório base para salvar os arquivos baixados.
        metadata_file (str): o caminho para o arquivo JSON que armazena metadados.
    """

    # Carrega os metadados existentes ou cria uma lista vazia se ela não existir
    existing_metadata = []
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r', encoding='utf-8') as f:
            existing_metadata = json.load(f)

    # Cria um dicionário para pesquisas por URL
    existing_metadata_dict = {item['url']: item for item in existing_metadata}

    with urllib.request.urlopen(api_url) as response:
        data = json.loads(response.read().decode())

    resources = data.get('result', {}).get('resources', [])

    for resource in tqdm(resources):
        url = resource.get('url')
        name = resource.get('name')
        last_modified = resource.get('last_modified')

        if not url or not url.endswith('.gz'):
            continue

        folder_name = name[:-4] if name[-4:].isdigit() else name
        normalized_folder_name = normalize_text(folder_name)
        output_dir = os.path.join(base_output_dir, normalized_folder_name)
        os.makedirs(output_dir, exist_ok=True)  # Cria o diretório se não existir

        # Verifica se há atualizações com base na data da última modificação
        if url in existing_metadata_dict:
            previous_last_modified = existing_metadata_dict[url]['last_modified']
            previous_last_modified_dt = datetime.strptime(previous_last_modified, '%Y-%m-%dT%H:%M:%S.%f')
            last_modified_dt = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%S.%f')

            if last_modified_dt <= previous_last_modified_dt:
                print(f"Arquivo {url} não foi atualizado.")
                continue

        print(f"Fazendo o download do arquivo {url} para {output_dir}")
        download_and_extract(url, output_dir)
        print(f"Arquivo extraído para {output_dir}")

        # Atualiza o registro nos metadados
        existing_metadata_dict[url].update({
            'last_modified': last_modified,
            'name': name,
            'size': resource.get('size')
        })

    # Salvar os metadados
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(list(existing_metadata_dict.values()), f, ensure_ascii=False, indent=4)
#%%
api_url = 'https://dados.mg.gov.br/api/3/action/package_show?id=despesa'
base_output_directory = '../data/raw/'
metadata_file = '../data/metadados_despesa.json'
fetch_and_process(api_url, base_output_directory, metadata_file)
# %%