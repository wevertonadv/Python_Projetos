### lista de produtos do excel ###
import random
import requests
import json
import time
from tqdm import tqdm
import ast
import re
from pprint import pprint
import pandas as pd
import datetime
from modules.logger_control import Logger, __main__,os
from modules.date_helper import get_current_date

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))
log.info("Inicio")
# Função de debug
def debug(message, mode=False):
    if mode:
        print(f"DEBUG: {message}")


# Configuração inicial
debug_mode = False

# Caminho do arquivo Excel #
arquivo_excel = r"assets\lista devices.xlsx"

# Ler o arquivo Excel
#df = pd.read_excel(arquivo_excel)


df = pd.DataFrame(["Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung T224 64GB Tablet","Samsung T224 64GB Tablet","Samsung T575N","Samsung T575N","Samsung F721B 128GB","Samsung F721B 128GB","Samsung F721B 128GB","Samsung F721B 128GB","Samsung F721B 256GB","Samsung F721B 256GB","Samsung F721B 256GB","Samsung F721B 256GB","Samsung Galaxy F731B 512GB","Samsung Galaxy F731B 512GB","Samsung Galaxy F731B 512GB","Samsung Galaxy F731B 512GB","Samsung F936B 256GB","Samsung F936B 256GB","Samsung F936B 256GB","Samsung F936B 512GB","Samsung F936B 512GB","Samsung F936B 512GB","Samsung Galaxy F946B 1TB","Samsung Galaxy F946B 1TB","Samsung Galaxy F946B 1TB","Samsung Galaxy F946B 512GB","SEMP TCL GO3c Plus","Alcatel 5033E","SEMP TCL L5 5033E"])
# Verificar se a coluna 'MODELO_COM_COR' existe no DataFrame
if "MODELO_COM_COR" in df.columns:
    # Salvar o conteúdo da coluna 'MODELO_COM_COR' em uma lista
    produtos = df["MODELO_COM_COR"].tolist()
    # debug(print("Lista de produtos:", produtos), debug_mode)
else:
    log.info("A coluna 'MODELO_COM_COR' não foi encontrada no arquivo.")


### pega a lista de todos os produtos encontrados em todas as paginas da busca ###
headers = {
    "authority": "cassandra-tim.alliedstore.com.br",
    "accept": "application/json",
    "accept-language": "pt-BR,pt;q=0.9",
    "api_v2_token": "8do5srfnhmxxwfvzncjeuwy2ybfn6d6gyztc8lvx6kt431gbposvem8af7v2wtxj",
    "origin": "https://lojaonline.tim.com.br",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "x-frame-options": "sameorigin",
}

response = requests.get(
    "https://cassandra-tim.alliedstore.com.br/v2/ConsultarProduto?&ddd=11&page=1&hasStock=false&offset=20&hasFilter=false",
    headers=headers,
)

data = response.json()

total_pages = data["Pagination"]["TotalPages"]

slugs = []
for page in tqdm(range(1, total_pages),desc='Buscando lista de produtos.'):
    response = requests.get(
        f"https://cassandra-tim.alliedstore.com.br/v2/ConsultarProduto?&ddd=11&page={page}&hasStock=false&offset=20&hasFilter=false",
        headers=headers,
    )

    data = response.json()
    for produto in data["Produtos"]:
        slugs.append(produto["Slug"])

    time.sleep(0.5)

### buscando as informações de preço de cada produto ###
lista_to_concat = []
produtos_com_erro_de_requisicao = []
for slug in tqdm(slugs, desc=f"Processando Produtos"):
    try:
        headers = {
            "authority": "cassandra-tim.alliedstore.com.br",
            "accept": "application/json",
            "accept-language": "pt-BR,pt;q=0.9",
            "api_v2_token": "8do5srfnhmxxwfvzncjeuwy2ybfn6d6gyztc8lvx6kt431gbposvem8af7v2wtxj",
            "origin": "https://lojaonline.tim.com.br",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "x-frame-options": "sameorigin",
        }

        params = {
            "slug": slug,
            "ddd": "11",
        }

        response = requests.get(
            "https://cassandra-tim.alliedstore.com.br/v2/ConsultarProduto",
            params=params,
            headers=headers,
        )

        data = response.json()

        produto = data["Produtos"][0]
        especificacoes = {
            esp["Propriedade"]: esp["Valor"]
            for esp in produto["EspecificacoesTecnicas"]
        }

        for planos_categoria in produto["GrupoPlano"]: # categorias de plano
            for plano in planos_categoria['Planos']: # planos disponiveis dentro da categoria
                for cor in produto["Cores"]:  # Iterar sobre cada cor disponível
                    lista_to_concat.append(
                        {
                            "fabricante": None,
                            "marca": especificacoes.get("Fabricante")
                            or produto.get("Fabricante"),
                            "ean": None,
                            "sku": cor.get("SkuCor"),  # produto.get("CodigoProduto"),
                            "no_modelo": None,
                            "nome_comercial": produto.get("Nome"),
                            "capacidade_armazenamento": especificacoes.get("Memória Interna"),
                            "cor": cor.get("CorModelo"),
                            "nome_produto": produto.get("Nome"),
                            "empresa": "Tim",
                            "vendedor": "Tim",
                            "estoque": cor.get("TemEstoque"),
                            "preco_pix": None,
                            "preco_boleto": None,
                            "preco_credito_1x": plano.get("PrecoAparelhoDe"),
                            "preco_prazo_sem_juros_cartao_normal": plano.get("PrecoAparelhoDe"),
                            "qtd_parcelas_sem_juros_cartao_normal": plano.get(
                                "ParcelasQuantidade"
                            ),
                            "preco_prazo_com_juros_cartao_normal": None,
                            "qtd_parcelas_com_juros_cartao_normal": None,
                            "taxa_juros_cartao_normal": None,
                            "preco_x1_cartao_proprio": None,
                            "preco_prazo_sem_juros_cartao_proprio": None,
                            "qtd_parcelas_sem_juros_cartao_proprio": None,
                            "preco_prazo_com_juros_cartao_proprio": None,
                            "qtd_parcelas_com_juros_cartao_proprio": None,
                            "taxa_juros_cartao_proprio": None,
                            "frete": None,
                            "cep": None,
                            "url": "https://lojaonline.tim.com.br/produto/" + slug,
                            # "data_extração": datetime.datetime.now().strftime(
                            #     "%Y-%m-%d %H:%M:%S"
                            # ),
                            "data_extração": get_current_date(),
                            'plano_nome': f"{plano.get('Nome')} - {plano.get('Slug')}" if plano.get('Nome') else None,
                            'plano_preco': plano.get('Preco'),
                            'plano_valor_aparelho':plano.get('PrecoAparelho'),
                            'plano_categoria':planos_categoria.get('Titulo')
                        }
                    )

    except:
        produtos_com_erro_de_requisicao.append(slug)

    time.sleep(0.5)

# Exportar os dados para um DataFrame
df_export_final = pd.DataFrame(lista_to_concat)
df_export_final = df_export_final[
    [
        "fabricante",
        "marca",
        "ean",
        "sku",
        "no_modelo",
        "nome_comercial",
        "capacidade_armazenamento",
        "cor",
        "empresa",
        "nome_produto",
        "empresa",
        "vendedor",
        "preco_pix",
        "preco_boleto",
        "preco_credito_1x",
        "preco_prazo_sem_juros_cartao_normal",
        "qtd_parcelas_sem_juros_cartao_normal",
        "preco_prazo_com_juros_cartao_normal",
        "qtd_parcelas_com_juros_cartao_normal",
        "taxa_juros_cartao_normal",
        "preco_x1_cartao_proprio",
        "preco_prazo_sem_juros_cartao_proprio",
        "qtd_parcelas_sem_juros_cartao_proprio",
        "preco_prazo_com_juros_cartao_proprio",
        "qtd_parcelas_com_juros_cartao_proprio",
        "taxa_juros_cartao_proprio",
        "frete",
        "cep",
        "estoque",
        "url",
        "data_extração",
        "plano_nome",
        "plano_preco",
        "plano_valor_aparelho",
        "plano_categoria",

    ]
]
df_export_final.to_excel("Extrações Tim.xlsx", index=False)
log.info("Fim")
