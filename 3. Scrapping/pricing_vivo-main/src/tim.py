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
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__
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
# arquivo_excel = r"assets\lista devices.xlsx"

# # Ler o arquivo Excel
# df = pd.read_excel(arquivo_excel)

# # Verificar se a coluna 'MODELO_COM_COR' existe no DataFrame
# if "MODELO_COM_COR" in df.columns:
#     # Salvar o conteúdo da coluna 'MODELO_COM_COR' em uma lista
#     produtos = df["MODELO_COM_COR"].tolist()
#     # debug(print("Lista de produtos:", produtos), debug_mode)
# else:
#     print("A coluna 'MODELO_COM_COR' não foi encontrada no arquivo.")

gcs_manager = GCSParquetManager()
produtos = gcs_manager.ler_coluna_excel()

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

        # Encontrar o plano "Apenas aparelhos"
        plano_aparelhos = next(
            (
                plano
                for plano in produto["GrupoPlano"]
                if plano["Titulo"] == "Apenas aparelhos"
            ),
            None,
        )

        # Se 'plano_aparelhos' existir, pegar o primeiro plano listado
        if plano_aparelhos and plano_aparelhos["Planos"]:
            plano = plano_aparelhos["Planos"][0]
        else:
            plano = {}

        # Iterar sobre cada cor disponível
        for cor in produto["Cores"]:
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
                    "preco_credito_1x": plano.get("PrecoAVista"),
                    "preco_prazo_sem_juros_cartao_normal": plano.get("PrecoAVista"),
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
                    "data_extração": get_current_date()
                }
            )

    except:
        log.error(f"Erro requisicao slug {slug}")
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
    ]
]
# df_export_final.to_excel("Extrações Tim.xlsx", index=False)

# Upload to bucket

# Formatar a data e hora no formato desejado
pasta = "extracoes/"
loja = "tim"
agora = datetime.datetime.now()
nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

gcs_manager.upload_parquet(df_export_final, nome_arquivo)
log.info("Fim")