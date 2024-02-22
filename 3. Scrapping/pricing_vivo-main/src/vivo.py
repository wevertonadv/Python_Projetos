### lista de produtos do excel ###
import requests
import json
import time
from tqdm import tqdm
import ast
import re
from pprint import pprint
import pandas as pd
import datetime
from tqdm import tqdm
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

# Caminho do arquivo Excel
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


### pega a lista de todos os produtos encontrados pela busca de cada item do excel
produtos_novos = []
produtos_nao_encontrados = []

for produto in tqdm(produtos, desc="Gerando lista de produtos"):
    try:
        busca_codificada = produto.replace(" ", "%20").lower()

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Connection": "keep-alive",
            "Origin": "https://store.vivo.com.br",
            "Referer": "https://store.vivo.com.br/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "X-Anonymous-Consents": "%5B%5D",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }

        response = requests.get(
            f"https://api.store.vivo.com.br/occ/v2/vivo/products/search?fields=products(code%2Cname%2Csummary%2Cconfigurable%2CconfiguratorType%2Cimages(DEFAULT)%2CaverageRating%2CvariantOptions%2Cmanufacturer)%2Cfacets%2Cbreadcrumbs%2Cpagination(DEFAULT)%2Csorts(DEFAULT)%2CfreeTextSearch%2CcurrentQuery%2CkeywordRedirectUrl&query={busca_codificada}&pageSize=5&lang=pt&curr=BRL",
            headers=headers,
        )

        data = response.json()

        # Lista para armazenar os slugs
        slugs = []

        # Adiciona os slugs dos produtos
        for produto in data["products"]:
            produtos_novos.append(produto["code"])
    except:
        log.warning(f"Produtoo não encontrado {produto}")
        produtos_nao_encontrados.append(produto)

produtos_novos = list(produtos_novos)

### pega a lista de todos os produtos encontrados em todas as paginas da busca
produtos_ja_buscados = set()
res = []

headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://store.vivo.com.br",
    "Referer": "https://store.vivo.com.br/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "X-Anonymous-Consents": "%5B%5D",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

response = requests.get(
    "https://api.store.vivo.com.br/occ/v2/vivo/products/search?fields=products(code%2Cname%2Csummary%2Cconfigurable%2CconfiguratorType%2Cprice(FULL)%2Cimages(DEFAULT)%2Cstock(FULL)%2CaverageRating%2CvariantOptions%2Cmanufacturer)%2Cfacets%2Cbreadcrumbs%2Cpagination(DEFAULT)%2Csorts(DEFAULT)%2CfreeTextSearch%2CcurrentQuery%2CkeywordRedirectUrl&currentPage=20&lang=pt&curr=BRL",
    headers=headers,
)

data = response.json()

total_pages = data["pagination"]["totalPages"]

slugs = []
for page in range(1, total_pages):
    try:
        response = requests.get(
            f"https://api.store.vivo.com.br/occ/v2/vivo/products/search?fields=products(code%2Cname%2Csummary%2Cconfigurable%2CconfiguratorType%2Cprice(FULL)%2Cimages(DEFAULT)%2Cstock(FULL)%2CaverageRating%2CvariantOptions%2Cmanufacturer)%2Cfacets%2Cbreadcrumbs%2Cpagination(DEFAULT)%2Csorts(DEFAULT)%2CfreeTextSearch%2CcurrentQuery%2CkeywordRedirectUrl&currentPage={page}&lang=pt&curr=BRL",
            headers=headers,
        )

        data = response.json()
        for produto in data["products"]:
            slugs.append(produto["code"])

        time.sleep(0.5)
    except:
        pass

### filtrando produtos que já foram encontrados ###
produtos_novos = produtos_novos + slugs
produtos_novos = set(produtos_novos)

headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://store.vivo.com.br",
    "Referer": "https://store.vivo.com.br/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "X-Anonymous-Consents": "%5B%5D",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

### buscando as informações de preço de cada produto ###
lista_to_concat = []

for slug in tqdm(produtos_novos, desc="Processando produtos"):
    try:
        response = requests.get(
            f"https://api.store.vivo.com.br/occ/v2/vivo/products/{slug}?changeCache=112&fields=code,name,summary,price(formattedValue),images(DEFAULT,galleryIndex),installmentInfo(FULL),baseProduct,classifications&lang=pt&curr=BRL",
            headers=headers,
        )

        data = response.json()

        response2 = requests.get(
            f"https://api.store.vivo.com.br/occ/v2/vivo/products/{slug}",
            headers=headers,
        )

        data2 = response2.json()

        # O produto é o próprio dicionário principal no JSON fornecido
        produto = data

        # Iterar sobre variantMatrix para extrair cores e capacidades
        capacidades = []
        cores = []

        # 'Zerando' os valores para não serem utilizados no próximo produto caso o valor não seja encontrado
        marca, ean, linha, capacidade, cor, modelo = None, None, None, None, None, None

        # Iterar pela matriz de variantes para extrair as informações
        for classification in data.get("classifications", []):
            for feature in classification.get("features", []):
                if feature.get("name") == "Marca":
                    marca = feature.get("featureValues", [{}])[0].get("value")
                if feature.get("name") == "EAN":
                    ean = feature.get("featureValues", [{}])[0].get("value")
                if feature.get("name") == "Linha":
                    linha = feature.get("featureValues", [{}])[0].get("value")
                if feature.get("name") == "Memoria Interna":
                    capacidade = feature.get("featureValues", [{}])[0].get("value")
                if feature.get("name") == "Cor":
                    cor = feature.get("featureValues", [{}])[0].get("value")
                if feature.get("name") == "Modelo":
                    modelo = feature.get("featureValues", [{}])[0].get("value")

        lista_to_concat.append(
            {
                "fabricante": None,
                "marca": marca,
                "ean": ean,
                "sku": slug,
                "modelo": modelo,
                "nome_comercial": linha,
                "capacidade_armazenamento": capacidade,
                "cor": cor,
                "nome_produto": produto["name"],
                "empresa": "Vivo",
                "vendedor": "Vivo",
                "preco_pix": None,
                "preco_boleto": None,
                "preco_credito_1x": data2["price"].get("value"),
                "preco_prazo_sem_juros_cartao_normal": data2["price"].get("value"),
                "qtd_parcelas_sem_juros_cartao_normal": data2["installmentInfo"].get(
                    "installment"
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
                # "estoque": data2["stock"]["stockLevel"],
                "estoque": bool(data2["stock"]["stockLevel"]) if data2["stock"]["stockLevel"] is not None else None,
                "url": "https://store.vivo.com.br" + data2["url"],
                # "data_extração": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_extração": get_current_date()
            }
        )

        time.sleep(0.5)
    except:
        pass
# Exportar os dados para um DataFrame
df_export_final = pd.DataFrame(lista_to_concat)
# df_export_final.to_excel("Extrações Vivo.xlsx", index=False)

# Upload to bucket

# Formatar a data e hora no formato desejado
pasta = "extracoes/"
loja = "vivo"
agora = datetime.datetime.now()
nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

gcs_manager.upload_parquet(df_export_final, nome_arquivo)
log.info("Fim")