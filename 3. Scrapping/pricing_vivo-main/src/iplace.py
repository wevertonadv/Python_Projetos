import random
import requests
import json
import time
from tqdm import tqdm
import ast
import re
import pandas as pd
import datetime
from bs4 import BeautifulSoup
import os
from urllib.parse import quote_plus
from modules.crud_bucket_gcp import GCSParquetManager
from modules.date_helper import get_current_date
from modules.logger_control import Logger, __main__

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))


def buscar_e_processar_produtos(busca_codificada, headers):
    """
    Realiza uma busca por produtos em um site e processa os resultados obtidos.

            Parameters:
                    busca_codificada (str): String de busca codificada
                    headers (dict): Cabeçalhos para a requisição HTTP

            Returns:
                    df (DataFrame): DataFrame contendo os atributos dos produtos encontrados
    """
    r = requests.get(
        f"https://www.iplace.com.br/ccstoreui/v1/search?Nrpp=24&visitorId=11CDDubbY78cOPVdO35J_q9ETOr59IR1DF5MQcVvuPEYQVU0639&visitId=-395ec7f9%3A18c697fee1f%3A-66a8-4094308871&totalResults=true&No=0&searchType=simple&Nr=AND(product.active%3A1%2CNOT(product.repositoryId%3A700700)%2CNOT(product.repositoryId%3A750750))&Ntt={busca_codificada}",
        headers=headers,
    )
    data = r.json()

    totalMatchingRecords = data["searchEventSummary"]["resultsSummary"][0][
        "totalMatchingRecords"
    ]

    if totalMatchingRecords == 0:
        return (
            pd.DataFrame()
        )  # Retorna um DataFrame vazio se nenhum produto for encontrado

    num_iterations = totalMatchingRecords // 24 + 1
    x_list = [24 * i for i in range(num_iterations)]
    x_list = x_list[:1]

    res = []
    for x in x_list:
        r = requests.get(
            f"https://www.iplace.com.br/ccstoreui/v1/search?Nrpp=24&visitorId=11CDDubbY78cOPVdO35J_q9ETOr59IR1DF5MQcVvuPEYQVU0639&visitId=-395ec7f9%3A18c697fee1f%3A-66a8-4094308871&totalResults=true&No=0&searchType=simple&Nr=AND(product.active%3A1%2CNOT(product.repositoryId%3A700700)%2CNOT(product.repositoryId%3A750750))&Ntt={busca_codificada}",
            headers=headers,
        )

        data = r.json()

        for record in data["resultsList"]["records"]:
            if "records" in record:
                records_list = record["records"]
                for nested_record in records_list:
                    if "attributes" in nested_record:
                        attributes = nested_record["attributes"]
                        if ((attributes["product.repositoryId"][-1][-1] == "R") | (attributes["product.repositoryId"][-1][-1] == "E")): # reembalado R, seminovo E
                            continue
                        res.append(attributes)


        time.sleep(0.5)

    df = pd.json_normalize(res)
    return df


def processar_informacoes_produto(df):
    """
    Processa um DataFrame contendo informações de produtos, formatando e
    adicionando colunas específicas.

    Esta função realiza as seguintes operações:
    - Adiciona colunas com valores padrão (None ou valores específicos).
    - Filtra e renomeia colunas conforme necessário.
    - Converte listas em strings, se aplicável.

            Parameters:
                    df (pd.DataFrame): DataFrame contendo informações dos produtos.

            Returns:
                    pd.DataFrame: DataFrame processado com as colunas formatadas.
    """
    # Configuração de data e hora
    data_hora_atual = datetime.datetime.now()
    formato = "%d/%m/%Y"
    data_hora_formatada = data_hora_atual.strftime(formato)

    # Adicionando e modificando colunas
    df["fabricante"] = None
    df["ean"] = None
    df["empresa"] = "iPlace"
    df["vendedor"] = "iPlace"

    # Lista de colunas desejadas
    colunas_desejadas = [
        "fabricante",
        "product.brand",
        "ean",
        "product.repositoryId",
        "product.x_type",
        "sku.x_capacidade",
        "sku.stylePropertyValue",
        "empresa",
        "product.displayName",
        "vendedor",
        "sku.availabilityStatus",
        "product.route",
        "record.type",
    ]

    # Adicionando colunas faltantes
    for coluna in colunas_desejadas:
        if coluna not in df.columns:
            df[coluna] = None

    # Filtrando e renomeando colunas
    df = df[
        [
            "fabricante",
            "product.brand",
            "ean",
            "product.repositoryId",
            "product.x_type",
            "sku.x_capacidade",
            "sku.stylePropertyValue",
            "empresa",
            "product.displayName",
            "vendedor",
            "sku.availabilityStatus",
            "product.route",
            "product.x_type",
            "record.type",
        ]
    ]
    df.columns = [
        "fabricante",
        "marca",
        "ean",
        "sku",
        "nome_comercial",
        "capacidade_armazenamento",
        "cor",
        "empresa",
        "nome_produto",
        "vendedor",
        "estoque",
        "url",
        "x_type",
        "type",
    ]

    # Convertendo listas em strings
    df = df.applymap(lambda x: str(x[0]) if isinstance(x, list) else x)

    return df


def filtrar_produtos_novos(df):
    """
    Filtra uma lista de produtos, retornando apenas aqueles que ainda não foram buscados.

            Parameters:
                    produtos_encontrados (list): Uma lista de SKUs de produtos encontrados.
                    produtos_ja_buscados (set): Um conjunto de SKUs de produtos que já foram buscados anteriormente.

            Returns:
                    produtos_novos (list): Uma lista dos SKUs dos produtos que ainda não foram buscados.
    """
    produtos_encontrados = list(df["sku"])
    produtos_novos = []
    for sku in produtos_encontrados:
        if sku not in produtos_ja_buscados:
            produtos_ja_buscados.add(sku)
            produtos_novos.append(sku)
    return produtos_novos


def buscar_precos(produtos_novos, df, headers):
    """
    Busca e compila informações de preço para cada produto em uma lista, a partir de uma API.

            Parameters:
                    produtos_novos (list): Lista de IDs de produtos
                    df (DataFrame): DataFrame Pandas base para mesclagem

            Returns:
                    df_export_final (DataFrame): DataFrame com informações de preço e detalhes adicionais de cada produto
    """

    lista_to_concat = []

    for x in produtos_novos:
        json_data = {
            "produtos": [
                {
                    "id": x,  # produto_id
                    "quantity": 1,
                },
            ],
            "siteId": "100002",
            "catalogId": "iplace",
            "retornarPromo": True,
        }

        response = requests.post(
            "https://www.iplace.com.br/ccstorex/custom/v1/parcelamento/getParcelamentos",
            headers=headers,
            json=json_data,
        )

        data = response.json()

        for produto in data["produtos"]:
            # Dados iniciais
            preco_pix = produto["parcelas"]["boleto"][0]["total"]
            preco_boleto = produto["parcelas"]["boleto"][0]["total"]

            # Encontrando os dados para cartão normal e cartão próprio
            parcelas_outros = produto["parcelas"]["outros"]
            promo_bancos = produto["promoBancos"]

            # Cartão normal - Considerando a chave 'outros'
            parcelas_sem_juros_outros = [
                p for p in parcelas_outros if not p["temJuros"]
            ]
            preco_prazo_sem_juros_cartao_normal = (
                parcelas_sem_juros_outros[-1]["total"]
                if parcelas_sem_juros_outros
                else None
            )
            qtd_parcelas_sem_juros_cartao_normal = len(
                [p for p in parcelas_outros if not p["temJuros"]]
            )
            preco_prazo_com_juros_cartao_normal = (
                parcelas_outros[qtd_parcelas_sem_juros_cartao_normal]["total"]
                if qtd_parcelas_sem_juros_cartao_normal < len(parcelas_outros)
                else None
            )
            qtd_parcelas_com_juros_cartao_normal = (
                len(parcelas_outros) if preco_prazo_com_juros_cartao_normal else None
            )

            # Cartão próprio - Parcelas com juros obtidas de 'promoBancos'
            parcelas_cartao_proprio_com_juros = next(
                (
                    banco["parcelas"]
                    for banco in produto["promoBancos"]
                    if banco["nomeBanco"] == "Cartão Hoje"
                ),
                [],
            )
            preco_prazo_com_juros_cartao_proprio = (
                parcelas_cartao_proprio_com_juros[-1]["total"]
                if parcelas_cartao_proprio_com_juros
                else None
            )
            primeira_chave_cartao_proprio = (
                parcelas_cartao_proprio_com_juros[0]["key"]
                if parcelas_cartao_proprio_com_juros
                else 0
            )
            ultima_chave_cartao_proprio = (
                parcelas_cartao_proprio_com_juros[-1]["key"]
                if parcelas_cartao_proprio_com_juros
                else 0
            )
            qtd_parcelas_com_juros_cartao_proprio = ultima_chave_cartao_proprio

            # Ajustando os valores para parcelas sem juros do cartão próprio
            preco_prazo_sem_juros_cartao_proprio = preco_prazo_sem_juros_cartao_normal
            qtd_parcelas_sem_juros_cartao_proprio = (
                parcelas_cartao_proprio_com_juros[0]["key"] - 1
                if parcelas_cartao_proprio_com_juros
                else 0
            )

            lista_to_concat.append(
                {
                    "sku": x,
                    "preco_pix": preco_pix,
                    "preco_boleto": preco_boleto,
                    "preco_credito_1x": preco_prazo_sem_juros_cartao_normal,
                    "preco_prazo_sem_juros_cartao_normal": preco_prazo_sem_juros_cartao_normal,
                    "qtd_parcelas_sem_juros_cartao_normal": qtd_parcelas_sem_juros_cartao_normal,
                    "preco_prazo_com_juros_cartao_normal": preco_prazo_com_juros_cartao_normal,
                    "qtd_parcelas_com_juros_cartao_normal": qtd_parcelas_com_juros_cartao_normal,
                    "taxa_juros_cartao_normal": 0,  # taxa_juros_cartao_normal,
                    "preco_x1_cartao_proprio": preco_prazo_sem_juros_cartao_normal,
                    "preco_prazo_sem_juros_cartao_proprio": None,  # preco_prazo_sem_juros_cartao_proprio,
                    "qtd_parcelas_sem_juros_cartao_proprio": None,  # qtd_parcelas_sem_juros_cartao_proprio,
                    "preco_prazo_com_juros_cartao_proprio": None,  # preco_prazo_com_juros_cartao_proprio,
                    "qtd_parcelas_com_juros_cartao_proprio": None,  # qtd_parcelas_com_juros_cartao_proprio,
                    "taxa_juros_cartao_proprio": 0,  # taxa_juros_cartao_proprio,
                    "no_modelo": None,
                    "frete": None,
                    "cep": None,
                }
            )
        time.sleep(1)

    df_export_final = df.merge(pd.DataFrame(lista_to_concat), on="sku")
    # df_export_final["data_extracao"] = datetime.datetime.now().strftime(
    #     "%Y-%m-%d %H:%M:%S"
    # )
    df_export_final["data_extracao"] = get_current_date()
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
            "data_extracao",
        ]
    ]
    dataframes_temporarios.append(df_export_final)


def concat_dataframes(dataframes, base_url):
    """
    Concatena uma lista de DataFrames em um único DataFrame, modifica a coluna 'url'
    com a URL base fornecida e salva o DataFrame resultante em um arquivo Excel.

            Parameters:
                    dataframes (list of pd.DataFrame): Lista de DataFrames para serem concatenados.
                    base_url (str): URL base a ser adicionada à coluna 'url' do DataFrame.

            Returns:
                    None
    """
    df_final = pd.concat(dataframes, ignore_index=True)
    df_final["url"] = df_final["url"].apply(lambda x: base_url + x)
    return df_final


headers_lista_produtos = {
    "authority": "www.iplace.com.br",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "content-type": "application/json",
    "sec-ch-ua": '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "x-cc-meteringmode": "CC-NonMetered",
    "x-ccprofiletype": "storefrontUI",
    "x-ccsite": "100002",
    "x-ccviewport": "md",
    "x-ccvisitid": "-40e130d7:1899324b3fb:5596-4094297862",
    "x-ccvisitorid": "1113rXog-VMupRgvRnX3RHl8mm5mjuBJyNmb2J-pwt2jIuU86A6",
    "x-requested-with": "XMLHttpRequest",
}

headers_produtos = {
    "authority": "www.iplace.com.br",
    "accept": "*/*",
    "accept-language": "pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "content-type": "application/json; charset=UTF-8",
    "origin": "https://www.iplace.com.br",
    "sec-ch-ua": '"Not/A)Brand";v="99", "Microsoft Edge";v="115", "Chromium";v="115"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.183",
    "x-requested-with": "XMLHttpRequest",
}

if __name__ == "__main__":
    log.info("Inicio")
    gcs_manager = GCSParquetManager()
    produtos = gcs_manager.ler_coluna_excel()

    produtos_ja_buscados = set()
    res = []
    dataframes_temporarios = []
    produtos_nao_encontrados = []
    log.info("Obtendo códigos dos produtos")
    for produto in tqdm(produtos, desc="Obtendo códigos dos produtos"):
        busca_codificada = quote_plus(produto)

        df = buscar_e_processar_produtos(busca_codificada, headers_lista_produtos)
        df = processar_informacoes_produto(df)
        produtos_novos = filtrar_produtos_novos(df)
        if produtos_novos:
            try:
                buscar_precos(produtos_novos, df, headers_produtos)
            except Exception as e:
                log.error(f"Ocorreu um problema durante a busca de preços de {produto}: {e}")

    df = concat_dataframes(dataframes_temporarios, "https://www.iplace.com.br")
    # Filtrando o DataFrame para remover linhas onde 'nom_produto' começa com '(Assinatura)'

    # Criando as condições de filtro
    condicao_assinatura = df['nome_produto'].str.startswith('(Assinatura)')
    condicao_iplace_hoje = df['nome_produto'].str.startswith('(iPlace Hoje)')

    # Filtrando o DataFrame para remover linhas que atendam a qualquer uma das condições
    df = df[~(condicao_assinatura | condicao_iplace_hoje)]
    
    # df.to_excel("Extrações iPlace.xlsx", index=False)
    
    # Upload to bucket
    # Formatar a data e hora no formato desejado
    pasta = "extracoes/"
    loja = "iplace"
    agora = datetime.datetime.now()
    nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"
    
    gcs_manager.upload_parquet(df, nome_arquivo)
    log.info("Fim")