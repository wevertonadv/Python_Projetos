### lista de produto excel ###
import requests
import json
import time
from tqdm import tqdm
import ast
import re
import pandas as pd
import datetime
import datetime
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.date_helper import get_current_date
from modules.logger_control import Logger, __main__

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))

# Função de debug
def debug(message, mode=False):
    if mode:
        print(f"DEBUG: {message}")


# Configuração inicial
debug_mode = True

# # Caminho do arquivo Excel #
# arquivo_excel = r"assets\lista devices.xlsx"

# # Ler o arquivo Excel
# df = pd.read_excel(arquivo_excel)

# # Verificar se a coluna 'NOME GERAL' existe no DataFrame
# if "MODELO_COM_COR" in df.columns:
#     # Salvar o conteúdo da coluna 'NOME GERAL' em uma lista
#     produtos_excel = df["MODELO_COM_COR"].tolist()
#     # debug(print("Lista de produtos:", produtos_excel), debug_mode)
# else:
#     print("A coluna 'NOME GERAL' não foi encontrada no arquivo.")
gcs_manager = GCSParquetManager()
produtos_excel = gcs_manager.ler_coluna_excel()
### pegando lista de produtos do site ###


def extrair_itens(html):
    padrao = r'"url":null,"path":"(.*?)",'
    itens = re.findall(padrao, html)
    return itens


headers = {
    "authority": "www.magazineluiza.com.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9",
    "cache-control": "max-age=0",
    "referer": "https://www.bing.com/",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
}

MAX_ITENS_POR_PRODUTO = 5
produtos = []
log.info("Obtendo códigos dos produtos")
for nome_produto in tqdm(produtos_excel, desc="Obtendo códigos dos produtos"):
    try:
        query = nome_produto.replace(" ", "+")
        response = requests.get(
            f"https://www.magazineluiza.com.br/busca/{query}/",
            headers=headers,
        )

        html = response.text
        produtos_extraidos = extrair_itens(html)
        produtos.extend(produtos_extraidos[:MAX_ITENS_POR_PRODUTO])

    except Exception as e:
        log.error(f"Erro ao pesquisar por {nome_produto}: {e}")

    time.sleep(0.5)

produtos_unicos = set(produtos)
produtos_unicos = list(produtos_unicos)
produtos_unicos = [item for item in produtos_unicos if not item.startswith("usado-")]

### informações de produto ###

lista_to_concat = []
lista_produtos_com_falha = []

headers = {
    "authority": "www.magazineluiza.com.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9",
    "cache-control": "max-age=0",
    "if-none-match": '"181qohjue2zb7mb"',
    "referer": "https://www.magazineluiza.com.br/busca/ihpone+13+128+gb/",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
}
for produto_request in tqdm(produtos_unicos, desc="Buscando informações de produto"):
    try:
        response = requests.get(
            f"https://www.magazineluiza.com.br/{produto_request}",
            headers=headers,
        )
        data = json.loads(
            response.text.split('<script id="__NEXT_DATA__" type="application/json">')[
                1
            ].split("</script>")[0]
        )

        # Extração das informações relevantes
        produto = data["props"]["pageProps"]["data"]["product"]

        metodos_pagamento = produto["paymentMethods"]

        # Inicializando variáveis
        preco_pix = None
        preco_boleto = None
        preco_credito_1x = None
        preco_x1_cartao_proprio = None
        qtd_parcelas_com_juros_cartao_normal = None
        preco_prazo_com_juros_cartao_normal = 0
        qtd_parcelas_com_juros_cartao_proprio = None
        preco_prazo_com_juros_cartao_proprio = 0
        preco_prazo_sem_juros_cartao_normal = None
        qtd_parcelas_sem_juros_cartao_normal = None
        preco_prazo_sem_juros_cartao_proprio = None
        qtd_parcelas_sem_juros_cartao_proprio = None
        preco_prazo_sem_juros_cartao_normal = None
        qtd_parcelas_sem_juros_cartao_normal = 0
        preco_prazo_sem_juros_cartao_proprio = None
        qtd_parcelas_sem_juros_cartao_proprio = 0
        modelo = None
        cor = None
        capacidade = None
        nome_comercial = None
        ean = None

        f_tax_percentage = None
        f_cc_tax_percentage = None

        # Iterando pelos métodos de pagamento
        for metodo in metodos_pagamento:
            if metodo["id"] == "pix":
                preco_pix = metodo["installmentPlans"][0]["installmentAmount"]
            elif metodo["id"] == "boleto":
                preco_boleto = metodo["installmentPlans"][0]["installmentAmount"]
            elif metodo["id"] == "visa":
                preco_credito_1x = metodo["installmentPlans"][0]["installmentAmount"]
                # Encontrando o plano com maior juros e maior número de parcelas
                for plano in metodo["installmentPlans"]:
                    juros = float(plano["totalAmount"])
                    if juros > preco_prazo_com_juros_cartao_normal or (
                        juros == preco_prazo_com_juros_cartao_normal
                        and plano["installment"] > qtd_parcelas_com_juros_cartao_normal
                    ):
                        preco_prazo_com_juros_cartao_normal = juros
                        qtd_parcelas_com_juros_cartao_normal = plano["installment"]

                        f_tax_percentage = plano['interest']

            elif metodo["id"] == "luiza_ouro":
                preco_x1_cartao_proprio = metodo["installmentPlans"][0][
                    "installmentAmount"
                ]
                # Encontrando o plano com maior juros e maior número de parcelas
                for plano in metodo["installmentPlans"]:
                    juros = float(plano["totalAmount"])
                    if juros > preco_prazo_com_juros_cartao_proprio or (
                        juros == preco_prazo_com_juros_cartao_proprio
                        and plano["installment"] > qtd_parcelas_com_juros_cartao_proprio
                    ):
                        preco_prazo_com_juros_cartao_proprio = juros
                        qtd_parcelas_com_juros_cartao_proprio = plano["installment"]

                        f_cc_tax_percentage = plano['interest']

        # Iterando pelos métodos de pagamento
        for metodo in produto["paymentMethods"]:
            if metodo["id"] == "pix":
                preco_pix = metodo["installmentPlans"][0]["installmentAmount"]
            elif metodo["id"] == "boleto":
                preco_boleto = metodo["installmentPlans"][0]["installmentAmount"]
            elif metodo["id"] == "visa":
                # Procurando pelo plano com o maior número de parcelas sem juros
                for plano in metodo["installmentPlans"]:
                    if (
                        plano["interest"] == "0.00"
                        and plano["installment"] > qtd_parcelas_sem_juros_cartao_normal
                    ):
                        preco_prazo_sem_juros_cartao_normal = plano["totalAmount"]
                        qtd_parcelas_sem_juros_cartao_normal = plano["installment"]
            elif metodo["id"] == "luiza_ouro":
                # Procurando pelo plano com o maior número de parcelas sem juros
                for plano in metodo["installmentPlans"]:
                    if (
                        plano["interest"] == "0.00"
                        and plano["installment"] > qtd_parcelas_sem_juros_cartao_proprio
                    ):
                        preco_prazo_sem_juros_cartao_proprio = plano["totalAmount"]
                        qtd_parcelas_sem_juros_cartao_proprio = plano["installment"]

        # Verificando a existência e extraindo as informações
        if "factsheet" in produto:
            for categoria in produto["factsheet"]:
                for item in categoria["elements"]:
                    if item["keyName"] == "Referência":
                        modelo = (
                            item["elements"][0]["value"] if item["elements"] else None
                        )
                    elif item["keyName"] == "Cor":
                        cor = item["elements"][0]["value"] if item["elements"] else None
                    elif item["keyName"] == "Armazenamento Interno":
                        capacidade = (
                            item["elements"][0]["value"] if item["elements"] else None
                        )
                    elif item["keyName"] == "Modelo":
                        nome_comercial = (
                            item["elements"][0]["value"] if item["elements"] else None
                        )
                    elif item["keyName"] == "Certificado Homologado pela Anatel Número":
                        ean = item["elements"][0]["value"] if item["elements"] else None

        # Verificando se as informações não foram encontradas na primeira estrutura
        if not any([modelo, cor, capacidade, nome_comercial, ean]):
            # Extraindo as informações da segunda estrutura
            for categoria in produto["factsheet"]:
                for item in categoria["elements"]:
                    if item["keyName"] == "Informações complementares":
                        for sub_item in item["elements"]:
                            if sub_item["keyName"] == "Referência":
                                modelo = sub_item["value"]
                            elif sub_item["keyName"] == "Cor":
                                cor = sub_item["value"]
                            elif sub_item["keyName"] == "Armazenamento Interno":
                                capacidade = sub_item["value"]
                            elif sub_item["keyName"] == "Modelo":
                                nome_comercial = sub_item["value"]
                            elif (
                                sub_item["keyName"]
                                == "Certificado Homologado pela Anatel Número"
                            ):
                                ean = sub_item["value"]

        # Preenchendo os campos com as informações extraídas
        lista_to_concat.append(
            {
                "fabricante": None,
                "marca": produto["brand"]["label"],
                "ean": ean,
                "sku": produto["seller"]["sku"],
                "no_modelo": modelo,
                "nome_comercial": nome_comercial,
                "capacidade_armazenamento": capacidade,
                "cor": cor,
                "nome_produto": produto["title"],
                "empresa": "Magalu",
                "vendedor": produto["seller"]["description"],
                "preco_pix": preco_pix,
                "preco_boleto": preco_boleto,
                "preco_credito_1x": preco_prazo_sem_juros_cartao_normal,
                "preco_prazo_sem_juros_cartao_normal": preco_prazo_sem_juros_cartao_normal,
                "qtd_parcelas_sem_juros_cartao_normal": qtd_parcelas_sem_juros_cartao_normal,
                "preco_prazo_com_juros_cartao_normal": preco_prazo_com_juros_cartao_normal,
                "qtd_parcelas_com_juros_cartao_normal": qtd_parcelas_com_juros_cartao_normal,
                # "taxa_juros_cartao_normal": None,
                "taxa_juros_cartao_normal": f_tax_percentage,
                "preco_x1_cartao_proprio": preco_prazo_sem_juros_cartao_proprio,
                "preco_prazo_sem_juros_cartao_proprio": preco_prazo_sem_juros_cartao_proprio,
                "qtd_parcelas_sem_juros_cartao_proprio": qtd_parcelas_sem_juros_cartao_proprio,
                "preco_prazo_com_juros_cartao_proprio": preco_prazo_com_juros_cartao_proprio,
                "qtd_parcelas_com_juros_cartao_proprio": qtd_parcelas_com_juros_cartao_proprio,
                # "taxa_juros_cartao_proprio": None,
                "taxa_juros_cartao_proprio": f_cc_tax_percentage,
                "frete": None,
                "cep": None,
                "estoque": produto["available"],
                "url": "https://www.magazineluiza.com.br/" + produto["url"],
                # "data_extração": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_extracao": get_current_date()
            }
        )

        time.sleep(0.5)
    except:
        lista_produtos_com_falha.append(produto_request)
        log.error(f"Falha ao obter informações de {produto_request}")

# Exportar os dados para um DataFrame
df_export_final = pd.DataFrame(lista_to_concat)
# df_export_final.to_excel("Extrações Magalu.xlsx", index=False)

# Upload to bucket

# Formatar a data e hora no formato desejado
pasta = "extracoes/"
loja = "magalu"
agora = datetime.datetime.now()
nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

# df_export_final.to_excel('_tmp/_magalu.xlsx', index=False)
gcs_manager.upload_parquet(df_export_final, nome_arquivo)
log.info("Fim")