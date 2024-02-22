### lista de produto excel ###
import requests
import json
import time
from tqdm import tqdm
import ast
import re
from pprint import pprint
import pandas as pd
import datetime
from bs4 import BeautifulSoup
import re
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__
from modules.date_helper import get_current_date

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))

def debug(message, mode=False):
    if mode:
        print(f"DEBUG: {message}")


def extrair_itens(html):
    padrao = r'"friendlyName":"(.*?)",'
    itens = re.findall(padrao, html)
    return itens


def extrair_codes(html):
    padrao = r'"code":(.*?),"'
    itens = re.findall(padrao, html)
    return itens


def extrair_informacoes(html, descricao):
    soup = BeautifulSoup(html, "html.parser")
    texto_html = soup.get_text(separator="\n").lower()
    texto = (
        texto_html + "\n" + descricao
    ).lower()  # Combina texto HTML com descrição em minúsculas

    # Função auxiliar para buscar informações
    def buscar_info(padroes, texto):
        for padrao in padroes:
            busca = re.search(padrao, texto)
            if busca:
                return busca.group(1).strip()
        return None

    # Buscando informações específicas com expressões regulares flexíveis
    # marca = buscar_info([r"marca[:\s]*\n?([^\n]+)"], texto)
    # modelo_modelo = buscar_info([r"modelo[:\s]*\n?([^\n]+)"], texto)
    # modelo_referencia = buscar_info([r"referência[:\s]*\n?([^\n]+)"], texto)
    cor = buscar_info([r"cor[:\s]*\n?\s*([^\n]+)"], texto)
    capacidade = buscar_info([r"armazenamento interno[:\s]*\n?([^\n]+)"], texto)
    # nome_comercial = buscar_info([r"nome[:\s]*\n?([^\n]+)"], texto)
    ean = buscar_info(
        [
            r"código anatel[:\s]*\n?([^\n]+)",
            r"certificado homologado pela anatel número[:\s]*\n?([^\n]+)",
        ],
        texto,
    )

    # if modelo_referencia:
    #     modelo = modelo_referencia
    #     nome_comercial = buscar_info([r"modelo[:\s]*\n?([^\n]+)"], texto)
    # else:
    #     modelo = modelo_modelo

    marca = None
    l_results = re.findall(
        pattern="(?:M|m)arca:(.+?)<",
        string=html,
        flags=re.DOTALL
    )
    if len(l_results) > 0:
        marca = l_results[0]

    modelo_modelo = None
    l_results = re.findall(
        pattern="(?:M|m)odelo(?:<[a-z /]+>){1,2}(.+?)<",
        string=html,
        flags=re.DOTALL
    )
    if len(l_results) > 0:
        modelo_modelo = l_results[0]

    nome_comercial = None

    # return marca, modelo, cor, capacidade, nome_comercial, ean
    return marca, modelo_modelo, cor, capacidade, nome_comercial, ean


def extract_color(name):
    regex_color_list = [
        r"meianoite",
        r"starlight",
        r"space black",
        r"deep purple",
        r"midnight",
        r"meia noite",
        r"product red",
        r"preto",
        r"branco",
        r"vermelho",
        r"azul",
        r"verde",
        r"amarelo",
        r"dourado",
        r"ouro rosa",
        r"prata",
        r"grafite",
        r"pacífico azul",
        r"violeta",
        r"laranja",
        r"rosa",
        r"cinza",
        r"bronze",
        r"titan",
        r"cobre",
        r"azul-celeste",
        r"fúcsia",
        r"estelar",
        r"roxo",
        r"prateado",
        r"roxo",
        r"productred",
        r"creme",
        r"red",
        r"rose",
        r"cosmic black",
        r"luz das estrelas",
        r"gold",
        r"blue",
        r"graphite",
        r"violet",
        r"lima",
    ]
    for pattern in regex_color_list:
        if re.search(pattern, name, re.IGNORECASE):
            return pattern
    return None


def extract_capacity(name):
    matches = re.findall(r"(\d+)\s*(gb|tb|ssd|hd|hdd)", name, re.IGNORECASE)
    max_capacity = 0
    for match in matches:
        capacity, unit = match
        capacity = int(capacity)
        if unit.lower() == "tb":
            capacity *= 1024  # Convertendo TB para GB
        max_capacity = max(max_capacity, capacity)

    if max_capacity > 0:
        return f"{max_capacity}GB"
    else:
        return None


# Configuração inicial
debug_mode = False

# Caminho do arquivo Excel
# arquivo_excel = r"assets\lista devices.xlsx"

# # Ler o arquivo Excel
# df = pd.read_excel(arquivo_excel)

# # Verificar se a coluna 'NOME GERAL' existe no DataFrame
# if "MODELO_COM_COR" in df.columns:
#     # Salvar o conteúdo da coluna 'NOME GERAL' em uma lista
#     produtos_excel = df["MODELO_COM_COR"].tolist()
#     # debug(print("Lista de produtos:", produtos_excel), debug_mode)
# else:
#     print("A coluna 'MODELO_COM_COR' não foi encontrada no arquivo.")

gcs_manager = GCSParquetManager()
log.info("Inicio")
produtos_excel = gcs_manager.ler_coluna_excel()

### busca lista de produtos ###
headers = {
    "authority": "www.kabum.com.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9",
    "referer": "https://www.kabum.com.br/?msclkid=2a41ac3fc668169cb4e1c9bf9d06b35b&utm_source=bing&utm_medium=cpc&utm_campaign=search_inst_variacao&utm_term=kabum%5D&utm_content=variacao",
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

produtos = []
produtos_nao_encontrados = []
log.info("Obtendo códigos dos produtos")
for nome_produto in tqdm(produtos_excel, desc="Obtendo códigos dos produtos"):
    try:
        query = nome_produto.replace(" ", "%20")
        response = requests.get(
            f"https://www.kabum.com.br/busca/{query}",
            headers=headers,
        )
        html = response.text.split('"data":')[2]
        produtos_extraidos = extrair_itens(html)
        codigos_extraidos = extrair_codes(html)

        produtos_combinados = [
            f"{codigo}/{produto}"
            for produto, codigo in zip(produtos_extraidos, codigos_extraidos)
        ]
        produtos.extend(produtos_combinados)
    except:
        produtos_nao_encontrados.append(nome_produto)
        log.error(f"Nenhum item encontrado para busca de {nome_produto}")

produtos_unicos = set(produtos)
produtos_unicos = list(produtos_unicos)
produtos_unicos = [item for item in produtos_unicos if not item.startswith("usado-")]

### informações de produto ###
lista_to_concat = []
lista_produtos_com_falha = []

headers = {
    "authority": "www.kabum.com.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9",
    "cache-control": "max-age=0",
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
            f"https://www.kabum.com.br/produto/{produto_request}",
            headers=headers,
        )
        data = json.loads(
            response.text.split('<script id="__NEXT_DATA__" type="application/json">')[
                1
            ].split("</script>")[0]
        )

        # Extração das informações relevantes
        produto = json.loads(data["props"]["pageProps"]["data"]["productCatalog"])

        metodos_pagamento = produto["paymentMethodsDefault"]

        # Inicializando variáveis
        preco_pix = None
        preco_boleto = None
        preco_credito_1x = None
        preco_x1_cartao_proprio = None
        qtd_parcelas_com_juros_cartao_normal = None
        preco_prazo_com_juros_cartao_normal = 0
        qtd_parcelas_com_juros_cartao_proprio = 0
        preco_prazo_com_juros_cartao_proprio = 0
        preco_prazo_sem_juros_cartao_normal = None
        qtd_parcelas_sem_juros_cartao_normal = None
        preco_prazo_sem_juros_cartao_proprio = None
        preco_prazo_sem_juros_cartao_normal = None
        qtd_parcelas_sem_juros_cartao_normal = 0
        qtd_parcelas_sem_juros_cartao_proprio = 0
        modelo = None
        cor = None
        capacidade = None
        nome_comercial = None
        ean = None
        marca = None

        # Iterando pelos métodos de pagamento
        for metodo in metodos_pagamento:
            if metodo["category"] == "pix":
                preco_pix = metodo["installments"][0]["total"]
            elif metodo["category"] == "boleto":
                preco_boleto = metodo["installments"][0]["total"]
            elif metodo["category"] == "cartao":
                preco_credito_1x = metodo["installments"][0]["total"]
                # Inicializando variáveis para armazenar o plano com maior número de parcelas
                qtd_parcelas_com_juros_cartao_normal = (
                    qtd_parcelas_sem_juros_cartao_normal
                ) = 0
                preco_prazo_com_juros_cartao_normal = (
                    preco_prazo_sem_juros_cartao_normal
                ) = 0

                for plano in metodo["installments"]:
                    # Checando se tem juros
                    if plano["hasFee"]:
                        # Com Juros
                        if plano["installment"] > qtd_parcelas_com_juros_cartao_normal:
                            preco_prazo_com_juros_cartao_normal = plano["total"]
                            qtd_parcelas_com_juros_cartao_normal = plano["installment"]
                    else:
                        # Sem Juros
                        if plano["installment"] > qtd_parcelas_sem_juros_cartao_normal:
                            preco_prazo_sem_juros_cartao_normal = plano["total"]
                            qtd_parcelas_sem_juros_cartao_normal = plano["installment"]
            # elif metodo["category"] == "cobranded": # nupay não é cartão próprio é diferenciado (talvez entre como backlog)
            #     for plano in metodo["installments"]:
            #         # Checando se tem juros
            #         if plano["hasFee"]:
            #             # Com Juros
            #             if plano["installment"] > qtd_parcelas_com_juros_cartao_proprio:
            #                 preco_prazo_com_juros_cartao_proprio = plano["total"]
            #                 qtd_parcelas_com_juros_cartao_proprio = plano["installment"]
            #         else:
            #             # Sem Juros
            #             if plano["installment"] > qtd_parcelas_sem_juros_cartao_proprio:
            #                 preco_prazo_sem_juros_cartao_proprio = plano["total"]
            #                 qtd_parcelas_sem_juros_cartao_proprio = plano["installment"]

        marca, modelo, cor, capacidade, nome_comercial, ean = extrair_informacoes(
            produto["html"], produto["description"]
        )

        # Preenchendo os campos com as informações extraídas
        lista_to_concat.append(
            {
                "fabricante": None,
                "marca": marca,
                "ean": ean,
                "sku": produto["code"],
                "no_modelo": modelo,
                "nome_comercial": nome_comercial,
                "capacidade_armazenamento": extract_capacity(produto["name"]),
                "cor": extract_color(produto["name"]),
                "nome_produto": produto["name"],
                "empresa": "Kabum",
                "vendedor": produto["sellerName"],
                "preco_pix": preco_pix,
                "preco_boleto": preco_boleto,
                "preco_credito_1x": preco_credito_1x,
                "preco_prazo_sem_juros_cartao_normal": preco_prazo_sem_juros_cartao_normal,
                "qtd_parcelas_sem_juros_cartao_normal": qtd_parcelas_sem_juros_cartao_normal,
                "preco_prazo_com_juros_cartao_normal": preco_prazo_com_juros_cartao_normal,
                "qtd_parcelas_com_juros_cartao_normal": qtd_parcelas_com_juros_cartao_normal,
                "taxa_juros_cartao_normal": None,
                "preco_x1_cartao_proprio": preco_credito_1x,
                "preco_prazo_sem_juros_cartao_proprio": preco_prazo_sem_juros_cartao_proprio,
                "qtd_parcelas_sem_juros_cartao_proprio": qtd_parcelas_sem_juros_cartao_proprio,
                "preco_prazo_com_juros_cartao_proprio": preco_prazo_com_juros_cartao_proprio,
                "qtd_parcelas_com_juros_cartao_proprio": qtd_parcelas_com_juros_cartao_proprio,
                "taxa_juros_cartao_proprio": None,
                "frete": None,
                "cep": None,
                "estoque": produto["available"],
                "url": f"https://www.kabum.com.br/produto/{produto_request}",
                # "data_extração": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_extracao": get_current_date()
            }
        )

        time.sleep(1)
    except Exception as e:
        lista_produtos_com_falha.append(produto_request)
        log.error(f"\nFalha ao obter informações de {produto_request} - {e}")


# Exportar os dados para um DataFrame
df_export_final = pd.DataFrame(lista_to_concat)
# df_export_final.to_excel("Extrações Kabum.xlsx", index=False)

# Upload to bucket

# Formatar a data e hora no formato desejado
pasta = "extracoes/"
loja = "kabum"
agora = datetime.datetime.now()
nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"
# df_export_final.to_excel('_tmp_2/_test.xlsx', index=False)

gcs_manager.upload_parquet(df_export_final, nome_arquivo)