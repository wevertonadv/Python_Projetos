import requests
import json
import time
from tqdm import tqdm
import pandas as pd
import datetime
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__
from modules.date_helper import get_current_date

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))

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

# # Verificar se a coluna 'NOME GERAL' existe no DataFrame
# if "MODELO_COM_COR" in df.columns:
#     # Salvar o conteúdo da coluna 'NOME GERAL' em uma lista
#     excel_produtos = df["MODELO_COM_COR"].tolist()
#     # debug(print("Lista de produtos:", produtos), debug_mode)
# else:
#     print("A coluna 'MODELO_COM_COR' não foi encontrada no arquivo.")

gcs_manager = GCSParquetManager()
excel_produtos = gcs_manager.ler_coluna_excel()

#####################################

# Constantes
URL_BASE = "https://www.motorola.com.br/"
MAX_ITENS_POR_PRODUTO = 5 ### Número máximo de tentativas de conexão
TEMPO_ESPERA = 0.5 ### Tempo de espera entre as tentativas de conexão

# Headers para a requisição
headers = {
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "accept": "application/json",
    "sec-ch-ua-mobile": "?0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "sec-ch-ua-platform": '"Windows"',
}

# Lista para armazenar os produtos
lista_produtos = []


###Envia resquisições de pesquisa por palavra chave e retorna a lista de resultados
for nome_produto in tqdm( ##Exibe o progresso do código
    excel_produtos,
    desc="Obtendo códigos dos produtos",
):
    try:
        query = nome_produto.replace(" ", "%20") ##Converte o nome do produto para URL
        params = { ##Cria um parâmetro para requisição da página
            "_q": nome_produto,
            "map": "ft",
            "__pickRuntime": "appsEtag,blocks,blocksTree,components,contentMap,extensions,messages,page,pages,query,queryData,route,runtimeMeta,settings",
        }
        response = requests.get(##Envia requisição para a página
            f"{URL_BASE}{nome_produto}",
            params=params,
            headers=headers,
        )


        ##Converte o resultado da requisição em json e extrai os campos
        data = json.loads(response.json()["queryData"][0]["data"])
        produtos = data["productSearch"]["products"]

        ##Itera o resultado para extrair o SKU e o link do produto
        for produto in produtos[:MAX_ITENS_POR_PRODUTO]:
            link = produto["linkText"]
            items = produto["items"]
            for item in items:
                lista_produtos.append((item["itemId"], produto["linkText"]))

    ##Imprime mensagem de erro caso o item pesquisado não seja encontrado
    except Exception as e:
        log.error(f"Erro ao pesquisar por {nome_produto}: {e}")

    ##Insere um tempo de espera entre as requisições
    time.sleep(TEMPO_ESPERA)

# Removendo duplicatas e convertendo de volta para lista
produtos_unicos = list(set(lista_produtos))

def encontrar_idskus_no_mesmo_selector(json_data, busca):
    for item in json_data:
        if "selector" in item:
            if any(busca in selector.get("link", "") for selector in item["selector"]):
                return [
                    selector["link"].split("idsku=")[-1].split("&")[0]
                    if "idsku=" in selector["link"]
                    else None
                    for selector in item["selector"]
                ]
    return []


def extrair_skus(dados):
    skus = []
    for oferta in dados.get("offers", []):
        sku = oferta.get("sku")
        if sku:
            skus.append(sku)
    return skus


# Lista para armazenar os resultados
resultados_finais = []


# Headers para uso no request
headers = {
    "authority": "www.motorola.com.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9",
    "cache-control": "max-age=0",
    "if-none-match": '"143AD9B49D96BB35272BA137E5161669"',
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "service-worker-navigation-preload": "true",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
}

# Envia requisições para obter os dados dos produtos
for produto, link in tqdm(produtos_unicos):
    params = {
        "idsku": produto,
    }

    response = requests.get(
        f"https://www.motorola.com.br/{link}/p",
        params=params,
        headers=headers,
    )

    try: ##Extrai o arquivo json dentro da do HTML
        data_versoes = json.loads(
            (
                response.text.split(
                    '<template data-type="json" data-varname="__RUNTIME__">'
                )[1]
                .split("<script>")[1]
                .split("</script>")[0]
                .split(
                    '{"contentJSON":"{\\"text\\":\\"Selecione a versão:\\",\\"productSelector\\":'
                )[1]
                .split('}",')[0]
            )
            .encode()
            .decode("unicode_escape")
        )

        data_skus = json.loads(
            response.text.split(f'{produto}","offers":')[1].split("}</script>")[0]
        )
    except Exception as e:
        log.error(f"Erro ao processar produto {produto}: {e}")
        continue

    lista_skus = extrair_skus(data_skus)
    resultado_idsku = encontrar_idskus_no_mesmo_selector(data_versoes, link)

    # Adicionando os resultados à lista final
    resultados_finais.extend(lista_skus + resultado_idsku)

# Removendo duplicatas
resultados_unicos = list(set(resultados_finais))

##################################### pegando as urls de todos os produtos

# Constantes
URL_BASE = "https://www.motorola.com.br/"
MAX_ITENS_POR_PRODUTO = 5
TEMPO_ESPERA = 0.5

# Headers para a requisição
headers = {
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "accept": "application/json",
    "sec-ch-ua-mobile": "?0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "sec-ch-ua-platform": '"Windows"',
}

# Lista para armazenamento dos produtos
lista_produtos = []


for nome_produto in tqdm(
    resultados_unicos,
    desc="Obtendo códigos dos produtos",
):
    try:
        query = nome_produto.replace(" ", "%20")
        params = {
            "_q": nome_produto,
            "map": "ft",
            "__pickRuntime": "appsEtag,blocks,blocksTree,components,contentMap,extensions,messages,page,pages,query,queryData,route,runtimeMeta,settings",
        }
        response = requests.get(
            f"{URL_BASE}{nome_produto}",
            params=params,
            headers=headers,
        )

        data = json.loads(response.json()["queryData"][0]["data"])
        produtos = data["productSearch"]["products"]

        for produto in produtos[:MAX_ITENS_POR_PRODUTO]:
            link = produto["linkText"]
            items = produto["items"]
            for item in items:
                lista_produtos.append((item["itemId"], produto["linkText"]))

    except Exception as e:
        log.error(f"Erro ao pesquisar por {nome_produto}: {e}")
        continue

    time.sleep(TEMPO_ESPERA)

# Removendo duplicatas e convertendo de volta para lista
produtos_unicos = list(set(lista_produtos))


#####################################
def find_json_content_by_name(data, name):
    """
    Encontra o conteúdo do JSON correspondente ao nome fornecido.

    Args:
        data (dict): O JSON parseado como um dicionário Python.
        name (str): O nome a ser buscado no JSON.

    Returns:
        str: O conteúdo associado ao nome, se encontrado.
        None: Se o nome não for encontrado.
    """
    for key, value in data.items():
        if "name" in value and value["name"] == name:
            return value["values"]["json"][0]
    return None


def encontrar_informacao_produto(json_data, item_id, info_chave):
    """
    Encontra informações específicas de um produto em um JSON com base no item_id.

    Args:
        json_data (dict): Dados JSON carregados.
        item_id (str): O ID do item a ser procurado.
        info_chave (str): A chave da informação a ser retornada (por exemplo, 'ean' ou 'spotPrice').

    Returns:
        O valor da informação solicitada, ou None se não for encontrado.
    """
    for chave, valor in json_data.items():
        if isinstance(valor, dict) and "itemId" in valor and valor["itemId"] == item_id:
            partes = info_chave.split(".")
            info_atual = valor
            try:
                for parte in partes:
                    if isinstance(info_atual, dict) and parte in info_atual:
                        info_atual = info_atual[parte]
                    elif isinstance(info_atual, list):
                        parte = int(parte) if parte.isdigit() else 0
                        info_atual = (
                            info_atual[parte] if len(info_atual) > parte else None
                        )
                    else:
                        return None
                return info_atual
            except (KeyError, IndexError, ValueError):
                return None
    return None


def encontrar_valor_por_sistema_pagamento(json_data, item_id, payment_system_name):
    """
    Encontra o valor de pagamento para um sistema de pagamento específico em um produto.

    Args:
        json_data (list): Lista de dicionários com os dados JSON carregados.
        item_id (str): O ID do item a ser procurado.
        payment_system_name (str): O nome do sistema de pagamento (por exemplo, 'Pix').

    Returns:
        float: O valor associado ao sistema de pagamento, ou None se não for encontrado.
    """

    for produto in json_data:
        for chave, valor in produto.items():
            if (
                isinstance(valor, dict)
                and "itemId" in valor
                and valor["itemId"] == item_id
            ):
                for installment_chave, installment_valor in produto.items():
                    if "Installments" in installment_chave and isinstance(
                        installment_valor, dict
                    ):
                        if (
                            installment_valor.get("PaymentSystemName")
                            == payment_system_name
                        ):
                            return installment_valor.get("Value")
    return None


def encontrar_detalhes_parcelamento(json_data, item_id, payment_system_name):
    """
    Encontra detalhes de parcelamento para um sistema de pagamento específico, incluindo o valor à vista.

    Args:
        json_data (list): Lista de dicionários com os dados JSON carregados.
        item_id (str): O ID do item a ser procurado.
        payment_system_name (str): O nome do sistema de pagamento (por exemplo, 'Mastercard').

    Returns:
        dict: Um dicionário contendo informações sobre o parcelamento e o valor à vista para o sistema de pagamento especificado.
    """
    max_sem_juros = {"parcelas": 0, "valor": 0}
    max_com_juros = {"parcelas": 0, "valor": 0}
    valor_a_vista = 0

    for produto in json_data:
        for chave, valor in produto.items():
            if (
                isinstance(valor, dict)
                and "itemId" in valor
                and valor["itemId"] == item_id
            ):
                for installment_chave, installment_valor in produto.items():
                    if "Installments" in installment_chave and isinstance(
                        installment_valor, dict
                    ):
                        if (
                            installment_valor.get("PaymentSystemName")
                            == payment_system_name
                        ):
                            parcelas = installment_valor.get("NumberOfInstallments", 0)
                            valor_parcela = installment_valor.get(
                                "TotalValuePlusInterestRate", 0
                            )
                            taxa_juros = installment_valor.get("InterestRate", 0)

                            if parcelas == 1:
                                valor_a_vista = valor_parcela

                            if taxa_juros == 0 and parcelas > max_sem_juros["parcelas"]:
                                max_sem_juros = {
                                    "parcelas": parcelas,
                                    "valor": valor_parcela,
                                }
                            elif (
                                taxa_juros > 0 and parcelas > max_com_juros["parcelas"]
                            ):
                                max_com_juros = {
                                    "parcelas": parcelas,
                                    "valor": valor_parcela,
                                }

    return {
        "max_sem_juros": max_sem_juros,
        "max_com_juros": max_com_juros,
        "valor_a_vista": valor_a_vista,
    }


lista_to_concat = []

for produto_unico in tqdm(produtos_unicos):
    produto = produto_unico[0]
    url = produto_unico[1]

    try:
        params = {
            "idsku": produto,
        }
        url = "https://www.motorola.com.br/" + url + "/p?idsku=" + produto

        response = requests.get(
            url,
            params=params,
            headers=headers,
        )

        try:
            # data = response.text.split("__RUNTIME__ = ")[1].split("</script>")[0]
            json_data = (
                json.loads(
                    response.text.split(
                        '<template data-type="json" data-varname="__STATE__">'
                    )[1]
                    .split("<script>")[1]
                    .split("</script>")[0]
                ),
            )
        except Exception as e:
            log.error(f"Erro ao processar produto {produto}: {e}")
            continue

        marca = find_json_content_by_name(json_data[0], "Marca")
        ean = encontrar_informacao_produto(json_data[0], produto, "ean")
        modelo = find_json_content_by_name(json_data[0], "Modelo mktplace")
        nome_comercial = find_json_content_by_name(json_data[0], "Modelo")
        try:
            capacidade_armazenamento = find_json_content_by_name(
                json_data[0], "Armazenamento (ROM):"
            ).split("<br/>")[0]
        except:
            capacidade_armazenamento = find_json_content_by_name(
                json_data[0], "Armazenamento (ROM):"
            )
        cor = find_json_content_by_name(json_data[0], "Cor")
        nome_produto = encontrar_informacao_produto(
            json_data[0], produto, "nameComplete"
        )

        pix = encontrar_valor_por_sistema_pagamento(json_data, produto, "Pix")
        boleto = encontrar_valor_por_sistema_pagamento(
            json_data, produto, "Boleto Banc\u00e1rio"
        )

        try:
            cor = nome_produto.split("- ")[1]
        except:
            cor = None
        detalhes_parcelamento_visa = encontrar_detalhes_parcelamento(
            json_data, produto, "Visa"
        )  # Mastercard
        # qtd_parcelas_com_juros_cartao_normal = None

        estoque = encontrar_informacao_produto(
            json_data[0], produto, "AvailableQuantity"
        )
        url = url
        # data_extração = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data_extração = get_current_date()

        lista_to_concat.append(
            {
                "fabricante": None,
                "marca": marca,
                "ean": ean,
                "sku": produto,
                "modelo": modelo,
                "nome_comercial": nome_comercial,
                "capacidade_armazenamento": None,  # capacidade_armazenamento,
                "cor": cor,
                "nome_produto": nome_produto,
                "empresa": "Motorola",
                "vendedor": "Motorola",
                "preco_pix": pix,
                "preco_boleto": boleto,
                "preco_credito_1x": pix,  # 1x no cartão tem desconto igual pix
                "preco_prazo_sem_juros_cartao_normal": detalhes_parcelamento_visa[
                    "max_sem_juros"
                ]["valor"],
                "qtd_parcelas_sem_juros_cartao_normal": detalhes_parcelamento_visa[
                    "max_sem_juros"
                ]["parcelas"],
                "preco_prazo_com_juros_cartao_normal": detalhes_parcelamento_visa[
                    "max_com_juros"
                ]["valor"],
                "qtd_parcelas_com_juros_cartao_normal": detalhes_parcelamento_visa[
                    "max_com_juros"
                ]["parcelas"],
                "taxa_juros_cartao_normal": None,
                "preco_x1_cartao_proprio": None,
                "preco_prazo_sem_juros_cartao_proprio": None,
                "qtd_parcelas_sem_juros_cartao_proprio": None,
                "preco_prazo_com_juros_cartao_proprio": None,
                "qtd_parcelas_com_juros_cartao_proprio": None,
                "taxa_juros_cartao_proprio": None,
                "frete": None,
                "estoque": estoque,
                "url": url,
                "data_extração": data_extração,
            }
        )

    except Exception as e:
        log.error(f"Erro ao processar produto {produto}: {e}")
        continue

# Exportar os dados para um DataFrame
df_export_final = pd.DataFrame(lista_to_concat)
# df_export_final.to_excel("Extrações Motorola.xlsx", index=False)
# Upload to bucket

# Formatar a data e hora no formato desejado
pasta = "extracoes/"
loja = "motorola"
agora = datetime.datetime.now()
nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

gcs_manager.upload_parquet(df_export_final, nome_arquivo)
log.info("Fim")