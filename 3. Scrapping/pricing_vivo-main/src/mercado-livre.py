import requests
import json
import time
from tqdm import tqdm
import ast
import re
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

headers = {
    "authority": "lista.mercadolivre.com.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9",
    "device-memory": "8",
    "downlink": "10",
    "dpr": "0.8",
    "ect": "4g",
    "referer": "https://lista.mercadolivre.com.br/celulares-telefones/celulares-smartphones/celulares-smartphones_Desde_101_NoIndex_True",
    "rtt": "50",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "viewport-width": "1049",
}

product_ids = []
produtos_nao_encontrados = []

# Adicionando verificações de status da resposta e inspeção da estrutura JSON
for produto in tqdm(produtos, desc=f"Processando Produtos"):
    try:
        response = requests.get(
            f"https://lista.mercadolivre.com.br/{produto}_Loja_all_NoIndex_True",
            headers=headers,
        )

        # Verificando o status da resposta
        if response.status_code != 200:
            log.error(f"Erro na resposta HTTP: Status {response.status_code}")
            continue

        data = response.text

        string_lista = (
            "[" + data.split('meli_ga("set", "dimension49", ')[1].split(");")[0] + "]"
        )
        # Convertendo para uma lista real
        lista_real = ast.literal_eval(string_lista)
        lista_itens = lista_real[0].split(",")

        # Função para inserir hífen entre letras e números
        def inserir_hifen(item):
            return re.sub(r"([a-zA-Z]+)(\d+)", r"\1-\2", item)

        # Aplicando a função em cada item da lista
        prods = [inserir_hifen(item) for item in lista_itens]

        product_ids.extend(prods[:5])

        time.sleep(0.5)
    except:
        produtos_nao_encontrados.append(produto)

# Removendo IDs duplicados
product_ids = list(set(product_ids))

log.info(f"Total de product_ids coletados: {len(product_ids)}")

# Mapeamento dos IDs para os campos em 'details'
id_to_detail = {
    "Marca": "marca",
    "Modelo detalhado": "no_modelo",  # alguns produtos não tem o modelo detalhado, somente o modelo
    "Modelo": "nome_comercial",
    "Cor": "cor",
    "Memória interna": "capacidade_armazenamento",
}

# Lista para armazenar todos os detalhes combinados
all_details = []

# Inicialize a lista fora do loop
unique_catalog_product_ids = set()

headers = {
    "authority": "produto.mercadolivre.com.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9",
    "device-memory": "8",
    "downlink": "10",
    "dpr": "1",
    "ect": "4g",
    "rtt": "50",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "viewport-width": "2089",
}
produtos_com_erro_de_requisicao = []
for product_id in tqdm(product_ids, desc="Pegando as informações dos produtos"):
    try:
        response = requests.get(
            f"https://produto.mercadolivre.com.br/{product_id}",
            headers=headers,
        )

        try:
            vendedor = response.text.split('"seller_name":"')[1].split('",')[0]
        except:
            vendedor = response.text.split('"collectorNickname":"')[1].split('",')[0]

        if vendedor == "TROCAFONECOMERCIALIZACAO":
            continue

        ### informações de modelo
        data = response.text.split(',"specs":[')[1].split(',"components"')[0]
        json_str = json.loads("[" + data)

        # Dicionário 'details' inicial
        details = {
            "fabricante": None,
            "marca": None,
            "ean": None,
            "sku": product_id,
            "no_modelo": None,
            "nome_comercial": None,
            "capacidade_armazenamento": None,
            "cor": None,
            "nome_produto": response.text.split('"schema":[{"name":"')[1].split('",')[
                0
            ],
            "empresa": "Mercado Livre",
            "vendedor": vendedor,
        }

        # Varrendo o JSON para preencher 'details'
        for item in json_str:
            for attribute in item.get("attributes", []):
                detail_key = id_to_detail.get(attribute["id"])
                if detail_key:
                    details[detail_key] = attribute["text"]

        ### meios de pagamento
        try:
            txt = (
                "{"
                + response.text.split("window.__PRELOADED_STATE__ =")[1]
                .split("  {")[1]
                .split("},props")[0]
                .split("};")[0]
                + "}"
            )

        except:
            txt = (
                response.text.split("window.__PRELOADED_STATE__ = ")[1]
                .split("},props")[0]
                .split("};")[0]
                + "}"
            )

        json_str = json.loads(txt)

        preco_pix = json_str["initialState"]["components"]["track"]["melidata_event"][
            "event_data"
        ][
            "price"
        ]  # preço estimado, não tem tag direto de boleto para ele, mas tem a indicação no mesmo json da possibilidade de pagamento em boleto, mas não o valor dele.
        try:
            if (
                json_str["initialState"]["components"]["price"]["discount_label"][
                    "text"
                ]
                == "no Pix"
            ):
                preco_pix = json_str["initialState"]["components"]["price"]["price"][
                    "value"
                ]
        except:
            pass  # sem tag de pix no json

        preco_prazo_sem_juros_cartao_normal = json_str["initialState"]["components"][
            "track"
        ]["melidata_event"]["event_data"]["credit_view_components"]["pricing"][
            "installments_total"
        ]
        qtd_parcelas_sem_juros_cartao_normal = json_str["initialState"]["components"][
            "track"
        ]["melidata_event"]["event_data"]["credit_view_components"]["pricing"][
            "installments"
        ]
        preco_prazo_com_juros_cartao_normal = None
        qtd_parcelas_com_juros_cartao_normal = None
        # nem todo produto tem parcelamento sem juros
        if (
            json_str["initialState"]["components"]["track"]["melidata_event"][
                "event_data"
            ]["credit_view_components"]["pricing"]["is_free_installments"]
            == False
        ):  # parcelamento com juros
            preco_prazo_sem_juros_cartao_normal = json_str["initialState"][
                "components"
            ]["track"]["melidata_event"]["event_data"][
                "price"
            ]  # mesmo que x1
            qtd_parcelas_sem_juros_cartao_normal = 1
            preco_prazo_com_juros_cartao_normal = json_str["initialState"][
                "components"
            ]["track"]["melidata_event"]["event_data"]["credit_view_components"][
                "pricing"
            ][
                "installments_total"
            ]
            qtd_parcelas_com_juros_cartao_normal = json_str["initialState"][
                "components"
            ]["track"]["melidata_event"]["event_data"]["credit_view_components"][
                "pricing"
            ][
                "installments"
            ]

        # se tem a tag de pague até X vezes no cartão do MercadoLivre
        try:  # sem juros
            qtd_parcelas_sem_juros_cartao_proprio = int(
                re.findall(
                    r"(\d+)",
                    json_str["initialState"]["components"]["payment_methods"][
                        "messages"
                    ][0]["title"]["values"]["installments"]["text"],
                )[0]
            )
            preco_prazo_com_juros_cartao_proprio = None
            qtd_parcelas_com_juros_cartao_proprio = None
        except:  # com juros
            qtd_parcelas_sem_juros_cartao_proprio = 1
            preco_prazo_com_juros_cartao_proprio = json_str["initialState"][
                "components"
            ]["track"]["melidata_event"]["event_data"]["credit_view_components"][
                "pricing"
            ][
                "installments_total"
            ]
            qtd_parcelas_com_juros_cartao_proprio = json_str["initialState"][
                "components"
            ]["track"]["melidata_event"]["event_data"]["credit_view_components"][
                "pricing"
            ][
                "installments"
            ]

        try:
            estoque = int(
                re.findall(
                    r"(\d+)",
                    json_str["initialState"]["components"]["available_quantity"][
                        "picker"
                    ]["description"],
                )[0]
            )
        except:
            if (
                json_str["initialState"]["components"]["available_quantity"]["label"][
                    "text"
                ]
                == "Último disponível!"
            ):
                estoque = 1
            else:
                estoque = 0

        payment_details = {
            "preco_pix": preco_pix,  # parece que todos os produtos podem ser pagos com pix, mas alguns tem desconto. os que não tem desconto é o mesmo preço dos outros métodos
            "preco_boleto": json_str["initialState"]["components"]["track"][
                "melidata_event"
            ]["event_data"][
                "price"
            ],  # preço estimado, não tem tag direto de boleto para ele
            "preco_x1_cartao": json_str["initialState"]["components"]["track"][
                "melidata_event"
            ]["event_data"][
                "price"
            ],  # preço estimado
            "preco_prazo_sem_juros_cartao_normal": preco_prazo_sem_juros_cartao_normal,
            "qtd_parcelas_sem_juros_cartao_normal": qtd_parcelas_sem_juros_cartao_normal,
            "preco_prazo_com_juros_cartao_normal": preco_prazo_com_juros_cartao_normal,  # sem valores com juros na página do produto. se tiver, vai ter na pagina de pagamento (dúvida de possibilidade)
            "qtd_parcelas_com_juros_cartao_normal": qtd_parcelas_com_juros_cartao_normal,  # sem valores com juros na página do produto. se tiver, vai ter na pagina de pagamento (dúvida de possibilidade)
            "taxa_juros_cartao_normal": None,  # sem valores com juros na página do produto. se tiver, vai ter na pagina de pagamento (dúvida de possibilidade)
            "preco_x1_cartao_proprio": json_str["initialState"]["components"]["track"][
                "melidata_event"
            ]["event_data"][
                "price"
            ],  # preço estimado
            "preco_prazo_sem_juros_cartao_proprio": json_str["initialState"][
                "components"
            ]["track"]["melidata_event"]["event_data"]["price"],
            "qtd_parcelas_sem_juros_cartao_proprio": qtd_parcelas_sem_juros_cartao_proprio,
            "preco_prazo_com_juros_cartao_proprio": preco_prazo_com_juros_cartao_proprio,
            "qtd_parcelas_com_juros_cartao_proprio": qtd_parcelas_com_juros_cartao_proprio,
            "taxa_juros_cartao_proprio": None,  # cartão próprio não tem juros
            "frete": None,
            "cep": None,
            # "estoque": estoque,
            "estoque": bool(estoque) if estoque is not None else None,
            "url": f"https://produto.mercadolivre.com.br/{product_id}",  # f"https://www.mercadolivre.com.br/p/{product_id}",
            # "data_extracao": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_extracao": get_current_date()
        }

        # Combina os dicionários
        combined_details = {**details, **payment_details}

        # Adiciona à lista
        all_details.append(combined_details)

        try:
            ### guardando os produtos relacionados ao produto procurado
            data = json.loads(
                response.text.split('melidata("add", "event_data", ')[1].split(");")[0]
            )

            # Inicialmente, criamos um conjunto vazio para armazenar os valores únicos de 'catalog_product_id'
            catalog_product_ids = set()

            # Aqui, você adiciona os IDs ao conjunto existente
            # Verificando a chave 'catalog_product_id' na raiz do JSON
            if "catalog_product_id" in data:
                unique_catalog_product_ids.add(data["catalog_product_id"])

            # Verificando a chave 'pickers', que pode conter 'catalog_product_id'
            if "pickers" in data:
                for picker_type in data["pickers"].values():
                    for item in picker_type:
                        if "catalog_product_id" in item:
                            unique_catalog_product_ids.add(item["catalog_product_id"])

            # Verificando a chave 'alternative_buying_options', que também pode conter 'catalog_product_id'
            if "alternative_buying_options" in data:
                for item in data["alternative_buying_options"]:
                    if "catalog_product_id" in item:
                        unique_catalog_product_ids.add(item["catalog_product_id"])

            # time.sleep(1)
        except:
            pass
        time.sleep(0.5)
    except:
        log.error(f"Erro ao obter informações de {product_id}")
        produtos_com_erro_de_requisicao.append(product_id)
        if response.status_code > 400:
            log.error(response.status_code)
            break

# Convertendo o conjunto para uma lista para obter os valores únicos
unique_catalog_product_ids = list(unique_catalog_product_ids)

# Cria o DataFrame a partir da lista de todos os detalhes
df = pd.DataFrame(all_details)
# df.to_excel("Extrações Mercado Livre.xlsx", index=False)

# Upload to bucket

# Formatar a data e hora no formato desejado
pasta = "extracoes/"
loja = "mercadolivre"
agora = datetime.datetime.now()
nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

gcs_manager.upload_parquet(df, nome_arquivo)
log.info("Fim")