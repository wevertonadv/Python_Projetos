import requests
import pandas as pd
import time
import json
import datetime
from tqdm import tqdm
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__
from modules.date_helper import get_current_date

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))

def get_model_codes(start, params_type):
    headers = {
        "authority": "searchapi.samsung.com",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "pt-BR,pt;q=0.9",
        "origin": "https://www.samsung.com",
        "referer": "https://www.samsung.com/",
        "sec-ch-ua": '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    }

    params = {
        "type": params_type,
        "siteCode": "br",
        "start": str(start),
        "num": "12",
        "sort": "newest",
        "onlyFilterInfoYN": "N",
        "keySummaryYN": "Y",
        "specHighlightYN": "Y",
        "familyId": "",
    }

    response = requests.get(
        "https://searchapi.samsung.com/v6/front/b2c/product/finder/global",
        params=params,
        headers=headers,
    )

    return response.json()


def find_details(json_data):
    details = {
        "fabricante": [],
        "marca": [],
        "sku": [],
        "produtos": [],
        "cores": [],
        "memorias": [],
        "eans": [],
        "no_modelos": [],
        "nome_comercial": [],
        "estoque": [],
    }

    def recurse_json(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "name":
                    if value == "Cor" and "values" in obj and "json" in obj["values"]:
                        details["cores"].extend(obj["values"]["json"])
                    elif (
                        value == "Memória"
                        and "values" in obj
                        and "json" in obj["values"]
                    ):
                        details["memorias"].extend(obj["values"]["json"])
                    elif (
                        value == "Manufacturer"
                        and "values" in obj
                        and "json" in obj["values"]
                    ):
                        details["fabricante"].extend(obj["values"]["json"])
                    elif (
                        value == "BRAND" and "values" in obj and "json" in obj["values"]
                    ):
                        details["marca"].extend(obj["values"]["json"])
                elif key == "ean":
                    details["eans"].append(value)
                elif key == "Value" and isinstance(value, str):
                    details["no_modelos"].append(value)
                elif key == "productName":
                    details["nome_comercial"].append(value)
                elif key == "itemId":
                    details["sku"].append(value)
                elif key == "nameComplete":
                    details["produtos"].append(value)
                elif key == "AvailableQuantity":
                    # details["estoque"].append(value)
                    details["estoque"].append(bool(value) if value is not None else None)
                else:
                    recurse_json(value)
        elif isinstance(obj, list):
            for item in obj:
                recurse_json(item)

    recurse_json(json_data)
    return details


def find_detailed_payment_details(json_data):
    payment_details = {
        "preco_pix": None,
        "preco_boleto": None,
        "preco_x1_cartao": None,
        "preco_prazo_sem_juros_cartao_normal": None,
        "qtd_parcelas_sem_juros_cartao_normal": 0,
        "preco_prazo_com_juros_cartao_normal": None,
        "qtd_parcelas_com_juros_cartao_normal": 0,
        "taxa_juros_cartao_normal": 0,
        "preco_x1_cartao_proprio": None,
        "preco_prazo_sem_juros_cartao_proprio": None,
        "qtd_parcelas_sem_juros_cartao_proprio": 0,
        "preco_prazo_com_juros_cartao_proprio": None,
        "qtd_parcelas_com_juros_cartao_proprio": 0,
        "taxa_juros_cartao_proprio": 0,
    }

    def recurse_json(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "Name":
                    payment_system = obj.get("PaymentSystemName", "")
                    n_installments = obj.get("NumberOfInstallments", 0)
                    total_value = obj.get("TotalValuePlusInterestRate", 0)

                    if payment_system == "Pix" and n_installments == 1:
                        payment_details["preco_pix"] = total_value
                    elif payment_system == "Boleto Bancário" and n_installments == 1:
                        payment_details["preco_boleto"] = total_value
                    elif n_installments == 1:
                        payment_details["preco_x1_cartao"] = total_value

                    # Lógica para cartões normais e cartões próprios da loja
                    if (
                        "Express" in payment_system
                        or "Mastercard" in payment_system
                        or "Visa" in payment_system
                        or "Elo" in payment_system
                    ):
                        if obj.get("InterestRate", 0) == 0:  # Sem juros
                            if (
                                n_installments
                                > payment_details[
                                    "qtd_parcelas_sem_juros_cartao_normal"
                                ]
                            ):
                                payment_details[
                                    "qtd_parcelas_sem_juros_cartao_normal"
                                ] = n_installments
                                payment_details[
                                    "preco_prazo_sem_juros_cartao_normal"
                                ] = total_value
                        else:  # Com juros
                            if (
                                n_installments
                                > payment_details[
                                    "qtd_parcelas_com_juros_cartao_normal"
                                ]
                            ):
                                payment_details[
                                    "qtd_parcelas_com_juros_cartao_normal"
                                ] = n_installments
                                payment_details[
                                    "preco_prazo_com_juros_cartao_normal"
                                ] = total_value
                    elif "Samsung Itaucard" in payment_system:
                        if n_installments == 1:
                            payment_details["preco_x1_cartao_proprio"] = total_value
                        elif obj.get("InterestRate", 0) == 0:  # Sem juros
                            if (
                                n_installments
                                > payment_details[
                                    "qtd_parcelas_sem_juros_cartao_proprio"
                                ]
                            ):
                                payment_details[
                                    "qtd_parcelas_sem_juros_cartao_proprio"
                                ] = n_installments
                                payment_details[
                                    "preco_prazo_sem_juros_cartao_proprio"
                                ] = total_value
                        else:  # Com juros
                            if (
                                n_installments
                                > payment_details[
                                    "qtd_parcelas_com_juros_cartao_proprio"
                                ]
                            ):
                                payment_details[
                                    "qtd_parcelas_com_juros_cartao_proprio"
                                ] = n_installments
                                payment_details[
                                    "preco_prazo_com_juros_cartao_proprio"
                                ] = total_value
                else:
                    recurse_json(value)
        elif isinstance(obj, list):
            for item in obj:
                recurse_json(item)

    recurse_json(json_data)
    return payment_details


def extract_model_data(model_code, model_codes, pdpUrl, extracted_models, debug=False):
    max_attempts = 3  # Número máximo de tentativas
    attempt = 0  # Contador de tentativas

    # Verificar se o modelo já foi extraído
    if model_code in extracted_models:
        if debug:
            log.info(f"Modelo {model_code} já foi extraído anteriormente.")
        return pd.DataFrame()

    url = f"https://shop.samsung.com/br/getServicesProduct?productCode={model_code}"
    while attempt < max_attempts:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                html_content = response.text
                json_str = (
                    html_content.split(
                        '<template data-type="json" data-varname="__STATE__">'
                    )[1]
                    .split("<script>")[1]
                    .split("</script>")[0]
                )
                json_data = json.loads(json_str)

                details = find_details(json_data)
                detailed_payment_details = find_detailed_payment_details(json_data)

                # data_extracao = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data_extracao = get_current_date()

                if debug:
                    log.info(f"{details}")

                # Filtrar apenas os modelos que estão em model_codes
                filtered_modelos = [
                    modelo for modelo in details["no_modelos"] if modelo in model_codes
                ]
                details["fabricante"] = details["fabricante"][-len(filtered_modelos) :]
                details["marca"] = details["marca"][-len(filtered_modelos) :]
                details["sku"] = details["sku"][-len(filtered_modelos) :]
                details["produtos"] = details["produtos"][-len(filtered_modelos) :]
                details["cores"] = details["cores"][-len(filtered_modelos) :]
                details["eans"] = details["eans"][-len(filtered_modelos) :]
                details["nome_comercial"] = details["nome_comercial"][
                    -len(filtered_modelos) :
                ]
                details["estoque"] = details["estoque"][-len(filtered_modelos) :]

                # Verificar se as listas têm o mesmo tamanho
                max_length = max(
                    len(details["fabricante"]),
                    len(details["marca"]),
                    len(details["sku"]),
                    len(details["produtos"]),
                    len(details["nome_comercial"]),
                    len(details["estoque"]),
                    len(details["cores"]),
                    len(details["memorias"]),
                    len(details["eans"]),
                    len(filtered_modelos),
                )

                for key in [
                    "fabricante",
                    "marca",
                    "sku",
                    "produtos",
                    # "nome_comercial",
                    "cores",
                    "memorias",
                    "eans",
                    "no_modelos",
                    "estoque",
                ]:
                    while len(details[key]) < max_length:
                        details[key].append(
                            None
                        )  # Adiciona None para alinhar os tamanhos
                if debug:
                    log.info("-" * 50)
                    log.info(model_code)
                try:
                    if debug:
                        log.info(details["fabricante"])
                        log.info(details["marca"])
                        log.info(details["eans"])
                        log.info(details["sku"])
                        log.info(details["nome_comercial"][0])
                        log.info(details["memorias"][0])
                        log.info(details["cores"])
                        log.info(details["produtos"])
                        log.info("samsung")
                        log.info("samsung")
                        log.info(details["estoque"])
                        log.info("https://www.samsung.com/" + pdpUrl)
                        log.info(data_extracao)

                    # Criar DataFrame para o modelo atual
                    df_model = pd.DataFrame(
                        {
                            "fabricante": details["fabricante"],
                            "marca": details["marca"],
                            "ean": details["eans"],
                            "sku": details["sku"],
                            "no_modelo": filtered_modelos,
                            # "nome_comercial": details["nome_comercial"][0],
                            "nome_comercial": None,
                            "capacidade_armazenamento": details["memorias"][0],
                            "cor": details["cores"],
                            "nome_produto": details["produtos"],
                            "empresa": "Samsung",
                            "vendedor": "Samsung",
                            **detailed_payment_details,
                            "frete": None,
                            "cep": None,
                            "estoque": details["estoque"],
                            "url": "https://www.samsung.com/" + pdpUrl,
                            "data_extracao": data_extracao,
                        }
                    )

                    # Adicionar o modelo ao registro de modelos extraídos
                    extracted_models.update(filtered_modelos)

                    return df_model
                except:
                    return pd.DataFrame()
            else:
                if debug:
                    log.error(f"Failed to retrieve data for {model_code}. Status code: {response.status_code}")
                return pd.DataFrame()

        except requests.exceptions.ConnectionError as e:
            attempt += 1
            log.error(f"Falha na tentativa {attempt}: {e}")
            time.sleep(5)  # Espera 3 segundos antes de tentar novamente


def extraction(params_type, debug=False):
    start = 1
    model_codes = []
    pdpUrls = []  # Lista para armazenar as URLs
    total_records = 1  # Inicializa com 1 para entrar no loop
    params_type = params_type

    while start <= total_records:
        data = get_model_codes(start, params_type)
        if (
            "response" in data
            and "resultData" in data["response"]
            and "productList" in data["response"]["resultData"]
        ):
            productList = data["response"]["resultData"]["productList"]
            for product in productList:
                if "modelList" in product:
                    for model in product["modelList"]:
                        if "modelCode" in model and "pdpUrl" in model:
                            model_codes.append(model["modelCode"])
                            pdpUrls.append(
                                model["originPdpUrl"]
                            )  # Adiciona a URL à lista

        # Atualiza o total de registros e o próximo valor de 'start'
        total_records = int(data["response"]["resultData"]["common"]["totalRecord"])
        start += 12

    # Imprime a lista de códigos dos modelos e URLs
    if debug:
        log.info("Model Codes and PDP URLs:")
        for code, url in zip(model_codes, pdpUrls):
            log.info(f"{code} - {url}")

    # Conjunto para armazenar os modelos já extraídos
    extracted_models = set()

    # DataFrame final para todos os modelos
    df = pd.DataFrame()

    for model_code, pdpUrl in tqdm(zip(model_codes, pdpUrls), total=len(model_codes)):
        df_model = extract_model_data(
            model_code, model_codes, pdpUrl, extracted_models, debug=debug
        )

        df = pd.concat([df, df_model], ignore_index=True)
        time.sleep(1)

    return df


DEBUG = False

df_smartphones = extraction("01010000", debug=DEBUG)
df_tablets = extraction("01020000", debug=DEBUG)
df_smartwatches = extraction("01030000", debug=DEBUG)
df_galaxy_buds = extraction("01040000", debug=DEBUG)
df_acessorios = extraction("01050000", debug=DEBUG)
df_computadores = extraction("03010000", debug=DEBUG)
df = pd.concat(
    [
        df_smartphones,
        df_tablets,
        df_computadores,
        df_smartwatches,
        df_galaxy_buds,
        df_acessorios,
    ]
)

df.reset_index(drop=True, inplace=True)
# df.to_excel("Extrações Samsung.xlsx", index=False)

# Upload to bucket
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\assets\pro-ia-precificador-f1cde9d5eada.json"

gcs_manager = GCSParquetManager()

# Formatar a data e hora no formato desejado
pasta = "extracoes/"
loja = "samsung"
agora = datetime.datetime.now()
nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

gcs_manager.upload_parquet(df, nome_arquivo)
log.info("Fim")