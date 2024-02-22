
import undetected_chromedriver as uc
import json5
import re
from tqdm import tqdm
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from modules.verify_names import combined_similarity
import pandas as pd
import time
import random
from unidecode import unidecode
import datetime
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__
from modules.date_helper import get_current_date
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))
log.info("Inicio")
def extract_data_by_keywords(data, keywords, regex_target):
    """
    Extrai informações de uma estrutura de dados aninhada com base em palavras-chave e expressões regulares.

    Esta função busca recursivamente em um dicionário ou lista, procurando por chaves que correspondam a uma lista de
    palavras-chave ou que contenham padrões definidos por uma expressão regular. Quando uma correspondência é encontrada,
    o valor associado e a possível fonte de pagamento são armazenados.

    :param data: A estrutura de dados (dicionário ou lista) para ser explorada.
    :param keywords: Lista de palavras-chave a serem buscadas nas chaves do dicionário.
    :param regex_target: Expressão regular para identificar e capturar informações específicas nas chaves.
    :return: Uma lista de dicionários com as chaves 'key', 'value' e 'source' correspondentes às correspondências encontradas.
    """
    results = []

    def extract_info(data, source=None):
        if isinstance(data, dict):
            for key, value in data.items():
                # Verifica se a chave contém informações relevantes com base na expressão regular
                match = re.search(regex_target, key)
                if match:
                    # Caso a expressão regular defina grupos de captura
                    if regex_target and match.groups():
                        source = match.group(1)
                    else:
                        source = match.group(0)

                # Busca recursiva em subdicionários e listas
                if isinstance(value, dict) or isinstance(value, list):
                    extract_info(value, source)
                # Armazena os dados se a chave corresponder às palavras-chave
                elif any(kw in key for kw in keywords) or any(kw in str(value) for kw in keywords):
                    results.append({'key': key, 'value': value, 'source': source})

        elif isinstance(data, list):
            # Aplica a mesma lógica para cada item da lista
            for item in data:
                extract_info(item, source)

    extract_info(data)
    return results

def extract_data_by_id(data, product_id):
    """
    Esta função busca por um produto específico, identificado pelo seu ID, em uma estrutura de dados aninhada.
    Ela explora tanto dicionários quanto listas e realiza uma busca recursiva em caso de estruturas aninhadas.

    :param data: A estrutura de dados (dicionário ou lista) onde a busca será realizada.
    :param product_id: O ID do produto a ser buscado.
    :return: Um dicionário com as informações do produto, ou None se o produto não for encontrado.
    """
    if isinstance(data, dict):
        # Itera sobre cada par chave-valor no dicionário
        for key, value in data.items():
            # Verifica se a chave é um ID de produto e se o valor corresponde ao ID desejado
            if key == 'id' and value == product_id:
                return data  # Retorna o dicionário completo do produto

            # Se o valor for outro dicionário ou lista, realiza uma busca recursiva
            if isinstance(value, (dict, list)):
                result = extract_data_by_id(value, product_id)
                if result is not None:
                    return result

    elif isinstance(data, list):
        # Itera sobre cada item na lista
        for item in data:
            result = extract_data_by_id(item, product_id)
            if result is not None:
                return result

    # Retorna None se o produto não for encontrado
    return None

def find_json_objects(scripts_in_html, content_keyword_list):
    """
    Função que encontra objetos JSON em uma lista de scripts
    :param scripts_in_html: Lista de scripts
    :param content_keyword_list: Lista de palavras-chave para filtrar os scripts
    :return: Lista de objetos JSON encontrados
    """
    # Filtra scripts que contêm a palavra-chave especificada
    scripts_with_keyword = []
    for script in scripts_in_html:
        for keyword in content_keyword_list:
            if keyword.lower() in script.text.lower():
                scripts_with_keyword.append(script.text)
    
    # Procura por dicionários nos scripts filtrados com condições adicionais
    extracted_dictionaries = None
    for script in scripts_with_keyword:
        script_lower = script.lower()
        for keyword in content_keyword_list:
            if keyword.lower() in script_lower.lower() :
                # Utiliza expressões regulares para extrair dicionários do script
                extracted_dictionaries = re.findall(r'({.*})', script)
                break
    
    # Tentativa de transformar os dicionarios de extracted_dictionaries em json e colocar em uma lista
    json_content = []
    if extracted_dictionaries is None:
        return json_content
    
    for i in extracted_dictionaries:
        try:
            json_content.append(json5.loads(i))
        except:
            log.error('Erro ao carregar o json')
    
    return json_content

def get_value_from_key(dicionario, chave):
    """
    Busca recursivamente um valor a partir de uma chave em um dicionário, incluindo subdicionários.
    
    :param dicionario: O dicionário onde a busca será realizada.
    :param chave: A chave cujo valor está sendo buscado.
    :return: O valor correspondente à chave, ou None se a chave não for encontrada.
    """
    if isinstance(dicionario, list):
        for item in dicionario:
            if isinstance(item, dict):
                resultado = get_value_from_key(item, chave)
                if resultado is not None:
                    return resultado
    elif isinstance(dicionario, dict):
        for key, valor in dicionario.items():
            if key == chave:
                return valor
            elif isinstance(valor, dict):
                resultado = get_value_from_key(valor, chave)
                if resultado is not None:
                    return resultado
            elif isinstance(valor, list):
                for item in valor:
                    if isinstance(item, dict):
                        resultado = get_value_from_key(item, chave)
                        if resultado is not None:
                            return resultado

    return None

def find_all_get_value_from_key(dicionario, chave):
    """
    Busca recursivamente um valor a partir de uma chave em um dicionário, incluindo subdicionários.
    
    :param dicionario: O dicionário onde a busca será realizada.
    :param chave: A chave cujo valor está sendo buscado.
    :return: Uma lista com todos os valores correspondentes à chave, ou uma lista vazia se a chave não for encontrada.
    """
    resultados = []

    if isinstance(dicionario, list):
        for item in dicionario:
            if isinstance(item, dict):
                resultados.extend(find_all_get_value_from_key(item, chave))
    elif isinstance(dicionario, dict):
        for key, valor in dicionario.items():
            if key == chave:
                resultados.append(valor)
            elif isinstance(valor, dict):
                resultados.extend(find_all_get_value_from_key(valor, chave))
            elif isinstance(valor, list):
                for item in valor:
                    if isinstance(item, dict):
                        resultados.extend(find_all_get_value_from_key(item, chave))

    return resultados

# Função para calcular a similaridade
def calcular_similaridade(texto, referencia):
    vetorizador = CountVectorizer().fit([texto, referencia])
    vetor_texto = vetorizador.transform([texto])
    vetor_referencia = vetorizador.transform([referencia])
    return cosine_similarity(vetor_texto, vetor_referencia)[0][0]

def extrair_cor(texto):
    # Regex para extrair a cor (não case sensitive)
    regex_cor = r"Cor: (\w+)"
    regex_cor_2_try = r"Cor (\w+)"
    
    # Encontrar a correspondência do regex (não case sensitive)
    cor_match = re.search(regex_cor, texto, re.IGNORECASE)
    
    # Verificar se a correspondência foi encontrada
    if cor_match:
        cor = cor_match.group(1)  # Obter o grupo de captura com a cor
        return cor
    else:
        cor_match = re.search(regex_cor_2_try, texto, re.IGNORECASE)
        if cor_match:
            cor = cor_match.group(1)  # Obter o grupo de captura com a cor
            return cor
        else:
            return None
    
def extrair_modelo(texto):
    # Regex para extrair a cor (não case sensitive)
    regex_modelo = r"o Modelo\s*([\w/]+)"
    regex_modelo_2_try = r"Modelo:\s*([\w/]+)"
    
    # Encontrar a correspondência do regex (não case sensitive)
    modelo_match = re.search(regex_modelo, texto, re.IGNORECASE)
    
    # Verificar se a correspondência foi encontrada
    if modelo_match:
        modelo = modelo_match.group(1)  # Obter o grupo de captura com a cor
        return modelo
    else:
        modelo_match = re.search(regex_modelo_2_try, texto, re.IGNORECASE)
        if modelo_match:
            modelo = modelo_match.group(1)  # Obter o grupo de captura com a cor
            return modelo
        else:
            return None

def extrair_ean(texto):
    # Regex para extrair a cor (não case sensitive)
    regex_ean = r"EAN\s*(\d+)"
    
    # Encontrar a correspondência do regex (não case sensitive)
    ean_match = re.search(regex_ean, texto, re.IGNORECASE)
    
    # Verificar se a correspondência foi encontrada
    if ean_match:
        ean = ean_match.group(1)  # Obter o grupo de captura com a cor
        return ean
    else:
        return None
    
def calcular_parcela_com_juros(valor_principal, taxa_juros, num_parcelas):
    return valor_principal * taxa_juros / (1 - (1 + taxa_juros) ** -num_parcelas)

def get_payment_details(payment_data):
    # Extrai PIX
    result_pix = get_value_from_key(payment_data, 'pix')
    price_details_pix = get_value_from_key(result_pix, 'minQuantity')
    df_price_details_cartao = pd.DataFrame(price_details_pix)
    pix_price = df_price_details_cartao['total'].min()
    
    
    # Extrai Boleto
    result_boleto = get_value_from_key(payment_data, 'boleto')
    price_boleto = get_value_from_key(result_boleto, 'price')
    
    # Extrai Cartão Normal
    result_cartao = get_value_from_key(payment_data, 'cartaoVisa')
    price_details_cartao = get_value_from_key(result_cartao, 'installment')
    
    df_price_details_cartao = pd.DataFrame(price_details_cartao)
    cartao_sem_juros = df_price_details_cartao[df_price_details_cartao['interestRate'] == 0]
    cartao_com_juros = df_price_details_cartao[df_price_details_cartao['interestRate'] > 0]
    
    cartao_sem_juros_min_price = cartao_sem_juros['total'].min()
    cartao_sem_juros_max_quantidade =  cartao_sem_juros['quantity'].max()
    cartao_com_juros_max_price = cartao_com_juros['total'].max()
    cartao_com_juros_max_quantidade =  cartao_com_juros['quantity'].max()
    cartao_com_juros_taxa = cartao_com_juros['interestRate'].min()
    
    # Extrai Cartão Ame
    result_ame = get_value_from_key(payment_data, 'ame')
    price_details_ame = get_value_from_key(result_ame, 'installment')
    
    df_price_details_ame = pd.DataFrame(price_details_ame)
    ame_sem_juros = df_price_details_ame[df_price_details_ame['interestRate'] == 0]
    ame_com_juros = df_price_details_ame[df_price_details_ame['interestRate'] > 0]
    
    ame_sem_juros_min_price = ame_sem_juros['total'].min()
    ame_sem_juros_max_quantidade =  ame_sem_juros['quantity'].max()
    ame_com_juros_max_price = ame_com_juros['total'].max()
    ame_com_juros_max_quantidade =  ame_com_juros['quantity'].max()
    ame_com_juros_taxa = ame_com_juros['interestRate'].min()
    
    return {
        'pix': pix_price, 
        'boleto': price_boleto, 
        'ame': {
            'sem_juros': {
                'min_price': ame_sem_juros_min_price, 
                'max_quantidade': ame_sem_juros_max_quantidade
            }, 
            'com_juros': {
                'max_price': ame_com_juros_max_price, 
                'max_quantidade': ame_com_juros_max_quantidade, 
                'taxa': ame_com_juros_taxa
            }
        },
        'cartao': {
            'sem_juros': {
                'min_price': cartao_sem_juros_min_price, 
                'max_quantidade': cartao_sem_juros_max_quantidade
            }, 
            'com_juros': {
                'max_price': cartao_com_juros_max_price, 
                'max_quantidade': cartao_com_juros_max_quantidade, 
                'taxa': cartao_com_juros_taxa
            }
        }
    }

base_url = 'https://www.submarino.com.br'

# Leitura do excel
# df_vivo = pd.read_excel(r'assets/device_list.xlsx')

# list_product_name = df_vivo['NOME_COMERCIAL'].tolist()
gcs_manager = GCSParquetManager()
list_product_name = gcs_manager.ler_coluna_excel()
#list_product_name = random.sample(list_product_name, 3) # RETIRAR O RANDOM.SAMPLE se for pegar todos os produtos

# Configurando o undetected-chromedriver
options = uc.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
# options.add_argument('--headless')
# options.add_argument('--no-sandbox')
# options.add_argument('--disable-extensions')
# options.add_argument('--disable-plugins')
# options.add_argument('--blink-settings=imagesEnabled=false')
# options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
# options.add_experimental_option("excludeSwitches", ["enable-logging"])

# Inicializando o navegador com as opções configuradas
driver = uc.Chrome(service=Service(ChromeDriverManager().install()),options=options)
driver.execute_script("""
var links = document.getElementsByTagName('link');
for (var i = 0; i < links.length; i++) {
    if (links[i].getAttribute('rel') === 'stylesheet') {
        links[i].disabled = true;
    }
}
""")

max_items_by_product_name = 50
list_products_with_name_id_seller = []
list_json_search_page = []
for product_name in tqdm(list_product_name):
    # Substitui espaços por hífens e converte para letras minúsculas
    product_name_search_phrase = product_name.replace(' ', '-').lower()
    product_name_search_rc = product_name.replace(' ', '-')
    
    # Acessando a URL
    # driver.get(url_search_page)
    url_search_page = f'{base_url}/busca/{product_name_search_phrase}?rc={product_name_search_rc}&limit={max_items_by_product_name}&offset=0'
    # # Acessando a URL
    driver.get(url_search_page)
    
    # Espera até que o elemento <body> esteja disponível na página
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    except:
        # Caso o elemento não seja encontrado, uma exceção é levantada
        log.error(f'Não foi possível encontrar o elemento <body> na página {url_search_page}')
        continue
    
    # Obtendo o código fonte da página
    search_page_source = driver.page_source
    
    
    """
    Este trecho do script é responsável pela análise de uma página HTML para extrair informações específicas em formato JSON. 
    
    1. Primeiro, o código converte o código-fonte da página HTML em um objeto BeautifulSoup, facilitando a análise e extração de dados. 
    2. Em seguida, ele procura todos os elementos <script> dentro da página HTML. Esses elementos geralmente contêm dados e códigos JavaScript.
    3. A função 'find_json_objects' é utilizada para encontrar objetos JSON dentro desses scripts, filtrando aqueles que contêm a chave especificada ('ROOT', neste caso). 
    4. Se exatamente um objeto JSON é encontrado, ele é armazenado em 'json_content'. Caso contrário, uma exceção é levantada, indicando um problema na localização dos dados JSON.
    """
    
    # Transforma a fonte da página em um objeto BeautifulSoup para análise
    html_parser = bs(search_page_source, 'html.parser')
    
    # Encontra todos os elementos <script> na página HTML
    scripts = html_parser.find_all('script')
    
    # Busca objetos JSON nos scripts que contêm a chave 'ROOT'
    jsons_search_page = find_json_objects(scripts, ['ROOT'.lower()])
    
    # Verifica se exatamente um objeto JSON foi encontrado
    if len(jsons_search_page) == 1:
        json_content = jsons_search_page[0]
        list_json_search_page.append({product_name: json_content})
    else:
        # Caso não seja encontrado um único objeto JSON, uma exceção é levantada
        log.error('Não foi possível encontrar um único objeto JSON na página de busca')

    
for json_content in tqdm(list_json_search_page):
    """
    Este trecho do script extrai uma lista de IDs de produtos de uma estrutura de dados JSON. 
    A função 'get_value_from_key' é utilizada para buscar a chave 'productIds' no conteúdo JSON, 
    que é esperado conter uma lista dos IDs de produtos. 
    Essa lista é então armazenada na variável 'product_ids' para uso posterior.
    """
    
    # Extrai a lista de IDs de produtos do conteúdo JSON

    product_ids = get_value_from_key(json_content, 'productIds')

    product_name_vivo = list(json_content.keys())[0]

    
    if not product_ids:
        log.error('Não foi possível encontrar a chave \'productIds\' no conteúdo JSON.')
        continue
    
    """
    Este script define e utiliza a função 'extract_data_by_id' para buscar informações de produtos específicos em uma estrutura de dados aninhada, que pode ser um dicionário ou uma lista. A função utiliza uma abordagem recursiva para explorar subdicionários e listas aninhadas, buscando por um produto cujo ID corresponde ao fornecido como parâmetro. Se encontrado, retorna um dicionário contendo as informações desse produto.
    
    Após definir a função, o script executa um loop sobre uma lista de IDs de produtos ('product_ids'). Para cada ID, a função 'extract_data_by_id' é chamada com o conteúdo JSON ('json_content') e o ID do produto. Se um produto é encontrado, seu nome é extraído e armazenado em uma lista junto com seu ID. Se um produto com o ID especificado não for encontrado, uma mensagem de erro é exibida.
    
    O resultado final é uma lista de dicionários, onde cada dicionário contém o ID e o nome de um produto encontrado na estrutura de dados.
    """
    
    # Utilização da função 'extract_data_by_id' para buscar produtos
    
    for product_id in product_ids:
        product_info = extract_data_by_id(json_content, product_id)
        if product_info:
            # Extrai o nome do produto usando a chave 'name'
            
            seller_id = get_value_from_key(product_info, 'seller')
            if seller_id:
                seller_id = seller_id.get('__ref')
            
            attributos = get_value_from_key(product_info, 'attributes')
            
            product_name = product_info.get('name')
            if product_name:
                similarity_names = combined_similarity(product_name, product_name_vivo)
                if similarity_names < 0.5:
                    continue
                # Adiciona o ID e o nome do produto à lista
                list_products_with_name_id_seller.append({'sku': product_id, 'product_name': product_name, 'seller_id': seller_id, 'attributos': attributos})
    
df_page_product_info = pd.DataFrame(list_products_with_name_id_seller)
df_page_product_info = df_page_product_info.drop_duplicates(subset=['sku'])

script_to_get_network = """
var performance = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {};
var network = performance.getEntries() || {};
return network;
"""

list_page_source_product = []
for _, row in tqdm(df_page_product_info.iterrows(), total=df_page_product_info.shape[0]):
    # Obtendo o ID do produto
    product_id = row['sku']
    # Obtendo o nome do produto
    product_name = row['product_name']
    
    # Acessando a URL
    url_product_page = f'{base_url}/produto/{product_id}'
    driver.get(url_product_page)
    
    product_page_source = driver.page_source

    # Encontrar e clicar no botão pelo texto
    try:
        botao = driver.find_element("xpath","//*[contains(text(), 'formas de pagamento')]")

        botao.click()
    except:
        pass

    # Aguardar novamente para assegurar que a página carregou após o clique
    time.sleep(1)
    
    payment_data = None

    network_tracking = driver.execute_script(script_to_get_network)
    for item in network_tracking:
        name = item['name']
        if 'graphql' in name.lower() and 'payment' in name.lower():
            driver.get(name)
            json_source = driver.page_source
            # Analise o HTML
            soup = bs(json_source, 'html.parser')
            
            # Encontre o elemento <pre> e obtenha seu conteúdo
            json_string = soup.find('pre').text
            payment_data = json5.loads(json_string)
            
    if payment_data is None:
        log.error(f'Não foi possível encontrar o JSON de pagamento na página {url_product_page}')
        continue
    
    # data_extracao = pd.Timestamp.now()
    data_extracao = get_current_date()
    dict_product_info = {'sku': product_id, 'product_name': product_name, 'product_page_source': product_page_source, 'link': url_product_page, 'payment_details': get_payment_details(payment_data), 'data_extracao': data_extracao}
    list_page_source_product.append(dict_product_info)
driver.quit()

# Usando um dicionário para remover duplicatas
unique_products = {}
for product in list_page_source_product:
    product_id = product['sku']
    if product_id not in unique_products:
        unique_products[product_id] = product

# Convertendo o dicionário de volta para uma lista
list_page_source_product_unique = list(unique_products.values())

"""
Este trecho do script é responsável pela análise de uma página HTML para extrair informações específicas em formato JSON. 

1. Primeiro, o código converte o código-fonte da página HTML em um objeto BeautifulSoup, facilitando a análise e extração de dados. 
2. Em seguida, ele procura todos os elementos <script> dentro da página HTML. Esses elementos geralmente contêm dados e códigos JavaScript.
3. A função 'find_json_objects' é utilizada para encontrar objetos JSON dentro desses scripts, filtrando aqueles que contêm a chave especificada ('ROOT', neste caso). 
4. Se exatamente um objeto JSON é encontrado, ele é armazenado em 'json_content'. Caso contrário, uma exceção é levantada, indicando um problema na localização dos dados JSON.
"""

list_product_details_and_json = []
for dict_product in tqdm(list_page_source_product):
    # Obtendo o código fonte da página
    page_source = dict_product['product_page_source']
    
    # Obtendo o ID do produto
    product_id = dict_product['sku']
    
    # Obtendo o nome do produto
    product_name = dict_product['product_name']
    
    # Obtendo o link do produto
    link = dict_product['link']
    
    # Obtendo payment_details
    payment_details = dict_product['payment_details']

    # Obtendo a data de extração
    data_extracao = dict_product['data_extracao']
    
    # Transforma a fonte da página em um objeto BeautifulSoup para análise
    html_parser = bs(page_source, 'html.parser')

    # Encontra todos os elementos <script> na página HTML
    scripts = html_parser.find_all('script')
    
    # Busca objetos JSON nos scripts que contêm a chave 'ROOT'
    jsons_search_page = find_json_objects(scripts, ['ROOT'.lower()])
    
    # Verifica se exatamente um objeto JSON foi encontrado
    if len(jsons_search_page) == 1:
        json_content = jsons_search_page[0]
        dict_product_details = {'sku': product_id, 'product_name': product_name, 'json_content': json_content, 'link': link, 'payment_details':payment_details ,'data_extracao': data_extracao}
        list_product_details_and_json.append(dict_product_details)
    else:
        # Caso não seja encontrado um único objeto JSON, uma exceção é levantada
        log.error(f'Não foi possível encontrar um único objeto JSON na página de busca do produto {product_id}')


def process_product(dict_product):
    # Obtendo detalhes do produto
    product_id = dict_product['sku']
    product_name = dict_product['product_name']
    link = dict_product['link']
    payment_details = dict_product['payment_details']
    
    # Parse HTML
    html_parser = bs(dict_product['product_page_source'], 'html.parser')

    # Encontrar objetos JSON
    jsons_search_page = find_json_objects(html_parser.find_all('script'), ['root'])

    # Verificar se um objeto JSON foi encontrado
    if len(jsons_search_page) == 1:
        return {
            'sku': product_id,
            'product_name': product_name,
            'json_content': jsons_search_page[0],
            'link': link,
            'payment_details': payment_details,
            'data_extracao': dict_product['data_extracao']
        }
    else:
        return None

# Processar todos os produtos e coletar detalhes
list_product_details_and_json = [
    process_product(dict_product) for dict_product in tqdm(list_page_source_product)
    if 'product_page_source' in dict_product
]


# Exemplo de uso
key_price = 'price'
key_quantity = 'quantity'
key_total = 'total'
keywords_target = [key_price, key_quantity, key_total]
regex_alvo = r'"type":"([^"]+)"\}\)' # Regex para capturar a fonte de pagamento ( olhando apenas para a chave do alvo )

list_product_details = []
taxa_de_juros = None

for json_content in list_product_details_and_json:
    # Obtendo o ID do produto
    product_id = json_content['sku']
    
    # Obtendo o link do produto
    link = json_content['link']
    
    # Obtendo o seller_id
    seller_id = df_page_product_info[df_page_product_info['sku'] == product_id]['seller_id'].values[0]
    
    seller = get_value_from_key(json_content['json_content'], seller_id)
    if seller:
        seller = seller.get('name', None)
    
    # Obtendo a data de extração
    data_extracao = json_content['data_extracao']
    
    
    # Obtendo o nome do produto
    product_name = json_content['product_name']
    # Obtendo o conteúdo JSON
    json_content_product = json_content['json_content']
    
    # Obtendo os pagamentos
    payment_data = json_content['payment_details']
    
    # Obtendo a cor do produto
    list_description_content = find_all_get_value_from_key(json_content_product, 'description')
    
    color = None
    for description_content in list_description_content:
        if isinstance(description_content, dict):
            description_content = description_content.get('content', None)
            if description_content:
                color =  extrair_cor(description_content)
    
    modelo = None
    for description_content in list_description_content:
        if isinstance(description_content, dict):
            description_content = description_content.get('content', None)
            if description_content:
                modelo =  extrair_modelo(description_content)
                
    ean = None
    for description_content in list_description_content:
        if isinstance(description_content, dict):
            description_content = description_content.get('content', None)
            if description_content:
                ean =  extrair_ean(description_content)
    
    result = extract_data_by_keywords(json_content_product, keywords_target, regex_alvo)
    
    df_with_prices = pd.DataFrame(result)
    
    pix_price = None
    boleto_price = None
    cartao_price_min = None
    cartao_price_max = None
    cartao_qtd_parcelas_min = None
    cartao_qtd_parcelas_max = None
    cartao_com_juros_price_max = None

            
    dict_product_details = {
        'empresa': 'Submarino',
        'vendedor': seller,
        'sku': product_id,
        'product_name': product_name,
        'color_description': color,
        'ean_description': ean,
        'modelo_description': modelo,
        
        'preco_pix': payment_data['pix'],
        'preco_boleto': payment_data['boleto'],
        
        'preco_x1_cartao': payment_data['cartao']['sem_juros']['min_price'],
        'preco_prazo_sem_juros_cartao_normal': payment_data['cartao']['sem_juros']['min_price'],
        'qtd_parcelas_sem_juros_cartao_normal': payment_data['cartao']['sem_juros']['max_quantidade'],
        
        'preco_prazo_com_juros_cartao_normal': payment_data['cartao']['com_juros']['max_price'],
        'qtd_parcelas_com_juros_cartao_normal': payment_data['cartao']['com_juros']['max_quantidade'],
        'taxa_juros_cartao_normal': payment_data['cartao']['com_juros']['taxa'],
        
        'preco_x1_cartao_proprio': payment_data['ame']['sem_juros']['min_price'],
        'preco_prazo_sem_juros_cartao_proprio': payment_data['ame']['sem_juros']['min_price'],
        'qtd_parcelas_sem_juros_cartao_proprio': payment_data['ame']['sem_juros']['max_quantidade'],
        
        'preco_prazo_com_juros_cartao_proprio': payment_data['ame']['com_juros']['max_price'],
        'qtd_parcelas_com_juros_cartao_proprio': payment_data['ame']['com_juros']['max_quantidade'],
        'taxa_juros_cartao_proprio': payment_data['ame']['com_juros']['taxa'],
        'frete': None,
        'cep': None,
        'estoque': True,
        'url': link,
        'data_extracao': data_extracao
    }
    
    list_product_details.append(dict_product_details)
df_with_prices = pd.DataFrame(list_product_details).dropna(subset=['vendedor'])



merged_df = pd.merge(df_page_product_info[['sku', 'attributos']], df_with_prices, on='sku', how='right')
merged_df = merged_df.explode('attributos')
merged_df['attribute_name'] = merged_df['attributos'].apply(lambda x: x['name'] if isinstance(x, dict) else None)
merged_df['attribute_value'] = merged_df['attributos'].apply(lambda x: x['value'] if isinstance(x, dict) else None)
df_final = merged_df.drop('attributos', axis=1).dropna(subset=['attribute_name', 'attribute_value','sku','vendedor']).reset_index(drop=True)


atributos_desejados = ['marca', 'fabricante', 'cor', 'ean', 'modelo', 'armazenamento', 'capacidade', 'referencia do modelo']
# Calcula e armazena os scores em um dicionário temporário
scores_temp = {atributo.replace(" ", "_"): [] for atributo in atributos_desejados}

# Populando o dicionário com os scores
for index, row in df_final.iterrows():
    for atributo in atributos_desejados:
        score = calcular_similaridade(unidecode(row['attribute_name'].lower()), unidecode(atributo.lower()))
        scores_temp[atributo.replace(" ", "_")].append(score)
        
# Adiciona as colunas de score ao DataFrame
for atributo in atributos_desejados:
    df_final[atributo.replace(" ", "_") + '_score'] = scores_temp[atributo.replace(" ", "_")]

query = ' or '.join([f'{atributo.replace(" ", "_")}_score > 0.8' for atributo in atributos_desejados])
df_final = df_final.query(query)

colunas_caracteristicas = ['marca', 'fabricante', 'cor', 'ean', 'modelo','capacidade']
# Primeiro, inicialize as novas colunas com valores NaN ou uma string vazia
for atributo in colunas_caracteristicas:
    df_final[atributo] = None  # ou pd.NA se quiser usar o tipo de dado 'Nullable'

# Agora, itere pelas linhas e preencha as novas colunas com base no score
for index, row in df_final.iterrows():
    for atributo in atributos_desejados:
        atributo = atributo.replace(" ", "_")
        score_coluna = atributo + '_score'
        if row[score_coluna] > 0.8:
            # Se o atributo é 'Armazenamento' ou 'Capacidade', a coluna a ser preenchida é 'Capacidade'
            if atributo in ['armazenamento', 'capacidade']:
                df_final.at[index, 'capacidade'] = row['attribute_value']
            elif atributo in ['referencia do modelo']:
                if row['attribute_value'] is not None:
                    df_final.at[index, 'modelo'] = row['attribute_value']
            elif atributo in ['modelo']:
                df_final.at[index, 'modelo'] = row['attribute_value']
            else:
                df_final.at[index, atributo] = row['attribute_value']

# Suponha que df_final é o seu DataFrame

# Defina uma função personalizada para usar com agg que retorna o primeiro valor não nulo
def primeiro_nao_nulo(series):
    return series.dropna().iloc[0] if not series.dropna().empty else None

# Agora, agrupe por 'product_id' e aplique a função para cada coluna relevante
colunas_agregadas = {coluna: primeiro_nao_nulo for coluna in colunas_caracteristicas}
df_consolidado = df_final.groupby('sku').agg(colunas_agregadas).reset_index()

df_product = df_final.drop(columns=df_final.filter(regex='_score|_description|attribute|marca|cor|fabricante|ean|modelo|capacidade').columns).drop_duplicates(subset=['sku'])
df_product = df_product.merge(df_consolidado, on='sku', how='left')


def extrair_all_capacidade(texto):
    if texto is None:
        return None
    capacidades_encontradas = re.findall(r'(\d{2,}\s*GB)', texto, re.IGNORECASE)
    capacidades_validas = []
    for capacidade in capacidades_encontradas:
        capacidade_numerica = re.search(r'\d+', capacidade)
        if capacidade_numerica and int(capacidade_numerica.group()) >= 32:
            capacidades_validas.append(capacidade.lower().strip().replace(' ', ''))
    return ', '.join(capacidades_validas) if capacidades_validas else None

def extrair_one_capacidade(texto):
    if texto is None:
        return None
    capacidades_encontradas = re.findall(r'(\d{2,}\s*GB)', texto, re.IGNORECASE)
    capacidades_validas = []
    for capacidade in capacidades_encontradas:
        capacidade_numerica = re.search(r'\d+', capacidade)
        if capacidade_numerica and int(capacidade_numerica.group()) >= 24:
            capacidades_validas.append(capacidade.lower().strip().replace(' ', ''))
    return capacidades_validas[0] if capacidades_validas else None

# Aplicar a função ao DataFrame
df_product['capacidade_in_name'] = df_product['product_name'].apply(extrair_all_capacidade)
df_product['capacidade_in_name'] = df_product['capacidade_in_name'].apply(extrair_one_capacidade)
df_product['capacidade'] = df_product['capacidade'].astype(str)
df_product['capacidade'] = df_product['capacidade'].apply(extrair_one_capacidade)
df_product['capacidade'] = df_product['capacidade'].fillna(df_product['capacidade_in_name'])
df_product = df_product.rename(columns={'capacidade': 'capacidade_armazenamento', 'modelo':'no_modelo'})
df_product.drop(columns=['capacidade_in_name'], inplace=True)

# Upload to bucket

# Formatar a data e hora no formato desejado
pasta = "extracoes/"
loja = "submarino"
agora = datetime.datetime.now()
nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

gcs_manager.upload_parquet(df_product, nome_arquivo)
log.info("Fim")