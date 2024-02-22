from http.client import HTTPSConnection
from ssl import create_default_context
from random import shuffle, randint
from datetime import datetime
from re import findall, DOTALL
from json import loads
from time import sleep
from unidecode import unidecode
from pandas import DataFrame
from modules.string_normalizer import normalize_string
from modules.xlsx import get_names
from modules.verify_names import combined_similarity
from modules.product import create_product
import os
from modules.crud_bucket_gcp import GCSParquetManager

from modules.logger_control import Logger, __main__

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))

###Headers para uso no request
HEADERS = {
    'authority': 'www.girafa.com.br', ### Nome do site
    'accept':
        'text/html,application/xhtml+xml,application/xml;' +
        'q=0.9,image/webp,image/apng,*/*;' +
        'q=0.8,application/signed-exchange;' +
        'v=b3;' +
        'q=0.7', ### Tipo de conteúdo aceito pelo cliente
    'accept-language': 'en-US,en;q=0.9', ### Linguagem de preferência
    'cache-control': 'no-cache', ### Controle de cache
    'pragma': 'no-cache', ### Controle de cache
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"', ### Tipo de dispositivo
    'sec-ch-ua-mobile': '?0',### Tipo de dispositivo móvel
    'sec-ch-ua-platform': '"Windows"', ### Plataforma do dispositivo
    'sec-fetch-dest': 'document', ### Tipo de requisição
    'sec-fetch-mode': 'navigate', ### Modo de requisição
    'sec-fetch-site': 'none', ### Site de origem
    'sec-fetch-user': '?1', ### Usuário
    'upgrade-insecure-requests': '1', ### Upgrade de segurança
    'user-agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/120.0.0.0 ' +
        'Safari/537.36 ' +
        'Edg/120.0.0.0'### Informações do navegador
}




HOST_WEBPAGE = 'www.girafa.com.br'  ### Nome do host da página web
MAX_ATTEMPTS = 3  ### Número máximo de tentativas de conexão
SLEEP_TIME = 3  ### Tempo de espera entre as tentativas de conexão
ZIPCODE = '01021-100'  ### CEP do endereço de entrega


def _get_date() -> str:
    '''
    Return current date and time as string.
    '''
    return datetime.now().strftime("%d-%m-%Y %H:%M") ### Retorna a data e hora atual como uma string


def _parse_dictionary(data: str) -> dict | None:
    log.info('Parsing data to dictionary...') ### Imprime uma mensagem de informação
    try:
        d_data = loads(data) ### Converte a string JSON em um dicionário
    except Exception as err:
        log.error('Unable to parse data to dictionary due to ' + str(err)) ### Imprime uma mensagem de erro
        return None ### Retorna vazio caso ocorra erro

    return d_data ### Retorna o dicionário


def _send_post_request(headers: dict, host: str, path_and_params: str, data: str) -> (int | None, str | None):
    """
    Send "POST" request
    :param headers: Headers send with request
    :param host: Host of the request, dont use suffix "http://" or "https://"
    :param path_and_params: ONLY path (/something) and params (?something&something)
    :param data: Body of the post
    :return: Request status code and response data, or, None and None if something goes wrong
    """
    log.info(f'Sending post request to "{host + path_and_params}"...') ### Imprime uma mensagem de informação

    l_headers = [ ### Lista de headers
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.3'
    ]

    headers['user-agent'] = l_headers[randint(0, len(l_headers) - 1)]  ### Define um header aleatório

    try:
        hc = HTTPSConnection( ### Cria uma conexão HTTPS
            host=host, ### Host da conexão
            context=create_default_context() ### Contexto padrão
        )
        hc.request(  ### Faz a requisição
            method='POST',  ### Método da requisição
            url=path_and_params,  ### URL da requisição
            headers=headers,  ### Headers da requisição
            body=data  ### Corpo da requisição
        )
        hr = hc.getresponse()  ### Recebe a resposta
        i_response_code = hr.status  ### Código de status da resposta
        s_response_data = hr.read().decode('utf-8')  ### Dados da resposta
        sleep(SLEEP_TIME)  ### Espera por um tempo
    except Exception as err: ### Trata exceções
        log.error('Unable to send post request due to '+str(err)) ###Imprime a mensagem de erro
        return None, None ### Retorna vazio caso ocorra erro

    return i_response_code, s_response_data ### Retorna o código de status e os dados da resposta


def _send_request(headers: dict, host: str, path_and_params: str) -> (int | None, str | None):
    """
    Send "GET" request
    :param headers: Headers send with request
    :param host: Host of the request, dont use suffix "http://" or "https://"
    :param path_and_params: ONLY path (/something) and params (?something&something)
    :return: Request status code and response data, or, None and None if something goes wrong
    """
    log.info(f'Sending request to "{host + path_and_params}"...') ### Imprime uma mensagem de informação

    l_headers = [  ### Lista de headers
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.3'
    ]

    headers['user-agent'] = l_headers[randint(0, len(l_headers) - 1)] ### Define um header aleatório

    try:
        hc = HTTPSConnection(  ### Cria uma conexão HTTPS
            host=host,  ### Host da conexão
            context=create_default_context()  ### Contexto padrão
        )
        hc.request(  ### Faz a requisição
            method='GET',  ### Método da requisição
            url=path_and_params,  ### URL da requisição
            headers=headers  ### Headers da requisição
        )
        hr = hc.getresponse()  ### Recebe a resposta
        i_response_code = hr.status  ### Código de status da resposta
        s_response_data = hr.read().decode('utf-8')  ### Dados da resposta
        sleep(SLEEP_TIME)  ### Espera por um tempo
    except Exception as err:
        log.error('Unable to send request due to' + str(err)) ### Imprime a mensagem de erro
        return None, None ### Retorna vazio caso ocorra erro

    return i_response_code, s_response_data ### Retorna o código de status e os dados da resposta


def _get_results(keywords: list[str]) -> list[str] | None:
    """
    Retrieve results from search
    :param keywords: Products names
    :return: Product link list
    """
    d_results = {} ### Dicionário para armazenar os resultados

    if keywords is None:  ### Verifica se a lista de palavras-chave é nula
        log.error('Keyword list is None.')
        return None

    if len(keywords) == 0:  ### Verifica se a lista de palavras-chave está vazia
        log.error('Keyword list is empty.')
        return None

    shuffle(keywords)  ### Embaralha a lista de palavras-chave

    for i_keyword_index in range(0, len(keywords), 1):  ### Itera sobre a lista de palavras-chave
        s_keyword = keywords[i_keyword_index]  ### Seleciona a palavra-chave atual

        log.info(f'({i_keyword_index + 1} of {len(keywords)}) Retrieving results for keyword: '+s_keyword)

        s_path = '/busca/'  ### Define o caminho da requisição
        s_params = f'?q=' + normalize_string(s_keyword, '+')  ### Define os parâmetros da requisição
        i_attempt_counter = 1  ### Contador de tentativas
        while True:  ### Loop de tentativas
            i_response_code, s_response_data = _send_request(HEADERS, HOST_WEBPAGE, s_path + s_params)  ### Envia a requisição

            if i_response_code is not None: ### Verifica se o código de resposta não é nulo
                match i_response_code: ### Verifica o código de resposta
                    case 200: ### Código 200 indica sucesso
                        break ### Sai do loop
                    case 302: ### Código 302 indica redirecionamento
                        log.warning('Server returned redirect to product page, skipping...')
                        i_response_code = None
                        s_response_data = None
                        break ### Sai do loop
                    case _:
                        log.warning('Server returned unknown status code' + str(i_response_code))

            if i_attempt_counter >= MAX_ATTEMPTS: ### Verifica se o número máximo de tentativas foi atingido
                log.error('Max retries exceeded.')
                i_response_code = None
                s_response_data = None
                break

            log.error('Request failed, skipping...')
            i_attempt_counter += 1  ### Incrementa o contador de tentativas

        if i_response_code is None and s_response_data is None: ### Verifica se os dados da resposta são nulos
            log.error('Unable retrieve HTML, skipping...')
            continue

        l_85hhu3h = findall(  ##Extrai o arquivo Json do HTML
            pattern=r'<script type="text/javascript">.+?let itens = (.+?]);.+?</script>',
            string=s_response_data,
            flags=DOTALL
        )

        if len(l_85hhu3h) == 0 or len(l_85hhu3h) > 1: ### Imprime mensagens de resultados
            if len(l_85hhu3h) == 0:
                log.error('RegEx did not found necessary JSON in HTML, skipping...')

            if len(l_85hhu3h) > 1:
                log.error('RegEx found many JSON in HTML, skipping...')
            continue

        l_raw_products = _parse_dictionary(l_85hhu3h[0]) ### Converte os dados extraídos em um dicionário
        if l_raw_products is None: ### Verifica se o dicionário é nulo
            continue

        for d_raw_product in l_raw_products:  ### Itera sobre o dicionário
            s_name = d_raw_product.get('titulo')  ### Seleciona o título do produto
            s_link = d_raw_product.get('url')  ### Seleciona o link do produto
            s_id = d_raw_product.get('id')  ### Seleciona o ID do produto

            if s_name is None:  ### Verifica se o título é nulo
                continue  ### Pula para o próximo produto

            i_similarity_percentage = combined_similarity(s_keyword, s_name) * 100  ### Calcula a porcentagem de similaridade entre a palavra pequisada e os resultados
            if i_similarity_percentage >= 50:
                d_results.update(  ### Adiciona o resultado ao dicionário
                    {
                        s_id: s_link
                    }
                )

    l_links = list(d_results.values())  ### Converte os valores do dicionário em uma lista

    if l_links == 0:  ### Verifica se a lista de links
        log.error('Link list is empty.')

    return l_links ### Retorna uma lista de links resultantes da pesquisa


def _get_products(links: list[str]) -> list[dict] | None:
    """
    Retrieve products data
    :param links: Product path list
    :return: Products data
    """
    l_tmp_products = []  # Lista temporária para armazenar os produtos

    if links is None: # Verifica se a lista de links é nula e imprime mensagem de erro
        log.error('Link list is None.')
        return None

    if len(links) == 0: # Verifica se a lista de links está vazia e imprime mensagem de erro
        log.error('Link list is empty.')
        return None

    shuffle(links) # Embaralha a lista de links

    for i_link_index in range(0, len(links), 1): # Itera cada link para extrair os dados da página
        s_link = links[i_link_index]

        log.info(f'({i_link_index + 1} of {len(links)}) Retrieving product for link "{HOST_WEBPAGE + s_link}"') ##Imprime o progresso

        s_path = f'/p/{s_link}.htm'
        i_attempt_counter = 1
        while True:
            i_response_code, s_response_data = _send_request(HEADERS, HOST_WEBPAGE, s_path) ##Envia requisições até obter resposta

            if i_response_code is not None: ##Verifica se houve resposta da requisição
                match i_response_code:
                    case 200:
                        break
                    case _:
                        log.warning('Server returned unknown status code '+ str(i_response_code)) ##Imprime que não houve resposta

            if i_attempt_counter >= MAX_ATTEMPTS: ##Interrompe caso atinja o número máximo de iterações
                log.error('Max retries exceeded.') ##Imprime que as requisições foram excedidas
                i_response_code = None
                s_response_data = None
                break

            log.warning('Request failed, trying again...') ##Imprime que a tentativa falhou
            i_attempt_counter += 1 ##Acrescenta um incremento unitário

        if i_response_code is None and s_response_data is None:
            log.error('Unable retrieve HTML.') ##Imprime que não encontrou html
            return None  ## Retorna vazio para função


        s_brand = None  ##Cria as variáveis da extração
        s_ean = None
        s_identification = None
        s_universal_identification = None
        s_name = None
        s_universal_name = None
        s_storage = None
        s_color = None
        s_marketplace = 'Girafa'
        s_store = None
        f_shipping = None
        b_available = None
        s_url = 'https://' + HOST_WEBPAGE + s_path

        f_price_pix = None
        f_price_ticket = None
        f_price_credit = None
        f_price_m_i_n_f = None
        i_amount_m_i_n_f = None
        f_price_m_i_w_f = None
        i_amount_m_i_w_f = None
        f_percentage_f_m_i = None

        log.info('Finding PIX price...')
        l_ir39ir0 = findall( ##localiza o preço à vista no pix por RegEx
            pattern=r'<span[ ]*class="desconto-produto">.*?R\$[ ]*([0-9|\.|,]+).*?</span>',
            string=s_response_data,
            flags=DOTALL
        )
        if len(l_ir39ir0) == 1:
            f_price_pix = l_ir39ir0[0].replace('.', '').replace(',', '.')
        else:
            if len(l_ir39ir0) == 0:
                log.error('RegEx did not found PIX price.')

            if len(l_ir39ir0) > 1:
                log.error('RegEx found many PIX prices.')

        log.info('Finding availability...')
        l_f43t5i9 = findall( ##Localiza a disponibilidade do produto
            pattern='<meta[ ]*itemprop="availability"[ ]*content="(.+?)"[ ]*/>',
            string=s_response_data
        )
        if len(l_f43t5i9) == 1:
            if 'instock' in l_f43t5i9[0].lower():
                b_available = True
            elif 'outofstock' in l_f43t5i9[0].lower():
                b_available = False
        else:
            if len(l_f43t5i9) == 0:
                log.error('RegEx did not found availability.')

            if len(l_f43t5i9) > 1:
                log.error('RegEx found many availabilities.')

        log.info('Finding seller...')
        l_fu18t56 = findall( ##Localiza o vendedor
            pattern='<div[ ]*itemprop="seller"[ ]*itemtype=".+?"[ ]*itemscope>(.+?)</div>',
            string=s_response_data,
            flags=DOTALL
        )
        if len(l_fu18t56) == 1:
            l_rbc6674g = findall(
                pattern='<meta[ ]*itemprop="name"[ ]*content="(.+?)"[ ]*/>',
                string=l_fu18t56[0],
            )
            s_store = l_rbc6674g[0]
        else:
            if len(l_fu18t56) == 0:
                log.error('RegEx did not found seller.')

            if len(l_fu18t56) > 1:
                log.error('RegEx found many sellers.')

        log.info('Finding product name...')
        l_f52h3q0 = findall( ##Localiza o tipo do produto
            pattern='<p[ ]*id="titulo-produto">(.+?)</p>',
            string=s_response_data
        )
        if len(l_f52h3q0) == 1:
            s_name = l_f52h3q0[0]
        else:
            if len(l_f52h3q0) == 0:
                log.error('RegEx did not found product name, skipping...')

            if len(l_f52h3q0) > 1:
                log.error('RegEx found many product names, skipping...')
            continue

        log.info('Finding product SKU...')
        l_yhf5hg9 = findall( ##Localiza o SKU do produto
            pattern='<meta[ ]*itemprop="sku"[ ]*content="(.+?)"[ ]*/>',
            string=s_response_data,
        )
        if len(l_yhf5hg9) == 1:
            s_identification = l_yhf5hg9[0]
        else:
            if len(l_yhf5hg9) == 0:
                log.error('RegEx did not found SKU.')

            if len(l_yhf5hg9) > 1:
                log.error('RegEx found many SKUs.')

        log.info('Finding product prices and characteristics...')
        l_f43h8i0 = findall(
            pattern='<tr>(.+?)</tr>',
            string=s_response_data,
            flags=DOTALL
        )
        if len(l_f43h8i0) > 0:
            for s_f43h8i0 in l_f43h8i0:
                l_8f943uj = findall(
                    pattern=(  ##Localiza o preço e o número de parcelas com juros e sem juros
                        r'<td[ ]*style=".+?">([0-9]+)x.+?R\$ [0-9|\.|,]+.+?</td>.*?' +
                        r'<td[ ]*style=".+?">.*?R\$ ([0-9|\.|,]+).*?' +
                        r'<p[ ]*class=".+?">(...)[^0-9]+([0-9|\.]*).*?</p>.*?' +
                        r'</td>'
                    ),
                    string=s_f43h8i0,
                    flags=DOTALL
                )
                for t_8f943uj in l_8f943uj:

                    if 'sem' in t_8f943uj[2].lower():  ##Localiza o preço e o número de parcelas sem juros
                        if '1' == t_8f943uj[0]:
                            f_price_credit = t_8f943uj[1].replace('.', '').replace(',', '.')
                        f_price_m_i_n_f = t_8f943uj[1].replace('.', '').replace(',', '.')
                        i_amount_m_i_n_f = t_8f943uj[0]

                    if 'com' in t_8f943uj[2].lower():  ##Localiza o preço e o número de parcelas com juros
                        f_price_m_i_w_f = t_8f943uj[1].replace('.', '').replace(',', '.')
                        i_amount_m_i_w_f = t_8f943uj[0]
                        f_percentage_f_m_i = t_8f943uj[3]

                l_f42fhh4 = findall(
                    pattern=( ##Localiza a tabela que contém as especificações do produto
                        r'<td[ ]*class="especificacao-titulo">(.+?)</td>.*?' +
                        r'<td[ ]*class=".*?">(.+?)</td>'
                    ),
                    string=unidecode(s_f43h8i0),
                    flags=DOTALL
                )
                for i in l_f42fhh4:
                    s_specification = i[0].replace('\n', '').strip().lower().replace(' ', '')
                    s_value = i[1].replace('\n', '').strip().lower()

                    if 'marca' in s_specification:
                        s_brand = s_value ##Localiza a marca
                    elif 'modelo' in s_specification:
                        s_universal_name = s_value  ##Localiza o nome comercial
                    elif 'ean' in s_specification:
                        s_ean = s_value  ##Localiza o EAN
                    elif 'cor' in s_specification:
                        s_color = s_value  ##Localiza a cor
                    elif 'referencia' in s_specification:
                        s_universal_identification = s_value  ##Localiza o código do modelo
                    elif 'memoriainterna' in s_specification:
                        s_storage = s_value  ##Localiza os dados de armazenamento
        else:
            log.error('RegEx did not found prices data.')

        if s_identification is not None and s_identification != '': ##Caso o sku não seja nulo é feito uma requisição para o cálculo do frete
            log.info('Getting delivery price...')
            s_path = '/produto/calcularFrete/'
            s_data = f'postalCodeID1={ZIPCODE}&idproduto={s_identification}&valor=99%2C90&zoomarketplace_produto_id='
            i_attempt_counter = 1
            while True: ##Envia requisições até obter resposta
                i_response_code, s_response_data = _send_post_request(HEADERS, HOST_WEBPAGE, s_path, s_data)

                if i_response_code is not None:
                    match i_response_code:
                        case 200:
                            break
                        case _:
                            log.warning('Server returned unknown status code '+ str(i_response_code))

                if i_attempt_counter >= MAX_ATTEMPTS:
                    log.error('Max retries exceeded.')
                    i_response_code = None
                    s_response_data = None
                    break

                log.warning('Request failed, trying again...')
                i_attempt_counter += 1

            if i_response_code is None and s_response_data is None:
                log.error('Unable retrieve delivery HTML.')

            l_f4yed36 = findall(  ##Localiza o valor do frete
                pattern=r'R\$[ ]+([0-9|,]+)',
                string=s_response_data,
                flags=DOTALL
            )
            if len(l_f4yed36) == 0:
                f_shipping = 0
            else:
                f_current_shipping_price = None
                for s_f4yed36 in l_f4yed36:
                    s_f4yed36 = s_f4yed36.replace(',', '.')
                    f_f4yed36 = float(s_f4yed36)
                    if f_current_shipping_price is None:
                        f_current_shipping_price = f_f4yed36
                    if f_f4yed36 < f_current_shipping_price:
                        f_current_shipping_price = f_f4yed36
                f_shipping = f_current_shipping_price

        log.info('Creating product...')
        l_tmp_products.append( ##Concatena os resultados dos dados dos produtos
            create_product(
                manufacturer=None,
                brand=s_brand,
                ean=s_ean,
                identification=s_identification,
                universal_identification=s_universal_identification,
                name=s_name,
                universal_name=s_universal_name,
                storage=s_storage,
                color=s_color,
                marketplace=s_marketplace,
                store=s_store,
                zipcode=ZIPCODE.replace('-', ''),
                shipping=f_shipping,
                available=b_available,
                url=s_url,
                price_pix=f_price_pix,
                price_ticket=f_price_ticket,
                price_credit=f_price_credit,
                price_m_i_n_f=f_price_m_i_n_f,
                amount_m_i_n_f=i_amount_m_i_n_f,
                price_m_i_w_f=f_price_m_i_w_f,
                amount_m_i_w_f=i_amount_m_i_w_f,
                percentage_f_m_i=f_percentage_f_m_i,
                price_credit_cc=None,
                price_cc_m_i_n_f=None,
                amount_cc_m_i_n_f=None,
                price_cc_m_i_w_f=None,
                amount_cc_m_i_w_f=None,
                percentage_cc_f_m_i=None
            )
        )

    if len(l_tmp_products) == 0: ##Retorna vazio caso não houveram extrações para os produtos
        log.error('Product list is empty.')
        return None

    return l_tmp_products ##Retorna a lista com os dados de cada produto


if __name__ == '__main__':
    log.info(f' Starting ({_get_date()}) '.center(70, '-')) ## Imprime uma mensagem de início no console

    # l_keywords = get_names('assets/lista devices.xlsx') ## Lê uma lista de palavras-chave de um arquivo Excel e as armazena em uma lista
    gcs_manager = GCSParquetManager()
    l_keywords = gcs_manager.ler_coluna_excel()
    shuffle(l_keywords) ## Embaralha aleatoriamente a ordem dos elementos na lista de palavras-chave

    l_results = _get_results(l_keywords) ## Usa as palavras-chave para pesquisar resultados das palavras chaves

    l_products = _get_products(l_results) ## Analisa os resultados da pesquisa e extrai informações sobre produtos relevantes

    # DataFrame(l_products).to_excel(excel_writer='_products_girafa.xlsx', index=False, encoding='utf-8-sig') ## Converte a lista de produtos em DataFrame e salva-o em um arquivo Excel
    df = DataFrame(l_products)
    # Upload to bucket

    # Formatar a data e hora no formato desejado
    pasta = "extracoes/"
    loja = "girafa"
    agora = datetime.now()
    nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"
    
    gcs_manager.upload_parquet(df, nome_arquivo)
    
    log.info(f' Done ({_get_date()}) '.center(70, '-'))
