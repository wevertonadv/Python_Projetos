import requests
import base64
import json
import time
import pandas as pd
from datetime import datetime
from re import findall, DOTALL
from modules.logger_control import Logger, __main__, os

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))

#######Função para extração dos links
def get_names(keyword: str, headers: dict):
    variable_object = {
        'productOriginVtex': False,
        'simulationBehavior': 'default',
        'hideUnavailableItems': False,
        'fullText': keyword,
        'count': 99,
        'shippingOptions': [],
        'variant': None}

    variable_json = json.dumps(variable_object)

    variable_base64 = base64.b64encode(variable_json.encode())

    extensions = {
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "c6f3f04750f6176e275d0fe4baaaf295f9be9c7d6ee9b4bdee061d6bb4930fcb",
            "sender": "vtex.store-resources@0.x",
            "provider": "vtex.search-graphql@0.x"
        },
        "variables": variable_base64.decode()
    }

    params = {'extensions': json.dumps(extensions)}

    request_1 = requests.get('https://www.motorola.com.br/_v/segment/graphql/v1', params=params)

    s_json_1 = json.loads(request_1.text)

    if s_json_1['data']['productSuggestions']['count'] > 0:
        f_json=s_json_1['data']['productSuggestions']['products']

    else:
        variable_object = {
            'hideUnavailableItems': False,
            'skusFilter': 'ALL_AVAILABLE',
            'simulationBehavior': 'default',
            'installmentCriteria': 'MAX_WITHOUT_INTEREST',
            'productOriginVtex': False,
            'map': 'c',
            # 'query': keyword,#####'smartphones',
            'orderBy': 'OrderByScoreDESC',
            'from': 0,
            'to': 99,
            'selectedFacets': [{'key': 'c', 'value': 'smartphones'}],
            'operator': 'and',
            'fuzzy': '0',
            'searchState': None,
            'facetsBehavior': 'Static',
            'categoryTreeBehavior': 'default',
            'withFacets': False,
            'variant': ''}

        variable_json = json.dumps(variable_object)

        variable_base64 = base64.b64encode(variable_json.encode())

        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "40b843ca1f7934d20d05d334916220a0c2cae3833d9f17bcb79cdd2185adceac",
                "sender": "vtex.store-resources@0.x",
                "provider": "vtex.search-graphql@0.x"
            },
            "variables": variable_base64.decode()
        }

        extensions_json = json.dumps(extensions)
        params = {'extensions': json.dumps(extensions)}

        request_2=requests.get('https://www.motorola.com.br/_v/segment/graphql/v1', params=params)
        s_json_2 = json.loads(request_2.text)
        f_json=s_json_2['data']['productSearch']['products']

    links=[]

    for j in f_json:
        links.append(j['link'].replace('/p','').replace('/',''))

    links.sort()
    return links


######Função para extração dos dados do produto
def get_products(keyword: str, headers: dict):

    tempo_inicio = datetime.now()
    url = 'https://www.motorola.com.br/' + keyword + '/p'
    log.info(url)

    time.sleep(0)
    response = requests.get(url, headers=headers)

    s_json = findall('template data-type="json" data-varname="__STATE__">(.+?)</script>',
                     response.text.replace('\n', '').replace('<script>', ''),
                     DOTALL)[0]

    d_json = json.loads(s_json)

    fabricante = 'Motorola'
    marca = 'Motorola'
    ean=d_json['Product:'+keyword+'.items.0']['ean']
    sku = d_json['Product:' + keyword + '.items.0']['itemId']
    no_modelo = d_json['Product:' + keyword]['productReference']
    if no_modelo==ean:
        no_modelo=None

    nome_comercial = d_json['Product:' + keyword]['productName']

    nome_produto = d_json['Product:' + keyword + '.items.0']['nameComplete']
    empresa = 'Motorola'
    vendedor = 'Motorola'

    preco_prazo_com_juros_cartao_normal = None
    qtd_parcelas_com_juros_cartao_normal = None
    taxa_juros_cartao_normal = None
    preco_x1_cartao_proprio = None
    preco_prazo_sem_juros_cartao_proprio = None
    qtd_parcelas_sem_juros_cartao_proprio = None
    qtd_parcelas_sem_juros_cartao_proprio = None
    preco_prazo_com_juros_cartao_proprio = None
    qtd_parcelas_com_juros_cartao_proprio = None
    taxa_juros_cartao_proprio = None
    frete_minimo = None
    estoque = True
    url = url
    data_extracao = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    try:
        capacidade_armazenamento = findall(r"Armazenamento Total: (\d+ GB)", str(d_json))[0]
    except:
        capacidade_armazenamento = None

    try:
        # cor=d_json['Product:'+keyword+'.properties.5']['values']['json']
        # cor=d_json['Product:'+keyword+'.specificationGroups.0.specifications.1']['values']['json']
        if d_json['Product:' + keyword + '.specificationGroups.0.specifications.1']['name'] == 'Cor':
            cor = cor = d_json['Product:' + keyword + '.specificationGroups.0.specifications.1']['values']['json']
        else:
            cor = d_json['Product:' + keyword + '.items.0.variations.0']['values']['json']
    except:
        cor = None

    try:
        preco_pix = float(d_json['$Product:' + keyword + '.items.0.sellers.0.commertialOffer']['spotPrice'])
        preco_boleto = float(d_json['$Product:' + keyword + '.items.0.sellers.0.commertialOffer']['spotPrice'])
        preco_x1_cartao = float(d_json['$Product:' + keyword + '.items.0.sellers.0.commertialOffer']['spotPrice'])
        preco_prazo_sem_juros_cartao_normal = float(
            d_json['$Product:' + keyword + '.items.0.sellers.0.commertialOffer.Installments.0'][
                'TotalValuePlusInterestRate'])
        qtd_parcelas_sem_juros_cartao_normal = max(
            list(map(int, findall(r"\'NumberOfInstallments\': (\d+)", str(d_json)))))

    except:
        preco_pix = None
        preco_boleto = None
        preco_x1_cartao = None
        preco_prazo_sem_juros_cartao_normal = None
        qtd_parcelas_sem_juros_cartao_normal = None
        estoque = False

    dic = {
        'fabricante': fabricante,
        'marca': marca,
        'ean': ean,
        'sku': sku,
        'no_modelo': no_modelo,
        'nome_comercial': nome_comercial,
        'capacidade_armazenamento': capacidade_armazenamento,
        'cor': cor,
        'nome_produto': nome_produto,
        'empresa': empresa,
        'vendedor': vendedor,
        'preco_pix': preco_pix,
        'preco_boleto': preco_boleto,
        'preco_x1_cartao': preco_x1_cartao,
        'preco_prazo_sem_juros_cartao_normal': preco_prazo_sem_juros_cartao_normal,
        'qtd_parcelas_sem_juros_cartao_normal': qtd_parcelas_sem_juros_cartao_normal,
        'preco_prazo_com_juros_cartao_normal': preco_prazo_com_juros_cartao_normal,
        'qtd_parcelas_com_juros_cartao_normal': qtd_parcelas_com_juros_cartao_normal,
        'taxa_juros_cartao_normal': taxa_juros_cartao_normal,
        'preco_x1_cartao_proprio': preco_x1_cartao_proprio,
        'preco_prazo_sem_juros_cartao_proprio': preco_prazo_sem_juros_cartao_proprio,
        'qtd_parcelas_sem_juros_cartao_proprio': qtd_parcelas_sem_juros_cartao_proprio,
        'preco_prazo_com_juros_cartao_proprio': preco_prazo_com_juros_cartao_proprio,
        'qtd_parcelas_com_juros_cartao_proprio': qtd_parcelas_com_juros_cartao_proprio,
        'taxa_juros_cartao_proprio': taxa_juros_cartao_proprio,
        'frete_minimo': frete_minimo,
        'estoque': estoque,
        'url': url,
        'data_extracao': data_extracao
    }

    tempo_fim=datetime.now()
    log.info(tempo_fim - tempo_inicio)
    return pd.DataFrame(dic)



####Cabeçalho para a requisição da página
headers = {
    'authority': 'www.motorola.com.br',
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9',
    'content-type': 'application/json',
    'referer': 'https://www.motorola.com.br',
    'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
}


####Palavras para pesquisar
keyword=['caixa de som','fone de ouvido','relogio','carregador','tablet','fone ouvido','moto g73','celular'][7]





####Extrai o nome dos produtos dentro das urls resultantes da pesquisa por palavra chave
names=get_names(keyword,headers)



#####Extrai as informações sobre os produtos provenientes das url e almazena em um dataframe
l=len(names)

df=pd.DataFrame()

for i in range(0,l):
    log.info(str(i+1)+' de '+str(l))
    try:
        d=get_products(names[i],headers)
        df = pd.concat([df,d], ignore_index=True)
    except:
        log.error('redirecionado')

####Salva o dataframe em um arquivo parquet
df.to_parquet('dados_extracao.parquet', engine='pyarrow', compression='snappy')
#df.to_excel('dados_extracao.xlsx')
log.info("Fim")

