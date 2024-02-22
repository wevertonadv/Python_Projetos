import requests
import re
import json
import pandas as pd
import time
from datetime import datetime
from bs4 import BeautifulSoup
from modules.logger_control import Logger, __main__, os

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))


headers = {
    'authority': 'www.girafa.com.br',
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    # 'cookie': 'PHPSESSID=4ltc1pm0t887shntcmv9g37e6n; girafaModalNews=true; _vitrioGa=GA1.3.663592848.1702466594; _vitrioGa_gid=GA1.3.1324075726.1702466594; saiupagina=true; _gcl_au=1.1.1053803748.1702466595; _ga=GA1.1.663592848.1702466594; user_unic_ac_id=78bdbcc6-a17d-0c77-254b-efc13ba422a8; advcake_trackid=1eee5e99-f171-f903-92d3-c5f39e69b142; FPID=FPID2.3.iuerI4ovh9Z%2FaSPh0Ga6Obqs1xTVIi66wAUnjOC69kA%3D.1702466594; FPAU=1.1.1053803748.1702466595; trafficSource=(direct) / (none); _pk_id.1393843.8de7=0983ca0dd02441a7.1702470303.2.1702479853.1702479853.; aceitou_lgpd=1; _clck=1n7vtyx%7C2%7Cfhj%7C0%7C1442; FPLC=cAha5YNJmSHnDzEZPw%2BWxncqOErALuGxNvcV%2FJiq2iRWM8lthmOuYDwWQ8oAS1N5byMe2%2BPk880joYJ6ZBz9kI9BMt9auaQQ%2BJwcx5xDfAAHCbDVrs1lnf56W8lk3Q%3D%3D; qtdeCarrinho=0; ultimoCepInformado=01021-100; _ga_1234=GS1.1.1702578209.7.1.1702578573.0.0.0; FPGSID=1.1702578169.1702578533.G-FT2ZQHSRBT.7o3B47h9eiZytFWH5XeSHg; _ga_FT2ZQHSRBT=GS1.1.1702578209.7.1.1702578574.0.0.0; _gat=1; _ga_84MRBDRRMP=GS1.1.1702578209.12.1.1702578574.0.0.0; _ga_B0521B3853=GS1.1.1702578209.7.1.1702578574.0.0.0; _clsk=1ml5gip%7C1702578574747%7C6%7C1%7Cv.clarity.ms%2Fcollect',
    'origin': 'https://www.girafa.com.br',
    'referer': 'https://www.girafa.com.br/p/aparelho-de-pressao-de-pulso-com-bluetooth-omron-connect-hem-6232t.htm',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'x-requested-with': 'XMLHttpRequest',
}




######Função para extrair os links resultantes do resultado da pesquisa
def get_url(keyword:str, headers: dict):
    url = 'https://www.girafa.com.br/busca/?q=' + keyword

    site=requests.get(url, headers=headers)

    soup=BeautifulSoup(site.text,'html.parser')

    links=[]

    for x in soup.find_all("div",attrs={"class":"bloco-produto"}):
        links.append('https://www.girafa.com.br'+x.find("a").get("href"))

    if len(links)==0:
        log.info('Nenhum resultado')
    else:
        links.sort()
        return links

######Função para calcular o frete por meio de api
def cal_frete(cep:str,id_produto:str,valor:float,headers: dict):

    data = {
        'postalCodeID1': cep,
        'idproduto': id_produto,
        'valor': valor,
        'zoomarketplace_produto_id': '',
    }

    response = requests.post('https://www.girafa.com.br/produto/calcularFrete/', headers=headers, data=data)

    if 'Gr\\u00e1tis' in response.text:
        return 0
    elif 'u00e1 incluso' in response.text:
        return 0
    else:
        vlr=[]
        for vl in re.findall('>.+?R\$ (.+?)<',response.text):
            vlr.append(float(vl.replace(',','.')))
        return min(vlr)



####Função para extração das informações sobre o produto
def get_products(url: str, headers: dict,cep:str):
    log.info(url)
    tempo_inicio=datetime.now()
    time.sleep(0)

    request=requests.get(url, headers=headers)

    ######Extração através de json
    for x in re.findall(r"\{[^{}]*\}",request.text):
        if "setProduct" in x:
            d_json=json.loads(x)

    sku=d_json['product_sku']
    nome_produto=d_json['product_name']
    preco_pix=float(d_json['product_price'])



    #######Extração de tabelas dentro do html da página antravés do pandas

    tab = pd.concat([df for df in pd.read_html(request.text)])

    try:
        fabricante = tab[tab[0].str.contains('marca',case=False,na=False)].iloc[0,1]
    except:
        fabricante =None

    try:
        ean = tab[tab[0].str.contains('EAN',case=False,na=False)].iloc[0,1]
    except:
        ean = None

    try:
        no_modelo = tab[tab[0].str.contains('referência|referencia',case=False,na=False)].iloc[0, 1]
    except:
        no_modelo = None

    try:
        nome_comercial=tab[tab[0].str.contains('Modelo',case=False,na=False)].iloc[0,1]
    except:
        nome_comercial=None

    try:
        capacidade_armazenamento=tab[tab[0].str.contains('memória interna',case=False,na=False)].iloc[0,1]
    except:
        capacidade_armazenamento=None

    try:
        cor=tab[tab[0].str.contains('Cor',case=False,na=False)].iloc[0,1]
    except:
        cor=None

    tb_sem_juros = tab[tab[1].str.contains('sem juros',case=False,na=False)]
    qtd_parcelas_sem_juros_cartao_normal = len(tb_sem_juros)
    preco_x1_cartao = float(re.search(r"\d{1,3}(\.\d{3})*,\d{2}", tb_sem_juros.iloc[0, 1]).group().
                            replace('.', '').replace(',', '.'))

    tb_com_juros = tab[tab[1].str.contains('com juros',case=False,na=False)]

    qtd_parcelas_com_juros_cartao_normal=qtd_parcelas_sem_juros_cartao_normal+len(tb_com_juros)

    valor=tb_com_juros.iloc[len(tb_com_juros)-1,1]
    preco_prazo_com_juros_cartao_normal=float(re.search(r"\d{1,3}(\.\d{3})*,\d{2}", valor).group().
                                         replace('.', '').replace(',', '.'))

    taxa_juros_cartao_normal=float(re.search(r' (\d+\.\d+)%',valor).group().
                                   replace('%',''))


    ######Extração de Tags dentro do HTML

    site=BeautifulSoup(request.text,'html.parser')

    try:
        vendedor=site.find_all("span",attrs={"style":"padding-top: 10px;display: block;"})[0].get_text().replace('Vendido e Entregue por ','')
    except:
        vendedor ='Girafas USA'

    try:
        site.find_all("strong", attrs={"class":"me-avise-texto"})[0].get_text()
        estoque=False
    except:
        estoque=True


    frete_minimo=cal_frete(cep,sku,preco_x1_cartao,headers)

    data_extracao=datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    dic={
        'fabricante':fabricante,
        'marca':fabricante,
        'ean':ean,
        'sku':sku,
        'no_modelo':no_modelo,
        'nome_comercial':nome_comercial,
        'capacidade_armazenamento':capacidade_armazenamento,
        'cor':cor,
        'nome_produto':nome_produto,
        'empresa':None,
        'vendedor':vendedor,
        'preco_pix':preco_pix,
        'preco_boleto':None,
        'preco_x1_cartao':preco_x1_cartao,
        'preco_prazo_sem_juros_cartao_normal':preco_x1_cartao,
        'qtd_parcelas_sem_juros_cartao_normal':qtd_parcelas_sem_juros_cartao_normal,
        'preco_prazo_com_juros_cartao_normal':preco_prazo_com_juros_cartao_normal,
        'qtd_parcelas_com_juros_cartao_normal':qtd_parcelas_com_juros_cartao_normal,
        'taxa_juros_cartao_normal':taxa_juros_cartao_normal,
        'preco_x1_cartao_proprio':None,
        'preco_prazo_sem_juros_cartao_proprio':None,
        'qtd_parcelas_sem_juros_cartao_proprio':None,
        'preco_prazo_com_juros_cartao_proprio':None,
        'qtd_parcelas_com_juros_cartao_proprio':None,
        'taxa_juros_cartao_proprio':None,
        'frete_minimo':frete_minimo,
        'estoque':estoque,
        'url':url,
        'data_extracao':data_extracao}

    tempo_fim = datetime.now()
    log.info(tempo_fim-tempo_inicio)
    return pd.DataFrame([dic])



########CEP padrão para cáculo de frete
cep='01021-100'

####Extrai as url resultantes da pesquisa por palavra chave
keyword=['celular','caixa de som','fone de ouvido'][0]


links=get_url(keyword,headers)

###Extrai os dados dos produtos

df=pd.DataFrame()
l=len(links)

for i in range(0,l):
    log.info(str(i+1)+' de '+str(l))
    d=get_products(links[i],headers,cep)
    df = pd.concat([df,d], ignore_index=True)

####Salva o dataframe em um arquivo parquet
#df.to_parquet('dados_extracao.parquet', engine='pyarrow', compression='snappy')


df.to_excel('dados_extracao.xlsx', index=False)
