
import json
import os,sys
import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
import requests
import copy
import codecs
from pyquery import PyQuery as pq
import js2xml
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__
from modules.date_helper import get_current_date


class ExtracaoClaro():
    def __init__(self):
        self.log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))
        self.session = self.session_create()
        self.urlCelulares = "https://planoscelular.claro.com.br"
        self.urlAcessorios = "https://loja.claro.com.br/acessorios/api/apigee/list"
        self.Claro_Extracao()

    def session_create(self):
        requests.packages.urllib3.disable_warnings() 
        session = requests.Session()
        session.verify = False
        return session
    def criar_dicionario_padrao(self):
        return {
            "fabricante": None,
            "marca": None,
            "ean": None,
            "sku": None,
            "no_modelo": None,
            "nome_comercial": None,
            "capacidade_armazenamento": None,
            "cor": None,
            "nome_produto": None,
            "empresa": "Claro",
            "vendedor": None,
            "preco_pix": None,
            "preco_boleto": None,
            "preco_x1_cartao": None,
            "preco_prazo_sem_juros_cartao_normal": None,
            "qtd_parcelas_sem_juros_cartao_normal": None,
            "preco_prazo_com_juros_cartao_normal": None,
            "qtd_parcelas_com_juros_cartao_normal": None,
            "taxa_juros_cartao_normal": None,
            "preco_x1_cartao_proprio": None,
            "preco_prazo_sem_juros_cartao_proprio": None,
            "qtd_parcelas_sem_juros_cartao_proprio": None,
            "preco_prazo_com_juros_cartao_proprio": None,
            "qtd_parcelas_com_juros_cartao_proprio": None,
            "taxa_juros_cartao_proprio": None,
            "frete_minimo": None,
            "cep": None,
            "estoque": None,
            "url": None,
            "data_extracao": None
        }
    
    def Claro_Extracao(self):
        df_cel = self.Celulares_Extracao()
        df_ace = self.Acessorios_Extracao()

        df_final = pd.concat([df_cel, df_ace], ignore_index=True)

        # Adicionar data e hora ao nome do arquivo
        # current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        # file_name = f"claro_extracao_{current_time}.csv"

        # df_final.to_csv(file_name, index=False)
        
        # Upload to bucket
        gcs_manager = GCSParquetManager()

        # Formatar a data e hora no formato desejado
        pasta = "extracoes/"
        loja = "claro"
        agora = datetime.now()
        nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"
        
        for coluna in df_final.columns:
            tipo = tipos_desejados.get(coluna, str)  # Usa o tipo do dicionário ou 'str' como padrão
            df_final[coluna] = tratar_coluna(df_final[coluna], tipo)

        
        gcs_manager.upload_parquet(df_final, nome_arquivo)
    
        self.log.info("Fim")
        
    
    def Acessorios_Extracao(self):
        self.log.info("Realizando requisição a API")
        payload = {
            "params": {
                "fields": "FULL",
                "sort": "relevance",
                "currentPage": 0,
                "pageSize": 100, #maximo 100
                "query": ":relevance:category:accessories:inStockFlag:true",
            }
        }
        infos = []
        while True:
            resposta = self.session.post(self.urlAcessorios,json=payload)
            if resposta.status_code != 200:
                self.log.error(f"Falha ao se comunicar com {self.urlAcessorios}, status_code: {resposta.status_code}")
                raise Exception(f"Falha ao se comunicar com {self.urlAcessorios}, status_code: {resposta.status_code}")
            json_resp = json.loads(resposta.text)
            
            if json_resp['products'] == []:
                break
            else:
                payload["params"]["currentPage"]+=1
                infos.extend(json_resp['products'])

        acessorios = []
        # current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        current_time = get_current_date()
        for info in infos:
            produtcCapacity = None
            productBrand = None
            cor = None
            for feature in info['classifications'][0]['features']:
                if str(feature['description']).upper().__contains__("COR"):
                    cor = feature['featureValues'][0]['value']
                elif str(feature['description']).upper().__contains__("FABRICANTE"):
                    productBrand = feature['featureValues'][0]['value']
                elif str(feature['description']).upper().__contains__("CAPACIDAD"):
                    produtcCapacity = feature['featureValues'][0]['value']
            
            dic_infos = self.criar_dicionario_padrao()
            dic_infos["capacidade_armazenamento"] = produtcCapacity
            dic_infos["cor"] = cor
            dic_infos["nome_produto"] = info.get("name")
            dic_infos["fabricante"] = productBrand
            # dic_infos["estoque"] = int(info.get("installment", {}).get("quantity")) if info.get("installment", {}).get("quantity") is not None else None
            dic_infos["estoque"] = bool(info.get("installment", {}).get("quantity")) if info.get("installment", {}).get("quantity") is not None else None
            dic_infos["data_extracao"] = current_time
            dic_infos["qtd_parcelas_sem_juros_cartao_normal"] = 12  # todos são 12 sem juros
            dic_infos["url"] = self.urlCelulares + info.get("url", "")
            # dic_infos["preco_pix"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            # dic_infos["preco_boleto"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            # dic_infos["preco_x1_cartao"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            # dic_infos["preco_prazo_sem_juros_cartao_normal"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            # dic_infos["preco_prazo_com_juros_cartao_normal"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            dic_infos["preco_pix"] = float(info["price"].get("value")/10) if info["price"].get("value") is not None else None
            dic_infos["preco_boleto"] = float(info["price"].get("value")/10) if info["price"].get("value") is not None else None
            dic_infos["preco_x1_cartao"] = float(info["price"].get("value")/10) if info["price"].get("value") is not None else None
            dic_infos["preco_prazo_sem_juros_cartao_normal"] = float(info["price"].get("value")/10) if info["price"].get("value") is not None else None
            dic_infos["preco_prazo_com_juros_cartao_normal"] = float(info["price"].get("value")/10) if info["price"].get("value") is not None else None

            dic_infos["taxa_juros_cartao_normal"] = 0
            dic_infos["frete_minimo"] = 0

            acessorios.append(dic_infos)

        
        df = pd.DataFrame(acessorios)
        return df


    def Celulares_Extracao(self):
        self.log.info("Acessando pagina inicial")
        resposta = self.session.get(self.urlCelulares+'/claro/pt/c/smartphone')
        links = []

        if resposta.status_code != 200:
            self.log.error(f"Falha ao se comunicar com {self.urlCelulares}, status_code: {resposta.status_code}")
            raise Exception(f"Falha ao se comunicar com {self.urlCelulares}, status_code: {resposta.status_code}")
        paginacao = self.extrair_paginacao(resposta.content)[0]
        
        #GET EM TODAS PAGINAS COM TODOS PRODUTOS
        pag = 0
        while not(resposta.text.__contains__('class="pagination-next disabled"')):
            url_atul = self.urlCelulares+paginacao.replace("=1", "="+str(pag))
            pag=pag+1
            self.log.info(f"Acessando pagina {url_atul}")
            resposta = self.session.get(url_atul)
            if resposta.status_code != 200:
                self.log.error(f"Falha ao se comunicar com {url_atul}, status_code: {resposta.status_code}")
                raise Exception(f"Falha ao se comunicar com {url_atul}, status_code: {resposta.status_code}")
            links.extend(self.Celulares_links(resposta.content)) 


        #GET EM TODOS PRODUTOS ENCONTRADOS
        infos = []
        novosLinks = []
        for link in links:
            self.log.info(f"Acessando {link}")
            info, novoLink = self.extrair_informacoes_produto(link)
            if info:
                infos.append(info)
                novosLinks.extend(novoLink)
                self.log.info("Coletado")
            else:
                self.log.info("Esgotado")
        
        # PROCESSAR VARIACOES
        self.log.info("Verificar se houve variacao não mapeada")
        for link in novosLinks:
            novo_link_completo = self.urlCelulares + link
            if novo_link_completo not in links:
                self.log.info(f"Acessando {novo_link_completo}")
            else:
                continue
            info = self.extrair_informacoes_produto(novo_link_completo, True)
            if info:
                infos.append(info)
                self.log.info("Coletado")
            else:
                self.log.info("Esgotado")
        
        self.log.info("resultado final")
        df = pd.DataFrame(infos)

        return df

    def extrair_paginacao(self, html):
        paginacao = []
        soup = BeautifulSoup(html, 'html.parser')
        paginacao_tag = soup.find(class_='pagination-plp')
        if paginacao_tag:
            # Encontrar todas as tags 'a' dentro da tag de paginacao
            href_tags = paginacao_tag.find_all('a', href=True)
            
            # Extrair o valor do atributo 'href' de cada tag
            for tag in href_tags:
                href = tag['href']
                paginacao.append(href)
        
        return paginacao
    
    def extrair_variacoes_produto(self,soup:BeautifulSoup):
        # Encontrar o item atualmente selecionado
        variants = soup.find_all(class_='variant')
        variants_names = soup.find_all(class_='variant-name')

        # Criar um dicionário de correspondência cor e href
        variacoes = []
        for variant, variant_name in zip(variants, variants_names):
            cor = variant_name.text.strip()
            href = variant['href']
            variacoes.append({"cor": cor, "href": href})
        
        return variacoes


    def extrair_informacoes_produto(self,link, ignorarVariacoes=False):
        resposta = self.session.get(link)
        if resposta.status_code != 200:
            self.log.error(f"Falha ao se comunicar com {link}, status_code: {resposta.status_code}")
            raise Exception(f"Falha ao se comunicar com {link}, status_code: {resposta.status_code}")
        if resposta.text.upper().__contains__("ESGOTADO"):
            if ignorarVariacoes:
                return None
            else:
                return None, None
        soup = BeautifulSoup(resposta.content, 'html.parser')

        # Encontrar o preço com base
        preco_tag = soup.find(attrs={'itemprop': 'price'})
        preco = preco_tag['content'] if preco_tag else None

        # Encontrar o número de parcelas
        parcelas_tag = soup.find(class_='mdn-Price-prefix js-installment-quantity')
        parcelas = parcelas_tag.get_text() if parcelas_tag else None

        # Encontrar se o produto está disponivel 
        availability_tag = soup.find(attrs={'itemprop': 'availability'})
        availability = availability_tag['content'] if availability_tag else None

        # Encontrar o nome do produto (productName)
        product_name_tag = soup.find('input', {'name': 'productName'})
        product_name = product_name_tag['value'] if product_name_tag else None

        # Encontrar a marca do produto (productBrand)
        product_brand_tag = soup.find('input', {'name': 'productBrand'})
        product_brand = product_brand_tag['value'] if product_brand_tag else None


        # Encontrar a capacidade de armazenamento
        capacidade_tag = soup.find_all('div', class_='product-spec-characteristic')
        capacidade_armazenamento = ""
        for capacidade in capacidade_tag:
            for div in capacidade.find_all('div'):
                if 'Memória ROM:' in div.get_text():
                    capacidade_armazenamento = div.find_next('div', class_='mdn-u-flex').get_text(strip=True)
                    capacidade_armazenamento.replace(" ", "")
                    break
            if capacidade_armazenamento != "":
                break

        # Encontrar capacidade
        if capacidade_armazenamento=="":
            capacidade_armazenamento = self.extract_capacity(product_name)

        # Cores e demais itens
        variacoes = self.extrair_variacoes_produto(soup)

        dic_padrao = self.criar_dicionario_padrao()

        dic_padrao["capacidade_armazenamento"] = capacidade_armazenamento
        dic_padrao["cor"] = variacoes[0]['cor']
        dic_padrao["nome_produto"] = product_name
        dic_padrao["fabricante"] = product_brand
        # dic_padrao["data_extracao"] = datetime.now().strftime("%Y%m%d_%H%M%S")
        dic_padrao['data_extracao'] = get_current_date()
        dic_padrao["preco_pix"] = preco
        dic_padrao["preco_boleto"] = preco
        dic_padrao["preco_x1_cartao"] = preco
        dic_padrao["preco_prazo_sem_juros_cartao_normal"] = preco
        dic_padrao["qtd_parcelas_sem_juros_cartao_normal"] = 12
        dic_padrao["qtd_parcelas_sem_juros_cartao_proprio"] = parcelas
        dic_padrao["taxa_juros_cartao_normal"] = 0
        dic_padrao["preco_x1_cartao_proprio"] = preco
        dic_padrao["preco_prazo_sem_juros_cartao_proprio"] = preco
        dic_padrao["frete_minimo"] = 0
        dic_padrao["url"] = link


        variacoes.pop(0) #item 0 é o atual

        if not(ignorarVariacoes):
            novos_links = [variacao['href'] for variacao in variacoes]
            return dic_padrao, novos_links
        return dic_padrao
    
    def extract_capacity(self,name):
        matches = re.findall(r"(\d+)\s*(gb|tb)", name, re.IGNORECASE)
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
        
    def Celulares_links(self,html):
        links = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Encontrar todas as tags que contêm a classe 'card-planos'
        card_planos_tags = soup.find_all(class_=lambda x: x and 'card-planos' in x.split())
        
        # Extrair o valor do atributo 'data-link-redirect' de cada tag
        for tag in card_planos_tags:
            link = tag.get('data-link-redirect')
            if link:
                links.append(self.urlCelulares+link)
        
        return links
    
# Tipagem desejada para cada coluna
tipos_desejados = {
    'fabricante': str,
    'marca': str,
    'ean': str,
    'sku': str,
    'no_modelo': str,
    'nome_comercial': str,
    'capacidade_armazenamento': str,
    'cor': str,
    'nome_produto': str,
    'empresa': str,
    'vendedor': str,
    'preco_pix': 'float64',
    'preco_boleto': 'float64',
    'preco_x1_cartao': 'float64',
    'preco_prazo_sem_juros_cartao_normal': 'float64',
    'qtd_parcelas_sem_juros_cartao_normal': 'int32',
    'preco_prazo_com_juros_cartao_normal': 'float64',
    'qtd_parcelas_com_juros_cartao_normal': 'int32',
    'taxa_juros_cartao_normal': 'float64',
    'preco_x1_cartao_proprio': 'float64',
    'preco_prazo_sem_juros_cartao_proprio': 'float64',
    'qtd_parcelas_sem_juros_cartao_proprio': 'int32',
    'preco_prazo_com_juros_cartao_proprio': 'float64',
    'qtd_parcelas_com_juros_cartao_proprio': 'int32',
    'taxa_juros_cartao_proprio': 'float64',
    'frete': 'float64',
    'cep': str,
    'estoque': bool,
    'url': str,
    'data_extracao': str #'datetime64'
}

def tratar_coluna(coluna, tipo):
    """
    Trata uma coluna do DataFrame para o tipo desejado.

    Args:
        coluna (pd.Series): Coluna do DataFrame.
        tipo: Tipo de dado desejado para a coluna.

    Returns:
        pd.Series: Coluna tratada.
    """
    if tipo == 'float64':
        return coluna.replace(',', '.', regex=True).replace('[^\d\.]', '', regex=True).astype(float)
    elif tipo == 'int32':
        return coluna.fillna(0).astype(int)
    elif tipo == bool:
        return coluna.fillna(False).replace({'': False, 'None': False}).astype(bool)
    elif tipo == 'datetime64':
        return pd.to_datetime(coluna, format="%d-%m-%Y %H:%M:%S")
    else:
        return coluna.astype(str)



if __name__ == "__main__":
    extracao = ExtracaoClaro()
    


