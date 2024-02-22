
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
from modules.logger_control import Logger, __main__
from modules.date_helper import get_current_date


class ExtracaoClaro():
    def __init__(self):
        self.log = Logger(os.path.basename(__main__.__file__))
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
            "empresa": None,
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
            "data_extracao": None,

            'plano_nome': None,
            'plano_preco': None,
            'plano_valor_aparelho':None,
            'plano_categoria':None
        }
    
    def Claro_Extracao(self):
        df_cel = self.Celulares_Extracao()
        df_ace = self.Acessorios_Extracao()

        df_final = pd.concat([df_cel, df_ace], ignore_index=True)

        # Adicionar data e hora ao nome do arquivo
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"claro_extracao_{current_time}.xlsx"

        df_final.to_excel(file_name, index=False)
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
            dic_infos["estoque"] = int(info.get("installment", {}).get("quantity")) if info.get("installment", {}).get("quantity") is not None else None
            dic_infos["data_extracao"] = current_time
            dic_infos["qtd_parcelas_sem_juros_cartao_normal"] = 12  # todos são 12 sem juros
            dic_infos["url"] = self.urlCelulares + info.get("url", "")
            dic_infos["preco_pix"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            dic_infos["preco_boleto"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            dic_infos["preco_x1_cartao"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            dic_infos["preco_prazo_sem_juros_cartao_normal"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
            dic_infos["preco_prazo_com_juros_cartao_normal"] = float(info["price"].get("value")) if info["price"].get("value") is not None else None
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
                raise Exception(f"Falha ao se comunicar com {url_atul}, status_code: {resposta.status_code}")
            links.extend(self.Celulares_links(resposta.content)) 


        #GET EM TODOS PRODUTOS ENCONTRADOS
        infos = []
        novosLinks = []
        for link in links:
            self.log.info(f"Acessando {link}")
            info, novoLink = self.extrair_informacoes_produto(link)
            if info:
                if type(info) == list:
                    for i in info:
                        infos.append(i)
                else:
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
                if type(info)==list:
                    for i in info:
                        infos.append(i)
                else:
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

    def normalizar_valor(self,valor_str):
        valor_str = valor_str.replace('R$', '').replace('\xa0', '').replace(',', '').replace('.', '')
        valor_float = float(valor_str)
        valor_formatado = round(valor_float/100, 2)

        return valor_formatado
    def extrair_informacoes_produto_Plano(self, link):
        id_Produto = link.split("/p/")[1]
        #info plano
        url = f"{self.urlCelulares}/claro/pt/p/products/processType?processType=ACQUISITION&deviceCode={id_Produto}" 
        resposta = self.session.get(url)
        if resposta.status_code != 200:
            raise Exception(f"Falha ao se comunicar com {url}, status_code: {resposta.status_code}")        
        soup = BeautifulSoup(resposta.content, 'html.parser')
        planos_info = soup.find_all(class_='paymentMethodChoosePlan')
        lista_info_plan = []
        for planos in planos_info:
            dic_padrao = self.criar_dicionario_padrao()
            id_plano = planos.find(attrs={'name': 'planForDevice'})['value']
            url_plano_json = f"{self.urlCelulares}/claro/pt/p/products/campaign?planCode={id_plano}&processType=ACQUISITION&deviceCode={id_Produto}&loyalty=true"
            resposta = self.session.get(url_plano_json)
            json_detalhes = json.loads(resposta.content)

            for feat in json_detalhes['classifications'][0]['features']:
                if feat['name'].upper() == 'COR':
                    dic_padrao["cor"] = feat['featureValues'][0]['value']
                if feat['name'].upper() == "FABRICANTE":
                    dic_padrao["fabricante"] = feat['featureValues'][0]['value']
                if feat['name'].upper() == "MODELO":
                    dic_padrao["nome_produto"] = feat['featureValues'][0]['value']
                if feat['name'].upper() == "MEMÓRIA ROM":
                    dic_padrao["capacidade_armazenamento"] = feat['featureValues'][0]['value']
            dic_padrao["taxa_juros_cartao_normal"] = 0
            dic_padrao["frete_minimo"] = 0
            dic_padrao["url"] = link
            dic_padrao['plano_nome'] = planos.find(attrs={'name': 'plan-selected'})['value']
            dic_padrao['plano_preco'] = self.normalizar_valor(planos.find(attrs={'name': 'planPriceDebit'})['value'])
            dic_padrao['plano_valor_aparelho'] = json_detalhes['campaignPrice']["value"]
            dic_padrao['sku'] = json_detalhes['baseProduct']
            dic_padrao['plano_categoria'] = planos.find(attrs={'name': 'plan-category'})['value']
            dic_padrao['qtd_parcelas_sem_juros_cartao_normal'] = planos.find(attrs={'name': 'installment-quantity'})['value']
            dic_padrao["preco_pix"] = self.normalizar_valor(planos.find(attrs={'name': 'oldDevicePrice'})['value'])
            dic_padrao["preco_boleto"] = dic_padrao["preco_pix"]
            dic_padrao["preco_x1_cartao"] = dic_padrao["preco_pix"]
            dic_padrao["preco_prazo_sem_juros_cartao_normal"] = dic_padrao["preco_pix"]
            dic_padrao["preco_x1_cartao_proprio"] = dic_padrao["preco_pix"]
            dic_padrao["preco_prazo_sem_juros_cartao_proprio"] = dic_padrao["preco_pix"]
            # dic_padrao["data_extracao"] = datetime.now().strftime("%Y%m%d_%H%M%S")
            dic_padrao["data_extracao"] = get_current_date()
            lista_info_plan.append({**dic_padrao})
        return lista_info_plan  
        
    def extrair_informacoes_produto(self,link, ignorarVariacoes=False):
        resposta = self.session.get(link)
        if resposta.status_code != 200:
            raise Exception(f"Falha ao se comunicar com {link}, status_code: {resposta.status_code}")
        if resposta.text.upper().__contains__("ESGOTADO"):
            if ignorarVariacoes:
                return None
            else:
                return None, None
        soup = BeautifulSoup(resposta.content, 'html.parser')

        # Cores e demais itens
        variacoes = self.extrair_variacoes_produto(soup)

        #nova consulta para valores com variação do plano
        dic = self.extrair_informacoes_produto_Plano(link)

        variacoes.pop(0) #item 0 é o atual

        if not(ignorarVariacoes):
            novos_links = [variacao['href'] for variacao in variacoes]
            return dic, novos_links
        return dic
    
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



if __name__ == "__main__":
    extracao = ExtracaoClaro()
    


