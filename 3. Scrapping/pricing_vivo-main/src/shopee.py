import ast
from datetime import datetime
import json
import re
import time
import pandas as pd
import requests
from undetected_chromedriver import Chrome, ChromeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
import urllib.parse
from fake_useragent import UserAgent
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import urllib.parse
from unidecode import unidecode
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__
from modules.date_helper import get_current_date


class ShopeeScraper():
    def __init__(self,zipcode=31140280):
        self.log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))
        self.zipcode = zipcode
        self.tempoEspera = 10
        self.baseUrl = "https://shopee.com.br/"
        self.driver = None
        self._create_driver() 
        self.session = self.session_create()

    def session_create(self):
        requests.packages.urllib3.disable_warnings() 
        session = requests.Session()
        session.verify = False
        return session

    def _close_driver(self):
        self.driver.quit()

    def _reset_driver(self):
        self._close_driver()
        self._create_driver()

        #menos eficaz
        #self.driver.delete_all_cookies()
        #self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': self.fake_userChrome()})
        #try:
        #    self.importar_cookies() #cookies de login
        #except:
        #    print("Cookies não importado")


    def _create_driver(self ):
        option = ChromeOptions()
        # option.add_argument('--headless')
        option.add_argument('--no-sandbox')
        option.add_argument('--disable-dev-shm-usage')
        # option.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        # option.add_argument("--start-maximized")

        #user-agent fake
        option.add_argument(f'--user-agent={self.fake_userChrome()}')

        # Use undetected_chromedriver 
        self.driver = Chrome(service=Service(ChromeDriverManager().install()), options=option)

    #necessario ter entrado no dominio (SHOPEE)
    # def importar_cookies(self):
    #     try:
    #         with open('cookies.json', 'r') as file:
    #             cookies = json.load(file)
    #         for cookie in cookies:
    #             self.driver.add_cookie(cookie)
    #     except:
    #         self.log.info("Cookies não importado")
    def exportar_cookies(self):
        cookies = self.driver.get_cookies()
        with open('cookies.json', 'w') as file:
            json.dump(cookies, file)

    def fake_userChrome(self):
        ua = UserAgent()
        user_agent = ua.random
        return user_agent
    
    def calculo_frete(self,zipcode, item_id,shop_id):
        #caso necessite validar zipcode ->https://shopee.com.br/api/v4/location/get_zipcode_info?use_case=user_pdp&zipcode=31140280
        url_frete = f"{self.baseUrl}api/v4/pdp/shipping/get?buyer_zipcode={zipcode}&item_id={item_id}&shop_id={shop_id}"
        resposta = self.session.get(url_frete)
        if resposta.status_code !=200:
            raise Exception(f"Não foi possivel calcular o frete. Status_code {resposta.status_code}, {resposta.text}") 
        json_frete = json.loads(resposta.content)
        return self.normalizar_dinheiro(json_frete["data"]["product_shipping"]["shipping_fee_info"]["price"]["single_value"])

    
    def product_more_info(self, url):
        self._reset_driver()
        # self.importar_cookies()
        self.driver.get(url)
        self.wait_for_itens_completed('div[class="page-product__content"]') 
        body = self.find_url_log('api/v4/pdp/get_pc?shop_id') #json com dados 
        self._reset_driver()
        if body is None:
            raise Exception("Erro ao acessar pagina do produto")
        json_body = json.loads(body)["data"]
        dic_prod = {}
        dic_prod["fabricante"] = json_body["item"]["brand"]
        dic_prod["vendedor"]  = json_body["shop_detailed"]["name"]
        
        return dic_prod

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
            "empresa": "Shopee",
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


    def _get_chrome_user_data_path(self):
        import tempfile
        return tempfile.mkdtemp()
    
    def find_url_log(self, target_url):
        logs = scraper.driver.get_log("performance")
        for log in logs:
            message = log["message"]
            if "Network.responseReceived" in message:
                params = json.loads(message)["message"].get("params")
                if params:
                    response = params.get("response")
                    if response:
                        if response and target_url in response["url"]:
                            body = self.driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': params["requestId"]})
                            return body['body']
        return None
    
    def wait_for_itens_completed(self, referencia):
        try:
            # Aguardar até 10 segundos até que o elemento seja localizado
            WebDriverWait(self.driver, self.tempoEspera).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, referencia))
            )
            return True
        except Exception as e:
            self.log.error(f"Erro: {e}")
            return False


    def _generate_search_url_oficial(self, new_keyword):
        params = {
            'keyword': new_keyword
        }
        encoded_params = urllib.parse.urlencode(params, doseq=True)
        full_url = f"{self.baseUrl}oficial/search?{encoded_params}"

        return full_url
    
    def _generate_search_url(self, new_keyword):
        #Define os parâmetros iniciais (pode adicionar mais se necessário)
        #Nacionais e lojas oficiais
        params = {
            'filters': '5',
            'keyword': new_keyword,
            'locations': 'Nacional',
            'noCorrection': 'true',
            'page': '0'
        }
        
        encoded_params = urllib.parse.urlencode(params, doseq=True)
        full_url = f"{self.baseUrl}search?{encoded_params}"

        return full_url
    
    def get_parcelamento(self, shopid, itemid):
        url = f"{self.baseUrl}api/v4/item/get_installment_plans?shopid={shopid}&itemid={itemid}"
        resposta = self.session.get(url)
        if resposta.status_code!=200:
            raise Exception("Falha no get_parcelamento")
        resposta = json.loads(resposta.text)
        if "data" not in resposta or not resposta["data"]:
            return None

        # Obter informações da primeira e última parcela
        primeira_parcela = resposta["data"][0]["plans"][0]
        ultima_parcela = resposta["data"][0]["plans"][-1]

        
        if primeira_parcela["total_amount"] != ultima_parcela["total_amount"]:
            # Se não for atendida, verificar as parcelas intermediárias
            for parcela in resposta["data"][0]["plans"][1:-1]:
                if parcela["total_amount"] == primeira_parcela["total_amount"]:
                    qtd_parcelas_sem_juros_cartao_normal = parcela["duration"]
                else:
                    break
            else:
                # Se não encontrar parcela sem juros, definir como None
                qtd_parcelas_sem_juros_cartao_normal = None
        else:
            # Se a condição for atendida, usar a última parcela
            qtd_parcelas_sem_juros_cartao_normal = ultima_parcela["duration"]

        # Extrair valores específicos
        preco_x1_cartao = primeira_parcela["total_amount"]
        preco_prazo_sem_juros_cartao_normal = primeira_parcela["total_amount"]
        preco_prazo_com_juros_cartao_normal = ultima_parcela["total_amount"]
        qtd_parcelas_com_juros_cartao_normal = ultima_parcela["duration"]
        
        # Calcular a taxa de juros para a última parcela
        taxa_juros_cartao_normal = (
            (ultima_parcela["total_amount"] / preco_x1_cartao) ** (1 / ultima_parcela["duration"]) - 1
        ) * 100

        # Criar um dicionário com os resultados
        resultados = {
            "preco_x1_cartao": self.normalizar_dinheiro(preco_x1_cartao),
            "preco_prazo_sem_juros_cartao_normal": self.normalizar_dinheiro(preco_prazo_sem_juros_cartao_normal),
            "qtd_parcelas_sem_juros_cartao_normal": qtd_parcelas_sem_juros_cartao_normal,
            "preco_prazo_com_juros_cartao_normal": self.normalizar_dinheiro(preco_prazo_com_juros_cartao_normal),
            "qtd_parcelas_com_juros_cartao_normal": qtd_parcelas_com_juros_cartao_normal,
            "taxa_juros_cartao_normal": taxa_juros_cartao_normal,
        }

        return resultados
    


    def extrair_fabricante(self,nome_produto):
        # Lista de fabricantes de celulares conhecidos
        fabricantes = [
            "Apple", "Samsung", "Huawei", "Xiaomi", "Sony", "LG", "Motorola",
            "Google", "OnePlus", "Nokia", "BlackBerry", "HTC", "Lenovo", "Asus"
        ]


        for fabricante in fabricantes:
            if fabricante.lower() in nome_produto.lower():
                return fabricante
        return None

    def correlacionar_dados(self,json_resp):
        # Crie um dicionário padrão
        dic = self.criar_dicionario_padrao()
        shopid=json_resp.get('item_basic', {}).get('shopid')
        itemid=json_resp.get('item_basic', {}).get('itemid')
        valores = self.get_parcelamento(shopid=shopid, itemid=itemid)
        dic.update(valores)
        
        dic['nome_produto'] = json_resp.get('item_basic', {}).get('name')
        dic['estoque'] = json_resp.get('item_basic', {}).get('stock')
        dic['link'] = self.montar_url_produto(shopid,itemid)
        dic["capacidade_armazenamento"] = self.extract_capacity(dic['nome_produto'])
        # dic["data_extracao"] = datetime.now().strftime("%Y%m%d_%H%M%S")
        dic["data_extracao"] = get_current_date()
        
        variations = json_resp.get('item_basic', {}).get('tier_variations')
        if variations is not None:
            for v in variations:
                if str(v.get('name', {})).upper() == "COR":
                    dic["data_extracao"] = v['options'][0]
                    break
        try:
            dic_2 = self.product_more_info(dic['link'])
            dic.update(dic_2)
        except:
            self.log.error("Erro ao obter mais informações do produto")

        dic['cep'] = self.zipcode
        dic['frete_minimo'] = self.calculo_frete(zipcode=str(self.zipcode), item_id=itemid,shop_id=shopid)
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


    def Extracao_Shopee(self,keyword):
        tentativas = 0
        
        pesquisa = scraper._generate_search_url_oficial(keyword)
        while(tentativas<2):
            # self.importar_cookies()
            self.driver.get(pesquisa)
            try:
                if self.wait_for_itens_completed('li[data-sqe="item"]'):
                    break
                else:
                    time.sleep(5)
            except:
                time.sleep(5)
            tentativas+=1
            self._reset_driver()
        if not(tentativas<2):
            return None
        api_response = self.find_url_log("/api/v4/search/search_items")
        self._reset_driver()
        json_ = json.loads(api_response)['items']
        infos = []
        for j in json_:
            try:
                dados = self.correlacionar_dados(j)
                infos.append(dados)
            except:
                self.log.error(f'Não pode ser extraido {j}')
                continue
        return infos
    
    def salvar_lista(self,lista):
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        # file_name = f"Shopee_extracao_{current_time}.csv"
        df = pd.DataFrame(lista)
        # df.to_csv(file_name, index=False)
        
        # Upload to bucket
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\assets\pro-ia-precificador-f1cde9d5eada.json"

        gcs_manager = GCSParquetManager()

        # Formatar a data e hora no formato desejado
        pasta = "extracoes/"
        loja = "shopee"
        agora = datetime.now()
        nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"
        
        gcs_manager.upload_parquet(df, nome_arquivo)
        
        self.log.info("Fim")
    
    def processar_lista(self,lista):
        infos = []
        i = 1
        for l in lista:
            self.log.info(f"{i}/{len(lista)} {l}")
            try:
                info = self.Extracao_Shopee(l)
                if info:
                    infos.extend(info)
                else:
                    self.log.error("Erro ao extrair")
            except:
                self.log.error(f"Erro em {l}")
            i+=1
        self.salvar_lista(infos)
        self._close_driver()

    def montar_url_produto(self,name, shopid, itemid):
        #versão que coloca o nome
        name = name.replace(' ', '-').replace(',', '-').replace('-,', '-').replace('--', '-')
        name = ''.join(e if e.isalnum() or e in ('-', '(', ')') else '' for e in name)
        url = f"{self.baseUrl}{name}-i.{shopid}.{itemid}"
        return url
    
    def montar_url_produto(self,shopid, itemid):
        url = f"{self.baseUrl}product/{shopid}/{itemid}"
        return url
    
    def normalizar_dinheiro(self,valor_em_centavos):
        valor_em_reais = valor_em_centavos / 100000
        return valor_em_reais

if __name__ == "__main__":
    scraper = ShopeeScraper()

    gcs_manager = GCSParquetManager()
    lista = gcs_manager.ler_coluna_excel()
    
    # lista = ["Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 256GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S918B 512GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 256GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung Galaxy S916B 512GB","Samsung T224 64GB Tablet","Samsung T224 64GB Tablet","Samsung T575N","Samsung T575N","Samsung F721B 128GB","Samsung F721B 128GB","Samsung F721B 128GB","Samsung F721B 128GB","Samsung F721B 256GB","Samsung F721B 256GB","Samsung F721B 256GB","Samsung F721B 256GB","Samsung Galaxy F731B 512GB","Samsung Galaxy F731B 512GB","Samsung Galaxy F731B 512GB","Samsung Galaxy F731B 512GB","Samsung F936B 256GB","Samsung F936B 256GB","Samsung F936B 256GB","Samsung F936B 512GB","Samsung F936B 512GB","Samsung F936B 512GB","Samsung Galaxy F946B 1TB","Samsung Galaxy F946B 1TB","Samsung Galaxy F946B 1TB","Samsung Galaxy F946B 512GB","SEMP TCL GO3c Plus","Alcatel 5033E","SEMP TCL L5 5033E"]
    scraper.processar_lista(lista)


