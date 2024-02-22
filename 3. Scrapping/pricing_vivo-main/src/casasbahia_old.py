import json
import math
import time
import logging
import requests

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import FirefoxOptions

from modules.xlsx import get_names
from modules.logger_control import Logger, __main__,os




class CasasBahia:

    def __init__(self, searched_list, cep):
        self.logger = Logger(os.path.basename(__main__.__file__).replace(".py", ""))
        self.searched_list = searched_list
        # self.options = self._set_options()
        self.waiting_seconds = 2
        self.total_items = 0
        self.items_per_page = 20
        self.page = 1
        self.products = []
        self.cep = cep

    def _set_options(self):
        options = FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-plugins')
        options.headless = True
        driver = webdriver.Firefox(options=options)

        return driver

    # def _get_cep(self, sku):
    #     headers = {
    #         'authority': 'pdp-api.casasbahia.com.br',
    #         'accept': '*/*',
    #         'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    #         'cache-control': 'no-cache',
    #         'dnt': '1',
    #         'origin': 'https://www.casasbahia.com.br',
    #         'pragma': 'no-cache',
    #         'referer': 'https://www.casasbahia.com.br/',
    #         'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    #         'sec-ch-ua-mobile': '?0',
    #         'sec-ch-ua-platform': '"macOS"',
    #         'sec-fetch-dest': 'empty',
    #         'sec-fetch-mode': 'cors',
    #         'sec-fetch-site': 'same-site',
    #         'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    #         'x-cvip': 'IPI-CasasBahia=UsuarioGUID=cb3dafea-9436-4b39-9089-13e9ac961cb0&cepClienteProvavel=58423215',
    #     }
    #
    #     params = {
    #         'utm_medium': 'BuscaOrganica',
    #         'utm_source': 'Google',
    #         'utm_campaign': 'DescontoEspecial',
    #         'channel': 'DESKTOP',
    #         'orderby': 'price',
    #     }
    #
    #     response = requests.get(
    #         f'https://pdp-api.casasbahia.com.br/api/v2/sku/{sku}/freight/seller/10037/zipcode/{self.cep}/source/CB',
    #         params=params,
    #         headers=headers,
    #     )
    #
    #     page_source = self.driver.page_source
    #     if response.ok:
    #         widgets_data = json.loads(response.text.split('(', 1)[1].rsplit(')', 1)[0])['widgets']

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
            # "frete_minimo": None,
            "frete": None,
            "cep": None,
            "estoque": None,
            "url": None,
            "data_extracao": None
        }

    def _get_total_pages(self, pagination_section):
        if pagination_section:
            last_page_element = pagination_section.find('a', {'aria-label': 'Próxima página'})
            if last_page_element:
                self.total_items = last_page_element.previousSibling.parent.previous.replace("produtos", "")
                return math.ceil(int(self.total_items) / (self.items_per_page))
        return 0

    def _get_details(self, palavra_chave, produto_info):

        for chave, valor in produto_info.items():
            try:
                if chave.lower() == palavra_chave.lower():
                    return valor
                if 'name' in valor and valor['name'].lower() == palavra_chave.lower():
                    return produto_info[chave]['value']
            except:
                pass

    def scrape_product_details(self, id, sku, descricao, url, dic_infos):

        headers = {
            'authority': 'onsite.chaordicsystems.com',
            'accept': '*/*',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'dnt': '1',
            'referer': 'https://www.casasbahia.com.br/',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'script',
            'sec-fetch-mode': 'no-cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }

        api_params = {
            'apiKey': 'casasbahia',
            'page': {
                'name': 'product',
                'salesChannel': 'desktop',
                'url': url,
            },
            'source': 'desktop',
            'referenceProduct': {
                'id': id,
            },
            'timeout': 7000,
            'host': 'www.casasbahia.com.br',
            'identity': {
                'browserId': '0-bugpxD7PdeyxXq843sQGNI1ForjGRWMn1N7r17029226724733488',
                'anonymousUserId': 'anon-0-bugpxD7PdeyxXq843sQGNI1ForjGRWMn1N7r17029226724733488',
                'session': '1702928088885-0.9444277330549027',
            },
            'testGroup': {
                'experiment': 'CASASBAHIA_NEW_RANK_HOTSITE_2019-11-25',
                'group': 'B',
                'testCode': 'CASASBAHIA_NEW_RANK_HOTSITE_2019-11-25_B',
                'code': 'CASASBAHIA_NEW_RANK_HOTSITE_2019-11-25_B/wk8wrVpAAu1rtlhfiyiHgijSq8gCrteE',
                'session': 'wk8wrVpAAu1rtlhfiyiHgijSq8gCrteE',
            },
        }

        params = {
            'callback': 'jQuery17105607555805973563_1702929501075',
            'q': json.dumps(api_params),
            '_': '1702929501083',
        }

        response = requests.get('https://onsite.chaordicsystems.com/v5/recommend/all', params=params, headers=headers)

        if response.ok:
            widgets_data = json.loads(response.text.split('(', 1)[1].rsplit(')', 1)[0])['widgets']
            filtered_widgets = []
            for widget in widgets_data:
                if (
                        'result' in widgets_data[widget]
                        and widgets_data[widget]['priceMode'] == 'relative'
                        and widgets_data[widget]['result']['displays'].__len__() > 0
                ):
                    if widgets_data[widget]['result']['displays'][0]['refs'].__len__() > 0:
                        if widgets_data[widget]['result']['displays'][0]['refs'].__len__() > 0:
                            if widgets_data[widget]['result']['displays'][0]['refs'][0]["id"] == id:
                                filtered_widgets = widgets_data[widget]['result']['displays'][0]['refs']

            for widget in filtered_widgets:

                try:
                    self.driver = self._set_options()
                    url_detail = widget.get('url')
                    self.driver.get(url_detail)
                    page_source = self.driver.page_source
                    soup = BeautifulSoup(page_source, "html.parser")
                    script_content = soup.find('script', id='__NEXT_DATA__')
                    next_data_json = json.loads(script_content.string)

                    for data in next_data_json['props']['initialState']['ProductPrice']['installmentOptions']:
                        if data["type"] == "Pix":
                            dic_infos["preco_pix"] = data["conditions"][0]['price']
                        if data["type"] == "Boleto":
                            dic_infos["preco_boleto"] = data["conditions"][0]['price']
                        if data["type"] == "Bandeira":
                            dic_infos["preco_x1_cartao_proprio"] = data["conditions"][0]['price']
                            dic_infos["preco_prazo_com_juros_cartao_proprio"] = data["conditions"][-1]['totalPrice']
                            dic_infos["qtd_parcelas_com_juros_cartao_proprio"] = data["conditions"][-1]['qtyParcels']
                            dic_infos["taxa_juros_cartao_proprio"] = data["conditions"][-1]['monthlyInterest']

                        if data["type"] == "Outros":
                            dic_infos["cartao"] = data["conditions"][0]['price']
                            dic_infos["preco_x1_cartao"] = data["conditions"][0]['price']
                            dic_infos["preco_prazo_com_juros_cartao_normal"] = data["conditions"][-1]['totalPrice']
                            dic_infos["qtd_parcelas_com_juros_cartao_normal"] = data["conditions"][-1]['qtyParcels']
                            dic_infos["taxa_juros_cartao_normal"] = data["conditions"][-1]['monthlyInterest']
                    for data in next_data_json['props']['initialState']['ProductPrice']['bestInstallments']:
                        if (data["type"] == "Bandeira"):
                            dic_infos["preco_prazo_sem_juros_cartao_proprio"] = data['price']
                            dic_infos["qtd_parcelas_sem_juros_cartao_proprio"] = data['numberOfParcels']
                        if (data["type"] == "Outros"):
                            dic_infos["preco_prazo_sem_juros_cartao_normal"] = data['price']
                            dic_infos["qtd_parcelas_sem_juros_cartao_normal"] = data['numberOfParcels']
                    self.driver.quit()

                except Exception as e:
                    self.logger.error(f"Error navigating to {widget.get('url')}: {e}")

                dict_details = widget.get('details')

                dic_infos["sku"] = sku
                dic_infos["no_modelo"] = self._get_details('modelo', dict_details)
                dic_infos["capacidade_armazenamento"] = self._get_details('memória total', widget.get('details'))
                dic_infos["cor"] = self._get_details('cor', widget.get('details'))
                dic_infos["url"] = url
                dic_infos["nome_comercial"] = descricao
                dic_infos["nome_produto"] = descricao
                dic_infos["marca"] = self._get_details('marca', dict_details)
                dic_infos["empresa"] = "casasbahia"
                dic_infos["fabricante"] = self._get_details('marca', dict_details)
                dic_infos["empresa"] = "casasbahia"
                dic_infos["cep"] = self.cep
                return dic_infos
                # "categoria": widget.get('categories')[0]["name"],

        else:
            self.logger.error(f"Error in API request. Status code: {response.status_code}")

    def _get_product_info(self, next_data_script):
        if next_data_script:
            script_content = next_data_script.string

            try:
                next_data_json = json.loads(script_content)
                product_info = next_data_json['props']['pageProps']['initialState']['search']['results']['products']
                for item in product_info:
                    sku = item.get("idSku")
                    dic_infos = self.criar_dicionario_padrao()
                    self.scrape_product_details(item.get("id"), sku, item.get("title"), item.get("href"), dic_infos)
                    # dic_infos["frete_minimo"] = self._get_cep(sku)
                    self.products.append(dic_infos)
                    self.logger.info(product_info)
            except json.JSONDecodeError as e:
                self.logger.error(f"Error parsing JSON: {e}")
        else:
            self.logger.warning("Script tag with id '__NEXT_DATA__' not found in the HTML.")

    def save_to_excel(self, filename='casas_bahia_data.xlsx'):
        import pandas as pd

        df = pd.DataFrame(self.products)

        # df.to_excel("/Users/albertowagner/desafio-crawler/casas_bahia_data.xlsx", index=False)
        df.to_excel("casas_bahia_data.xlsx", index=False)

    def run(self):
        for searched in self.searched_list:
            while True:
                self.driver = self._set_options()
                URL = f'https://www.casasbahia.com.br/{searched}/b?page={self.page}'
                self.driver.get(URL)

                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                next_data_script = soup.find('script', id='__NEXT_DATA__')

                pagination_section = soup.find('nav', {'aria-label': 'Paginação'})
                total_pages = self._get_total_pages(pagination_section)
                self.driver.quit()

                self._get_product_info(next_data_script)
                time.sleep(self.waiting_seconds)
                self.page += 1
                self.logger.info("page "+ self.page)

                self.save_to_excel()
                # if self.page > total_pages:
                if self.page > 1:
                    break

        self.save_to_excel()
        return self.products


# casas_bahia_scraper = CasasBahia(["iphone 14"], "58423-215")
casas_bahia_scraper = CasasBahia(get_names('assets/lista devices.xlsx')[:2], "01021-100")
casas_bahia_scraper.run()
