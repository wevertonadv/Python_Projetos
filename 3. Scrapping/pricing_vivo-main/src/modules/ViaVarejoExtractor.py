from datetime import datetime
from time import sleep
from random import shuffle, randint
from re import findall, DOTALL
from json import loads
from unidecode import unidecode
from selenium.webdriver import Chrome, ChromeOptions
from modules.string_normalizer import normalize_string
from modules.verify_names import combined_similarity
from modules.product import create_product
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class ViaVarejoExtractor:
    def __init__(
            self,
            host_website: str,
            host_api: str,
            zipcode: str,
            marketplace_identifier: str,
            marketplace_name: str
        ):
        self._s_host_website = host_website
        self._s_host_api = host_api
        self._s_zipcode = zipcode
        self._s_marketplace_identifier = marketplace_identifier
        self._i_default_waiting_time = 3
        self._i_default_max_request_attempts = 3
        self._chrome_selenium = None
        self._s_marketplace_name = marketplace_name

    def get_search_results(self, keywords: list[str]) -> list[str] | None:
        """
        Retrieve results from search
        :param keywords: Product name list
        :return: Product path list
        """
        d_search_results = {}

        if len(keywords) == 0 or keywords is None:
            print('[ERROR]', 'Keyword list is empty.')
            return None

        shuffle(keywords)

        self._start_selenium()

        for i_keyword_index in range(0, len(keywords), 1):
            s_keyword = keywords[i_keyword_index]

            print()
            print(
                f'[INFO]',
                f'({i_keyword_index + 1} of {len(keywords)})',
                f'Retrieving products for keyword "{s_keyword}"'
            )

            if self._chrome_selenium is None:
                self._start_selenium()

                if self._chrome_selenium is None:
                    return None

            s_normalized_keyword = normalize_string(s_keyword, '-')

            s_path = f'/{s_normalized_keyword}/b'
            i_attempt_counter = 1
            while True:
                s_response = self._send_request(self._s_host_website + s_path)

                if s_response is not None or i_attempt_counter >= self._i_default_max_request_attempts:
                    break

                print('[WANING]', 'Failed to obtain page HTML, trying again...')
                i_attempt_counter += 1
                self._quit_selenium()
                self._start_selenium()

            if s_response is None:
                print('[ERROR]', 'Unable to obtain page HTML, skipping...')
                self._quit_selenium()
                continue

            print('[INFO]', 'Extracting JSON from HTML...')
            try:
                l_x45v0klk1 = findall(
                    pattern=r'<script id="__NEXT_DATA__" type="application/json" nonce=".+?">(.+?)</script>',
                    string=s_response,
                    flags=DOTALL
                )
            except Exception as err:
                print('[ERROR]', 'Unable to extract JSON from HTML due to', str(err))
                self._quit_selenium()
                continue

            if len(l_x45v0klk1) == 0:
                print('[ERROR]', 'RegEx did not found necessary JSON in HTML, skipping...')
                self._quit_selenium()
                continue

            d_json = self._parse_dictionary(l_x45v0klk1[0])

            print('[INFO]', 'Extracting products data from JSON...')
            try:
                l_raw_products = d_json['props']['pageProps']['initialState']['search']['results']['products']
            except Exception as err:
                print('[ERROR]', 'Unable to extract products data from JSON due to', str(err))
                self._quit_selenium()
                return None

            print('[INFO]', 'Filtering products...')
            for d_raw_product in l_raw_products:
                s_product_name = d_raw_product.get('title')
                s_link = d_raw_product.get('href')
                s_sku = d_raw_product.get('idSku')
                i_similarity_percentage = combined_similarity(s_product_name, s_keyword) * 100

                if i_similarity_percentage >= 50:
                    d_search_results.update(
                        {
                            s_sku: s_link
                        }
                    )

        self._quit_selenium()

        l_search_results = list(d_search_results.values())

        if len(l_search_results) == 0:
            print('[ERROR]', 'Results list is empty.')
            return None

        return l_search_results

    def get_products(self, links: list[str]) -> list[dict] | None:
        """
        Retrieve products
        :param links: Product path list
        :return: Product list
        """
        l_products = []

        if len(links) == 0 or links is None:
            print('[ERROR]', 'Link list is empty.')
            return None

        shuffle(links)

        self._start_selenium()

        for i_link_index in range(0, len(links), 1):
            s_link = links[i_link_index]

            print()
            print('[INFO]', f'({i_link_index + 1} of {len(links)})', f'Retrieving product data...')

            if self._chrome_selenium is None:
                self._start_selenium()

                if self._chrome_selenium is None:
                    return None

            i_attempt_counter = 1
            while True:
                s_response = self._send_request(s_link)

                if s_response is not None or i_attempt_counter >= self._i_default_max_request_attempts:
                    break

                print('[WANING]', 'Failed to obtain page HTML, trying again...')
                i_attempt_counter += 1
                self._quit_selenium()
                self._start_selenium()

            if s_response is None:
                print('[ERROR]', 'Unable to obtain page HTML, skipping...')
                self._quit_selenium()
                continue

            # tiow = open('_html.html', 'r', encoding='utf-8')
            # s_response = tiow.read()
            # tiow.close()

            print('[INFO]', 'Extracting JSON from HTML...')
            try:
                l_f78h478jf = findall(
                    pattern=r'<script id="__NEXT_DATA__" type="application/json".*?>(.+?)</script>',
                    string=s_response,
                    flags=DOTALL
                )
            except Exception as err:
                print('[ERROR]', 'Unable to extract JSON from HTML due to', str(err))
                self._quit_selenium()
                continue

            if len(l_f78h478jf) == 0:
                print('[ERROR]', 'RegEx did not found necessary JSON in HTML, skipping...')
                self._quit_selenium()
                continue

            d_json = self._parse_dictionary(l_f78h478jf[0])

            s_brand = None
            s_identification = None
            s_universal_identification = None
            s_name = None
            s_color = None
            s_storage = None
            b_available = None
            s_store = None
            f_price_pix = None
            f_price_ticket = None
            f_price_credit = None
            f_price_m_i_n_f = None
            i_amount_m_i_n_f = None
            f_price_m_i_w_f = None
            i_amount_m_i_w_f = None
            f_percentage_f_m_i = None
            f_price_cc = None
            f_price_cc_m_i_n_f = None
            i_amount_cc_m_i_n_f = None
            f_price_cc_m_i_w_f = None
            i_amount_cc_m_i_w_f = None
            f_percentage_cc_f_m_i = None

            d_json: dict = d_json.get('props')
            if d_json is not None:

                d_json = d_json.get('initialState')
                if d_json is not None:

                    d_raw_product: dict = d_json.get('Product')
                    if d_raw_product is not None:

                        d_raw_product_2: dict = d_raw_product.get('product')
                        if d_raw_product_2 is not None:

                            s_name = d_raw_product_2.get('name')

                            s_brand = d_raw_product_2.get('brand')
                            if s_brand is not None:
                                s_brand = s_brand.get('name')

                            l_base_specs: list[dict] = d_raw_product_2.get('specGroups')
                            if l_base_specs is not None:
                                if len(l_base_specs) > 0:
                                    for d_base_spec in l_base_specs:

                                        l_specs = d_base_spec.get('specs')
                                        if l_specs is not None:
                                            if len(l_specs) > 0:
                                                for d_spec in l_specs:

                                                    s_spec_name = d_spec.get('name')
                                                    if s_spec_name is not None:
                                                        s_spec_name = unidecode(s_spec_name).lower().replace(' ', '')
                                                        match s_spec_name:
                                                            case 'codigodereferencia':
                                                                s_universal_identification = d_spec.get('value')
                                                            case 'cor':
                                                                s_color = d_spec.get('value')
                                                            case 'memoriainterna':
                                                                s_storage = d_spec.get('value')
                        else:
                            print('[ERROR]', 'Unable to find "product" in "Product"')

                        d_sku: dict = d_raw_product.get('sku')
                        if d_sku is not None:
                            s_identification = d_sku.get('id')
                        else:
                            print('[ERROR]', 'Unable to find "sku" in "Product"')
                    else:
                        print('[ERROR]', 'Unable to find "Product" in "initialState", skipping...')
                        continue
                else:
                    print('[ERROR]', 'Unable to find "initialState" in "props", skipping...')
                    continue
            else:
                print('[ERROR]', 'Unable to find "props" in d_json, skipping...')
                self._quit_selenium()
                continue

            s_json_prices = self._send_request(
                f'{self._s_host_api}/api/v2/sku/{s_identification}/price/source/{self._s_marketplace_identifier}' +
                f'?utm_source=undefined&take=undefined&device_type=DESKTOP'
            )

            # tiow = open('_html.html', 'r', encoding='utf-8')
            # s_json_prices = tiow.read()
            # tiow.close()

            s_json_prices = findall(
                pattern=r'<pre style="word-wrap: break-word; white-space: pre-wrap;">(.+?)</pre>',
                string=s_json_prices,
                flags=DOTALL
            )[0]
            d_raw_price = None
            s_seller_id = None
            if s_json_prices is not None:
                d_raw_price = self._parse_dictionary(s_json_prices)

            if d_raw_price is not None:
                b_available = d_raw_price.get('buyButtonEnabled')

                d_sell_price = d_raw_price.get('sellPrice')
                if d_sell_price is not None:
                    s_seller_id = d_sell_price.get('sellerId')

                l_installment_options: list[dict] = d_raw_price.get('installmentOptions')
                if l_installment_options is not None:
                    if len(l_installment_options) > 0:
                        for d_installment_option in l_installment_options:

                            s_installment_type_1 = d_installment_option.get('type')
                            l_installment_conditions = d_installment_option.get('conditions')
                            if s_installment_type_1 is not None and l_installment_conditions is not None:

                                s_installment_type_1 = unidecode(s_installment_type_1).lower().replace(' ', '')
                                for d_installment_condition in l_installment_conditions:

                                    match s_installment_type_1:
                                        case 'bandeira' | 'outros':
                                            s_installment_name: str = d_installment_condition.get('option')
                                            i_installment_amount = d_installment_condition.get('qtyParcels')
                                            f_installment_fee = d_installment_condition.get('monthlyInterest')
                                            f_installment_price = d_installment_condition.get('totalPrice')
                                            if (
                                                s_installment_name is not None and
                                                i_installment_amount is not None and
                                                f_installment_fee is not None and
                                                f_installment_price is not None
                                            ):
                                                if s_installment_type_1 == 'outros':
                                                    if i_installment_amount == 1:
                                                        f_price_credit = f_installment_price

                                                    if 'sem juros' in s_installment_name.lower():
                                                        i_amount_m_i_n_f = i_installment_amount
                                                        f_price_m_i_n_f = f_installment_price

                                                    if 'com juros' in s_installment_name.lower():
                                                        i_amount_m_i_w_f = i_installment_amount
                                                        f_price_m_i_w_f = f_installment_price
                                                        f_percentage_f_m_i = f_installment_fee

                                                if s_installment_type_1 == 'bandeira':
                                                    if i_installment_amount == 1:
                                                        f_price_cc = f_installment_price

                                                    if 'sem juros' in s_installment_name.lower():
                                                        i_amount_cc_m_i_n_f = i_installment_amount
                                                        f_price_cc_m_i_n_f = f_installment_price

                                                    if 'com juros' in s_installment_name.lower():
                                                        i_amount_cc_m_i_w_f = i_installment_amount
                                                        f_price_cc_m_i_w_f = f_installment_price
                                                        f_percentage_cc_f_m_i = f_installment_fee

                                        case 'pix' | 'boleto':
                                            f_installment_price = d_installment_condition.get('price')
                                            if s_installment_type_1 == 'pix':
                                                f_price_pix = f_installment_price

                                            if s_installment_type_1 == 'boleto':
                                                f_price_ticket = f_installment_price

                l_sellers = d_raw_price.get('sellers')
                if l_sellers is not None:
                    if len(l_sellers) == 1:

                        d_seller = l_sellers[0]
                        s_store = d_seller.get('name')
                    else:
                        if len(l_sellers) > 1:
                            print('[WARNING]', 'Multiple sellers found.')
            else:
                print('[ERROR]', 'Unable to find "ProductPrice" in "initialState", skipping')
                self._quit_selenium()
                continue

            f_delivery_price = None
            if s_identification is not None and s_seller_id is not None and self._s_zipcode is not None:
                print('[INFO]', 'Retrieving delivery data...')
                s_path = f'/api/v2/sku/{s_identification}/freight/seller/{s_seller_id}/zipcode/{self._s_zipcode}/source/PF'
                s_params = '?channel=DESKTOP&orderby=price'
                s_delivery_result = self._send_request(self._s_host_api + s_path + s_params)

                s_delivery_result = findall(
                    pattern=r'<pre style="word-wrap: break-word; white-space: pre-wrap;">(.+?)</pre>',
                    string=s_delivery_result,
                    flags=DOTALL
                )[0]

                if s_delivery_result is not None:

                    d_delivery_result = self._parse_dictionary(s_delivery_result)
                    if d_delivery_result is not None:

                        l_delivery_option = d_delivery_result.get('options')
                        if l_delivery_option is not None:

                            for d_delivery_option in l_delivery_option:

                                f_tmp_delivery_price = d_delivery_option.get('price')
                                if f_delivery_price is not None and f_tmp_delivery_price is not None:

                                    if f_tmp_delivery_price < f_delivery_price:
                                        f_delivery_price = f_tmp_delivery_price

                                if f_delivery_price is None:
                                    f_delivery_price = f_tmp_delivery_price
                else:
                    print('[ERROR]', 'Unable to retrieve shipment data.')

            print('[INFO]', 'Creating product...')
            l_products.append(
                create_product(
                    manufacturer=None,
                    brand=s_brand,
                    ean=None,
                    identification=s_identification,
                    universal_identification=s_universal_identification,
                    name=s_name,
                    universal_name=None,
                    storage=s_storage,
                    color=s_color,
                    marketplace=self._s_marketplace_name,
                    store=s_store,
                    zipcode=self._s_zipcode.replace('-', ''),
                    shipping=f_delivery_price,
                    available=b_available,
                    url=s_link,
                    price_pix=f_price_pix,
                    price_ticket=f_price_ticket,
                    price_credit=f_price_credit,
                    price_m_i_n_f=f_price_m_i_n_f,
                    amount_m_i_n_f=i_amount_m_i_n_f,
                    price_m_i_w_f=f_price_m_i_w_f,
                    amount_m_i_w_f=i_amount_m_i_w_f,
                    percentage_f_m_i=f_percentage_f_m_i,
                    price_credit_cc=f_price_cc,
                    price_cc_m_i_n_f=f_price_cc_m_i_n_f,
                    amount_cc_m_i_n_f=i_amount_cc_m_i_n_f,
                    price_cc_m_i_w_f=f_price_cc_m_i_w_f,
                    amount_cc_m_i_w_f=i_amount_cc_m_i_w_f,
                    percentage_cc_f_m_i=f_percentage_cc_f_m_i
                )
            )

        if len(l_products) == 0:
            print('[ERROR]', 'Products list is empty.')
            self._quit_selenium()
            return None

        self._quit_selenium()

        return l_products

    def _send_request(self, link: str) -> str | None:
        """
        Send request via Selenium
        :param link: Desired URL
        :return: Webpage HTML
        """
        if not link.startswith('https://') and not link.startswith('http://'):
            link = 'https://' + link

        print('[INFO]', f'Sending request to "{link}"')
        try:
            self._chrome_selenium.get(link)
            sleep(self._i_default_waiting_time)
            s_result = self._chrome_selenium.page_source
        except Exception as err:
            print('[ERROR]', 'Unable to send request due to', str(err))
            return None
        try:
            self._chrome_selenium.delete_all_cookies()
        except Exception as err:
            print('[WARNING]', 'Unable to delete cookies due to', str(err))

        return s_result

    def _start_selenium(self, allow_js=False):
        """
        Create new "Chrome" instance
        :param allow_js: Allow JavaScript? (decrease performance)
        """
        print('[INFO]', 'Starting Selenium...')
        l_headers = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.3'
        ]
        try:
            co = ChromeOptions()
            co.add_argument('--no-sandbox')
            co.add_argument('--disable-dev-shm-usage')
            # if not allow_js:
            #     co.add_experimental_option("prefs", {'profile.managed_default_content_settings.javascript': 2})
            co.add_argument(f"user-agent={l_headers[randint(0, len(l_headers) - 1)]}")
            self._chrome_selenium = Chrome(service=Service(ChromeDriverManager().install()),options=co)
        except Exception as err:
            print('[ERROR]', 'Unable to start Selenium due to', str(err))
            return None

    def _quit_selenium(self):
        print('[INFO]', 'Quiting Selenium...')
        try:
            self._chrome_selenium.close()
            self._chrome_selenium.quit()
            self._chrome_selenium = None
        except Exception as err:
            print('[ERROR]', 'Unable to quit selenium due to', str(err))

    @staticmethod
    def _get_date() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _parse_dictionary(json: str) -> dict | None:
        print('[INFO]', 'Parsing JSON to dictionary...')
        try:
            d_result = loads(json)
        except Exception as err:
            print('[ERROR]', 'Unable to parse JSON to dictionary due to', str(err))
            return None

        return d_result
