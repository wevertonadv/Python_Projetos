from http.client import HTTPSConnection
from random import shuffle
from datetime import datetime
from re import findall, DOTALL
from time import sleep
from json import loads, JSONDecodeError
from ssl import create_default_context
from pandas import DataFrame
from modules.product import create_product
from modules.verify_names import combined_similarity
from modules.xlsx import get_names
from modules.string_normalizer import normalize_string
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))
MAX_ATTEMPTS = 3
MAX_PAGES = 1
PAUSE_SEC = 3
BASE_HEADERS = {
    'authority': 'apigw.fastshop.com.br',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'dnt': '1',
    'origin': 'https://www.fastshop.com.br',
    'referer': 'https://www.fastshop.com.br/',
    'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/119.0.0.0 ' +
        'Safari/537.36 ' +
        'Edg/119.0.0.0',
}
ZIPCODE = '01021-100'


def get_date() -> str:
    '''
    Return current date and time as string.
    '''
    return datetime.now().strftime("%d-%m-%Y %H:%M")


def send_request(host: str, path: str, headers: dict) -> (int | None, str | None):
    '''
    Send "GET" request.
    :param host: Webpage host (without https://) (www.something.com)
    :param path: Path and params (/something?something=something&something)
    :param headers: Headers for this request
    :return: request status code and requested data or (if something goes wrong) None and None
    '''
    log.info('Sending request to: "https://' + host + path + '"')

    try:
        hc = HTTPSConnection(
            host=host,
            context=create_default_context()
        )
        hc.request(
            method='GET',
            url=path,
            headers=headers
        )
        hr = hc.getresponse()
    except Exception as err:
        log.error(f'Unable to send request due to {err}')
        return None, None

    i_response_status = hr.status
    s_response_data = hr.read().decode('utf-8')

    return i_response_status, s_response_data


def parse_dictionary(data: str) -> dict | None:
    '''
    Parse "data" to Python dictionary
    :param data: JSON string to parse
    :return: JSON as dictionary
    '''
    log.info('Parsing data to dictionary...')

    try:
        data = loads(data)
    except JSONDecodeError as err:
        log.error(f'Unable to parse string to dictionary due to {err}')
        return None
    return data


def get_results_from_search(keywords: list[str]) -> list[dict] | None:
    '''
    Retrieve results from website search
    :param keywords: List of desired product names
    :return: List of dictionaries containing partial products info
    '''
    d_results = {}

    for i_keyword_index in range(0, len(keywords), 1):
        s_keyword = keywords[i_keyword_index]

        log.info(f'({i_keyword_index + 1} of {len(keywords)}). Retrieving results for keyword:  {s_keyword}')

        s_path = '/product/v0/regionalized/v1/catalog/by-search'
        s_params = f'?terms={normalize_string(s_keyword, "%20")}&page=1'
        i_attempt_counter = 1
        # This loop is necessary to automatically retries if something goes wrong
        while True:
            i_response_code, s_response_data = send_request(
                host=BASE_HEADERS['authority'],
                path=s_path + s_params,
                headers=BASE_HEADERS
            )

            sleep(PAUSE_SEC)

            if i_response_code is not None:
                match i_response_code:
                    case 200:
                        break
                    case 302:
                        # 302 is default code for redirects, so, handle it
                        log.warning('Server returned redirect, following it.')
                        l_category_id_results = findall(
                            pattern='"link":.+?/c/(.+?)/',
                            string=s_response_data,
                            flags=DOTALL
                        )
                        if len(l_category_id_results) == 1:
                            s_path = '/product/v0/regionalized/v1/catalog/by-category'
                            s_params = f'?category={l_category_id_results[0]}&pageNumber=1'
                        else:
                            if len(l_category_id_results) == 0:
                                log.error('RegEx did not find category id.')

                            if len(l_category_id_results) > 1:
                                log.error('RegEx found more than one category id.')

                    case _:
                        log.warning(f'Server returned unknown status code {i_response_code}')

            if i_attempt_counter >= MAX_ATTEMPTS:
                i_response_code = None
                s_response_data = None
                log.error('Max attempts exceeded.')
                break

            i_attempt_counter += 1

        if i_response_code is None and s_response_data is None:
            continue

        d_product = parse_dictionary(s_response_data)
        if d_product is None:
            continue

        # Retrieve products list
        l_products: list[dict] = d_product.get('products')
        if l_products is None:
            log.error('Unable to find "products" list in server response.')
            continue

        for d_raw_product in l_products:

            s_name = d_raw_product.get('name')
            if s_name is None:
                s_name = d_raw_product.get('shortDescription')
                if s_name is None:
                    log.error('Unable to find product name, skipping...')
                    continue

            # Verify names similarity
            i_names_similarity_percentage = combined_similarity(
                s_keyword,
                s_name
            ) * 100
            if i_names_similarity_percentage < 50:
                continue

            s_product_id = d_raw_product.get('id')
            if s_product_id is None:
                s_product_id = d_raw_product.get('partNumber')
                if s_product_id is None:
                    log.error('Unable to find "id" in "products", skipping...')
                    continue

            l_product_sku = d_raw_product.get('skus')
            s_product_sku = None
            if l_product_sku is not None:
                if len(l_product_sku) > 0 or len(l_product_sku) < 2:
                    d_product_sku: dict = l_product_sku[0]
                    s_product_sku = d_product_sku.get('sku')

            s_storage = None
            s_color = None
            d_details: dict = d_raw_product.get('details')
            if d_details is not None:

                l_storage = d_details.get('Armazenamento de Dados')
                if l_storage is not None:
                    if len(l_storage) == 1:
                        s_storage = l_storage[0]

                l_color = d_details.get('Cor')
                if l_color is not None:
                    if len(l_color) == 1:
                        s_color = l_color[0]

            s_url = d_raw_product.get('url')
            if s_url is None:
                s_url = f'https://www.fastshop.com.br/web/p/d/{s_product_id}/{normalize_string(s_name, "-")}'

            d_results.update(
                {
                    s_product_id: {
                        'id': s_product_id,
                        'sku': s_product_sku,
                        'url': s_url,
                        'storage': s_storage,
                        'color': s_color
                    }
                }
            )

    l_tmp_results = list(d_results.values())

    if len(l_tmp_results) == 0:
        log.error('Result list is empty.')
        return None

    return l_tmp_results


def get_products(products_identifications: list[dict]) -> list[dict] | None:
    """
    Retrieve products data
    :param products_identifications: Part products data
    :return: Products list
    """
    l_products = []

    shuffle(products_identifications)

    for i_pi_index in range(0, len(products_identifications), 1):
        d_pi = products_identifications[i_pi_index]

        log.info(f'({i_pi_index + 1} of {len(products_identifications)}) Retrieving product...')

        log.info('Retrieving product data...')
        s_path = '/product/v0/regionalized/v1/catalog/byPartNumber'
        s_params = '?partNumber=' + str(d_pi['id'])
        i_attempt_counter = 1
        s_product_data = None
        # This loop is necessary to automatically retries if something goes wrong
        while True:
            i_response_result, s_response_data = send_request(
                host=BASE_HEADERS['authority'],
                path=s_path + s_params,
                headers=BASE_HEADERS
            )

            sleep(PAUSE_SEC)

            if i_response_result is not None:
                match i_response_result:
                    case 200:
                        break

            if i_attempt_counter >= MAX_ATTEMPTS:
                i_response_result = None
                s_response_data = None
                log.error('Max attempts exceeded.')
                break

            i_attempt_counter += 1

        if i_response_result is None and s_response_data is None:
            continue

        s_product_data = s_response_data
        d_product_data = parse_dictionary(s_product_data)
        if d_product_data is None:
            log.error('Unable to parse dictionary product data, skipping...')
            continue

        if d_pi.get('sku') is None:
            log.warning('Sku not found in search result, finding this one...')

            l_skus = d_product_data.get('skus')
            if l_skus is None:
                log.error('Unable to find "skus" list, skipping...')
                continue

            if len(l_skus) != 1:
                if len(l_skus) < 1:
                    log.error('Unable to find sku, "skus" list is empty, skipping...')
                    continue
                if len(l_skus) > 1:
                    log.error('Unable to find sku, "skus" list has more than one item, skipping...')
                    continue

            d_pi['sku'] = l_skus[0].get('partNumber')

            if d_pi['sku'] is None:
                log.error('Unable to find "partNumber"(sku) in "skus" list, skipping...')
                continue

        log.info('Retrieving product price...')
        s_path = '/price/v0/management/price-promotion/price/detail'
        s_params = f"?sku={str(d_pi['sku'])}&store=fastshop&channel=webapp"
        i_attempt_counter = 1
        s_product_price = None
        # This loop is necessary to automatically retries if something goes wrong
        while True:
            i_response_result, s_response_data = send_request(
                host=BASE_HEADERS['authority'],
                path=s_path + s_params,
                headers=BASE_HEADERS
            )

            sleep(PAUSE_SEC)

            if i_response_result is not None:
                match i_response_result:
                    case 200:
                        break

            if i_attempt_counter >= MAX_ATTEMPTS:
                i_response_result = None
                s_response_data = None
                log.error('Max attempts exceeded.')
                break

            i_attempt_counter += 1

        if i_response_result is None and s_response_data is None:
            continue

        s_product_price = s_response_data
        d_product_price = parse_dictionary(s_product_price)
        if d_product_price is None:
            log.error('Unable to parse dictionary product price, skipping...')
            continue

        f_price_pix = None
        f_price_ticket = None
        f_price_credit = None
        f_price_cc = None
        f_price_m_i_n_f = None
        i_amount_m_i_n_f = None
        f_price_m_i_w_f = None
        i_amount_m_i_w_f = None
        f_percentage_f_m_i = None

        f_price_cc_m_i_n_f = None
        i_amount_cc_m_i_n_f = None
        f_price_cc_m_i_w_f = None
        i_amount_cc_m_i_w_f = None
        f_percentage_cc_f_m_i = None

        f_price_for_shipment = None

        l_product_price: list[dict] = d_product_price.get('result')
        if l_product_price is not None:
            if len(l_product_price) == 1:

                d_payment_methods: dict = l_product_price[0]
                l_payment_methods = d_payment_methods.get('paymentMethods')
                if l_payment_methods is not None:
                    if len(l_payment_methods) > 0:

                        for d_payment_method in l_payment_methods:
                            s_payment_method_code:str = d_payment_method.get('paymentMethodCode')
                            if s_payment_method_code is not None:
                                match s_payment_method_code.upper():
                                    case 'PIX':
                                        f_price_pix = d_payment_method.get('value')

                                    case 'INVOICE':
                                        f_price_ticket = d_payment_method.get('value')

                                    case 'CREDIT_CARD_DEFAULT' | 'FAST_SHOP_PAY':
                                        f_price_for_shipment = d_payment_method.get('value')
                                        l_installments = d_payment_method.get('installments')
                                        if l_installments is not None:
                                            if len(l_installments) > 0:

                                                if s_payment_method_code == 'CREDIT_CARD_DEFAULT':
                                                    f_price_credit = l_installments[0].get('installmentValue')
                                                if s_payment_method_code == 'FAST_SHOP_PAY':
                                                    f_price_cc = l_installments[0].get('installmentValue')

                                                for d_installment in l_installments:
                                                    f_month_fee = d_installment.get('interestMonth')
                                                    f_total = d_installment.get('total')
                                                    i_installment_number = d_installment.get('installmentNumber')
                                                    b_has_interest = d_installment.get('hasInterest')

                                                    if s_payment_method_code == 'CREDIT_CARD_DEFAULT':
                                                        if not b_has_interest:
                                                            f_price_m_i_n_f = f_total
                                                            i_amount_m_i_n_f = i_installment_number
                                                        else:
                                                            f_price_m_i_w_f = f_total
                                                            i_amount_m_i_w_f = i_installment_number
                                                            f_percentage_f_m_i = f_month_fee

                                                    if s_payment_method_code == 'FAST_SHOP_PAY':
                                                        if not b_has_interest:
                                                            f_price_cc_m_i_n_f = f_total
                                                            i_amount_cc_m_i_n_f = i_installment_number
                                                        else:
                                                            f_price_cc_m_i_w_f = f_total
                                                            i_amount_cc_m_i_w_f = i_installment_number
                                                            f_percentage_cc_f_m_i = f_month_fee
                            else:
                                log.error('Unable to find prices, "paymentMethodCode" not found.')

                    else:
                        log.error('Unable to find prices, "paymentMethods" list len is empty.')

                else:
                    log.error('Unable to find prices, "paymentMethods" list len is None.')

            else:
                log.error('Unable to find prices, "result" list len is not 1.')

        else:
            log.error('Unable to find prices, "result" list len is None, skipping...')
            continue

        log.info('Retrieving shipment...')
        s_path = '/shipping/v0/shippings/product'
        s_params = f"?sku={d_pi['sku']}&zipCode={ZIPCODE}&primePlanId=&price={f_price_for_shipment}"
        while True:
            i_response_result, s_response_data = send_request(
                host=BASE_HEADERS['authority'],
                path=s_path + s_params,
                headers=BASE_HEADERS
            )

            sleep(PAUSE_SEC)

            if i_response_result is not None:
                match i_response_result:
                    case 200:
                        break
                    case 404:
                        i_response_result = None
                        s_response_data = None
                        break

            if i_attempt_counter >= MAX_ATTEMPTS:
                i_response_result = None
                s_response_data = None
                log.error('Max attempts exceeded.')
                break

            i_attempt_counter += 1

        f_delivery_fee = None
        if i_response_result is not None and s_response_data is not None:
            d_shipment_data = parse_dictionary(s_response_data)
            if d_shipment_data is not None:
                l_shipment_data = d_shipment_data.get('logisticService')
                if l_shipment_data is not None:
                    for d_service in l_shipment_data:
                        if f_delivery_fee is None:
                            f_delivery_fee = d_service.get('freightValue')
                        elif d_service.get('freightValue') is not None and f_delivery_fee is not None:
                            if d_service.get('freightValue') < f_delivery_fee:
                                f_delivery_fee = d_service.get('freightValue')

        else:
            log.error('Unable to retrieve delivery fee.')

        if d_product_data.get('marketPlace') and d_product_data.get('marketPlaceText') is not None:
            s_store = d_product_data.get('marketPlaceText')
        elif not d_product_data.get('marketPlace'):
            s_store = 'Fast Shop'
        s_name = d_product_data.get('shortDescription')
        s_url: str = d_pi['url']
        if s_url is None or s_url == '':
            s_url = f"https://www.fastshop.com.br/web/p/d/{str(d_pi['id'])}/{normalize_string(s_name, '-')}"
        if 'percycle' in s_url:
            s_url = f"https://www.fastshop.com.br/web/p/d/{str(d_pi['id'])}/{normalize_string(s_name, '-')}"

        l_products.append(
            create_product(
                manufacturer=d_product_data.get('manufacturer'),
                brand=None,
                ean=None,
                identification=d_pi['sku'],
                universal_identification=None,
                name=s_name,
                universal_name=None,
                storage=d_pi['storage'],
                color=d_pi['color'],
                marketplace='Fast Shop',
                store=s_store,
                zipcode=ZIPCODE.replace('-', ''),
                shipping=f_delivery_fee,
                available=d_product_data.get('buyable'),
                url=s_url,
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

    return l_products


if __name__ == '__main__':
    log.info(f' Starting ({get_date()}) '.center(70, '-'))

    # l_keywords = get_names('assets/lista devices.xlsx')
    gcs_manager = GCSParquetManager()
    l_keywords = gcs_manager.ler_coluna_excel()

    shuffle(l_keywords)

    l_results_1 = get_results_from_search(l_keywords)

    l_products = get_products(l_results_1)

    log.info('Saving xlsx...')
    # DataFrame(l_products).to_excel(excel_writer='_products_fastshop.xlsx', index=False, encoding='utf-8-sig')
    df = DataFrame(l_products)
    # Upload to bucket

    # Formatar a data e hora no formato desejado
    pasta = "extracoes/"
    loja = "fastshop"
    agora = datetime.now()
    nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"
    
    gcs_manager.upload_parquet(df, nome_arquivo)
    
    log.info(f' Done ({get_date()}) '.center(70, '-'))
