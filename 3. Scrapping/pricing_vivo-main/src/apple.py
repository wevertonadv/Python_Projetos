from datetime import datetime
from http.client import HTTPSConnection
from ssl import create_default_context
from time import sleep
from re import findall, DOTALL
from random import shuffle
from pandas import DataFrame
from modules.string_normalizer import normalize_string
from modules.product import create_product
from modules.xlsx import get_names
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))

HOST = 'www.apple.com'
MAX_ATTEMPTS = 3
SLEEP_SECS = 3


def get_date() -> str:
    """
    Returns current date following pattern "%d-%m-%Y %H:%M"
    :return: String containig current date
    """
    return datetime.now().strftime("%d-%m-%Y %H:%M")


def send_request(headers: dict, host: str, path_and_params: str) -> (int | None, str | None):
    """
    Send "GET" request, do not use prefix "http://" or "https://"
    :param headers: Headers send in request
    :param host: Host of the request
    :param path_and_params: ONLY path (/something) and params (?something&something=something)
    :return:
    """
    log.info(f'Sending request to: "https://{host + path_and_params}"...')

    try:
        hc = HTTPSConnection(
            host=host,
            context=create_default_context()
        )
        hc.request(
            method='GET',
            url=path_and_params,
            headers=headers
        )
        hr = hc.getresponse()
        i_response_code = hr.status
        s_response_data = hr.read().decode('utf-8')
    except Exception as err:
        log.error(f'Unable to send request due to {err}')
        return None, None

    return i_response_code, s_response_data


def get_results_from_search(products_names: list[str]) -> list:
    """
    Extract products paths from search HTML
    :param products_names: Desired products names
    :return: List of paths to products
    """
    d_found_results = {}

    shuffle(products_names)

    for i_products_names_index in range(0, len(products_names), 1):
        s_product_name = products_names[i_products_names_index]

        if i_products_names_index > 0:
            log.info(f'({i_products_names_index + 1} of {len(products_names)}) Retrieving results for product "{s_product_name}"')

        s_normalized_product_name = normalize_string(s_product_name, '+')
        if s_normalized_product_name is None:
            continue

        s_path = '/br/shop/searchresults/internalmvc'
        s_params = f'?find={s_normalized_product_name}&sel=explore&src=serp&tab=explore'
        i_attempt_counter = 1
        while True:
            i_response_code, s_response_data = send_request(
                headers={},
                host=HOST,
                path_and_params=s_path + s_params
            )

            sleep(SLEEP_SECS)

            if i_response_code is not None:
                match i_response_code:
                    case 200:
                        break
                    case _:
                        log.warning(f'Server returned unknown code: {i_response_code}')

            if i_attempt_counter >= MAX_ATTEMPTS:
                log.error('Max retries exceeded.')
                i_response_code = None
                s_response_data = None
                break

            i_attempt_counter += 1

        if i_response_code is None or s_response_data is None:
            continue

        log.info('Finding links...')

        l_regex_results = findall(
            pattern=r'"value":{"title":"(.+?)","link":{"text":".+?","url":"(.+?)"',
            string=s_response_data,
            flags=DOTALL
        )

        if len(l_regex_results) == 0:
            log.error('No links found, skipping...')
            continue

        for t_regex_result in l_regex_results:
            if f'https://{HOST}/br/shop/buy-' in t_regex_result[1]:

                # i_similarity_percentage = combined_similarity(s_product_name, t_regex_result[0]) * 100

                # if i_similarity_percentage > 20:
                d_found_results.update(
                    {
                        t_regex_result[1]: 0
                    }
                )

    return list(d_found_results.keys())


# Apple dont have default HTML/JSON for products
def handle_product_page_2(html: str) -> list[dict] | None:
    l_regex_result_price_and_part_number = findall(
        pattern=r'"price":(?: |)([0-9]+\.[0-9]+).+?"sku":(?: |)"(.+?)"',
        string=html,
        flags=DOTALL
    )

    if len(l_regex_result_price_and_part_number) == 0:
        log.error(f'Unable to find price and part number.')
        return None

    l_regex_result_sku_and_part_number = findall(
        pattern=r'"parentPartNumber":.*?"(.+?)".+?"partNumber":.*?"(.+?)"',
        string=html,
        flags=DOTALL
    )

    if len(l_regex_result_sku_and_part_number) == 0:
        log.error(f'Unable to find sku and part number.')
        return None

    l_regex_result_sku_and_name = findall(
        pattern=r'"sku":(?: |)"(.....)".+?"name":.*?"(.+?)"',
        string=html,
        flags=DOTALL
    )

    if len(l_regex_result_sku_and_part_number) == 0:
        log.error(f'Unable to find sku and name.')
        return None

    d_price_and_part_number = {}
    d_sku_and_name = {}

    for l_result_item_1 in l_regex_result_price_and_part_number:
        d_price_and_part_number.update(
            {
                l_result_item_1[1]: l_result_item_1[0]
            }
        )

    for l_result_item_1 in l_regex_result_sku_and_name:
        d_sku_and_name.update(
            {
                l_result_item_1[0]: l_result_item_1[1]
            }
        )

    d_return_part_products = {}
    for l_result_item_1 in l_regex_result_sku_and_part_number:
        s_sku = l_result_item_1[0]
        s_pn = l_result_item_1[1]
        s_name = d_sku_and_name.get(s_sku)
        s_price = d_price_and_part_number.get(s_pn)

        if s_name is None:
            log.error(f'Unable to find name for product {s_pn}, skipping')
            continue

        if s_name is None:
            log.error(f'Unable to find price for product {s_pn}, skipping')
            continue

        d_return_part_products.update(
            {
                s_pn: {
                    'base_part_number': s_sku,
                    'price': s_price,
                    'name': s_name,
                    'part_number': s_pn,
                }
            }
        )

    return list(d_return_part_products.values())


def handle_product_page(html: str) -> list[dict] | None:
    log.info('Handling product HTML...')

    l_regex_results = findall(
        pattern=r'"products":(\[{.+?(?:"sku"|"dimensionColor").+?}\])',
        string=html,
        flags=DOTALL
    )

    if len(l_regex_results) < 2 or len(l_regex_results) > 2:
        if len(l_regex_results) < 2:
            log.warning('RegEx did not find enough products data, trying again.')
            return handle_product_page_2(html)

        if len(l_regex_results) > 2:
            log.warning('RegEx found garbage with products data.')

    s_part_product_data_1 = l_regex_results[0]
    s_part_product_data_2 = l_regex_results[1]

    l_regex_part_product_data_1 = findall(
        pattern='"sku":"(.+?)","price":{"fullPrice":(.+?)},.+?"name":"(.+?)"',
        string=s_part_product_data_1,
        flags=DOTALL
    )
    if len(l_regex_part_product_data_1) == 0:
        log.error('RegEx did not find products base sku, price and name.')
        return None

    l_regex_part_product_data_2 = findall(
        pattern=(
            '"partNumber":"(.+?)".+?' +
            '"basePartNumber":"(.+?)".+?' +
            '"fullPrice":"(.+?)".+?' +
            '"dimensionCapacity":"(.+?)".+?' +
            '"dimensionColor":"(.+?)"'
        ),
        string=str(s_part_product_data_2),
        flags=DOTALL
    )
    if len(l_regex_part_product_data_2) == 0:
        log.error('RegEx did not find products part number, price identifier, storage and color.')
        return None

    d_mixed_data = {}

    for t_regex_part_product_data_1 in l_regex_part_product_data_1:
        d_mixed_data.update(
            {
                t_regex_part_product_data_1[0]: {
                    'base_part_number': t_regex_part_product_data_1[0],
                    'price': t_regex_part_product_data_1[1],
                    'name': t_regex_part_product_data_1[2]
                }
            }
        )

    for l_regex_part_product_data_2 in l_regex_part_product_data_2:
        d_regex_part_product_data_1: dict = d_mixed_data.get(l_regex_part_product_data_2[1])
        if d_regex_part_product_data_1 is not None:
            d_regex_part_product_data_1.update(
                {
                    'part_number': l_regex_part_product_data_2[0],
                    'price_identifier': l_regex_part_product_data_2[2],
                    'storage': l_regex_part_product_data_2[3],
                    'color': l_regex_part_product_data_2[4]
                }
            )
            d_mixed_data[l_regex_part_product_data_2[1]].update(d_regex_part_product_data_1)
        else:
            log.error(f'Base part number "{l_regex_part_product_data_2[1]}" not found.')

    return list(d_mixed_data.values())
# -------------------------------------------------------------


def get_products(links: list[str]) -> list[dict]:
    """
    Retrieve products from paths list
    :param links: Path list to products
    :return: Products list
    """
    l_return_products = []

    shuffle(links)

    for i_link_index in range(0, len(links), 1):
        s_link = links[i_link_index]

        log.info(f'({i_link_index + 1} of {len(links)}). Retrieving products from link: "{s_link}"')

        s_link = s_link.removeprefix('https://' + HOST)
        i_attempt_counter = 1
        while True:
            i_response_code, s_response_data = send_request(
                headers={},
                host=HOST,
                path_and_params=s_link
            )

            sleep(SLEEP_SECS)

            if i_response_code is not None:
                match i_response_code:
                    case 200:
                        break
                    case _:
                        log.warning(f'Server returned unknown status code: {i_response_code}')

            if i_attempt_counter >= MAX_ATTEMPTS:
                log.error('Max attempts exceeded.')
                i_response_code = None
                s_response_data = None
                break

            i_attempt_counter += 1

        if i_response_code is None or s_response_data is None:
            continue

        l_part_products = handle_product_page(s_response_data)

        if l_part_products is None:
            continue

        if len(l_part_products) > 0:
            shuffle(l_part_products)

        for i_part_product in range(0, len(l_part_products), 1):
            d_part_product = l_part_products[i_part_product]

            log.info(f'({i_part_product + 1} of {len(l_part_products)}) Retrieving credit options for product "{d_part_product.get("part_number")}"...')

            s_path = f'/br/shop/installmentsOverlayData/{d_part_product.get("part_number")}'
            s_params = f'?ca={d_part_product.get("price")}'
            i_attempt_counter = 1
            while True:
                i_response_code, s_response_data = send_request(
                    headers={},
                    host=HOST,
                    path_and_params=s_path + s_params
                )

                sleep(SLEEP_SECS)

                if i_response_code is not None:
                    match i_response_code:
                        case 200:
                            break
                        case _:
                            log.warning(f'Server returned unknown status code: {i_response_code}')

                if i_attempt_counter >= MAX_ATTEMPTS:
                    log.error('Max attempts exceeded.')
                    i_response_code = None
                    s_response_data = None
                    break

                i_attempt_counter += 1

            if i_response_code is None or s_response_data is None:
                continue

            l_regex_result_installments = findall(
                pattern=r'"text".+?"([0-9]+).+?"',
                string=s_response_data,
                flags=DOTALL
            )

            f_price_credit = d_part_product.get("price")
            f_price_m_i_n_f = None
            i_max_installment = None

            if len(l_regex_result_installments) == 0:
                log.error('No installments found.')
            else:

                l_regex_result_max_installment_value = findall(
                    pattern=r'"text".+?"([0-9]+).+?([0-9]+.[0-9]+.[0-9]+)"',
                    string=s_response_data,
                    flags=DOTALL

                )
                for t_regex_result_max_installment_value in l_regex_result_max_installment_value:
                    try:
                        i_installment_index = int(t_regex_result_max_installment_value[0])
                        f_installment_value = float(
                            t_regex_result_max_installment_value[1].replace('.', '').replace(',', '.')
                        )

                        if f_price_m_i_n_f is None:
                            f_price_m_i_n_f = f_installment_value
                        elif (i_installment_index * f_installment_value) > f_installment_value:
                            f_price_m_i_n_f = i_installment_index * f_installment_value

                    except Exception as err:
                        log.error(f'Unable to find max installment value due to {err}')
                        f_price_m_i_n_f = None

                l_regex_result_discount_percentage = findall(
                    pattern=r'"text".+?".*?1.*?x.+?R\$.*?([0-9]+\.[0-9]+,[0-9]+).+?\(.+?"',
                    string=s_response_data,
                    flags=DOTALL
                )
                if len(l_regex_result_discount_percentage) == 1:
                    f_price_credit = l_regex_result_discount_percentage[0]
                else:
                    log.error('Unable to find correct credit/ticket price.')

                for s_regex_result_installment in l_regex_result_installments:
                    if i_max_installment is None:
                        i_max_installment = int(s_regex_result_installment)
                    elif int(s_regex_result_installment) > i_max_installment:
                        i_max_installment = int(s_regex_result_installment)

            l_return_products.append(
                create_product(
                    manufacturer=None,
                    brand=None,
                    ean=None,
                    identification=d_part_product.get('base_part_number'),
                    universal_identification=d_part_product.get('part_number'),
                    name=d_part_product.get('name'),
                    universal_name=None,
                    storage=d_part_product.get('storage'),
                    color=d_part_product.get('color'),
                    marketplace='Apple',
                    store=None,
                    zipcode=None,
                    shipping=None,
                    available=None,
                    url='https://' + HOST + s_link,
                    price_pix=None,
                    price_ticket=float(str(f_price_credit).replace(',', '').replace(',', '.'))*1000,
                    price_credit=float(str(f_price_credit).replace(',', '').replace(',', '.'))*1000,
                    price_m_i_n_f=str(f_price_m_i_n_f).replace(',', '').replace(',', '.'),
                    amount_m_i_n_f=str(i_max_installment).replace(',', '').replace(',', '.'),
                    price_m_i_w_f=None,
                    amount_m_i_w_f=None,
                    percentage_f_m_i=None,
                    price_credit_cc=None,
                    price_cc_m_i_n_f=None,
                    amount_cc_m_i_n_f=None,
                    price_cc_m_i_w_f=None,
                    amount_cc_m_i_w_f=None,
                    percentage_cc_f_m_i=None
                )
            )

    return l_return_products


if __name__ == '__main__':
    log.info(f' Starting ({get_date()}) '.center(70, '-'))

    # l_keywords = get_names('assets/lista devices.xlsx')
    gcs_manager = GCSParquetManager()
    l_keywords = gcs_manager.ler_coluna_excel()

    shuffle(l_keywords)

    l_results = get_results_from_search(l_keywords[:40])

    l_products = get_products(l_results)

    print()
    # print('[INFO]', 'Saving xlsx...')
    # DataFrame(l_products).to_excel(excel_writer='_products_apple.xlsx', index=False, encoding='utf-8-sig')
    
    df = DataFrame(l_products)
    log.info('Uploading to bucket...')
    
    # Upload to bucket

    # Formatar a data e hora no formato desejado
    pasta = "extracoes/"
    loja = "apple"
    agora = datetime.now()
    nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"
    
    gcs_manager.upload_parquet(df, nome_arquivo)
    
    log.info(f' Done ({get_date()}) '.center(70, '-'))
