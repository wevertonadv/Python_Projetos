from datetime import datetime
from re import findall, sub, DOTALL
from random import shuffle, randint
from time import sleep
from unidecode import unidecode
from pandas import DataFrame
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from seleniumwire.webdriver import Chrome
from modules.string_normalizer import normalize_string
from modules.product import create_product
from modules.verify_names import combined_similarity
from modules.xlsx import get_names
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))
HOST = 'www.amazon.com.br'
SLEEP_SECS = 3
MAX_ATTEMPTS = 3
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
]


def get_date() -> str:
    """
    Returns current date following pattern "%d-%m-%Y %H:%M"
    :return: String containig current date
    """
    return datetime.now().strftime("%d-%m-%Y %H:%M")


def set_selenium(apply_cookies=False) -> Chrome | None:
    """
    Create new "Chrome" instance
    :param apply_cookies: Apply zipcode cookies
    :return: New "Chrome" instance
    """
    log.info('Setting Selenium...')
    try:
        co = ChromeOptions()
        co.add_argument('--no-sandbox')
        co.add_argument('--disable-dev-shm-usage')
        # co.add_experimental_option("prefs", {'profile.managed_default_content_settings.javascript': 2})
        co.add_argument(f"user-agent={USER_AGENTS[randint(0, len(USER_AGENTS) - 1)]}")
        c_selenium = Chrome(service=Service(ChromeDriverManager().install()),options=co)
        if apply_cookies:
            c_selenium.get('https://' + HOST)
            c_selenium.add_cookie(
                {
                    "domain": ".amazon.com.br",
                    "expiry": 1734806127,
                    "httpOnly": False,
                    "name": "ubid-acbbr",
                    "path": "/",
                    "sameSite": "Lax",
                    "secure": True,
                    "value": "133-3096773-4611353"
                }
            )
            c_selenium.add_cookie(
                {
                    "domain": ".amazon.com.br",
                    "expiry": 1737830127,
                    "httpOnly": False,
                    "name": "session-id",
                    "path": "/",
                    "sameSite": "Lax",
                    "secure": True,
                    "value": "137-0760636-8511050"
                }
            )

    except Exception as err:
        log.error(f'Unable to create Selenium due to {err}')
        return None
    return c_selenium


def quit_selenium(selenium: Chrome):
    log.info('Quiting Selenium...')
    selenium.close()
    selenium.quit()


def send_request_with_selenium(selenium: Chrome, url: str) -> str | None:
    """
    Send request via Selenium
    :param selenium: Chrome used to send request
    :param url: Desired url
    :return: Webpage as HTML or None if something goes wrong
    """
    if not url.startswith('https://'):
        url = 'https://' + url

    log.info(f'Invoking selenium on "{url}"...')

    try:
        sleep(SLEEP_SECS)
        selenium.get(url=url)
        s_html = selenium.page_source
        l_cookies = selenium.get_cookies()
        for d_cookie in l_cookies:
            s_name = d_cookie.get('name')
            if s_name is not None:
                # Only delete non-zipcode cookies.
                if s_name != 'ubid-acbbr' and s_name != 'session-id':
                    selenium.delete_cookie(s_name)
    except Exception as err:
        log.error(f'Unable to complete request due to selenium error: {err}')
        return None

    if s_html is None:
        log.error('Selenium returned empty HTML.')

    if s_html.startswith('<html dir="ltr" lang="en">'):
        log.error('Selenium has blocked.')
        return None

    return s_html


def get_results_from_search(keywords: list[str]) -> list[str]:
    """
    Get products ids from search
    :param keywords: Product name list
    :return: List of product id
    """
    l_return_results = []

    c_selenium = set_selenium()

    shuffle(keywords)

    for i_keyword_index in range(0, len(keywords), 1):
        s_keyword = keywords[i_keyword_index]

        log.info(f'({i_keyword_index + 1} of {len(keywords)}) Retrieving results for "{s_keyword}"...')

        s_path = '/s'
        s_params = '?k=' + normalize_string(s_keyword, '+')
        i_attempt_counter = 1
        while True:
            s_html = send_request_with_selenium(c_selenium, HOST + s_path + s_params)

            if s_html is not None:
                break

            i_attempt_counter += 1

            log.error('Trying again...')
            quit_selenium(c_selenium)
            sleep(10)
            c_selenium = set_selenium()

            if s_html is not None or i_attempt_counter >= MAX_ATTEMPTS:
                break

            i_attempt_counter += 1

        if s_html is None:
            log.error('Unable to retrieve webpage, skipping...')
            continue

        log.info('Extracting links...')
        l_results_from_html = findall(
            pattern=(
                r'<a class=".+?" href="(.+?)">.*?' +
                r'<span class=".+?">(.+?)</span>.*?' +
                r'</a>'
            ),
            string=s_html,
            flags=DOTALL
        )
        if len(l_results_from_html) == 0:
            log.error('No links found, skipping...')
            sleep(10)
            continue

        log.info('Filtering links...')
        d_results = {}
        for t_result_from_html in l_results_from_html:
            i_similarity_percentage = combined_similarity(s_keyword, t_result_from_html[1]) * 100
            if i_similarity_percentage < 65:
                continue
            l_tmp_results_from_links = findall(
                pattern=r'/.+?/dp/(.+?)/ref=.+?',
                string=t_result_from_html[0],
                flags=DOTALL
            )
            for s_result_from_link in l_tmp_results_from_links:
                d_results.update(
                    {
                        s_result_from_link: 0
                    }
                )

        l_tmp_results = list(d_results.keys())

        l_return_results.extend(l_tmp_results)

    return l_return_results


def find_price_cash(selenium: Chrome) -> float | None:
    """
    Extract ticket/PIX price from HTML
    :param selenium: Product webpage HTML
    :return: Ticket/PIX price
    """
    log.info('Retrieving cash price...')

    try:
        s_price_cash = selenium.find_element(
            by=By.CSS_SELECTOR,
            value='span.a-price-whole'
        ).get_attribute("textContent").strip()
    except Exception as err:
        log.error(f'Unable to retrieve product cash price due to {err}')
        return None

    s_price_cash_fraction = None
    try:
        s_price_cash_fraction = selenium.find_element(
            by=By.CSS_SELECTOR,
            value='span.a-price-fraction'
        ).get_attribute("textContent").strip()
    except Exception as err:
        log.warning(f'Unable to retrieve product price fraction due to {err}')

    try:
        s_tmp_price_cash = str(s_price_cash) + str(s_price_cash_fraction if s_price_cash_fraction is not None else "")
        s_tmp_price_cash = s_tmp_price_cash.replace('.', '').replace(',', '.')
        s_price_cash = float(s_tmp_price_cash)
    except Exception as err:
        log.error(f'Unable to parse price to float due to {err}')
        return None

    if s_price_cash > 10000:
        log.warning(f'Cash prise is "{s_price_cash}", Amazon only allow PIX/Ticket under R$ 10.000,00')
        return None

    return s_price_cash


def find_pix(selenium: Chrome) -> bool:
    """
    Verify if PIX is enabled
    :param selenium: Current "Chrome" at target product webpage
    :return: True if PIX is enabled
    """
    log.info('Retrieving PIX price...')
    try:
        s_content = selenium.find_element(
            by=By.ID,
            value='oneTimePaymentPrice_feature_div'
        ).get_attribute("textContent")
        s_content = s_content.strip().lower()
        if 'pix' in s_content:
            return True
        else:
            return False
    except Exception as err:
        log.error(f'Unable to identify PIX due to {err}')
        return False


def find_price_credit(html: str) -> float | None:
    """
    Extract price for 1x credit card
    :param html: Target product HTML
    :return: Price for 1x credit card
    """
    log.info('Retrieving price...')
    l_results_price = findall(
        pattern=r'"priceAmount":([0-9]+\.[0-9]+),.+?"buyingOptionType":"(.+?)",',
        string=html
    )

    f_price = None

    if len(l_results_price) == 0:
        log.error('Credit price not found.')
        return None
    elif len(l_results_price) == 1:
        try:
            f_price = float(l_results_price[0][0])
        except Exception as err:
            log.error(f'Unable to parse price to float due to {err}')
            return None
    elif len(l_results_price) > 1:
        for t_result_price in l_results_price:
            if 'new' in t_result_price[1].lower():
                try:
                    f_price = float(t_result_price[0])
                except Exception as err:
                    log.error(f'Unable to parse price to float due to {err}')
                    return None

    return f_price


def find_installment(html: str) -> (int | None, float | None, int | None, float | None):
    """
    Extract prices for installment options
    :param html: Target product HTML
    :return: Prices and installment options for credit card.
    """
    log.info('Retrieving installment info...')
    i_max_installment_n_f = None
    f_installment_value_n_f = None
    i_max_installment_w_f = None
    f_installment_value_w_f = None
    l_results_installment = findall(
        pattern=r'<table id="InstallmentCalculatorTable" class=".+?">(.+?)</table>',
        string=html,
        flags=DOTALL
    )

    if len(l_results_installment) == 0:
        log.error('Unable to extract installment data from HTML.')
        return None, None, None, None

    l_results_installment = findall(
        pattern=(
            r'<tr class="[A-Za-z\- ]+">.*?' +
            r'<td class="[A-Za-z\- ]+">([0-9]+)x[ |](.*?)</td>.*?' +
            r'<td class="[A-Za-z\- ]+">R\$.*?</td>.*?' +
            r'<td class="[A-Za-z\- ]+">R\$[^0-9]+(.+?)</td>.*?' +
            r'</tr>'
        ),
        string=l_results_installment[0],
        flags=DOTALL
    )

    if len(l_results_installment) == 0:
        log.error('RegEx did not found installment data.')
        return None, None, None, None

    for t_result_installment in l_results_installment:
        try:
            i_installment_index = int(t_result_installment[0])
        except Exception as err:
            log.error(f'Unable to parse installment index to int due to {err}')
            return None, None, None, None

        try:
            b_installment_with_fee = 'sem' not in t_result_installment[1].lower()
        except Exception as err:
            log.error(f'Unable to determine if installment is with/out fee due to {err}')
            return None, None, None, None

        try:
            f_installment_value = float(t_result_installment[2].replace('.', '').replace(',', '.'))
        except Exception as err:
            log.error('Unable to determine installment value due to '+str(err))
            return None, None, None, None

        if b_installment_with_fee:
            if f_installment_value_w_f is None:
                f_installment_value_w_f = f_installment_value
                i_max_installment_w_f = i_installment_index

            if i_installment_index > i_max_installment_w_f:
                f_installment_value_w_f = f_installment_value
                i_max_installment_w_f = i_installment_index

        else:
            if f_installment_value_n_f is None:
                f_installment_value_n_f = f_installment_value
                i_max_installment_n_f = i_installment_index

            if i_installment_index > i_max_installment_n_f:
                f_installment_value_n_f = f_installment_value
                i_max_installment_n_f = i_installment_index

    return f_installment_value_n_f, i_max_installment_n_f, f_installment_value_w_f, i_max_installment_w_f


def find_seller(selenium: Chrome, html: str) -> str | None:
    """
    Find product seller
    :param selenium: Current Chrome at target product
    :param html: Product webpage HTML
    :return: Seller name
    """
    log.info('Retrieving seller info...')
    l_results_seller = findall(
        pattern=r"id=['|\"]sellerProfileTriggerId['|\"]>(.+?)</a>",
        string=html,
        flags=DOTALL
    )

    if len(l_results_seller) == 0:

        s_merchant_id = selenium.find_element(By.ID, 'merchantID').get_attribute('value')

        if s_merchant_id is not None:
            if s_merchant_id == 'A1ZZFT5FULY4LN':
                return 'Amazon.com.br'
            else:
                log.error('Seller not found.')
                return None

        else:
            log.error('Seller not found.')
            return None

    return l_results_seller[0]


def find_characteristics(html: str) -> (str | None, str | None, str | None, str | None):
    """
    Extract product characteristics from HTML
    :param html: Product webpage HTML
    :return: Product characteristics
    """
    log.info('Retrieving product characteristics...')
    s_storage = None
    s_model = None
    s_color = None
    s_brand = None
    l_results_characteristics = findall(
        pattern=(
            f'<tr>.*?' +
            f'<th class="a-color-secondary a-size-base prodDetSectionEntry">(.+?)</th>.*?' +
            f'<td class="a-size-base prodDetAttrValue">(.+?)</td>.*?' +
            f'</tr>'
        ),
        string=html,
        flags=DOTALL
    )

    if len(l_results_characteristics) == 0:
        log.error('Characteristics not found.')
        return None, None, None, None

    for t_result_characteristic in l_results_characteristics:
        s_characteristic_identification: str = unidecode(t_result_characteristic[0]).lower()
        s_characteristic_value: str = t_result_characteristic[1].strip()
        s_characteristic_value = unidecode(
            sub(
                pattern=r'\n',
                repl='',
                string=s_characteristic_value
            ).strip().replace('&lrm;', '').lower()
        )

        if 'armazenamento' in s_characteristic_identification and 'memoria' in s_characteristic_identification:
            s_storage = s_characteristic_value
        elif 'modelo' in s_characteristic_identification and 'compativeis' not in s_characteristic_identification:
            s_model = s_characteristic_value
        elif 'cor' in s_characteristic_identification:
            s_color = s_characteristic_value
        elif 'marca' in s_characteristic_identification and 'processador' not in s_characteristic_identification:
            s_brand = s_characteristic_value

    return s_storage, s_model, s_color, s_brand


def find_name(html: str) -> str | None:
    """
    Extract product name from HTML
    :param html:  Product webpage HTML
    :return: Product name
    """
    if html is None:
        log.error('HTML is empty.')
        return None

    print('[INFO]', 'Retrieving product name...')
    l_results_name = findall(
        pattern=r'<span id="productTitle" class="[a-z\- ]+">(.+?)</span>',
        string=html,
        flags=DOTALL
    )

    if len(l_results_name) == 0:
        log.error('Product name not found.')
        return None

    if len(l_results_name) > 1:
        log.error('Found many product names.')
        return None

    return l_results_name[0]


def find_delivery_price(html: str) -> float | None:
    """
    Extract delivery price
    :param html: Product webpage HTML
    :return: Product delivery price
    """
    log.info('Extracting delivery price...')
    l_r74934n = findall(
        pattern=(
            r'data-csa-c-delivery-price="(.+?)"'
        ),
        string=html,
        flags=DOTALL
    )

    if len(l_r74934n) == 0:
        log.error('RegEx did not found any delivery price.')
        return None

    f_price = None
    if 'gratis' in unidecode(l_r74934n[0]).lower():
        f_price = 0
    else:
        try:
            f_price = float(
                sub(
                    pattern='[^0-9,]',
                    repl='',
                    string=l_r74934n[0]
                ).replace(',', '.')
            )
        except Exception as err:
            log.error(f'Unable to parse delivery price to float due to {err}')

    return f_price


def get_products(ids: list[str]) -> list[dict]:
    """
    Retrieve products data
    :param ids: Product id list
    :return: List of products
    """
    l_return_products = []

    c_selenium = set_selenium(apply_cookies=True)

    shuffle(ids)

    for i_ids_index in range(0, len(ids), 1):
        s_id = ids[i_ids_index]

        
        log.info(f'({i_ids_index + 1} of {len(ids)}). Retrieving product for id "{s_id}"...')

        s_path = '/dp/' + s_id
        i_attempt_counter = 1
        while True:
            s_html = send_request_with_selenium(c_selenium, HOST + s_path)

            if s_html is not None:
                break

            i_attempt_counter += 1

            log.error('Trying again...')
            quit_selenium(c_selenium)
            sleep(10)
            c_selenium = set_selenium(apply_cookies=True)

            if s_html is not None or i_attempt_counter >= MAX_ATTEMPTS:
                break

            i_attempt_counter += 1

        if s_html is None:
            log.error('Unable to retrieve product webpage, skipping...')
            continue

        f_shipping = find_delivery_price(s_html)

        s_name = find_name(s_html)
        if s_name is None:
            log.info('Trying again...')
            c_selenium.refresh()
            s_name = find_name(s_html)

            if s_name is None:
                quit_selenium(c_selenium)
                sleep(10)
                c_selenium = set_selenium(apply_cookies=True)
                s_html = send_request_with_selenium(c_selenium, HOST + s_path)
                s_name = find_name(s_html)

                if s_name is None:
                    continue

        f_price_credit = find_price_credit(s_html)

        f_price_cash = find_price_cash(c_selenium)

        f_price_pix = None
        if find_pix(c_selenium):
            f_price_pix = f_price_cash

        f_price_m_i_n_f, i_amount_m_i_n_f, f_price_m_i_w_f, i_amount_m_i_w_f = find_installment(s_html)

        s_store = find_seller(c_selenium, s_html)

        s_storage, s_model, s_color, s_brand = find_characteristics(s_html)

        l_return_products.append(
            create_product(
                manufacturer=None,
                brand=s_brand,
                ean=None,
                identification=s_model,
                universal_identification=None,
                name=s_name.strip(),
                universal_name=None,
                storage=s_storage,
                color=s_color,
                marketplace='Amazon',
                store=s_store,
                zipcode='01021-100'.replace('-', ''),
                shipping=f_shipping,
                available=None,
                url='https://' + HOST + s_path,
                price_pix=f_price_pix,
                price_ticket=f_price_cash,
                price_credit=f_price_credit,
                price_m_i_n_f=f_price_m_i_n_f,
                amount_m_i_n_f=i_amount_m_i_n_f,
                price_m_i_w_f=f_price_m_i_w_f,
                amount_m_i_w_f=i_amount_m_i_w_f,
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

    l_results = get_results_from_search(l_keywords)

    l_products = get_products(l_results)

    print()
    # print('[INFO]', 'Saving xlsx...')
    # DataFrame(l_products).to_excel(excel_writer='_products_amazon.xlsx', index=False)
    
    df = DataFrame(l_products)
    log.info('Uploading to bucket...')
    
    # Upload to bucket
    # Formatar a data e hora no formato desejado
    pasta = "extracoes/"
    loja = "amazon"
    agora = datetime.now()
    nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

    
    gcs_manager.upload_parquet(df, nome_arquivo)


    log.info(f' Done ({get_date()}) '.center(70, '-'))
