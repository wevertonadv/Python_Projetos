from datetime import datetime


def _desired_type(desired_type: type, data):
    if desired_type is not None and data is not None:
        try:
            data = desired_type(data)
        except Exception as err:
            print('[ERROR]', f'Unable to parse data "{data}" to desired type "{type(desired_type(0))}" due to', str(err))

    return data


def create_product(
        manufacturer: str,
        brand: str,
        ean: int,
        identification: str,
        universal_identification: str,
        name: str,
        universal_name: str,
        storage: int,
        color: str,
        marketplace: str,
        store: str,
        zipcode: str,
        shipping: float,
        available: bool,
        url: str,

        price_pix: float,
        price_ticket: float,

        price_credit: float,  # Price credit 1x.
        price_m_i_n_f: float,  # Price max installment no fee.
        amount_m_i_n_f: int,  # Amount max installment no fee.
        price_m_i_w_f: float,  # Price max installment with fee.
        amount_m_i_w_f: int,  # Amount max installment with fee.
        percentage_f_m_i: float,  # Percentage fee max installment.

        price_credit_cc: float,  # Price custom card 1x.
        price_cc_m_i_n_f: float,  # Price custom card max installment no fee.
        amount_cc_m_i_n_f: int,  # Amount custom card max installment no fee.
        price_cc_m_i_w_f: float,  # Price custom card max installment with fee.
        amount_cc_m_i_w_f: int,  # Amount custom card max installment with fee.
        percentage_cc_f_m_i: float,  # Percentage custom card fee max installment.
) -> dict:
    """
    Cria e retorna um produto como dicionario
    :param manufacturer: Fabricante
    :param brand: Marca
    :param ean: Código de barras global
    :param identification: Id usado pela loja (SKU)
    :param universal_identification: Id unico
    :param name: Nome comercial
    :param universal_name: Nome unico
    :param storage: Armazenamento
    :param color: Cor
    :param marketplace: Site da extracao
    :param store: Loja que esta vendendo no site da extracao
    :param zipcode: Cep do frete
    :param shipping: Frete
    :param available: Disponibilidade
    :param url: Link para a pagina do produto
    :param price_pix: Preco no PIX
    :param price_ticket: Preco no boleto
    :param price_credit: Preco em 1x no cartao comum
    :param price_m_i_n_f: Preco com maximo de parcelas sem juros no cartao comum
    :param amount_m_i_n_f: Quantidade do maximo de parcelas sem juros no cartao comum
    :param price_m_i_w_f: Preco com maximo de parcelas com juros no cartao comum
    :param amount_m_i_w_f: Quantidade do maximo de parcelas com juros no cartao comum
    :param percentage_f_m_i: Juros do cartao comum
    :param price_credit_cc: Preco em 1x no cartao proprio
    :param price_cc_m_i_n_f: Preco com maximo de parcelas sem juros no cartao proprio
    :param amount_cc_m_i_n_f: Quantidade do maximo de parcelas sem juros no cartao proprio
    :param price_cc_m_i_w_f: Preco com maximo de parcelas com juros no cartao proprio
    :param amount_cc_m_i_w_f: Quantidade do maximo de parcelas com juros no cartao proprio
    :param percentage_cc_f_m_i: Juros do cartao proprio
    :return: Um dicionario contendo os dados formatados informados a funcao
    """

    brand = _desired_type(str, (brand.replace('\n', '').strip()) if brand is not None else brand)
    manufacturer = _desired_type(str, (manufacturer.replace('\n', '').strip()) if manufacturer is not None else manufacturer)
    if (brand is None or brand == '') and (manufacturer is not None and manufacturer != ''):
        brand = manufacturer

    return {
        'fabricante': manufacturer,
        'marca': brand,
        'ean': _desired_type(int, ean),
        'sku': _desired_type(str, identification),
        'no_modelo': _desired_type(str, universal_identification),
        'nome_comercial': _desired_type(str, universal_name),
        'capacidade_armazenamento': _desired_type(str, storage),
        'cor': _desired_type(str, color),
        'nome_produto': _desired_type(str, name),
        'empresa': _desired_type(str, marketplace),
        'vendedor': _desired_type(str, store),
        'cep': _desired_type(int, zipcode),
        'frete': _desired_type(float, shipping),
        'estoque': _desired_type(bool, available),
        'url': _desired_type(str, url),

        'preco_pix': _desired_type(float, price_pix),
        'preco_boleto': _desired_type(float, price_ticket),

        'preco_x1_cartao': _desired_type(float, price_credit),
        'preco_prazo_sem_juros_cartao_normal': _desired_type(float, price_m_i_n_f),
        'qtd_parcelas_sem_juros_cartao_normal': _desired_type(int, amount_m_i_n_f),
        'preco_prazo_com_juros_cartao_normal': _desired_type(float, price_m_i_w_f),
        'qtd_parcelas_com_juros_cartao_normal': _desired_type(int, amount_m_i_w_f),
        'taxa_juros_cartao_normal': _desired_type(float, percentage_f_m_i),

        'preco_x1_cartao_proprio': _desired_type(float, price_credit_cc),
        'preco_prazo_sem_juros_cartao_proprio': _desired_type(float, price_cc_m_i_n_f),
        'qtd_parcelas_sem_juros_cartao_proprio': _desired_type(int, amount_cc_m_i_n_f),
        'preco_prazo_com_juros_cartao_proprio': _desired_type(float, price_cc_m_i_w_f),
        'qtd_parcelas_com_juros_cartao_proprio': _desired_type(int, amount_cc_m_i_w_f),
        'taxa_juros_cartao_proprio': _desired_type(float, percentage_cc_f_m_i),
        'data_extracao': datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    }
