from datetime import datetime
from pandas import DataFrame
from modules.ViaVarejoExtractor import ViaVarejoExtractor
from modules.xlsx import get_names
import os
from modules.crud_bucket_gcp import GCSParquetManager
from modules.logger_control import Logger, __main__

log = Logger(os.path.basename(__main__.__file__).replace(".py", ""))

def _get_date() -> str:
    """
    Returns current date following pattern "%d-%m-%Y %H:%M"
    :return: String containig current date
    """
    return datetime.now().strftime("%d-%m-%Y %H:%M")


if __name__ == '__main__':
    log.info(f' Starting ({_get_date()}) '.center(70, '-'))

    vve = ViaVarejoExtractor(
        host_website='www.casasbahia.com.br',
        host_api='pdp-api.casasbahia.com.br',
        zipcode='01021-100',
        marketplace_identifier='CB',
        marketplace_name='Casas Bahia'
    )

    # l_names = get_names('assets/lista devices.xlsx')
    gcs_manager = GCSParquetManager()
    l_names = gcs_manager.ler_coluna_excel()

    l_results = vve.get_search_results(l_names)

    l_products = None
    if l_results is not None:
        l_products = vve.get_products(l_results)

    if l_products is not None:
        # DataFrame(l_products).to_excel(excel_writer='_products_casasbahia.xlsx', index=False, encoding='utf-8-sig')
        df = DataFrame(l_products)
        
        # Upload to bucket
        # Formatar a data e hora no formato desejado
        pasta = "extracoes/"
        loja = "casasbahia"
        agora = datetime.now()
        nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

        
        gcs_manager.upload_parquet(df, nome_arquivo)
        
    log.info(f' Done ({_get_date()}) '.center(70, '-'))
