from datetime import datetime
from re import findall
from time import sleep
from re import sub
from pandas import DataFrame, read_excel, concat
from tqdm import tqdm
from modules.printing_helper import print_info, print_info_2, print_warning, print_error
from modules.verify_names import lexical_similarity, word_overlap_similarity, combined_similarity
from modules.string_normalizer import normalize_string
from modules.crud_bucket_gcp import GCSParquetManager
import pandas as pd

def normalize_and_filter_words(df1, df2, col_name):
    # Função para normalizar strings (minúsculas e remoção de símbolos)
    def normalize_string(s):
        return sub(r'[^a-zA-Z0-9 ]', '', s.lower())

    # Normalizar e extrair as palavras únicas do primeiro DataFrame
    words_set = set(word for phrase in df1[col_name].apply(normalize_string) for word in phrase.split())

    # Função para filtrar as palavras no segundo DataFrame
    def filter_phrase(phrase):
        normalized_phrase = normalize_string(phrase)
        return ' '.join(word for word in normalized_phrase.split() if word in words_set)

    # Aplicar a função de filtragem vetorizada ao segundo DataFrame
    df2_filtered = df2[col_name].apply(filter_phrase)

    return df2_filtered


def get_date() -> str:
    return datetime.now().strftime('%y-%m-%d %H:%M:%S')


def open_xlsx(path: str, sheet: str = 0) -> DataFrame | None:
    print_info_2(f'Opening xlsx at "{path}"...')
    try:
        return read_excel(
            io=path,
            sheet_name=sheet
        )
    except Exception as err:
        print_error(f'Unable to open "{path}" due to {str(err)}')
        return None


def save_xlsx(path: str, df: DataFrame):
    print_info_2(f'Saving xlsx at "{path}"...')
    try:
        df.to_excel(
            excel_writer=path,
            index=False
        )
    except Exception as err:
        print_error(f'Unable to save xlsx due to {str(err)}')


def check_columns(df: DataFrame) -> DataFrame:
    print_info_2('Checking columns...')
    l_required_columns = [
        'fabricante',
        'marca',
        'sku',
        'no_modelo',
        'nome_comercial',
        'capacidade_armazenamento',
        'cor',
        'nome_produto',
        'empresa',
        'vendedor',
        'frete',
        'estoque',
        'url',
        'preco_pix',
        'preco_boleto',
        'preco_x1_cartao',
        'preco_prazo_sem_juros_cartao_normal',
        'qtd_parcelas_sem_juros_cartao_normal',
        'preco_prazo_com_juros_cartao_normal',
        'qtd_parcelas_com_juros_cartao_normal',
        'taxa_juros_cartao_normal',
        'preco_x1_cartao_proprio',
        'preco_prazo_sem_juros_cartao_proprio',
        'qtd_parcelas_sem_juros_cartao_proprio',
        'preco_prazo_com_juros_cartao_proprio',
        'qtd_parcelas_com_juros_cartao_proprio',
        'taxa_juros_cartao_proprio',
        'ean',
        'cep',
        'data_extracao'
    ]

    for s_column in l_required_columns:
        if s_column not in df.columns:
            print_warning(f'Column "{s_column}" not found.')

            match s_column:
                case 'fabricante':
                    l_wrong_columns = ['Fabricante']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'marca':
                    l_wrong_columns = ['Marca']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'sku':
                    l_wrong_columns = ['SKU']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'no_modelo':
                    l_wrong_columns = ['N° modelo', 'modelo']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'nome_comercial':
                    l_wrong_columns = ['Nome comercial']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'capacidade_armazenamento':
                    l_wrong_columns = ['Capacidade', 'capacidade']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'cor':
                    l_wrong_columns = ['Cor']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'nome_produto':
                    l_wrong_columns = ['Produto', 'product_name']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'empresa':
                    l_wrong_columns = ['Empresa']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'vendedor':
                    l_wrong_columns = ['Seller']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'frete':
                    l_wrong_columns = ['Frete mínimo', 'frete_minimo']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'estoque':
                    l_wrong_columns = ['Estoque']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'url':
                    l_wrong_columns = ['Link']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'preco_pix':
                    l_wrong_columns = ['Preço pix']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'preco_boleto':
                    l_wrong_columns = ['Preço boleto']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'preco_x1_cartao':
                    l_wrong_columns = ['Preço cartão 1x', 'preco_credito_1x']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'preco_prazo_sem_juros_cartao_normal':
                    l_wrong_columns = ['Preço cartão ~x s/ juros']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'qtd_parcelas_sem_juros_cartao_normal':
                    l_wrong_columns = ['Quantidade parcelas s/ juros']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'preco_prazo_com_juros_cartao_normal':
                    l_wrong_columns = ['Preço cartão ~x c/ juros']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'qtd_parcelas_com_juros_cartao_normal':
                    l_wrong_columns = ['Quantidade parcelas c/ juros']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'taxa_juros_cartao_normal':
                    l_wrong_columns = ['Taxa de juros']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'preco_x1_cartao_proprio':
                    l_wrong_columns = ['Preço cartão próprio 1x']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'preco_prazo_sem_juros_cartao_proprio':
                    l_wrong_columns = ['Preço cartão próprio ~x s/ juros']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'qtd_parcelas_sem_juros_cartao_proprio':
                    l_wrong_columns = ['Quantidade parcelas s/ juros cartão próprio']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'preco_prazo_com_juros_cartao_proprio':
                    l_wrong_columns = ['Preço cartão próprio ~x c/ juros']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'qtd_parcelas_com_juros_cartao_proprio':
                    l_wrong_columns = ['Quantidade parcelas c/ juros cartão próprio']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'taxa_juros_cartao_proprio':
                    l_wrong_columns = ['Taxa de juros cartão próprio']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'ean':
                    l_wrong_columns = ['ean']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'cep':
                    l_wrong_columns = ['cep']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

                case 'data_extracao':
                    l_wrong_columns = ['Data da extração', 'data_extração']
                    for s_wrong_column in l_wrong_columns:
                        if s_wrong_column in df.columns:
                            print_info(f'Found column "{s_wrong_column}", renaming it to "{s_column}"...')
                            df.rename(
                                columns={s_wrong_column: s_column},
                                inplace=True
                            )

        if s_column not in df.columns:
            print_info(f'Creating column "{s_column}"...')
            df[s_column] = None

    l_wrong_columns = []
    for s_column in df.columns:
        if s_column not in l_required_columns:
            l_wrong_columns.append(s_column)

    df.drop(columns=l_wrong_columns, inplace=True)

    return df


def extract_storage_from_name(product_name: str) -> int | None:
    product_name = product_name.lower()
    try:
        l_r49nf8 = findall(
            pattern='([0-9]{1,3}) ?(gb|tb)',
            string=product_name
        )
    except Exception as err:
        print_error(f'Unable to extract storage from product name due to {str(err)}')
        return None

    i_storage = None
    for t_r49nf8 in l_r49nf8:
        try:
            i_value = int(t_r49nf8[0])
        except Exception as err:
            print_error(f"Can't parse RegEx result to integer due to {str(err)}")
            return None

        if 'tb' in t_r49nf8[1]:
            i_value *= 1024

        if i_storage is None:
            i_storage = i_value
        else:
            if i_value > i_storage:
                i_storage = i_value

    if i_storage < 96:
        i_storage = None

    return i_storage


def concat_dataframes(paths: list[str]) -> DataFrame:
    l_df = []
    for s_xlsx_path in paths:
        print()
        df = open_xlsx(path=s_xlsx_path)

        if df is not None:
            df = check_columns(df)

            if df is not None:
                df.dropna(how='all', inplace=True)
                df.dropna(axis='columns', how='all', inplace=True)
                l_df.append(df)

    # sleep(1)

    df = concat(l_df)
    df.drop_duplicates(inplace=True)
    return df


def concat_dataframes_2(dfs: list[DataFrame]) -> DataFrame:
    l_df = []
    for df in dfs:

        if df is not None:
            print()
            df = check_columns(df)

            if df is not None:
                # df.dropna(how='all', inplace=True)
                # df.dropna(axis='columns', how='all', inplace=True)
                l_df.append(df)

    # sleep(1)

    df = concat(l_df)
    df.drop_duplicates(inplace=True)
    return df


def filter_dataframe(df_devices: DataFrame, df_extractions: DataFrame) -> DataFrame:
    d_rows_to_remove_indexes = {}

    df_devices = df_devices.reset_index()
    df_extractions = df_extractions.reset_index()

    l_extractions_names = list(df_extractions['nome_produto'])
    l_devices_names = list(df_devices['Modelo com cor'])
    # l_devices_colors = list(df_devices['Cor'])

    i_min_desired_similarity_percentage = 70

    for i_extractions_name_index_2 in tqdm(range(0, len(l_extractions_names), 1)):
        # print_info_2(
        #     msg=f'Filtering products ({i_extractions_name_index_2 + 1} of {len(l_extractions_names)})...',
        #     start='\r',
        #     end=''
        # )

        s_product_name_2 = normalize_string(string=l_extractions_names[i_extractions_name_index_2], sep=' ', show_debug_msg=False)

        i_biggest_similarity = 0
        i_biggest_similarity_index = -1
        for i_product_name_index_1 in range(0, len(l_devices_names), 1):
            s_product_name_1 = l_devices_names[i_product_name_index_1]

            f_lexical_similarity_percentage = lexical_similarity(s_product_name_1, s_product_name_2) * 100
            f_overlap_similarity_percentage = word_overlap_similarity(s_product_name_1, s_product_name_2) * 100
            f_average_similarity_percentage = (f_lexical_similarity_percentage + f_overlap_similarity_percentage) / 2

            if f_average_similarity_percentage >= i_min_desired_similarity_percentage:

                f_bert_similarity_percentage = combined_similarity(s_product_name_1, s_product_name_2) * 100

                if f_bert_similarity_percentage >= i_min_desired_similarity_percentage and f_bert_similarity_percentage > i_biggest_similarity:
                    i_biggest_similarity = f_bert_similarity_percentage
                    i_biggest_similarity_index = i_product_name_index_1

        if i_biggest_similarity < i_min_desired_similarity_percentage:
            d_rows_to_remove_indexes.update(
                {
                    i_extractions_name_index_2:0
                }
            )
        else:
            df_extractions.loc[i_extractions_name_index_2, ('COD_SAP')] = df_devices.loc[i_biggest_similarity_index, ('COD_SAP')]
            df_extractions.loc[i_extractions_name_index_2, ('Cor')] = df_devices.loc[i_biggest_similarity_index, ('Cor')]
            df_extractions.loc[i_extractions_name_index_2, ('Modelo com cor')] = df_devices.loc[i_biggest_similarity_index, ('Modelo com cor')]
            df_extractions.loc[i_extractions_name_index_2, ('Modelo')] = df_devices.loc[i_biggest_similarity_index, ('Modelo')]
            df_extractions.loc[i_extractions_name_index_2, ('NOME_COMERCIAL')] = df_devices.loc[i_biggest_similarity_index, ('NOME_COMERCIAL')]
            df_extractions.loc[i_extractions_name_index_2, ('CATEGORIA')] = df_devices.loc[i_biggest_similarity_index, ('CATEGORIA')]
            df_extractions.loc[i_extractions_name_index_2, ('VERTICAL')] = df_devices.loc[i_biggest_similarity_index, ('VERTICAL')]
            df_extractions.loc[i_extractions_name_index_2, ('SUBCATEGORIA_GERENCIAL')] = df_devices.loc[i_biggest_similarity_index, ('SUBCATEGORIA_GERENCIAL')]
            df_extractions.loc[i_extractions_name_index_2, ('ID_DPGC')] = df_devices.loc[i_biggest_similarity_index, ('ID_DPGC')]

    print()
    l_rows_to_remove_indexes = list(d_rows_to_remove_indexes.keys())
    if len(l_rows_to_remove_indexes) > 0:
        df_extractions.drop(index=l_rows_to_remove_indexes, inplace=True, axis=0)
    df_extractions = df_extractions.reset_index()

    return df_extractions

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
    

if __name__ == '__main__':
    print(f' Starting ({get_date()}) '.center(70, '-'))

    gcspm = GCSParquetManager()

    l_parquet_names = gcspm.listar_arquivos_na_pasta('extracoes/')

    l_dataframes = []
    for s_parquet_name in l_parquet_names:
        l_dataframes.append(gcspm.read_parquet(s_parquet_name))

    # df_devices: DataFrame = open_xlsx('_tmp/lista devices.xlsx', 'devices')
    df_devices = gcspm.read_excel('dados_cadastrados/lista devices.xlsx')

    df_extractions = concat_dataframes_2(l_dataframes)

    print()
    df = filter_dataframe(df_devices, df_extractions)

    print()
    print_info_2('Uploading parquet to database...')
    # df['estoque'] = df['estoque'].astype(str)
    # df['COD_SAP'] = df['COD_SAP'].astype(str)
    # Aplicar a função de tratamento para cada coluna no DataFrame
    for coluna in df.columns:
        tipo = tipos_desejados.get(coluna, str)  # Usa o tipo do dicionário ou 'str' como padrão
        df[coluna] = tratar_coluna(df[coluna], tipo)
        
    agora = datetime.now()
    gcspm.upload_parquet(df, f"dados_consolidados/{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}_dados_consolidados.parquet")
    # df.to_excel('_tmp/_all.xlsx', index=False)

    print()
    print(f' Done ({get_date()}) '.center(70, '-'))
