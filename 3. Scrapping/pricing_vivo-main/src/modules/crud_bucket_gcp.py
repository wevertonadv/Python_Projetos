import os
import io
import pandas as pd
import datetime
from google.cloud import storage


class GCSParquetManager:
    """Gerenciador para operações CRUD em arquivos Parquet no Google Cloud Storage.

    Esta classe fornece métodos para upload, download e deleção de arquivos
    Parquet em um bucket específico do Google Cloud Storage.

    Attributes:
        bucket_name (str): Nome do bucket no Google Cloud Storage.
        storage_client (storage.Client): Cliente para interação com o GCS.

    Methods:
        upload_parquet(data_frame, destination_blob_name): Faz o upload de um DataFrame como arquivo Parquet.
        read_parquet(source_blob_name): Baixa um arquivo Parquet e retorna como DataFrame.
        delete_blob(blob_name): Deleta um arquivo no bucket do GCS.
    """

    def __init__(self):
        """Inicializa o gerenciador com o nome do bucket especificado."""
        json_path_local = "modules/pro-ia-precificador-f1cde9d5eada.json"
        json_path_container = "/app/modules/pro-ia-precificador-f1cde9d5eada.json"

        if os.path.exists(json_path_local):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path_local
        elif os.path.exists(json_path_container):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path_container
        else:
            raise FileNotFoundError("O arquivo de credenciais do GCP não foi encontrado.")

        self.bucket_name = "crawler-data-extraction"
        self.storage_client = storage.Client()

    def upload_parquet(self, data_frame, destination_blob_name, cache_control='no-cache'):
        """Faz o upload de um DataFrame como arquivo Parquet para o GCS.

        Args:
            data_frame (pd.DataFrame): DataFrame a ser enviado.
            destination_blob_name (str): Nome do arquivo de destino no GCS.

        Returns:
            None
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Define o cabeçalho de controle de cache
        blob.cache_control = cache_control
        
        # Converte o DataFrame para Parquet e faz o upload
        blob.upload_from_string(
            data_frame.to_parquet(index=False), "application/octet-stream"
        )
        print(f"Arquivo {destination_blob_name} enviado para {self.bucket_name}.")

    def read_parquet(self, source_blob_name, cache_control='no-cache'):
        """Baixa um arquivo Parquet do GCS e retorna como DataFrame.

        Args:
            source_blob_name (str): Nome do arquivo Parquet no GCS.

        Returns:
            pd.DataFrame: DataFrame carregado do arquivo Parquet.
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(source_blob_name)

        # Define o cabeçalho de controle de cache
        blob.cache_control = cache_control
        
        # Baixa o arquivo Parquet e carrega como DataFrame
        data = blob.download_as_bytes()
        return pd.read_parquet(io.BytesIO(data))

    def read_excel(self, source_blob_name, cache_control='no-cache'):
        """Baixa um arquivo Excel do GCS e retorna como DataFrame.

        Args:
            source_blob_name (str): Nome do arquivo Excel no GCS.

        Returns:
            pd.DataFrame: DataFrame carregado do arquivo Excel.
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(source_blob_name)
        
        # Define o cabeçalho de controle de cache
        blob.cache_control = cache_control

        # Baixa o arquivo Excel e carrega como DataFrame
        data = blob.download_as_bytes()
        return pd.read_excel(io.BytesIO(data))

    def delete_blob(self, blob_name):
        """Deleta um arquivo no GCS.

        Args:
            blob_name (str): Nome do arquivo a ser deletado.

        Returns:
            None
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        print(f"Arquivo {blob_name} deletado.")

    def listar_arquivos_na_pasta(self, pasta):
        """Lista todos os arquivos em uma pasta específica do bucket.

        Args:
            pasta (str): Pasta dentro do bucket cujos arquivos serão listados.

        Returns:
            list: Lista contendo os nomes dos arquivos na pasta especificada.
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=pasta)
        
        return [blob.name for blob in blobs if '/' in blob.name and blob.name.endswith('/') is False]


    def ler_coluna_excel(self, source_blob_name="dados_cadastrados/lista devices.xlsx", nome_coluna="Modelo com cor", cache_control='no-cache'):
        """Lê uma coluna específica de um arquivo Excel no GCS e retorna como lista.

        Args:
            source_blob_name (str): Nome do arquivo Excel no GCS.
            nome_coluna (str): Nome da coluna a ser lida. Padrão é 'Modelo com cor'.

        Returns:
            list: Lista contendo os valores da coluna especificada.
        """
        # Baixa o arquivo Excel do GCS
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(source_blob_name)
        
        # Define o cabeçalho de controle de cache
        blob.cache_control = cache_control

        data = blob.download_as_bytes()
        try:
            # Tenta ler o arquivo Excel com cabeçalhos
            df = pd.read_excel(io.BytesIO(data))
            return df[nome_coluna].tolist()
        except KeyError:
            # Se a coluna não for encontrada, tenta definir a primeira linha como cabeçalho e lê novamente
            df = pd.read_excel(io.BytesIO(data), header=None)
            df.columns = df.iloc[0]
            df = df[1:]
            return df[nome_coluna].tolist()

### Necessario em todas as funções
# gcs_manager = GCSParquetManager()
###


# # Upload
# # Formatar a data e hora no formato desejado
# pasta = "extracoes/"
# loja = "loja"
# agora = datetime.datetime.now()
# nome_arquivo = f"{pasta}{loja}_{agora.strftime('%Y%m%d')}_{agora.strftime('%H%M%S')}.parquet"

# df = pd.DataFrame({"exemplo": [1, 2, 3]})
# gcs_manager.upload_parquet(df, nome_arquivo)

# Read
# df = gcs_manager.read_parquet("dados_cadastrados/lista_devices.parquet")
# df = gcs_manager.read_excel("dados_cadastrados/lista devices.xlsx")
# print(df)

# Delete # usuario do .json sem permissão de deletar
# gcs_manager.delete_blob("meu_arquivo.parquet")

# Lista arquivos das pasta
# arquivos = gcs_manager.listar_arquivos_na_pasta("extracoes/")
# print(arquivos)


# # Retorna lista de produtos - ler_coluna_excel
# gcs_manager = GCSParquetManager()
# produtos = gcs_manager.ler_coluna_excel("dados_cadastrados/lista devices.xlsx")
# print(produtos)