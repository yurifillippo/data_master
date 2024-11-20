import os
import argparse
import logging
from azure.storage.blob import BlobClient

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para enviar um arquivo para o Azure Blob Storage
def upload_to_blob(sas_token, source_file_name, destination_blob_name):
    # Defina as variáveis para o nome da conta, container e blob
    account_name = "stagedatamaster"
    container_name = "filedataimport"
    file_name = destination_blob_name

    logger.info(f"Iniciando o upload do arquivo '{source_file_name}' para o blob '{file_name}' no container '{container_name}'.")

    # Verifica se o SAS token foi obtido corretamente
    if sas_token is None:
        logger.error("SAS token não encontrado. Certifique-se de que a variável de ambiente AZURE_SAS_TOKEN está definida.")
        raise Exception("SAS token não encontrado.")

    # Monta a URL do blob com o SAS token
    blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{file_name}?{sas_token}"
    logger.debug(f"Blob URL gerada: {blob_url}")

    # Cria o cliente do blob usando a URL com o SAS token
    blob_client = BlobClient.from_blob_url(blob_url)

    # Realiza o upload do arquivo para o blob
    try:
        with open(source_file_name, "rb") as data:
            logger.info(f"Enviando o arquivo '{source_file_name}' para o Azure Blob Storage.")
            blob_client.upload_blob(data)
        logger.info(f"Arquivo '{source_file_name}' enviado com sucesso para o Azure Blob Storage no blob '{file_name}'.")
    except Exception as e:
        logger.error(f"Erro ao enviar o arquivo '{source_file_name}': {e}")
        raise

if __name__ == "__main__":
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Upload de um arquivo para o Azure Blob Storage.')
    parser.add_argument('path_to_file', type=str, help='Caminho do arquivo a ser enviado.')
    parser.add_argument('destination_blob_name', type=str, help='Nome do arquivo no Azure Blob Storage.')

    # Pega o SAS token da variável de ambiente
    sas_token = os.getenv('AZURE_SAS_TOKEN')

    # Parseia os argumentos
    args = parser.parse_args()

    # Usa o caminho do arquivo passado como argumento
    FILE_NAME = args.path_to_file

    # Chama a função de upload
    try:
        upload_to_blob(sas_token, FILE_NAME, args.destination_blob_name)
    except Exception as e:
        logger.error(f"Falha no processo de upload: {e}")
