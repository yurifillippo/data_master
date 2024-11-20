# Databricks notebook source
# MAGIC %md
# MAGIC ##Template de Ingestão

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ##Pré-requisitos:
# MAGIC
# MAGIC - Criar cluster com lib tipo PyPI: google-cloud-bigquery.
# MAGIC - Executar notebook para inserir "service account key file".
# MAGIC - Executar notebook de criação de tabelas delta.

# COMMAND ----------

from pyspark.sql.functions import col, sum, expr, count, row_number, lit, input_file_name, when, trim
from pyspark.sql import DataFrame
from pyspark.sql.functions import col as spark_col, sum as spark_sum
from pyspark.sql.window import Window
from datetime import datetime
import logging
import os
import pytz
import uuid
import ast
import requests
import hashlib
import hmac
import base64
import json

# COMMAND ----------

#Funções Compartilhadas (Utilitárias)
#Autenticator
def autenticator(logger):
    try:
        sas_token = dbutils.secrets.get(scope="storage_datamaster", key="data_master")
        storage_account_name = "datalake1datamaster"
        secret_scope_name = "storage_datamaster"
        secret_key_name = "data_master_account_key"

        # Retrieve the storage account key from the secret scope
        storage_account_key = dbutils.secrets.get(scope=secret_scope_name, key=secret_key_name)

        # Configure the storage account access key
        spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

        return logger.info(f"Authentication carried out successfully")
    
    except ValueError as e:
        return logger.error(f"Authentication failed: {e}")


#Definições para log:
def build_signature(message, secret):
    key_bytes = base64.b64decode(secret)
    message_bytes = bytes(message, encoding="utf-8")
    hmacsha256 = hmac.new(key_bytes, message_bytes, digestmod=hashlib.sha256).digest()
    encoded_hash = base64.b64encode(hmacsha256).decode()
    return encoded_hash

#Post do log no azure monitor
def post_data(log_data, WORKSPACE_ID, SHARED_KEY):
    date_string = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    content_length = len(log_data)
    string_to_hash = f"POST\n{content_length}\napplication/json\nx-ms-date:{date_string}\n/api/logs"
    hashed_string = build_signature(string_to_hash, SHARED_KEY)
    signature = f"SharedKey {WORKSPACE_ID}:{hashed_string}"

    query = "api-version=2016-04-01"
    url = f"https://{WORKSPACE_ID}.ods.opinsights.azure.com/api/logs?{query}"

    headers = {
        'content-type': "application/json",
        'Authorization': signature,
        'Log-Type': "MyCustomLogType",  # Certifique-se de que o Log-Type seja válido
        'x-ms-date': date_string
    }
    response = requests.post(url, data=log_data, headers=headers)
    if (response.status_code >= 200 and response.status_code <= 299):
        print('Accepted')
    else:
        print("Response: {}".format(response.content))
        print("Response code: {}".format(response.status_code))


#Realiza o load dos dados no path raiz
def load_data_ingestion(table, sep, logger):
    """
    Loads the raw data into the source.

    Args:
        path (string): stage path where the raw data is located..
        header (string/boolean): true or false
        sep (string): Data separator within the file.

    Returns:
        Dataframe: O resultado da multiplicação.

    Example:
        +-----+-----+---------------+
        |nome |idade|cidade         |
        +-----+-----+---------------+
        |Alice|30   |São Paulo      |
        |Bob  |25   |Rio de Janeiro |
        |Carol|28   |Belo Horizonte |
        +-----+-----+---------------+
    """
    try:

        data_hora = datetime.now()

        # Define the actual container name and storage account name
        storage_account_name = "stagedatamaster"
        sas_token = dbutils.secrets.get(scope="storage_datamaster", key="data_master")
        storage_account_key = dbutils.secrets.get(scope="stage_datamaster", key="stage_datamaster_key")

        # Configure the credentials in Spark
        spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

        # Define the path of the container bronze
        container = "filedataimport"
        path = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/table_ingestion_files/{table}"

        df = spark.read.format("csv").option("header", "true").option("sep", sep).load(path)

        if len(df.columns) > 1:

            timezone = pytz.timezone('America/Sao_Paulo')
            # Obtenha a data e hora atuais na sua região
            current_date = datetime.now(timezone)
            dat_carga = current_date.strftime("%Y%m%d")
            df_dat = df.withColumn("dat_ref_carga", lit(dat_carga))

            logger.info("Data loaded successfully")

            return df_dat
        
        else:
            # Lançar uma exceção para indicar que a condição não foi atendida
            error_message = "The DataFrame does not have more than one column. Check the separator used to read the file."
            df.show(1)
            logger.error(f"{data_hora} - {table} - {error_message}")
            raise ValueError(error_message)

    except ValueError as e:
        return logger.error(f"{data_hora} - {table} - Data load failure: {e}")
    
#Verificar dados nulos
def check_nulls(df: DataFrame, required_columns: list, table, logger):
    """
    Checks for null values ​​in columns that should not have null values.

    Args:
        df (Dataframe): Dataframe loaded with raw data.
        required_columns (list): List with the name of columns that must not have null values.

    Returns:
        string: Status log
    """
    # Verificar se todas as colunas obrigatórias estão presentes no DataFrame
    data_hora = datetime.now()
    df_columns = set(df.columns)
    missing_columns = [col for col in required_columns if col not in df_columns]

    if missing_columns:
        raise ValueError(f"The following required columns are missing from the DataFrame: {', '.join(missing_columns)}")

    try:
        # Calcular a contagem de valores nulos por coluna
        null_counts = df.select([spark_sum(spark_col(c).isNull().cast("int")).alias(c) for c in df.columns])

        # Convertendo o resultado para um dicionário
        null_counts_dict = null_counts.collect()[0].asDict()

        # Exibindo colunas com valores nulos e suas respectivas contagens
        nulls_info = {column: count for column, count in null_counts_dict.items() if count > 0}

        # Verificar se há valores nulos nas colunas que não podem conter nulos
        invalid_columns = {col: nulls_info[col] for col in required_columns if col in nulls_info and nulls_info[col] > 0}

        # Se houver colunas obrigatórias com nulos, lançar exceção
        if invalid_columns:
            raise ValueError(f"Error: The following required columns contain null values: {invalid_columns}")

        # Logar outras colunas com nulos, sem lançar exceção
        if nulls_info:
            logger.info("Other columns with null values ​​(not required):")
            for column, count in nulls_info.items():
                if column not in required_columns:
                    logger.info(f"Column: {column}, Null count: {count}")
        
        logger.info("Mandatory columns are valid.")

    except ValueError as e:
        logger.error(f"{data_hora} - {table} - {e}")
        raise


#Coletar tempo de execução em segundos
def monitor_execution_time(start_time):
    """
    Receives the initial time of the operation to be monitored and informs the total execution time in seconds.

    Args:
        start_time (datetime): datetime.datetime(2024, 9, 12, 1, 20, 22, 940241)
    
    Returns:
        float: 0.000142.

    """
    
    end_time = datetime.now()
    duration = end_time - start_time
    duration_minutes = duration.total_seconds()

    return duration_minutes 


#Limpar espacos em branco em nome de colunas     
def clean_column_names(df):
    """
    Clear blank spaces that may appear in the column field

    Args:
        df (DataFrame): Dataframe loaded with raw data.

    Returns:
        df (DataFrame): Dataframe with column names without blanks.

    """
    # Obter os nomes das colunas
    column_names = df.columns
    
    # Criar um dicionário de mapeamento para renomear as colunas
    new_column_names = {name: name.strip() for name in column_names}
    
    # Aplicar as renomeações
    for old_name, new_name in new_column_names.items():
        if old_name != new_name:  # Verificar se o nome precisa ser alterado
            df = df.withColumnRenamed(old_name, new_name)
    
    return df


def verificar_campos(metricas):
    """
    Verifica se todos os campos no dicionário 'metricas' são diferentes de None
    e atualiza o campo 'execution_succes' de acordo.

    Args:
        metricas (dict): Dicionário contendo as métricas.

    Returns:
        dict: Dicionário atualizado com o campo 'execution_succes'.
    """
    # Verifica se todos os valores no dicionário são diferentes de None, exceto 'execution_succes'
    all_fields_present = all(value is not None for key, value in metricas.items() if key != 'execution_succes')

    # Atualiza o campo 'execution_succes'
    metricas['execution_succes'] = all_fields_present

    return metricas

# COMMAND ----------

# Função template da ingestão
def ingestion(db_name, table_name, sep, required_columns, mode_ingestion="append"):
    """
    Template de ingestão para arquivos csv.

    Args:
        db_name (string): Name of the table database/catalog.
        table_name (string): Table name.
        sep (string): Data separator within the file.
        required_columns (list):  List with the name of columns that must not have null values.

    Returns:
        dict: Metricas
    """


    #variáveis de log de erro
    data_hora = datetime.now()

    try:
        #Definição de parametros para envio de logs
        WORKSPACE_ID = dbutils.secrets.get(scope="logsdatamaster", key="WORKSPACE_ID")
        SHARED_KEY = dbutils.secrets.get(scope="logsdatamaster", key="SHARED_KEY")

        #Definir name par ao logger
        name_logger = f"IngestionTableBronze_{db_name}_{table_name}"
        
        # Lista para armazenar os logs
        log_entries = []

        # Configurar o logger
        class ListHandler(logging.Handler):
            def emit(self, record):
                log_entries.append(self.format(record))

        logger = logging.getLogger(name_logger)
        logger.setLevel(logging.INFO)
        list_handler = ListHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        list_handler.setFormatter(formatter)
        logger.addHandler(list_handler)

        #Parametros de autenticação
        autenticator(logger)

        # Coletar a data e hora atual em UTC
        timezone = pytz.timezone('America/Sao_Paulo')

        # Obtenha a data e hora atuais na sua região
        local_datetime = datetime.now(timezone)

        # Converter para string usando strftime
        datetime_string = local_datetime.strftime('%Y-%m-%d %H:%M:%S')

        # Inicialização de variáveis de métricas
        metricas = {
            "table_name": table_name,
            "load_total_time": None,
            "number_lines_loaded": None,
            "data_size_mb_formatted": None,
            "write_total_time": None,
            "qtd_total_rows_insert": None,
            "num_columns_table": None,
            "num_files": None,
            "total_execution": None,
            "dat_carga": None,
            "alerta": None,
            "execution_succes": True,
            "datetime_execution": datetime_string, 
        }

        logger.info(f"name_logger: {name_logger}")

        # Coletar tempo inicial da execução
        start_time_total_execution = datetime.now()
        logger.info(f"Start of execution: {start_time_total_execution}")

        # Realiza load dos dados e inclusão do campo com data de carga
        logger.info(f"Starting to load data into the path")
        load_start_time = datetime.now()

        df = load_data_ingestion(table_name, sep, logger)
        metricas["load_total_time"] = monitor_execution_time(load_start_time)
        logger.info(f"Total time to load data: {metricas['load_total_time']} seconds")

        # Tamanho dos dados carregados em bytes
        data_size_bytes = df.rdd.map(lambda row: len(str(row))).reduce(lambda x, y: x + y)
        data_size_mb = data_size_bytes / (1024 * 1024)
        metricas["data_size_mb_formatted"] = float(f"{data_size_mb:.2f}")
        logger.info(f"Size of loaded data: {metricas['data_size_mb_formatted']} MB")

        # Quantidade de dados carregados
        metricas["number_lines_loaded"] = df.count()
        logger.info(f"Number of lines loaded {metricas['number_lines_loaded']}")

        # Transformando todos os campos com a string Null ou null
        logger.info(f"Verificando colunas com string Null ou null e substituindo por None")
        df_transf = df

        # Substitui 'NULL' e 'null' por None em todas as colunas
        for column in df.columns:
            df_transf = df_transf.withColumn(
                column, 
                when(trim(col(column)).isin("NULL", "null"), None).otherwise(col(column)))

        # Realizando limpeza de espacos em branco no nome das colunas
        df_write_clean = clean_column_names(df_transf)

        # Realizar validação de campos nulos
        check_nulls(df_write_clean, required_columns, table_name, logger)

        # Gravar dados na tabela
        write_start_time = datetime.now()
        logger.info(f"Writing data to the table: {table_name}")

        # Define the actual container name and storage account name
        sas_token = dbutils.secrets.get(scope="storage_datamaster", key="data_master")
        storage_account_name = "datalake1datamaster"
        secret_scope_name = "storage_datamaster"
        secret_key_name = "data_master_account_key"

        # Retrieve the storage account key from the secret scope
        storage_account_key = dbutils.secrets.get(scope=secret_scope_name, key=secret_key_name)

        # Configure the storage account access key
        spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

        # Verificar existência da tabela
        if spark.catalog.tableExists(f"{db_name}.{table_name}"):
            logger.info(f"The table {db_name}.{table_name} exists.")
        else:
            logger.info(f"The table {db_name}.{table_name} does not exist.")

        df_write_clean.write.format("delta") \
            .mode(mode_ingestion) \
            .option("mergeSchema", "true") \
            .option("parquet.file.size", "128MB") \
            .saveAsTable(f"{db_name}.{table_name}")

        metricas["write_total_time"] = monitor_execution_time(write_start_time)
        logger.info(f"Data recording execution time: {metricas['write_total_time']} seconds")

        # Verificar quantidade de dados inseridos
        current_date = datetime.now()
        dat_carga = local_datetime.strftime("%Y%m%d")
        metricas["dat_carga"] = dat_carga
        df_verify = spark.read.format("delta").table(f"{db_name}.{table_name}").where(col("dat_ref_carga") == dat_carga)
        metricas["qtd_total_rows_insert"] = df_verify.count()
        num_columns_table = len(df_verify.columns)
        metricas["num_columns_table"] = num_columns_table
        logger.info(f"A total of {metricas['qtd_total_rows_insert']} rows and a total of {metricas['num_columns_table']} columns were inserted into the table")

        # Verificar numero de arquivos gerados
        df_with_file_name = df.withColumn("file_name", input_file_name())
        metricas["num_files"] = df_with_file_name.select("file_name").distinct().count()
        logger.info(f"Total files generated: {metricas['num_files']}")

        # Coletar tempo final da execução
        total_execution = monitor_execution_time(start_time_total_execution)
        metricas["total_execution"] = total_execution
        final_time_total_execution = datetime.now()
        logger.info(f"End of execution: {final_time_total_execution}")
        logger.info(f"Total execution time: {total_execution}")

        # Verificar alertas
        if metricas["number_lines_loaded"] == metricas["qtd_total_rows_insert"]:
            metricas["alerta"] = False
            logger.info(f"Table {table_name} ingested successfully")
            logger.info(f"No alerts regarding validation of entered quantities")
        else:
            metricas["alerta"] = True
            logger.info(f"Table {table_name} ingested successfully")
            logger.warning(f"Check table ingestion, has an ALERT regarding the difference in data found on the load date")

        # Insertir dados no Azure Monitor
        logger.info("Inserting logs in Azure Monitor")
        log_data = json.dumps([{"message": log} for log in log_entries])
        post_data(log_data, WORKSPACE_ID, SHARED_KEY)

        return metricas
            
    except Exception as e:
        logger.error(f"{data_hora} - {db_name}.{table_name} - Error ingesting table {table_name}: {e}")
        logger.info("Collecting metrics data in the event of an error...")
        metric_valid = verificar_campos(metricas)

        # Insertir dados no Azure Monitor        
        logger.info("Inserting logs in Azure Monitor")
        log_data = json.dumps([{"message": log} for log in log_entries])
        post_data(log_data, WORKSPACE_ID, SHARED_KEY)

        return metric_valid


# COMMAND ----------

#Coleta de variáveis

table_name = dbutils.widgets.get("param1")
db_name = dbutils.widgets.get("param2")
sep = dbutils.widgets.get("param3")

# Capturar o valor como string
param4_string = dbutils.widgets.get("param4")

# Converter para lista
try:
    required_columns = ast.literal_eval(param4_string)
    if not isinstance(required_columns, list):
        raise ValueError("O valor fornecido em param4 não é uma lista.")
except Exception as e:
    raise ValueError(f"Erro ao converter param4 para lista: {e}")


print(f"table_name_clientes: {table_name}")
print(f"db_name_clientes: {db_name}")
print(f"sep: {sep}")
print(f"required_columns: {required_columns}")

# COMMAND ----------

#Template de ingestão e atribuição de métricas
metricas= ingestion(db_name, table_name, sep, required_columns)

print(metricas)
