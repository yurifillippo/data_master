# Databricks notebook source
from pyspark.sql.functions import col, sum, expr, count, row_number, lit, input_file_name, sha2, current_date, datediff, floor, when, broadcast
from pyspark.sql import DataFrame
from pyspark.sql.functions import col as spark_col, sum as spark_sum
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import DateType
import logging
import pytz
from azure.identity import ManagedIdentityCredential
import json
import requests
import datetime
import hashlib
import hmac
import base64

# COMMAND ----------

#Funções Compartilhadas (Utilitárias)
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
    
    end_time = datetime.now()
    duration = end_time - start_time
    duration_minutes = duration.total_seconds()

    return duration_minutes 

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

dat_carga = "20241116" #Campo virá do parametro do job
table_name = "prod_contrat_diario"
db_name = "g_vend"

# COMMAND ----------

def ingestion(db_name, table_name, dat_carga):

  """
  Template de ingestão para arquivos csv.

  Args:
    db_name (string): Name of the table database/catalog.
    table_name (string): Table name.
    dat_carga (string): Data de carga para geração da tabela silver.

  Returns:  dict: Metricas
  """

  try:
    #Definição de parametros para envio de logs
    WORKSPACE_ID = dbutils.secrets.get(scope="logsdatamaster", key="WORKSPACE_ID")
    SHARED_KEY = dbutils.secrets.get(scope="logsdatamaster", key="SHARED_KEY")

    #Definir name par ao logger
    name_logger = f"ingestion_table_bronze_{db_name}_{table_name}"
    
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

    metricas = {
        "table_name": table_name, #Nome da tabela ok
        "load_total_time": None, #Tempo total de load dos dados brutos ok
        "qtd_total_rows_insert": None, #Quantidade total de linhas inseridas ok
        "write_total_time": None, #Tempo total de escrita na tabela delta ok
        "data_size_mb_formatted": None, #Tamanho em MB dos dados brutos carregados
        "qts_total_rows_insert_verify": None, #Quantidade de linhas ao ler a tabela com o odate ok
        "num_columns_table": None, #Numero de colunas da tabelaok
        "num_files": None, #Número de arquivos parquet gerados ok
        "total_execution": None, #Tempo total de execução do template de ingestão
        "dat_carga": dat_carga, #Data de execução
        "alerta": None, #Alerta em divergência de quantidade de dados inseridos no o mesmo odate
        "execution_succes": True,
        "datetime_execution": datetime_string, 
    }

    # Coletar tempo inicial da execução
    start_time_total_execution = datetime.now()
    logger.info(f"Start of execution: {start_time_total_execution}")

    # Carregar tabela de clientes
    logger.info("Carregando tabela s_vend.clie_limit")
    load_start_time = datetime.now()
    df = spark.read.table("s_vend.clie_limit").where(col("dat_ref_carga") == dat_carga)

    # Tempo total de load das tabelas
    metricas["load_total_time"] = monitor_execution_time(load_start_time)
    logger.info(f"[* Metrics *] - Total time to load data: {metricas['load_total_time']} seconds")

    # Tamanho dos dados carregados em bytes
    data_size_bytes = df.rdd.map(lambda row: len(str(row))).reduce(lambda x, y: x + y)
    data_size_mb = data_size_bytes / (1024 * 1024)
    metricas["data_size_mb_formatted"] = float(f"{data_size_mb:.2f}")
    logger.info(f"[* Metrics *] - Size of loaded data: {metricas['data_size_mb_formatted']} MB")

    #Agreagação
    df_add_columns = df.select("prod_contratado", "valor_prod_contratado", "limite_credito", "dat_ref_carga") \
      .withColumn("valor_prod_contratado", col("valor_prod_contratado").cast("double")) \
      .withColumn("limite_credito", col("limite_credito").cast("double")) \
      
    df_agg = df_add_columns.groupBy("prod_contratado") \
      .sum("valor_prod_contratado", "limite_credito")\
      .withColumn("dat_ref_carga", lit(dat_carga))

    df_agg_renamed = df_agg.withColumnRenamed("sum(valor_prod_contratado)", "valor_total_prod_contratado") \
          .withColumnRenamed("sum(limite_credito)", "valor_total_limite_credito")

    #Verificando quantidade de registros após agragação
    metricas["qtd_total_rows_insert"] = df_agg_renamed.count()
    logger.info(f"[* Metrics *] - Numero total de linhas: {metricas['qtd_total_rows_insert']}")

    #Inserir dados na tabela clie_limit
    write_start_time = datetime.now()
    df_agg_renamed.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("parquet.file.size", "128MB") \
        .saveAsTable(f"{db_name}.{table_name}")

    metricas["write_total_time"] = monitor_execution_time(write_start_time)
    logger.info(f"[* Metrics *] - Data recording execution time: {metricas['write_total_time']} seconds")

    #Verificar quantidade de dados inseridos
    logger.info(f"Checking amount of data entered")
    current_date = datetime.now()

    df_verify = spark.read.format("delta").table(f"{db_name}.{table_name}").where(col("dat_ref_carga") == dat_carga)
    metricas["qts_total_rows_insert_verify"] = df_verify.count()
    metricas["num_columns_table"] = len(df_verify.columns)
    logger.info(f"[* Metrics *] - A total of {metricas['qtd_total_rows_insert']} rows and a total of {metricas['num_columns_table']} columns were inserted into the table")

    #Verificar numero de arquivos gerados
    logger.info(f"Checking total generated files")
    df_with_file_name = df_verify.withColumn("file_name", input_file_name())
    metricas["num_files"] = df_with_file_name.select("file_name").distinct().count()
    logger.info(f"[* Metrics *] - Total files generated: {metricas['num_files']}")

    #Coletar tempo final da execução
    total_execution = monitor_execution_time(start_time_total_execution)
    final_time_total_execution = datetime.now()
    logger.info(f"End of execution: {final_time_total_execution}")
    logger.info(f"[* Metrics *] - Total execution time: {total_execution}")

    if metricas["qtd_total_rows_insert"] == metricas["qts_total_rows_insert_verify"]:
                metricas["alerta"] = False
                logger.info(f"Table {table_name} ingested successfully")
                logger.info(f"No alerts regarding validation of entered quantities")
    else:
        metricas["alerta"] = True 
        logger.info(f"Table {table_name} ingested successfully")
        logger.warning(f"Check table ingestion, has an ALERT regarding the difference in data found on the load date")

    metricas["total_execution"] = monitor_execution_time(start_time_total_execution)
    logger.info(f"Total time to execution: {metricas['total_execution']} seconds")   

    # Insertir dados no Azure Monitor
    logger.info("Inserting logs in Azure Monitor")
    log_data = json.dumps([{"message": log} for log in log_entries])
    post_data(log_data, WORKSPACE_ID, SHARED_KEY)

    return metricas

  except Exception as e:
    logger.error(f"Error ingesting table {table_name}: {e}")
    logger.info("Collecting metrics data in the event of an error...")
    metric_valid = verificar_campos(metricas)

    # Insertir dados no Azure Monitor
    logger.info("Inserting logs in Azure Monitor")
    log_data = json.dumps([{"message": log} for log in log_entries])
    post_data(log_data, WORKSPACE_ID, SHARED_KEY)

    return metric_valid 



# COMMAND ----------

#Variaveis esperadas para criação da tabela silver "s_vend.clie_limit"
#db_name = dbutils.widgets.get("param1")
#table_name = dbutils.widgets.get("param2")
#dat_carga = dbutils.widgets.get("param3")

dat_carga = "20241116" #Campo virá do parametro do job
table_name = "prod_contrat_diario"
db_name = "g_vend"

#Template de criação da tabela e atribuição de métricas
df = ingestion(db_name, table_name, dat_carga)
