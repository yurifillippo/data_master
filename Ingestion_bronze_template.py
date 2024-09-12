# Databricks notebook source
# MAGIC %md
# MAGIC ##Template de Ingestão

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pré-requisitos:
# MAGIC
# MAGIC - Criar cluster com lib tipo PyPI: google-cloud-bigquery.
# MAGIC - Executar notebook para inserir "service account key file".
# MAGIC - Executar notebook de criação de tabelas delta.

# COMMAND ----------

from pyspark.sql.functions import col, sum, expr, count, row_number, lit, input_file_name
from pyspark.sql import DataFrame
from pyspark.sql.functions import col as spark_col, sum as spark_sum
from pyspark.sql.window import Window
from datetime import datetime
import logging
import os
from google.cloud import bigquery
from google.cloud import storage

# COMMAND ----------

#Instancia logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

#Atribuir variável de embiente
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/your_key.json"

# COMMAND ----------

#Funções Compartilhadas (Utilitárias)

#Cria conexao com Storage GCP
def conex_gcp():
    """
    Creates connection to Google Cloud by reading service accounts key file.

    Returns:
        string: Connection status log

    """
    try:
        service_account_key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # Configurar as credenciais para acessar o Google Cloud Storage
        spark.conf.set("fs.gs.auth.service.account.enable", "true")
        spark.conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)

        # Exemplo de leitura de dados do GCS
        bucket_name = "lake_data_master"

        return logger.info("Connection successfully")
    
    except Exception as e:
        return logger.error("Connection failed")
    

#Criar conexão com Big Query
def insert_bigquery(dict_metrics):
    """
    Inserts metrics data into Big Query.

    Args:
        dict_metrics (dict): Dictionary with metrics extracted from engine execution.

    Returns:
        string: Insert status log

    """
    try:
        key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        
        logger.info("Opening connection with Big Query")
        try:
            client = bigquery.Client()
            logger.info("Connection successfully")
        except Exception as e:
            logger.error("Connection failed")

        table_id = "datamaster01.ingestion_metrics_data_master.ingestion_metrics_data_lake"

        logger.info(f"Inserting data into table Id: {table_id}")
        errors = client.insert_rows_json(table_id, dict_metrics)

        if errors == []:
            return logger.info("Data entered successfully.")
        else:
            return logger.info(f"Errors when entering data: {errors}")
    
    except Exception as e:
        return logger.error("Connection failed")    


#Realiza o load dos dados no path raiz
def load_data_ingestion(path, header, sep):
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

        df = spark.read.format("csv").option("header", header).option("sep", sep).load(path)

        if len(df.columns) > 1:

            current_date = datetime.now()
            dat_carga = current_date.strftime("%Y%m%d")
            df_dat = df.withColumn("dat_ref_carga", lit(dat_carga))

            logger.info("Data loaded successfully")

            return df_dat
        
        else:
            # Lançar uma exceção para indicar que a condição não foi atendida
            error_message = "The DataFrame does not have more than one column. Check the separator used to read the file."
            df.show(1)
            logger.error(error_message)
            raise ValueError(error_message)

    except ValueError as e:
        return logger.error(f"Data load failure: {e}")
    
#Verificar dados nulos
def check_nulls(df: DataFrame, required_columns: list):
    """
    Checks for null values ​​in columns that should not have null values.

    Args:
        df (Dataframe): Dataframe loaded with raw data.
        required_columns (list): List with the name of columns that must not have null values.

    Returns:
        string: Status log
    """
    # Verificar se todas as colunas obrigatórias estão presentes no DataFrame
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
        logger.error(e)
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

#Deletar arquivos da ingestão após inclusão dos dados na tabela. 
def delete_files(bucket_name, blob_name):
    """
    Delete files from ingestion after adding data to the table.

    Args:
        bucket_name (string): Bucket name.
        blob_name (string): Name of the path within the bucket.

    Returns:
        string: Status log
    """

    service_account_key_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    # Criar cliente de storage
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    
    # Obter referência ao bucket
    bucket = storage_client.bucket(bucket_name)
    
    # Listar todos os arquivos (blobs) com o prefixo especificado
    blobs = bucket.list_blobs(prefix=blob_name)
        
    # Deletar cada arquivo encontrado
    for blob in blobs:
        # Verifica se o blob é um arquivo e não uma pasta
        if not blob.name.endswith('/'):
            blob.delete()
            print(f"File {blob.name} successfully deleted.")
    
    print("All files were successfully deleted.")
    
    logger.info(f"File {blob_name} successfully deleted from bucket {bucket_name}.")


# COMMAND ----------

#Função template da ingestão
def ingestion(db_name, table_name, sep, required_columns):
    """
    Template de ingestão para arquivos csv.

    Args:
        db_name (string): Name of the table database/catalog.
        table_name (string): Table name.
        sep (string): Data separator within the file.
        required_columns (list):  List with the name of columns that must not have null values.

    Returns:
        string: Status log.

    """
    try:
        #Coletar tempo inicial da execução
        start_time_total_execution = datetime.now()
        logger.info(f"Start of execution: {start_time_total_execution}")

        #Gerar conexão com Storage GCP
        conex_gcp()
        
        #Definição de variáveis
        bucket_name = "data-ingestion-bucket-datamaster"
        blob_name = f"table_ingestion_files/{table_name}/"

        #Realiza load dos dados e inclusão do campo com data de carga
        path_load = f"gs://{bucket_name}/{blob_name }"
        logger.info(f"Starting to load data into the path {path_load}/")
        load_start_time = datetime.now()
        df = load_data_ingestion(f"{path_load}", header="true",sep=sep)
        load_total_time = monitor_execution_time(load_start_time)
        logger.info(f"Total time to load data: {load_total_time} seconds")

        # Tamanho dos dados carregados em bytes
        data_size_bytes = df.rdd.map(lambda row: len(str(row))).reduce(lambda x, y: x + y)
        data_size_mb = data_size_bytes / (1024 * 1024)
        data_size_mb_formatted = float(f"{data_size_mb:.2f}")
        logger.info(f"Size of loaded data: {data_size_mb_formatted} MB")

        #Quantidade de dados carregados
        number_lines_loaded = df.count()
        logger.info(f"Number of lines loaded {number_lines_loaded }")

        #Realizando limpeza de espacos em branco no nome das colunas
        df_write_clean = clean_column_names(df)

        #Realizar validação de campos nulos, obrigatorios e nao obrigatorios
        check_nulls(df_write_clean, required_columns)

        #Verificar existência da tabela
        try:
            if spark.catalog.tableExists(f"{db_name}.{table_name}"):
                logger.info(f"The table {db_name}.{table_name} exists.")
            else:
                logger.info(f"The table {db_name}.{table_name} does not exist.")
        except Exception as e:
            logger.error(f"An error occurred while checking the table: {e}")

        #Gravar dados na tabela
        write_start_time = datetime.now()
        logger.info(f"Writing data to the table: {table_name}")
        df_write_clean.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .option("parquet.file.size", "128MB") \
            .saveAsTable(f"{db_name}.{table_name}")
            
        write_total_time = monitor_execution_time(write_start_time)
        logger.info(f"Data recording execution time: {write_total_time} seconds")

        #Verificar quantidade de dados inseridos
        logger.info(f"Checking amount of data entered")
        current_date = datetime.now()
        dat_carga = current_date.strftime("%Y%m%d")
        df_verify = spark.read.format("delta").table(f"{db_name}.{table_name}").where(col("dat_ref_carga") == dat_carga)
        qtd_total_rows_insert = df_verify.count()
        num_columns_table = len(df_verify.columns)
        logger.info(f"A total of {qtd_total_rows_insert} rows and a total of {num_columns_table} columns were inserted into the table")

        #Verificar numero de arquivos gerados
        logger.info(f"Checking total generated files")
        df_with_file_name = df.withColumn("file_name", input_file_name())
        num_files = df_with_file_name.select("file_name").distinct().count()
        logger.info(f"Total files generated: {num_files}")

        #Coletar tempo final da execução
        total_execution = monitor_execution_time(start_time_total_execution )
        final_time_total_execution = datetime.now()
        logger.info(f"End of execution: {final_time_total_execution}")
        logger.info(f"Total execution time: {total_execution}")

        if number_lines_loaded == qtd_total_rows_insert:
            alerta = False
            logger.info(f"Table {table_name} ingested successfully")
            logger.info(f"No alerts regarding validation of entered quantities")
        else:
            alerta = True 
            logger.info(f"Table {table_name} ingested successfully")
            logger.warning(f"Check table ingestion, has an ALERT regarding the difference in data found on the load date")

        metricas = [{
            "table_name": table_name, #Nome da tabela
            "load_total_time": load_total_time, #Tempo total de load dos dados brutos
            "number_lines_loaded": number_lines_loaded, #Número de linhas na tabela com o date em execução
            "data_size_mb_formatted": data_size_mb_formatted, #Tamanho em MB dos dados brutos carregados
            "write_total_time": write_total_time, #Tempo total de escrita na tabela delta
            "qtd_total_rows_insert": qtd_total_rows_insert, #Quantidade total de linhas inseridas
            "num_columns_table": num_columns_table, #Numero de colunas da tabela
            "num_files": num_files, #Número de arquivos parquet gerados
            "total_execution": total_execution, #Tempo total de execução do template de ingestão
            "dat_carga": dat_carga, #Data de execução
            "alerta": alerta #Alerta em divergência de quantidade de dados inseridos no o mesmo odate
        }]

        #Insertir dados na tabela de métricas do Big Query
        logger.info("Inserting metrics data into Big Query")
        insert_bigquery(metricas)  

        logger.info("Deletando arquivos do path de origem")
        #delete_files(bucket_name, blob_name)

        return metricas
    
    except Exception as e:
        return logger.error(f"Error ingesting table {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Execução do Template de Ingestão

# COMMAND ----------

#Ingestião tabela clientes
#Variaveis esperadas para template de carga
table_name_clientes = "clientes"
db_name_clientes = "cadastros"
sep = ";"
required_columns = ['nome', 'cpf'] 

#Template de ingestão e atribuição de métricas
metricas_clientes = ingestion(db_name_clientes, table_name_clientes, sep, required_columns, format_file)

# COMMAND ----------

#Ingestião tabela produtos
#Variaveis esperadas para template de carga
table_name_produtos = "produtos"
db_name_produtos = "cadastros"
sep = ","
required_columns = ['id', 'nome', 'descricao', 'categoria'] 

#Template de ingestão e atribuição de métricas
metricas_produtos = ingestion(db_name_produtos, table_name_produtos, odate_produtos, sep, required_columns)

# COMMAND ----------

#Ingestião tabela clientesxprod
#Variaveis esperadas para template de carga
table_name_clientesxprod = "clientesxprod"
db_name_clientesxprod = "vendas"
sep = ","
required_columns = ['cliente_id', 'produto_id'] 

#Template de ingestão e atribuição de métricas
metricas_clientesxprod = ingestion(db_name_clientesxprod, table_name_clientesxprod, odate_clientesxprod, sep, required_columns)
