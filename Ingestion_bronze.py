from pyspark.sql.functions import col, sum, expr, count, row_number, lit, input_file_name
from pyspark.sql import DataFrame
from pyspark.sql.functions import col as spark_col, sum as spark_sum
from pyspark.sql.window import Window
from datetime import datetime
import logging

###############################################################

#Instancia logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

###############################################################

#Funções Compartilhadas (Utilitárias)

#Cria conexao com Storage GCP
def conex_gcp():
    try:
        dbutils.fs.cp("dbfs:/FileStore/keyfiles/your_key.json", "file:/tmp/your_key.json")
        service_account_key_file = "/tmp/your_key.json"

        # Configurar as credenciais para acessar o Google Cloud Storage
        spark.conf.set("fs.gs.auth.service.account.enable", "true")
        spark.conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)

        # Exemplo de leitura de dados do GCS
        bucket_name = "lake_data_master"

        return logger.info("Connection successfully")
    
    except Exception as e:
        return logger.error("Connection failed")
    
#Realiza o load dos dados no path raiz
def load_data_ingestion(path, header, sep):
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
    
def check_nulls(df: DataFrame, required_columns: list):
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
    
    end_time = datetime.now()
    duration = end_time - start_time
    duration_minutes = duration.total_seconds()

    return duration_minutes 

#Limpar espacos em branco em nome de colunas     
def clean_column_names(df):
    # Obter os nomes das colunas
    column_names = df.columns
    
    # Criar um dicionário de mapeamento para renomear as colunas
    new_column_names = {name: name.strip() for name in column_names}
    
    # Aplicar as renomeações
    for old_name, new_name in new_column_names.items():
        if old_name != new_name:  # Verificar se o nome precisa ser alterado
            df = df.withColumnRenamed(old_name, new_name)
    
    return df


###############################################################

#Função template da ingestão
def ingestion(db_name, table_name, odate, sep, required_columns):
    try:
        #Coletar tempo inicial da execução
        start_time_total_execution = datetime.now() #métricas
        logger.info(f"Start of execution: {start_time_total_execution}")

        #Gerar conexão com Storage GCP
        conex_gcp()

        #Realiza load dos dados e inclusão do campo com data de carga
        path_load = f"gs://data-ingestion-bucket-datamaster/table_ingestion_files/{table_name}"
        logger.info(f"Starting to load data into the path {path_load}/{table_name}_{odate}")
        load_start_time = datetime.now()
        df = load_data_ingestion(f"{path_load}/{table_name}_{odate}.csv", header="true",sep=sep)
        load_total_time = monitor_execution_time(load_start_time )
        logger.info(f"Total time to load data: {load_total_time} seconds")

        # Tamanho dos dados carregados em bytes
        data_size_bytes = df.rdd.map(lambda row: len(str(row))).reduce(lambda x, y: x + y)
        data_size_mb = data_size_bytes / (1024 * 1024)
        data_size_mb_formatted = f"{data_size_mb:.2f}"
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

        metricas = {
            "table_name": table_name,
            "load_total_time": load_total_time,
            "number_lines_loaded": number_lines_loaded,
            "data_size_mb_formatted": data_size_mb_formatted,
            "write_total_time": write_total_time,
            "qtd_total_rows_insert": qtd_total_rows_insert,
            "num_columns_table": num_columns_table,
            "num_files": num_files,
            "total_execution": total_execution,
            "dat_carga": dat_carga,
            "alerta": alerta
        }
        
        return metricas
    
    except Exception as e:
        return logger.error(f"Error ingesting table {table_name}: {e}")

###############################################################

#Ingestião tabela clientes
#Variaveis esperadas para template de carga
table_name_clientes = "clientes"
db_name_clientes = "cadastros"
odate_clientes = "20240909"
sep = ";"
required_columns = ['nome', 'cpf'] 

#Template de ingestão e atribuição de métricas
metricas_clientes = ingestion(db_name, table_name_clientes, odate, sep, required_columns)
print(metricas_clientes)


###############################################################

#Ingestião tabela produtos
#Variaveis esperadas para template de carga
table_name_produtos = "produtos"
db_name_produtos = "cadastros"
odate_produtos = "20240909"
sep = ","
required_columns = ['id', 'nome', 'descricao', 'categoria'] 

#Template de ingestão e atribuição de métricas
metricas_produtos = ingestion(db_name_produtos, table_name_produtos, odate_produtos, sep, required_columns)
print(metricas_produtos)

###############################################################

#Ingestião tabela clientesxprod
#Variaveis esperadas para template de carga
table_name_clientesxprod = "clientesxprod"
db_name_clientesxprod = "vendas"
odate_clientesxprod = "20240909"
sep = ","
required_columns = ['cliente_id', 'produto_id'] 

#Template de ingestão e atribuição de métricas
metricas_clientesxprod = ingestion(db_name_clientesxprod, table_name_clientesxprod, odate_clientesxprod, sep, required_columns)
print(metricas_clientesxprod)
