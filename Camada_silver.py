from pyspark.sql.functions import col, sum, expr, count, row_number, lit, input_file_name, sha2, current_date, datediff, floor, when, broadcast
from pyspark.sql import DataFrame
from pyspark.sql.functions import col as spark_col, sum as spark_sum
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import DateType
import logging
import os
from google.cloud import bigquery


##########################################

#Instancia logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

#Atribuir variável de embiente
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/your_key.json"

##########################################

#Funções Compartilhadas (Utilitárias)

#Cria conexao com Storage GCP
def conex_gcp():
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
            return logger.info("Dados inseridos com sucesso.")
        else:
            return logger.info(f"Erros ao inserir dados: {errors}")
    
    except Exception as e:
        return logger.error("Connection failed")    
    
#Coletar tempo de execução em segundos
def monitor_execution_time(start_time):
    
    end_time = datetime.now()
    duration = end_time - start_time
    duration_minutes = duration.total_seconds()

    return duration_minutes 
    

##########################################

conex_gcp()

##########################################

dat_carga = "20240922"

##########################################

# Coletar tempo inicial da execução
start_time_total_execution = datetime.now()
logger.info(f"Start of execution: {start_time_total_execution}")

# Carregar tabela de clientes
logger.info("Carregando tabela b_cad.clientes")
load_start_time = datetime.now()
df_clientes = spark.read.table("b_cad.clientes").select('client_id', 'cpf', 'rg', 'data_nascimento', 'est_civil', 'genero', 'estado', 'renda', 'tp_cliente').where(col("dat_ref_carga") == dat_carga)
logger.info(f"Tabela cadastros.clientes carregada com sucesso")

# Carregar tabela de produtos
logger.info("Carregando tabela b_cad.produtos")
df_produtos = spark.read.table("b_cad.produtos").select('produto_id', 'nome', 'categoria', 'limite_credito').where(col("dat_ref_carga") == dat_carga)
logger.info(f"Tabela cadastros.produtos carregada com sucesso")

# Carregar tabela de clientesxprod
logger.info("Carregando tabela b_vend.clientesxprod")
df_clientesxprod = spark.read.table("b_vend.clientesxprod").where(col("dat_ref_carga") == dat_carga)
logger.info(f"Tabela vendas.clientesxprod carregada com sucesso")

# Tempo total de load das tabelas
load_total_time = monitor_execution_time(load_start_time)
logger.info(f"[* Metrics *] - Total time to load data: {load_total_time} seconds")

# Tamanho dos dados carregados em bytes
df_produtos_size_bytes = df_produtos.rdd.map(lambda row: len(str(row))).reduce(lambda x, y: x + y)
df_clientes_size_bytes = df_clientes.rdd.map(lambda row: len(str(row))).reduce(lambda x, y: x + y)
df_clientesxprod_size_bytes = df_clientesxprod.rdd.map(lambda row: len(str(row))).reduce(lambda x, y: x + y)

data_size_bytes = (df_produtos_size_bytes + df_clientes_size_bytes + df_clientesxprod_size_bytes)

data_size_mb = data_size_bytes / (1024 * 1024)
data_size_mb_formatted = float(f"{data_size_mb:.2f}")
logger.info(f"[* Metrics *] - Size of loaded data: {data_size_mb_formatted} MB")

# Join entre clientesxprod e clientes
df_clientesxprod = df_clientesxprod.withColumnRenamed('cliente_id', 'client_id')
df_join_clientesxprod_cli = df_clientesxprod.join(df_clientes, on='client_id', how='left')

# Join entre df_join_clientesxprod_cli e tabela de produtos
df_final_join = df_join_clientesxprod_cli.join(broadcast(df_produtos), on='produto_id', how='left')

#Criar novas colunas
df_final = df_final_join.withColumn("num_doc", sha2(col("cpf"), 256)) \
                        .withColumn("data_nascimento", col("data_nascimento").cast(DateType())) \
                        .withColumn("idade", floor(datediff(current_date(), col("data_nascimento")) / 365.25).cast("string")) \
                        .withColumn(
                        "classe_renda", when(col("renda") <= 521, "Classe E")
                        .when((col("renda") > 521) & (col("renda") <= 1042), "Classe D")
                        .when((col("renda") > 1042) & (col("renda") <= 4427), "Classe C")
                        .when((col("renda") > 4427) & (col("renda") <= 8856), "Classe B")
                        .otherwise("Classe A")) \
                        .withColumn("prod_contratado", col("nome")) \
                        .withColumn("valor_prod_contratado", col("valor_aquisicao")) \
                        .withColumn("dat_ref_carga", lit(dat_carga)) 


cols_df = ['num_doc','est_civil','idade','tp_cliente','classe_renda','genero','estado','renda','prod_contratado','valor_prod_contratado','categoria','limite_credito','dat_ref_carga']

#Definir apenas registros com limite de crédito
df_insert = df_final.select(cols_df).where(col("limite_credito").isNotNull())

#Verificando quantidade de registros após o filtro de limite de crédito
qtd_registros = df_insert.count()
logger.info(f"[* Metrics *] -  Numero total de linhas: {qtd_registros}")

#Inserir dados na tabela clie_limit
write_start_time = datetime.now()
db_name = "s_vend"
table_name = "clie_limit"
logger.info(f"Writing data to the table: {table_name}")
df_insert.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("parquet.file.size", "128MB") \
    .saveAsTable(f"{db_name}.{table_name}")
    
write_total_time = monitor_execution_time(write_start_time)
logger.info(f"[* Metrics *] - Data recording execution time: {write_total_time} seconds")

#Verificar quantidade de dados inseridos
logger.info(f"Checking amount of data entered")
current_date = datetime.now()

df_verify = spark.read.format("delta").table(f"{db_name}.{table_name}").where(col("dat_ref_carga") == dat_carga)
qtd_total_rows_insert = df_verify.count()
num_columns_table = len(df_verify.columns)
logger.info(f"[* Metrics *] - A total of {qtd_total_rows_insert} rows and a total of {num_columns_table} columns were inserted into the table")

#Verificar numero de arquivos gerados
logger.info(f"Checking total generated files")
df_with_file_name = df_verify.withColumn("file_name", input_file_name())
num_files = df_with_file_name.select("file_name").distinct().count()
logger.info(f"[* Metrics *] - Total files generated: {num_files}")

#Coletar tempo final da execução
total_execution = monitor_execution_time(start_time_total_execution)
final_time_total_execution = datetime.now()
logger.info(f"End of execution: {final_time_total_execution}")
logger.info(f"[* Metrics *] - Total execution time: {total_execution}")

if qtd_registros == qtd_total_rows_insert:
    alerta = False
    logger.info(f"Table {table_name} ingested successfully")
    logger.info(f"No alerts regarding validation of entered quantities")
else:
    alerta = True 
    logger.info(f"Table {table_name} ingested successfully")
    logger.warning(f"Check table ingestion, has an ALERT regarding the difference in data found on the load date")


metricas = [{
    "table_name": table_name, #Nome da tabela ok
    "load_total_time": load_total_time, #Tempo total de load dos dados brutos ok
    "qtd_total_rows_insert": qtd_registros, #Quantidade total de linhas inseridas ok
    "write_total_time": write_total_time, #Tempo total de escrita na tabela delta ok
    "data_size_mb_formatted": data_size_mb_formatted, #Tamanho em MB dos dados brutos carregados
    "qts_total_rows_insert_verify": qtd_total_rows_insert, #Quantidade de linhas ao ler a tabela com o odate ok
    "num_columns_table": num_columns_table, #Numero de colunas da tabelaok
    "num_files": num_files, #Número de arquivos parquet gerados ok
    "total_execution": total_execution, #Tempo total de execução do template de ingestão
    "dat_carga": dat_carga, #Data de execução
    "alerta": alerta #Alerta em divergência de quantidade de dados inseridos no o mesmo odate
}]

#Insertir dados na tabela de métricas do Big Query
logger.info("Inserting metrics data into Big Query")
#insert_bigquery(metricas)  

logger.info("Deletando arquivos do path de origem")
#delete_files(bucket_name, blob_name)

logger.info("Successfully Completed")

#####################

# Tempo total de execução
execution_total_time = monitor_execution_time(start_time_total_execution)
logger.info(f"Total time to execution: {execution_total_time} seconds")

##########################################

#TODO
Validar métricas
Validar tempos e o que é necessário medir
Criar tabela de métricas da silver
Testar insert dos dados no big query
Verificar coleta de métricas do spark no ingestion e na silver

##########################################
