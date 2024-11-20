# Databricks notebook source
import logging

# COMMAND ----------

#Instancia logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# COMMAND ----------

# Define the actual container name and storage account name
sas_token = dbutils.secrets.get(scope="storage_datamaster", key="data_master")
storage_account_name = "datalake1datamaster"
secret_scope_name = "storage_datamaster"
secret_key_name = "data_master_account_key"

# Retrieve the storage account key from the secret scope
storage_account_key = dbutils.secrets.get(scope=secret_scope_name, key=secret_key_name)

# Configure the storage account access key
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

# Define the path of the container bronze
container_name_bronze = "bronze"
container_path_bronze = f"abfss://{container_name_bronze}@{storage_account_name}.dfs.core.windows.net/"
dbutils.fs.ls(f"abfss://{container_name_bronze}@{storage_account_name}.dfs.core.windows.net/")

# Define the path of the container silver
container_name_silver = "silver"
container_path_silver = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/"
dbutils.fs.ls(f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/")

# Define the path of the container gold
container_name_gold = "gold"
container_path_gold = f"abfss://{container_name_gold}@{storage_account_name}.dfs.core.windows.net/"
dbutils.fs.ls(f"abfss://{container_name_gold}@{storage_account_name}.dfs.core.windows.net/")

# COMMAND ----------



# COMMAND ----------

#Criar tabela delta clientes - Bronze
def create_delta_table_clientes(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                client_id STRING,
                nome STRING,
                sobrenome STRING,
                cpf STRING,
                rg STRING,
                data_nascimento STRING,
                est_civil STRING,
                genero STRING,
                email STRING,
                tel_res STRING,
                tel_cel STRING,
                endereco STRING,
                pais STRING,
                cidade STRING,
                estado STRING,
                cep STRING,
                nacionalidade STRING,
                renda STRING,
                cargo STRING,
                tp_cliente STRING,
                data_criacao STRING,
                dat_ref_carga STRING
            )
            USING DELTA
            PARTITIONED BY (dat_ref_carga)
            LOCATION '{delta_table_path}'
            """


    # Criar a tabela Delta com nomes de coluna válidos
    try:
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada com sucesso")

    except Exception as e:
        return logger.error(f"Erro na criação da tabela: {e}")

# COMMAND ----------

#Criar tabela delta produtos - Bronze
def create_delta_table_produtos(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                produto_id STRING,
                nome STRING,
                descricao STRING,
                categoria STRING,
                data_lancamento STRING,
                taxa_juros STRING,
                taxa_administracao STRING,
                limite_credito STRING,
                prazo STRING,
                contato_suporte STRING,
                taxa_adesao STRING,
                data_ultima_atualizacao STRING,
                prazo_carencia STRING,
                taxa_rentabilidade STRING,
                periodo_investimento STRING,
                multa_cancelamento STRING,
                dat_ref_carga STRING
            )
            USING DELTA
            PARTITIONED BY (dat_ref_carga)
            LOCATION '{delta_table_path}'
            """


    # Criar a tabela Delta com nomes de coluna válidos
    try:
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada com sucesso")

    except Exception as e:
        return logger.error(f"Erro na criação da tabela: {e}")

# COMMAND ----------

#Criar tabela delta clientesxprod - Bronze
def create_delta_table_clientesxprod(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                cliente_id STRING,
                produto_id STRING,
                data_aquisicao STRING,
                valor_aquisicao STRING,
                dat_ref_carga STRING
            )
            USING DELTA
            PARTITIONED BY (dat_ref_carga)
            LOCATION '{delta_table_path}'
            """


    # Criar a tabela Delta com nomes de coluna válidos
    try:
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada com sucesso")

    except Exception as e:
        return logger.error(f"Erro na criação da tabela: {e}")

# COMMAND ----------

#Criar tabela delta produtos - Silver
def create_delta_table_clie_limit(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                num_doc STRING,
                est_civil STRING,
                idade STRING,
                tp_cliente STRING,
                classe_renda STRING,
                genero STRING,
                estado STRING,
                renda STRING,
                prod_contratado STRING,
                valor_prod_contratado STRING,
                categoria STRING,
                limite_credito STRING,
                dat_ref_carga STRING
            )
            USING DELTA
            PARTITIONED BY (dat_ref_carga)
            LOCATION '{delta_table_path}'
            """


    # Criar a tabela Delta com nomes de coluna válidos
    try:
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada com sucesso")

    except Exception as e:
        return logger.error(f"Erro na criação da tabela: {e}")

# COMMAND ----------

#Criar tabela delta prod_contrat_diario - Gold
def create_delta_prod_contrat_diario(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                prod_contratado STRING,
                valor_total_prod_contratado DOUBLE,
                valor_total_limite_credito DOUBLE,
                dat_ref_carga STRING
            )
            USING DELTA
            PARTITIONED BY (dat_ref_carga)
            LOCATION '{delta_table_path}'
            """


    # Criar a tabela Delta com nomes de coluna válidos
    try:
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada com sucesso")

    except Exception as e:
        return logger.error(f"Erro na criação da tabela: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Executar Create

# COMMAND ----------

# Criando o banco de dados se ele não existir
db_name = "b_cad"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

#Tabela Clientes
#Variáveis de path e nome da tabela
name_table_clientes = "clientes"
delta_table_path_clientes = f"{container_path_bronze}{name_table_clientes}/"

#Criar tabela delta - Bronze
create_delta_table_clientes(delta_table_path_clientes, db_name, name_table_clientes)


#Tabela produtos
#Variáveis de path e nome da tabela
name_table_produtos = f"produtos"
delta_table_path_produtos = f"{container_path_bronze}{name_table_produtos}/"

#Criar tabela delta - Bronze
create_delta_table_produtos(delta_table_path_produtos, db_name, name_table_produtos)

# COMMAND ----------

# Criando o banco de dados se ele não existir
db_name = "b_vend"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

#Tabela produtos
#Variáveis de path e nome da tabela
name_table_clientesxprod = "clientesxprod"
delta_table_path_clientesxprod = f"{container_path_bronze}{name_table_clientesxprod}/"


#Criar tabela delta - Bronze
create_delta_table_clientesxprod(delta_table_path_clientesxprod, db_name, name_table_clientesxprod)

# COMMAND ----------

# Criando o banco de dados se ele não existir
db_name = "s_vend"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

#Tabela produtos
#Variáveis de path e nome da tabela
name_table_cclie_limit = "clie_limit"
delta_table_path_clie_limit = f"{container_path_silver}{name_table_cclie_limit}/"


#Criar tabela delta - Bronze
create_delta_table_clie_limit(delta_table_path_clie_limit, db_name, name_table_cclie_limit)

# COMMAND ----------

# Criando o banco de dados se ele não existir
db_name = "g_vend"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

#Tabela produtos
#Variáveis de path e nome da tabela
name_table_prod_contrat_diario = "prod_contrat_diario"
delta_table_path_prod_contrat_diario = f"{container_path_gold}{name_table_prod_contrat_diario}/"

create_delta_prod_contrat_diario(delta_table_path_prod_contrat_diario , db_name, name_table_prod_contrat_diario)
