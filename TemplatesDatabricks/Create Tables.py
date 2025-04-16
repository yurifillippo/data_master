# Databricks notebook source
import logging

# COMMAND ----------

#Instancia logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# COMMAND ----------

# Define the actual container name and storage account name
sas_token = dbutils.secrets.get(scope="storage_datamaster", key="data_master")
storage_account_name = "datalakedtm"
secret_scope_name = "storage_datamaster"
secret_key_name = "data_master_account_key"

# Recupera a chave da conta de armazenamento
storage_account_key = dbutils.secrets.get(scope=secret_scope_name, key=secret_key_name)

# Configura a chave de acesso da conta de armazenamento
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

# Defina o caminho do container bronze
container_name_bronze = "bronze"
container_path_bronze = f"abfss://{container_name_bronze}@{storage_account_name}.dfs.core.windows.net/"
dbutils.fs.ls(f"abfss://{container_name_bronze}@{storage_account_name}.dfs.core.windows.net/")

# Defina o caminho do container silver
container_name_silver = "silver"
container_path_silver = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/"
dbutils.fs.ls(f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/")

# Defina o caminho do container gold
container_name_gold = "gold"
container_path_gold = f"abfss://{container_name_gold}@{storage_account_name}.dfs.core.windows.net/"
dbutils.fs.ls(f"abfss://{container_name_gold}@{storage_account_name}.dfs.core.windows.net/")

# COMMAND ----------

#Criar tabela delta clientes - Bronze
def create_delta_table_clientes(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                client_id STRING COMMENT 'Identificador único do cliente',
                nome STRING COMMENT 'Nome do cliente',
                sobrenome STRING COMMENT 'Sobrenome do cliente',
                cpf STRING COMMENT 'Cadastro de Pessoa Física (CPF) do cliente',
                rg STRING COMMENT 'Registro Geral (RG) do cliente',
                data_nascimento STRING COMMENT 'Data de nascimento do cliente',
                est_civil STRING COMMENT 'Estado civil do cliente',
                genero STRING COMMENT 'Gênero do cliente',
                email STRING COMMENT 'Endereço de e-mail do cliente',
                tel_res STRING COMMENT 'Telefone residencial do cliente',
                tel_cel STRING COMMENT 'Telefone celular do cliente',
                endereco STRING COMMENT 'Endereço completo do cliente',
                pais STRING COMMENT 'País de residência do cliente',
                cidade STRING COMMENT 'Cidade de residência do cliente',
                estado STRING COMMENT 'Estado de residência do cliente',
                cep STRING COMMENT 'Código de Endereçamento Postal (CEP) do cliente',
                nacionalidade STRING COMMENT 'Nacionalidade do cliente',
                renda STRING COMMENT 'Renda mensal do cliente',
                cargo STRING COMMENT 'Cargo ou profissão do cliente',
                tp_cliente STRING COMMENT 'Tipo de cliente (exemplo: PF, PJ)',
                data_criacao STRING COMMENT 'Data de criação do cadastro do cliente',
                dat_ref_carga STRING COMMENT 'Data de referência da carga de dados'
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
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")


# COMMAND ----------

#Criar tabela delta produtos - Bronze
def create_delta_table_produtos(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
		        produto_id STRING COMMENT 'Identificador único do produto',
                nome STRING COMMENT 'Nome do produto',
                descricao STRING COMMENT 'Descrição detalhada do produto',
                categoria STRING COMMENT 'Categoria do produto',
                data_lancamento STRING COMMENT 'Data de lançamento do produto',
                taxa_juros STRING COMMENT 'Taxa de juros aplicada ao produto',
                taxa_administracao STRING COMMENT 'Taxa de administração do produto',
                limite_credito STRING COMMENT 'Limite de crédito associado ao produto',
                prazo STRING COMMENT 'Prazo de duração do produto ou contrato',
                contato_suporte STRING COMMENT 'Contato de suporte para o produto',
                taxa_adesao STRING COMMENT 'Taxa de adesão para aquisição do produto',
                data_ultima_atualizacao STRING COMMENT 'Data da última atualização do produto',
                prazo_carencia STRING COMMENT 'Prazo de carência antes da ativação do produto',
                taxa_rentabilidade STRING COMMENT 'Taxa de rentabilidade esperada para o produto',
                periodo_investimento STRING COMMENT 'Período de investimento associado ao produto',
                multa_cancelamento STRING COMMENT 'Multa aplicável em caso de cancelamento',
                dat_ref_carga STRING COMMENT 'Data de referência da carga de dados'
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
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")

# COMMAND ----------

#Criar tabela delta clientesxprod - Bronze
def create_delta_table_clientesxprod(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
		        id STRING COMMENT 'Identificador único da aquisição',
                cliente_id STRING COMMENT 'Identificador do cliente que realizou a aquisição',
                produto_id STRING COMMENT 'Identificador do produto adquirido',
                data_aquisicao STRING COMMENT 'Data em que a aquisição foi realizada',
                valor_aquisicao STRING COMMENT 'Valor total da aquisição',
                dat_ref_carga STRING COMMENT 'Data de referência da carga de dados'
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
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")

# COMMAND ----------

#Criar tabela delta produtos - Silver
def create_delta_table_clie_limit(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                num_doc STRING COMMENT 'Número do documento do cliente',
                est_civil STRING COMMENT 'Estado civil do cliente',
                idade STRING COMMENT 'Idade do cliente',
                tp_cliente STRING COMMENT 'Tipo de cliente (exemplo: PF, PJ)',
                classe_renda STRING COMMENT 'Faixa de renda do cliente',
                genero STRING COMMENT 'Gênero do cliente',
                estado STRING COMMENT 'Estado de residência do cliente',
                renda STRING COMMENT 'Renda mensal do cliente',
                prod_contratado STRING COMMENT 'Produto financeiro contratado pelo cliente',
                valor_prod_contratado STRING COMMENT 'Valor do produto contratado',
                categoria STRING COMMENT 'Categoria do produto contratado',
                limite_credito STRING COMMENT 'Limite de crédito disponível para o cliente',
                dat_ref_carga STRING COMMENT 'Data de referência da carga de dados'
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
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")

# COMMAND ----------

#Criar tabela delta prod_contrat_diario - Gold
def create_delta_prod_contrat_diario(delta_table_path, db_name, table_name):
    create_string = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                prod_contratado STRING COMMENT 'Nome do produto financeiro contratado',
                valor_total_prod_contratado DOUBLE COMMENT 'Valor total dos produtos contratados',
                valor_total_limite_credito DOUBLE COMMENT 'Valor total do limite de crédito disponível',
                dat_ref_carga STRING COMMENT 'Data de referência da carga de dados'
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
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")

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

#Criar tabela delta clientes - Bronze
create_delta_table_clientes(delta_table_path_clientes, db_name, name_table_clientes)


#Tabela produtos
#Variáveis de path e nome da tabela
name_table_produtos = f"produtos"
delta_table_path_produtos = f"{container_path_bronze}{name_table_produtos}/"

#Criar tabela delta produtos - Bronze
create_delta_table_produtos(delta_table_path_produtos, db_name, name_table_produtos)

# COMMAND ----------

# Criando o banco de dados se ele não existir
db_name = "b_vend"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

#Tabela produtos
#Variáveis de path e nome da tabela
name_table_clientesxprod = "clientesxprod"
delta_table_path_clientesxprod = f"{container_path_bronze}{name_table_clientesxprod}/"


#Criar tabela delta clientesxprod - Bronze
create_delta_table_clientesxprod(delta_table_path_clientesxprod, db_name, name_table_clientesxprod)

# COMMAND ----------

# Criando o banco de dados se ele não existir
db_name = "s_vend"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

#Tabela produtos
#Variáveis de path e nome da tabela
name_table_cclie_limit = "clie_limit"
delta_table_path_clie_limit = f"{container_path_silver}{name_table_cclie_limit}/"

#Criar tabela delta clie_limit - Silver
create_delta_table_clie_limit(delta_table_path_clie_limit, db_name, name_table_cclie_limit)

# COMMAND ----------

# Criando o banco de dados se ele não existir
db_name = "g_vend"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

#Tabela produtos
#Variáveis de path e nome da tabela
name_table_prod_contrat_diario = "prod_contrat_diario"
delta_table_path_prod_contrat_diario = f"{container_path_gold}{name_table_prod_contrat_diario}/"

#Criar tabela delta prod_contrat_diario - Gold
create_delta_prod_contrat_diario(delta_table_path_prod_contrat_diario , db_name, name_table_prod_contrat_diario)
