# Databricks notebook source
# MAGIC %md
# MAGIC Origem dos dados: https://catalog.data.gov/dataset/electric-vehicle-population-data

# COMMAND ----------

# MAGIC %md
# MAGIC LIBS

# COMMAND ----------

from pyspark.sql.functions import col, sum, expr, count, row_number, lit
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC Functions

# COMMAND ----------

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

        #dbutils.fs.rm("file:/tmp/your_key.json", recurse=False)

        return "Conexao realizada com sucesso"
    
    except:
        return "Conexao falhou"


# COMMAND ----------

# MAGIC %md
# MAGIC #ETL

# COMMAND ----------

# MAGIC %md
# MAGIC Load

# COMMAND ----------

#Criar conexão com Storage GCP
conex_gcp()

#Path com dados brutos
path = f"gs://lake_data_master/ingestion/Electric_Vehicle_Population_Data.csv"

#Load dos dados
df = spark.read.format("csv").option("header", "true").load(path)

# Obter a data atual
current_date = datetime.now()

# Formatar a data no formato 'ano-mês-dia'
dat_carga = current_date.strftime("%Y%m%d")
df_dat_carga = df.withColumn("Dat_ref_carga", lit(dat_carga))

# COMMAND ----------

df_dat_carga.display()

# COMMAND ----------

#DEfiniar path Bronze
delta_table_path = "gs://lake_data_master/Bronze/Electric_Vehicle_Population"

#Create table
create_string = f"""
        CREATE TABLE IF NOT EXISTS Electric_Vehicle_Population
            (
                VIN_1_to_10 STRING COMMENT '',
                County STRING COMMENT 'Cidade em estado de Washington',
                City STRING COMMENT '',
                State STRING COMMENT 'Estado',
                Postal_Code STRING COMMENT 'Codigo Postal',
                Model_Year STRING COMMENT 'Ano do modelo do veículo',
                Make STRING COMMENT 'Local de fabricacao',
                Model STRING COMMENT 'Modelo do veiculo',
                Electric_Vehicle_Type STRING COMMENT 'Tipo do veiculo',
                Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility STRING COMMENT '',
                Electric_Range STRING COMMENT '',
                Base_MSRP STRING COMMENT '',
                Legislative_District STRING COMMENT '',
                DOL_Vehicle_ID STRING COMMENT '',
                Vehicle_Location STRING COMMENT '',
                Electric_Utility STRING COMMENT '',
                Census_Tract_2020 STRING COMMENT '',
                Dat_ref_carga STRING COMMENT 'Data de carga dos dados'
            )
            USING DELTA
            LOCATION '{delta_table_path}'
            """

# Criar a tabela Delta com nomes de coluna válidos
spark.sql(create_string)

# COMMAND ----------

# Listar dados gravados no diretório do GCP
display(dbutils.fs.ls("gs://lake_data_master/Bronze/"))

# COMMAND ----------

#Analisar integridade dos dados

#Exibir Linhas com Valores Nulos
null_counts = df_dat_carga.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_dat_carga.columns])
null_counts.display()



# COMMAND ----------

#Inserir dados na tabela
df_renamed = df_dat_carga.select(
    col("VIN (1-10)").alias("VIN_1_to_10"),
    col("County"),
    col("City"),
    col("State"),
    col("Postal Code").alias("Postal_Code"),
    col("Model Year").alias("Model_Year"),
    col("Make"),
    col("Model"),
    col("Electric Vehicle Type").alias("Electric_Vehicle_Type"),
    col("Clean Alternative Fuel Vehicle (CAFV) Eligibility").alias("Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility"),
    col("Electric Range").alias("Electric_Range"),
    col("Base MSRP").alias("Base_MSRP"),
    col("Legislative District").alias("Legislative_District"),
    col("DOL Vehicle ID").alias("DOL_Vehicle_ID"),
    col("Vehicle Location").alias("Vehicle_Location"),
    col("Electric Utility").alias("Electric_Utility"),
    col("2020 Census Tract").alias("Census_Tract_2020"),
    col("Dat_ref_carga")
)

# Agora, faça o write para a tabela Delta
df_renamed.write.format("delta").mode("append").save("gs://lake_data_master/Bronze/Electric_Vehicle_Population")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Electric_Vehicle_Population
