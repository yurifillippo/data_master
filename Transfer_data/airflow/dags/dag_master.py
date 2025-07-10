from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import pendulum
from pendulum import timezone
import os
from conf.utils import start_databricks_job, wait_for_job_to_finish

data_atual = datetime.now()
DATA_ATUAL = data_atual.strftime('%Y%m%d')
timestamp = int(data_atual.timestamp())

local_tz = timezone("America/Sao_Paulo")

# Acessando variáveis de ambiente
databricks_url = os.getenv("DATABRICKS_URL")
databricks_token = os.getenv("DATABRICKS_TOKEN")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.now(local_tz).subtract(days=1).replace(hour=1, minute=5),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

#Criar DAG
dag_master = DAG(
    'schedule_data_master',
    default_args=default_args,
    description='Scheduler datamaster',
    schedule_interval=None, #Não schedulado para demonstração do case
    catchup=False,  # Evita execução retroativa
)


############################################################################################
########################### TAREFAS PARA DADOS DE CLIENTES #################################
############################################################################################


#Tarefa 1
run_copy_clientes = BashOperator(
    task_id='run_copy_clientes',
    bash_command="docker exec linux_python /bin/bash -c '/workspace/scripts/run_copy.sh /var/lib/postgresql/exports/clientes.csv /path/to/move_cloud/clientes && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)


#Tarefa 2
move_cloud_clientes = BashOperator(
    task_id='move_cloud_clientes',
    bash_command=f"docker exec linux_python /bin/bash -c '/workspace/scripts/run_move_file.sh /path/to/move_cloud/clientes/clientes.csv table_ingestion_files/clientes/{DATA_ATUAL}/clientes_{timestamp}.csv && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)

#Tarefa 3
expurgo_clientes = BashOperator(
    task_id='expurgo_clientes',
    bash_command=f"docker exec linux_python /bin/bash -c '/workspace/scripts/expurgo_server.sh /path/to/move_cloud/clientes/ && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)

#Tarefa 4

# ID do job Databricks
job_id_clientes = "536258902708431"  # ID do seu job Databricks
param_clientes = {"param1": "b_cad",
                  "param2": "clientes",
                  "param3": "['nome', 'cpf']",
                  "param4": "append",
                  "param5": "csv",
                  "param6": ",",
                  "param7": f"{DATA_ATUAL}"}


# Tarefa para disparar o job no Databricks
def trigger_and_wait_clientes():
    run_id = start_databricks_job(job_id_clientes, param_clientes, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)


# Tarefa para disparar o job no Databricks
ingestao_bronze_clientes = PythonOperator(
    task_id='ingestao_bronze_clientes',
    python_callable=trigger_and_wait_clientes,
    trigger_rule='all_success',
    dag=dag_master,
)


#Tarefa 5
# ID do job Databricks
job_id_expurgo_clientes = "139800566680944"  # ID do seu job Databricks
param_expurgo_clientes = {"param1": "filedataimport",
                          "param2": "clientes",
                          "param3": f"{DATA_ATUAL}"}


# Tarefa para disparar o job no Databricks
def trigger_and_wait_expurgo_clientes():
    run_id = start_databricks_job(job_id_expurgo_clientes, param_expurgo_clientes, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)


expurgo_stage_clientes = PythonOperator(
    task_id='expurgo_stage_clientes',
    python_callable=trigger_and_wait_expurgo_clientes,
    trigger_rule='all_success',
    dag=dag_master,
)


#Ordem de execução das tarefas
run_copy_clientes >> move_cloud_clientes >> expurgo_clientes >> ingestao_bronze_clientes >> expurgo_stage_clientes



############################################################################################
########################### TAREFAS PARA DADOS DE PRODUTOS #################################
############################################################################################

# Tarefa 1
run_copy_produtos_bancarios = BashOperator(
    task_id='run_copy_produtos_bancarios',
    bash_command="docker exec linux_python /bin/bash -c '/workspace/scripts/run_copy.sh /var/lib/postgresql/exports/produtos_bancarios.csv /path/to/move_cloud/produtos_bancarios && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)

# Tarefa 2
move_cloud_produtos_bancarios = BashOperator(
    task_id='move_cloud_produtos_bancarios',
    bash_command=f"docker exec linux_python /bin/bash -c '/workspace/scripts/run_move_file.sh /path/to/move_cloud/produtos_bancarios/produtos_bancarios.csv table_ingestion_files/produtos/{DATA_ATUAL}/produtos_{timestamp}.csv && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)

# Tarefa 3
expurgo_produtos_bancarios = BashOperator(
    task_id='expurgo_produtos_bancarios',
    bash_command=f"docker exec linux_python /bin/bash -c '/workspace/scripts/expurgo_server.sh /path/to/move_cloud/produtos_bancarios/ && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)

# Tarefa 4

# ID do job Databricks
job_id_produtos_bancarios = "1047983050072156"  # ID do seu job Databricks
param_produtos_bancarios = {"param1": "b_cad", "param2": "produtos",
                            "param3": "['produto_id', 'nome', 'descricao', 'categoria']", "param4": "append",
                            "param5": "csv", "param6": ",", "param7": f"{DATA_ATUAL}"}


# Tarefa para disparar o job no Databricks
def trigger_and_wait_produtos_bancarios():
    run_id = start_databricks_job(job_id_produtos_bancarios, param_produtos_bancarios, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)


ingestao_bronze_produtos_bancarios = PythonOperator(
    task_id='ingestao_bronze_produtos_bancarios',
    python_callable=trigger_and_wait_produtos_bancarios,
    trigger_rule='all_success',
    dag=dag_master,
)

#Tarefa 5
# ID do job Databricks
job_id_expurgo_produtos_bancarios = "645927607587685"  # ID do seu job Databricks
param_expurgo_produtos_bancarios = {"param1": "filedataimport",
                                    "param2": "produtos",
                                    "param3": f"{DATA_ATUAL}"}


#Tarefa para disparar o job no Databricks
def trigger_and_wait_expurgo_produtos_bancarios():
    run_id = start_databricks_job(job_id_expurgo_produtos_bancarios, param_expurgo_produtos_bancarios, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)


expurgo_stage_produtos_bancarios = PythonOperator(
    task_id='expurgo_stage_produtos_bancarios',
    python_callable=trigger_and_wait_expurgo_produtos_bancarios,
    trigger_rule='all_success',
    dag=dag_master,
)



# Ordem de execução das tarefas
run_copy_produtos_bancarios >> move_cloud_produtos_bancarios >> expurgo_produtos_bancarios >> ingestao_bronze_produtos_bancarios >> expurgo_stage_produtos_bancarios




############################################################################################
###################### TAREFAS PARA DADOS DE CLIENTESXPRODUTOS #############################
############################################################################################



#Tarefa 1
run_copy_clientes_produtos = BashOperator(
    task_id='run_copy_clientes_produtos',
    bash_command="docker exec linux_python /bin/bash -c '/workspace/scripts/run_copy.sh /var/lib/postgresql/exports/clientes_produtos.csv /path/to/move_cloud/clientes_produtos && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)


#Tarefa 2
move_cloud_clientes_produtos = BashOperator(
    task_id='move_cloud_clientes_produtos',
    bash_command=f"docker exec linux_python /bin/bash -c '/workspace/scripts/run_move_file.sh /path/to/move_cloud/clientes_produtos/clientes_produtos.csv table_ingestion_files/clientesxprod/{DATA_ATUAL}/clientes_produtos_{timestamp}.csv && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)

#Tarefa 3
expurgo_clientes_produtos = BashOperator(
    task_id='expurgo_clientes_produtos',
    bash_command=f"docker exec linux_python /bin/bash -c '/workspace/scripts/expurgo_server.sh /path/to/move_cloud/clientes_produtos/ && echo \"Script Executado com Sucesso!\"'",
    dag=dag_master,
)


#Tarefa 4

# ID do job Databricks
job_id_clientes_produtos = "270506435210698"  # ID do seu job Databricks
param_clientes_produtos = {"param1": "b_vend",
                           "param2": "clientesxprod",
                           "param3": "['cliente_id', 'produto_id']",
                           "param4": "append",
                           "param5": "csv",
                           "param6": ",",
                           "param7": f"{DATA_ATUAL}"}

# Tarefa para disparar o job no Databricks
def trigger_and_wait_clientes_produtos():
    run_id = start_databricks_job(job_id_clientes_produtos, param_clientes_produtos, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)


# Tarefa para disparar o job no Databricks
ingestao_bronze_clientes_produtos = PythonOperator(
    task_id='ingestao_bronze_clientes_produtos',
    python_callable=trigger_and_wait_clientes_produtos,
    trigger_rule='all_success',
    dag=dag_master,
)

#Tarefa 5
# ID do job Databricks
job_id_expurgo_clientes_produtos = "650259219810019"  # ID do seu job Databricks
param_expurgo_clientes_produtos = {"param1": "filedataimport",
                                   "param2": "clientesxprod",
                                   "param3": f"{DATA_ATUAL}"}


# Tarefa para disparar o job no Databricks
def trigger_and_wait_expurgo_clientes_produtos():
    run_id = start_databricks_job(job_id_expurgo_clientes_produtos, param_expurgo_clientes_produtos, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)

expurgo_stage_clientes_produtos = PythonOperator(
    task_id='expurgo_stage_clientes_produtos',
    python_callable=trigger_and_wait_expurgo_clientes_produtos,
    trigger_rule='all_success',
    dag=dag_master,
)


#Ordem de execução das tarefas
run_copy_clientes_produtos >> move_cloud_clientes_produtos >> expurgo_clientes_produtos >> ingestao_bronze_clientes_produtos >> expurgo_stage_clientes_produtos


############################################################################################
########################## TAREFAS PARA DADOS DE CLIELIMIT #################################
############################################################################################



# ID do job Databricks
job_id_silver_clie_limit = "281100647508288"  # ID do seu job Databricks
param_silver_clie_limit = {"param1": "s_vend",
                           "param2": "clie_limit",
                           "param3": f"{DATA_ATUAL}"}


# Tarefa para disparar o job no Databricks
def trigger_and_wait_silver_clie_limit():
    run_id = start_databricks_job(job_id_silver_clie_limit, param_silver_clie_limit, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)


# Tarefa para disparar o job no Databricks
ingestao_silver_silver_clie_limit = PythonOperator(
    task_id='silver_clie_limit',
    python_callable=trigger_and_wait_silver_clie_limit,
    trigger_rule='all_success',
    dag=dag_master,
)

[expurgo_stage_clientes, expurgo_stage_produtos_bancarios, expurgo_stage_clientes_produtos] >> ingestao_silver_silver_clie_limit


############################################################################################
###################### TAREFAS PARA DADOS DE PROD_CONTRAT_DIARIO ###########################
############################################################################################


# ID do job Databricks
job_id_gold_g_prod_contrat_diario = "1079798976522851"  # ID do seu job Databricks
param_gold_g_prod_contrat_diario = {"param1": "g_vend",
                                    "param2": "prod_contrat_diario",
                                    "param3": f"{DATA_ATUAL}"}


# Tarefa para disparar o job no Databricks
def trigger_and_wait_gold_prod_contrat_diario():
    run_id = start_databricks_job(job_id_gold_g_prod_contrat_diario, param_gold_g_prod_contrat_diario, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)


# Tarefa para disparar o job no Databricks
ingestao_gold_g_prod_contrat_diario = PythonOperator(
    task_id='gold_g_prod_contrat_diario',
    python_callable=trigger_and_wait_gold_prod_contrat_diario,
    trigger_rule='all_success',
    dag=dag_master,
)

[ingestao_silver_silver_clie_limit] >> ingestao_gold_g_prod_contrat_diario
