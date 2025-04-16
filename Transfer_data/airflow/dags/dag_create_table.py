from airflow import DAG
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


dag_create_table = DAG(
    'dag_create_table',
    default_args=default_args,
    schedule_interval=None,
    catchup=True
)

# ID do job Databricks
job_id_create_table = "459472940773271"  # ID do seu job Databricks
param_create_table = {}


# Tarefa para disparar o job no Databricks
def trigger_and_wait_create_table():
    run_id = start_databricks_job(job_id_create_table, param_create_table, databricks_url, databricks_token)
    wait_for_job_to_finish(run_id, databricks_url, databricks_token)


# Tarefa para disparar o job no Databricks
create_table = PythonOperator(
    task_id='create_table',
    python_callable=trigger_and_wait_create_table,
    trigger_rule='all_success',
    dag=dag_create_table,
)
