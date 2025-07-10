import requests
import time


# Função que chama a API do Databricks
def start_databricks_job(job_id, param, databricks_url, databricks_token):

    run_url = f"{databricks_url}/api/2.0/jobs/run-now"

    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }

    # Defina os parâmetros que você deseja passar para o job (se necessário)
    payload = {
        "job_id": job_id,
        "notebook_params": param
    }

    # Fazendo a requisição para iniciar o job
    response = requests.post(run_url, headers=headers, json=payload)

    if response.status_code == 200:
        run_id = response.json()['run_id']
        print(f"Job iniciado com sucesso. Run ID: {run_id}")
        return run_id
    else:
        raise Exception(f"Erro ao iniciar o job: {response.status_code}, {response.text}")


#Aguarda finalização do job databricks
def wait_for_job_to_finish(run_id, databricks_url, databricks_token):
    run_status_url = f"{databricks_url}/api/2.0/jobs/runs/get"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }

    while True:
        # Faz a consulta do status do job
        response = requests.get(run_status_url, headers=headers, params={"run_id": run_id})

        if response.status_code != 200:
            raise Exception(f"Erro ao consultar o status do job: {response.status_code}, {response.text}")

        status = response.json()
        life_cycle_state = status['state']['life_cycle_state']

        if life_cycle_state in ['TERMINATED', 'FAILED', 'SUCCESS']:
            break

        time.sleep(10)  # Espera 10 segundos antes de verificar novamente

    # Verificação adicional: caso o job tenha falhado ou tenha sido terminado com falha
    if response.json()['state']['result_state'] == 'FAILED':
        raise Exception(f"Job falhou. Detalhes: {response.json()}")


    print(f"Job finalizado com sucesso. Detalhes: {response.json()}")
