# Use a imagem base do Airflow
FROM apache/airflow:2.6.1-python3.8

# Defina o diretório de trabalho
WORKDIR /opt/airflow

# Crie o diretório de scripts com permissões adequadas
#RUN mkdir -p /opt/airflow/scripts && chown airflow:airflow /opt/airflow/scripts

# Copie os requisitos primeiro
COPY ./scripts/requirements.txt /opt/airflow/scripts/requirements.txt

# Troca para o usuário airflow antes de instalar as dependências
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/scripts/requirements.txt