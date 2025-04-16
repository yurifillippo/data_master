# Usa a imagem oficial do Python
FROM python:3.12-slim

# Define o diretório de trabalho dentro do container
WORKDIR /workspace

# Criar a pasta
RUN mkdir -p /path/to/move_cloud/

# Dar permissão total para todos os usuários
RUN chmod 777 /path/to/move_cloud/

COPY ./scripts/ /workspace/scripts

RUN pip install --no-cache-dir -r /workspace/scripts/requirements.txt

RUN chmod +x /workspace/scripts/*

# Acessando a variável AZURE_SAS_TOKEN
#azure_sas_token = os.getenv("AZURE_SAS_TOKEN")

# Mantém o container rodando
CMD ["tail", "-f", "/dev/null"]
