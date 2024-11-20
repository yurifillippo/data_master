# Use uma imagem base do Python
FROM python:3.10-slim

# Defina o diretório de trabalho
WORKDIR /usr/src/app

# Copie o arquivo de requisitos e instale as dependências
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copie chave de acesso google cloud
#COPY key.json /app/key.json

# Definir a variável de ambiente GOOGLE_APPLICATION_CREDENTIALS
# que aponta para o arquivo JSON
#ENV GOOGLE_APPLICATION_CREDENTIALS="/app/key.json"

# Definir a variável de ambiente AZURE_SAS_TOKEN no Dockerfile
#ENV AZURE_SAS_TOKEN="sp=w&st=2024-10-20T01:17:33Z&se=2026-10-20T09:17:33Z&spr=https&sv=2022-11-02&sr=c&sig=NIhMm3Rtal%2BdLncppcF2q9qPXv8TTtDZE%2FfPLcGPo%2FQ%3D"

# Copie o arquivo Python para o container
COPY copy_files.py ./
COPY transfer.py ./

# Copie o arquivo run_copy.sh e o schedule
COPY run_copy.sh /usr/src/app/run_copy.sh
COPY schedule.sh /usr/src/app/schedule.sh
COPY run_move_file.sh /usr/src/app/run_move_file.sh

# Dê permissão de execução ao script run_copy.sh
RUN chmod +x /usr/src/app/run_copy.sh
RUN chmod +x /usr/src/app/schedule.sh
RUN chmod +x /usr/src/app/run_move_file.sh

# Comando a ser executado ao iniciar o container: Executar o script .sh
CMD ["./schedule.sh"]

# Comando a ser executado ao iniciar o container
#CMD ["python", "./copy_files.py"]
