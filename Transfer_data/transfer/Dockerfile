# Imagem base do Python
FROM python:3.10-slim

# Define o diretório de trabalho
WORKDIR /usr/src/app

# Copia o arquivo de requisitos e instala as dependências
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia os arquivos Python para o container
COPY copy_files.py ./
COPY transfer.py ./

# Copia os arquivos .sh para o container
COPY run_copy.sh /usr/src/app/run_copy.sh
COPY schedule.sh /usr/src/app/schedule.sh
COPY run_move_file.sh /usr/src/app/run_move_file.sh
COPY expurgo_server.sh /usr/src/app/expurgo_server.sh

# Permissão de execução aos scripts .sh
RUN chmod +x /usr/src/app/run_copy.sh
RUN chmod +x /usr/src/app/schedule.sh
RUN chmod +x /usr/src/app/run_move_file.sh
RUN chmod +x /usr/src/app/expurgo_server.sh

# Executar o script schedule.sh
CMD ["./schedule.sh"]
