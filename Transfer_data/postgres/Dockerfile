# Use a imagem base do PostgreSQL
FROM postgres:latest

# Atualiza o gerenciador de pacotes e instale o Vim
RUN apt-get update && apt-get install -y vim

# Cria o diretório de exportação e defina permissões
RUN mkdir -p /var/lib/postgresql/exports && \
    chown -R postgres:postgres /var/lib/postgresql/exports && \
    chmod -R 755 /var/lib/postgresql/exports

# Copia o arquivo SQL para dentro do contêiner
COPY 01_create_table.sql /docker-entrypoint-initdb.d/
COPY 05_produtos.sql /docker-entrypoint-initdb.d/
COPY 03_clientes.sql /docker-entrypoint-initdb.d/
COPY 04_clientes_produtos.sql /docker-entrypoint-initdb.d/
COPY 02_triggers_to_cloud.sql /docker-entrypoint-initdb.d/

#Defina variáveis de ambiente para configurar o PostgreSQL
ENV POSTGRES_DB="request_manager_product"
ENV POSTGRES_USER="postgres"
ENV POSTGRES_PASSWORD="11930143605"

#Exponha a porta padrão do PostgreSQL (5432)
EXPOSE 5432
