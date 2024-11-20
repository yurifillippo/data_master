#!/bin/bash

# Função para registrar logs
log() {
    local MESSAGE="$1"
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $MESSAGE"
}

# Registrar o início do script
log "Iniciando o script."

# Função para formatar o timestamp
get_timestamp() {
    date +"%Y%m%d%H%M%S"
}

# Registrar o início do script
log "Obtendo timestamp atual."
# Obter o timestamp atual
TIMESTAMP=$(get_timestamp)

log "$TIMESTAMP"

log "Iniciando copia dos arquivos"

log "Copiando dados de produtos"
./run_copy.sh /var/lib/postgresql/exports/produtos_bancarios.csv /path/to/move_cloud/produtos_bancarios

log "Copiando dados de clientes"
./run_copy.sh /var/lib/postgresql/exports/clientes.csv /path/to/move_cloud/clientes

log "Copiando dados de clientes x produtos"
./run_copy.sh /var/lib/postgresql/exports/clientes_produtos.csv /path/to/move_cloud/clientes_produtos

log "Arquivos copiados com sucesso!"

log "Iniciando envio dos arquivos para Azure Blob Storage"

log "Enviando arquivo .csv de produtos"
./run_move_file.sh /path/to/move_cloud/produtos_bancarios/produtos_bancarios.csv table_ingestion_files/produtos/produtos_"$TIMESTAMP".csv

log "Enviando arquivo .csv de clientes"
./run_move_file.sh /path/to/move_cloud/clientes/clientes.csv table_ingestion_files/clientes/clientes_"$TIMESTAMP".csv

log "Enviando arquivo .csv de clientes x produtos"
./run_move_file.sh /path/to/move_cloud/clientes_produtos/clientes_produtos.csv table_ingestion_files/clientesxprod/clientes_produtos_"$TIMESTAMP".csv

log "Arquivos enviados com sucesso para Azure Blob Storage"