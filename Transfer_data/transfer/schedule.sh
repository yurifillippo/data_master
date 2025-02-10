#!/bin/bash

#Arquivo para simular malha control-m que executa job a job

# Função para registrar logs
log() {
    local MESSAGE="$1"
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $MESSAGE"
}

log "Iniciando o script."

# Função para formatar o timestamp
get_timestamp() {
    date +"%Y%m%d%H%M%S"
}

log "Obtendo timestamp atual."
# Obter o timestamp atual
TIMESTAMP=$(get_timestamp)

# Exibe a data
DATA_ATUAL=$(date +"%Y%m%d")
echo "Data atual: $DATA_ATUAL"

log "$TIMESTAMP"

sleep 5

log "Iniciando copia dos arquivos - JOB 1"

log "Copiando dados de produtos"
./run_copy.sh /var/lib/postgresql/exports/produtos_bancarios.csv /path/to/move_cloud/produtos_bancarios
echo ""

log "Copiando dados de clientes"
./run_copy.sh /var/lib/postgresql/exports/clientes.csv /path/to/move_cloud/clientes
echo ""

log "Copiando dados de clientes x produtos"
./run_copy.sh /var/lib/postgresql/exports/clientes_produtos.csv /path/to/move_cloud/clientes_produtos
echo ""

log "Arquivos copiados com sucesso!"

sleep 5

log "Iniciando envio dos arquivos para Azure Blob Storage - JOB 2"

log "Enviando arquivo .csv de produtos"
./run_move_file.sh /path/to/move_cloud/produtos_bancarios/produtos_bancarios.csv table_ingestion_files/produtos/${DATA_ATUAL}/produtos_"$TIMESTAMP".csv
echo ""

log "Enviando arquivo .csv de clientes"
./run_move_file.sh /path/to/move_cloud/clientes/clientes.csv table_ingestion_files/clientes/${DATA_ATUAL}/clientes_"$TIMESTAMP".csv
echo ""

log "Enviando arquivo .csv de clientes x produtos"
./run_move_file.sh /path/to/move_cloud/clientes_produtos/clientes_produtos.csv table_ingestion_files/clientesxprod/${DATA_ATUAL}/clientes_produtos_"$TIMESTAMP".csv
echo ""


sleep 10

log "Arquivos enviados com sucesso para Azure Blob Storage"
echo ""
sleep 5

log "Executando expurgo dos dados - Job 3"
echo ""

log "Expurgo do arquivo .csv de produtos"
./expurgo_server.sh /path/to/move_cloud/produtos_bancarios/
echo ""

log "Expurgo do arquivo .csv de clientes"
./expurgo_server.sh /path/to/move_cloud/clientes/
echo ""

log "Expurgo do arquivo .csv de clientes x produtos"
./expurgo_server.sh /path/to/move_cloud/clientes_produtos/