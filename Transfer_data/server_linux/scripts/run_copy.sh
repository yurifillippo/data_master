#!/bin/bash

echo "-----------------------------------------------------------------------"
echo "------------------------- START SCRIPT --------------------------------"
echo "-----------------------------------------------------------------------"

# Verifica se foram passados dois parâmetros (path_to_copy e path_to_destination)
if [ $# -ne 2 ]; then
    echo "Uso: $0 <path_to_copy> <path_to_destination>"
    exit 1
fi

# Caminhos passados como parâmetros
PATH_TO_COPY="$1"
PATH_TO_DESTINATION="$2"

# Intervalo de tempo (em segundos) entre as verificações
CHECK_INTERVAL=10

# Função para verificar se o arquivo já existe
wait_for_file() {
    while [ ! -f "$1" ]; do
        echo "Arquivo $1 não encontrado. Aguardando criação..."
        sleep $CHECK_INTERVAL
    done
    echo "Arquivo $1 encontrado!"
}

# Verifica a existência do arquivo e aguarda até que seja criado
wait_for_file "$PATH_TO_COPY"

# Chama o script Python e passa os caminhos como argumentos
python3 /workspace/scripts/copy_files.py "$PATH_TO_COPY" "$PATH_TO_DESTINATION"
