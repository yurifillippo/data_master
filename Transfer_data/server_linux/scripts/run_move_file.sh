#!/bin/bash

# Verifica se foram passados dois parâmetros
if [ $# -ne 2 ]; then
    echo "Uso: $0 <path_to_copy> <path_to_destination>"
    exit 1
fi

# Caminhos passados como parâmetros
PATH_TO_COPY="$1"
NAME_FILE="$2"

#Inicia arquivo transfer.py e passa os parâmetros
python3 /workspace/scripts/transfer.py "$PATH_TO_COPY" "$NAME_FILE"
