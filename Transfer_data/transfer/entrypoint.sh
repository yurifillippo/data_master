#!/bin/bash
set -e

# Execute o comando padrão do PostgreSQL
docker-entrypoint.sh postgres &

# Aguarde alguns segundos para garantir que o PostgreSQL esteja pronto
sleep 10

# Execute o seu script Python para fazer o upload
python3 /usr/local/bin/transfer.py

# Aguarde o processo do PostgreSQL
wait
