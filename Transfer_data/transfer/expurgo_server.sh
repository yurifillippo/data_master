#!/bin/bash

# Verifica se foi passado parâmetro
if [ $# -ne 1 ]; then
    echo "Uso: $0 "
    exit 1
fi

# Caminho passado como parâmetro
DIRETORIO="$1"

echo "Verificando diretorio: $1"

# Verifica se o diretório existe
if [ -d "$DIRETORIO" ]; then
    echo "Conteúdo do diretório antes da limpeza:"
    #Lista conteudo do diretorio
    ls "$DIRETORIO"

    echo "Apagando itens do diretório..."
    # Remove todos os arquivos e subpastas dentro do diretório
    rm -rf "$DIRETORIO"/*

    echo "Verificando se o diretório está vazio..."
    #Verificando se o diretório está vazio
    if [ -z "$(ls -A $DIRETORIO)" ]; then
        echo "O diretório está vazio."
    else
        echo "Ainda há itens no diretório:"
        # Lista os itens restantes (se houver)
        ls -l "$DIRETORIO"
    fi
else
    echo "O diretório '$DIRETORIO' não existe."
fi