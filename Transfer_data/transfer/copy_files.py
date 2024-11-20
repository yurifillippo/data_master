import os
import shutil
import logging
import csv
import sys

# Configuração de logging
logging.basicConfig(level=logging.INFO)


def copy_file(source_file, destination_dir):
    """Copia um arquivo de origem para o destino e verifica se é CSV para ler a primeira linha."""
    if os.path.isfile(source_file):
        destination_file = os.path.join(destination_dir, os.path.basename(source_file))
        logging.info(f"Copia {source_file} para {destination_file}")
        shutil.copy(source_file, destination_file)
        logging.info(f"{source_file} copiado com sucesso.")

        # Lê a primeira linha do arquivo CSV copiado
        if source_file.endswith('.csv'):
            with open(destination_file, 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                try:
                    first_row = next(csv_reader)  # Lê a primeira linha
                    logging.info(f"### A primeira linha de {os.path.basename(source_file)} é: {first_row}")
                except StopIteration:
                    logging.warning(f"O arquivo {os.path.basename(source_file)} está vazio.")
    else:
        logging.warning(f"Arquivo não encontrado: {source_file}")


def main():
    # Verifica se o caminho de origem foi passado como argumento
    if len(sys.argv) < 2:
        logging.error("Nenhum caminho de arquivo foi passado como argumento.")
        sys.exit(1)

    source_file = sys.argv[1]  # Caminho do arquivo de origem passado como argumento
    destination_dir = sys.argv[2]  # Altere para o seu caminho de destino desejado

    # Crie o diretório de destino se não existir
    os.makedirs(destination_dir, exist_ok=True)

    # Copia o arquivo especificado
    copy_file(source_file, destination_dir)


if __name__ == '__main__':
    main()
