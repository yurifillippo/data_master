# Nome do projeto
PROJECT_NAME = datamaster

# Versão do projeto
VERSION = 1.0

# Diretórios dos serviços
POSTGRES_DIR = postgres
PYTHON_DIR = transfer

# Nome das imagens Docker
POSTGRES_IMAGE = $(PROJECT_NAME)_postgres:$(VERSION)
PYTHON_IMAGE = $(PROJECT_NAME)_python_app:$(VERSION)

.PHONY: all build up down clean

# Comando padrão
all: up

# Construir todas as imagens
build: build-postgres build-python

build-postgres:
	docker build -t $(POSTGRES_IMAGE) $(POSTGRES_DIR)

build-python:
	docker build -t $(PYTHON_IMAGE) $(PYTHON_DIR)

# Subir os serviços com Docker Compose
up: build
	docker-compose up --build -d

# Derrubar os serviços com Docker Compose
down:
	docker-compose down

# Limpar as imagens Docker criadas
clean:
	docker rmi $(POSTGRES_IMAGE) $(PYTHON_IMAGE) || true
