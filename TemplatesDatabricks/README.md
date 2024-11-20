__Projeto para certificação__

Criação de Datalake com ingestão Bronze, Silver e Gold.
Criação de métricas, dashboard de visualização das métricas e alertas.

Definições:
  - Particionamento: Data de carga dos dados.
  - Ferramenta: Template Databricks
  - Versionamento de dados: Delta Lake
  - Ferramentas de visualização de métricas: Google Data Studio
  - Monitoramento de desempenho: Google Stackdriver (VERIFICAR)
  - Definição de tamanho dos arquivos: 128MB (Evita arquivos pequenos demais que podem causar overhead devido à criação de muitas tarefas pequenas)
  - Em caso de uma segunda ingestão por odate, o ingestor agrupa os arquivos para evitar small files.
  - Formatos de compressão (Snappy, Gzip, ou Zlib) (VERIFICAR)

Motivos da escolha do Delta
  - Transações ACID: Garante a consistência e confiabilidade dos dados em operações complexas de leitura e gravação.
  - Gerenciamento de esquemas: Isso garante que os dados gravados respeitem o esquema definido e facilita a adição de novas colunas. O Delta Lake não permitirá gravações de dados que violem o esquema atual, evitando inconsistências.


Compactação automática de pequenos arquivos: O Delta Lake gerencia arquivos menores automaticamente e os combina em arquivos maiores para otimizar o desempenho de leitura e gravação:
 - spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true") (VERIFICAR)
Vacuum: Use o comando vacuum para remover arquivos antigos e liberar espaço de armazenamento. Por padrão, ele remove arquivos com mais de 7 dias, mas isso pode ser configurado.
 - deltaTable.vacuum(retentionHours=168)  # Mantém arquivos de 7 dias





**Link Databricks Comunnity:** https://community.cloud.databricks.com/browse?o=3786104145975269

**Portal GCP:** https://console.cloud.google.com/welcome/new?orgonly=true&project=datamaster-434121&supportedpurview=organizationId

![image](https://github.com/user-attachments/assets/358ed006-30ca-4692-94f6-0010db8ea41a)


**Estrutura GCP:**

**Projeto:** Datamaster01

**Buckets:**
  - data-ingestion-bucket-datamaster (Bucket para receber os dados brutos para ingestão nas tabelas)
  - datalake_datamaster (Bucket onde é construido o DataLake com as camadas)
  - analytics_datamaster (Bucket onde o Big Query utiliza para armazenar suas tabelas de analytics)

**Principal:**
  - data-master-account@datamaster01.iam.gserviceaccount.com - Role: Owner

**Big Query:**
  - **Dataset:** ingestion_metrics_data_master
    - **Tabela com métricas de ingestão __Bronze__:** ingestion_metrics_data_lake
      - **Table ID:** datamaster01.ingestion_metrics_data_master.ingestion_metrics_data_lake

        |      **Field name**       |**Type**|                           **Description**                               |
        |---------------------------|--------|-------------------------------------------------------------------------|
        | table_name                | STRING | Nome da tabela                                                          |
        | load_total_time           | DOUBLE | Tempo total de load dos dados brutos                                    |
        | number_lines_loaded       | INT64  | Número de linhas na tabela com o date em execução                       |
        | data_size_mb_formatted    | DOUBLE | Tamanho em MB dos dados brutos carregados                               |
        | write_total_time          | DOUBLE | Tempo total de escrita na tabela delta                                  |
        | qtd_total_rows_insert     | INT64  | Quantidade total de linhas inseridas                                    |
        | num_columns_table         | INT64  | Numero de colunas da tabela                                             |
        | num_files                 | INT64  | Número de arquivos parquet gerados                                      |
        | total_execution           | DOUBLE | Tempo total de execução do template de ingestão                         | 
        | dat_carga                 | STRING | Data de execução                                                        |
        | alerta                    | BOOL   | Alerta em divergência de quantidade de dados inseridos no o mesmo odate |

  - **Dataset:** ingestion_metrics_data_master
    - **Tabela com métricas de ingestão Silver:**
  - **Dataset:** ingestion_metrics_data_master
    - **Tabela com métricas de ingestão Gold:**
      

**Execução do Projeto:**

**Databricks**
Criação do Cluster databricks
  **Instalar LIB:** google-cloud-bigquery - type: PyPI
                    google-cloud-storage - type: PyPI

**Ordem de execução dos Notebooks:**
  - **Input_key** (Inclui chave de acesso ao GCP, uma vez que o Databricks Community possui algumas limitações)
  - **create_table** (Cria as tabelas delta)
  - **Ingestion_bronze_template** (Template e execução da ingestão da tabela clientes, produtos e clientesxprod)
  - 


