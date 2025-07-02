from pyspark.sql import SparkSession
import yaml
import logging

"""Script para criação de diretórios no sistema de arquivos e databases no Spark.

Este script lê as configurações definidas no arquivo `config.yml`, cria os diretórios
necessários para as camadas de dados no Data Lake e cria os bancos de dados (schemas)
no metastore do Spark, caso ainda não existam.

Funcionalidades principais:
- Criação dos diretórios para cada camada definida em `config['files']`.
- Criação dos bancos de dados conforme definido em `config['db']`.
- Logs informativos e tratamento de exceções para cada operação.

Requisitos:
- Ambiente Databricks ou similar com suporte ao `dbutils`.
- Configuração válida no arquivo `config.yml`.

Exemplo de uso:
    python setup_environment.py
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("ifood-case").getOrCreate()

with open('config.yml', 'r') as file:
    config = yaml.safe_load(file)

#Criação do volume para o projeto
spark.sql("""
  CREATE VOLUME IF NOT EXISTS workspace.default.data
  COMMENT 'Volume gerenciado para dados brutos do NYC Taxi'
""")

for layer, path in config['files'].items():
  try:
    dbutils.fs.mkdirs(path)
    logger.info(f"Camada '{layer}' criada em '{path}' com sucesso!")
  except Exception as e:
    logger.error(f"Erro ao criar o diretorio de {path}: {e}")

for db, db_name in config['db'].items(): 
  try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    logger.info(f"Database '{db}' criado com sucesso!")
  except Exception as e:
    logger.error(f"Erro ao criar o banco de dados de {db}:{db_name}: {e}")