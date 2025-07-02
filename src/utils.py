import logging
from datetime import datetime
import os
import yaml
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    LongType, TimestampType
)

"""Define schemas e funções utilitárias para processar dados de táxis de Nova York com PySpark.

Este módulo contém os esquemas das tabelas 'green' e 'yellow' e funções auxiliares
para configuração de log, leitura de arquivos de configuração, geração de comandos SQL 
de metadados e criação de listas de meses com base em intervalos de datas.
"""

schema_raw = {
    "green": StructType([
        StructField("VendorID", LongType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("ehail_fee", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", DoubleType(), True),
        StructField("trip_type", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True)
    ]),
    "yellow": StructType([
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
    ])
}


def setup_logger(name):
    """Configura e retorna um logger com saída no console.

    Args:
        name (str): Nome identificador do logger.

    Returns:
        logging.Logger: Instância de logger configurada com handler de console.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def get_config(file_name: str ):
    """Carrega um arquivo de configuração YAML.

    Args:
        file_name (str): Nome do arquivo YAML (sem a extensão `.yml`).

    Returns:
        dict: Dicionário com o conteúdo do arquivo de configuração.

    Raises:
        FileNotFoundError: Se o arquivo especificado não for encontrado.
        yaml.YAMLError: Se o conteúdo do arquivo não estiver em formato YAML válido.
    """
    with open(f'{file_name}.yml', 'r') as file:
        config = yaml.safe_load(file)
    return config

def _month_from_date(date_start, date_end):
    """Gera uma lista de strings no formato 'YYYY-MM' entre duas datas.

    Args:
        date_start (str): Data inicial no formato 'DD/MM/AAAA'.
        date_end (str): Data final no formato 'DD/MM/AAAA'.

    Returns:
        list[str]: Lista de strings representando os meses no intervalo fornecido.

    Raises:
        ValueError: Se a data final for anterior à data inicial.

    Exemplos:
        >>> _month_from_date('01/01/2023', '01/03/2023')
        ['2023-01', '2023-02', '2023-03']
    """
    start = datetime.strptime(date_start, '%d/%m/%Y').replace(day=1)
    end = datetime.strptime(date_end, '%d/%m/%Y').replace(day=1)
    if end > start:
        return [
            start.replace(month=m).strftime('%Y-%m')
            for m in range(start.month, start.month + (end.year - start.year) * 12 + end.month - start.month + 1)
        ]
    raise ValueError("data_inicio deve ser maior que data_fim")

def _gerar_metadados(tabela_nome: str, metadata: dict, schema: str = "default"):
    """Gera comandos SQL para adicionar comentários em tabelas e colunas.

    Args:
        tabela_nome (str): Nome da tabela de destino.
        metadata (dict): Dicionário contendo 'description' e 'columns_dict'.
        schema (str, opcional): Nome do schema da tabela. Padrão é "default".

    Returns:
        list[str]: Lista de comandos SQL para adicionar comentários.

    Exemplos:
        >>> _gerar_metadados("tb_corridas", {
        ...     "description": "Tabela com dados de corridas",
        ...     "columns_dict": {
        ...         "trip_distance": "Distância percorrida durante a corrida"
        ...     }
        ... })
        ["COMMENT ON TABLE default.tb_corridas IS 'Tabela com dados de corridas';",
         "COMMENT ON COLUMN default.tb_corridas.trip_distance IS 'Distância percorrida durante a corrida';"]
    """
    table_full = f'{schema}.{tabela_nome}'
    desc_tabela = metadata.get("description", "")
    colunas = metadata.get("columns_dict", {})
    comandos = []
    comandos.append(f"COMMENT ON TABLE {table_full} IS '{desc_tabela}';")
    for coluna, descricao in colunas.items():
        desc_coluna = ' '.join(descricao.split())
        comandos.append(f"COMMENT ON COLUMN {table_full}.{coluna} IS '{desc_coluna}';")
    return comandos