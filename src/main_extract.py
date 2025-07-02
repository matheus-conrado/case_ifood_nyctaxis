from pyspark.sql import SparkSession
from extract_data import TaxiExtractor
from utils import _month_from_date
import argparse

"""Script principal para execução da ingestão de dados da camada RAW.

Este script utiliza a classe `TaxiExtractor` para realizar o download e a
persistência de arquivos Parquet referentes aos dados brutos de táxis (verde ou amarelo).
Os dados são extraídos com base em um intervalo de datas e tipos fornecidos como
argumentos de linha de comando.

Uso:
    python extract_main.py --data_inicio 2023-01 --data_fim 2023-03 --types yellow green

Argumentos:
    --data_inicio: Data inicial no formato 'AAAA-MM'.
    --data_fim: Data final no formato 'AAAA-MM'.
    --types: Lista de tipos de dados/táxis a serem processados (ex: yellow, green).
"""

spark = SparkSession.builder.appName("ifood-case").getOrCreate()
extract = TaxiExtractor(spark=spark)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Processa lista de datas no formato ["AAAA-MM","AAAA-MM"]')
    parser.add_argument(
        '--data_inicio',
        type=str,
        required=True,
        help='Data início da busca (AAAA-MM)'
    )
    parser.add_argument(
        '--data_fim',
        type=str,
        required=True,
        help='Data fim da busca (AAAA-MM)'
    )
    parser.add_argument(
        '--types',
        type=str,
        nargs='+',
        help='Lista de tipos separados por espaço (ex: --types yellow green)'
    )
    args = parser.parse_args()
    months = _month_from_date(date_start=args.data_inicio, date_end=args.data_fim)
    for type in args.types:
        extract.extract_save_data(months=months,type=type)