from pyspark.sql import SparkSession
from transform_data import TaxiTransformer
import argparse

"""Script principal para executar a transformação dos dados para a camada Refined.

Este script utiliza a classe `TaxiTransformer` para processar dados da camada Raw,
transformá-los e salvá-los na camada Refined em formato Delta. Os tipos de serviços
a serem processados (por exemplo: yellow, green) devem ser informados como argumentos
de linha de comando.

Uso:
    python main.py --types yellow green

Argumentos:
    --types: Lista de tipos de táxis a serem processados (ex: yellow, green).
"""

spark = SparkSession.builder.appName("ifood-case").getOrCreate()
transform = TaxiTransformer(spark=spark)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Processa lista de tipos (ex: yellow green)')
    parser.add_argument(
        '--types',
        type=str,
        nargs='+',
        help='Lista de tipos separados por espaço (ex: --types yellow green)'
    )
    args = parser.parse_args()
    transform.process_refined_layer(types=args.types)