from pyspark.sql import SparkSession
from trusted_data import TaxiLoader
import argparse

"""Script principal para execução da transformação da camada Trusted.

Este script utiliza a classe `TaxiLoader` para realizar a leitura da camada Refined,
aplicar transformações e gerar as tabelas finais da camada Trusted.
A execução é feita diretamente sem necessidade de argumentos externos.

Uso:
    python trusted_main.py

Notas:
    - O processamento considera os tipos de serviço/táxi definidos na configuração.
    - A lógica de transformação e escrita está encapsulada no método `process_trusted_layer`.
"""

spark = SparkSession.builder.appName("ifood-case").getOrCreate()
trusted = TaxiLoader(spark=spark)

if __name__ == "__main__":
  trusted.process_trusted_layer()