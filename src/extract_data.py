import requests as req
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from io import BytesIO
import pandas as pd
import yaml
from utils import schema_raw, setup_logger, get_config

"""Classe responsável pela extração e ingestão de dados brutos de táxis (Raw).

A classe `TaxiExtractor` realiza o download de arquivos Parquet diretamente de uma URL
remota, converte os dados para DataFrame Spark com schema definido, adiciona colunas
de controle e salva os dados localmente no Data Lake em formato Parquet.
"""

class TaxiExtractor:
    """Gerenciador de extração e gravação de dados da camada Raw."""
    def __init__(self, spark):
        """Inicializa a classe TaxiExtractor.

        Args:
            spark (SparkSession): Sessão Spark ativa para persistência de dados.
        """
        self.spark = spark
        self.logger = setup_logger("ifood-case-extract")
        self.config = get_config("config")
        self.base_url = self.config['base_url']
    
    def extract_save_data(self,months, type):
        """Baixa e armazena dados Parquet de viagens de táxi no Data Lake.

        Para cada mês informado, baixa o arquivo Parquet de acordo com o tipo de viagem
        (yellow ou green), aplica o schema correspondente, adiciona colunas auxiliares
        e salva o DataFrame em formato Parquet na camada Raw.

        Args:
            months (list[str]): Lista de strings no formato 'AAAA-MM' representando os meses a serem processados.
            type (str): Tipo da corrida (ex: 'yellow', 'green').

        Raises:
            Exception: Se ocorrer falha no download, leitura ou escrita dos dados.
        """
        try:
            for month_year in months:
                url = self.base_url.format(type, month_year)
                self.logger.info(f"Baixando os arquivo de {url}")
                resp = req.get(url)
                if resp.status_code == 200:
                    df = pd.read_parquet(BytesIO(resp.content))
                    spark_df = self.spark.createDataFrame(df, schema_raw[type])
                    spark_df = spark_df.withColumns({
                        "type": F.lit(type),
                        "dt_partition": F.to_date(F.lit(month_year + '-01'), 'yyyy-MM-dd')
                    })
                    self._load_to_lake(type, month_year, spark_df)
        except Exception as e:
            self.logger.error(f"Falha no download do arquivo: {url}\
                            Erro: {e}")
    
    def _load_to_lake(
        self, 
        type, 
        month_year, 
        df
    ):
        """Persiste o DataFrame como arquivo Parquet no caminho bruto da camada Raw.

        Gera o path de saída com base no tipo e mês de referência e grava o DataFrame
        em modo de sobrescrita.

        Args:
            type (str): Tipo da corrida (ex: 'yellow', 'green').
            month_year (str): Mês de referência no formato 'AAAA-MM'.
            df (DataFrame): DataFrame já processado com schema e colunas auxiliares.

        Raises:
            Exception: Se ocorrer erro ao salvar o arquivo no Data Lake.
        """
        try:
            path = f"{self.config['files']['raw']}/{type}_tripdata_{month_year+'-01'}"
            df.write.parquet(path, mode="overwrite")
            self.logger.info(f"Salvando arquivo no path: {path}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar o arquivo no path: {path} \
                                Erro: {e}")