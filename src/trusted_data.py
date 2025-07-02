import pyspark.sql.functions as F
from utils import setup_logger, get_config, _gerar_metadados

"""Classe responsável por transformar dados da camada Refined em indicadores analíticos na camada Trusted.

A `TaxiLoader` realiza a leitura de dados da camada Refined, calcula indicadores estatísticos
(como média de valor por mês e média de passageiros por hora) e escreve os resultados em
tabelas Delta na camada Trusted, com metadados descritivos adicionados.
"""

class TaxiLoader:
  """Gerenciador de transformação de dados da camada Refined para a Trusted."""
  def __init__(self, spark):
    """Inicializa a classe TaxiLoader.

    Args:
        spark (SparkSession): Sessão Spark ativa.
    """
    self.spark = spark
    self.logger = setup_logger("ifood-case-load")
    self.config = get_config("config")
    self.config_tables = get_config("config_tables")

  def _generate_trusted_layer(self,df):
    """Gera dois indicadores estatísticos a partir da camada Refined.

    Realiza duas transformações principais:
    - Calcula a média do valor total da corrida por mês e tipo de serviço.
    - Calcula a média de passageiros por hora do dia.

    Args:
        df (DataFrame): DataFrame lido da camada Refined.

    Returns:
        tuple[DataFrame, DataFrame]: 
            - DataFrame com média de valor por mês/tipo.
            - DataFrame com média de passageiros por hora.

    Raises:
        Exception: Se ocorrer erro durante as transformações.
    """
    self.logger.info(f"Lendo a camada refined")

    # Calcular média de valor por Mẽs
    df_media_por_mes_tipo = df.groupBy(F.date_trunc("month", F.col("dt_hr_pickup_datetime")).alias("mes_corrida"),"nm_type_service").agg(
        F.round(F.avg("vl_total_amount"),2).alias("media_valor_corrida")
    ).orderBy("mes_corrida")

    # Extrair hora da corrida e calcular média de passageiros
    df_passageiros_por_hora = df.withColumn(
        "hora", F.hour(F.col("dt_hr_pickup_datetime"))
    ).groupBy("hora").agg(
        F.round(F.avg("qt_passanger_count"),2).alias("media_passageiros")
    ).orderBy("hora")

    return df_media_por_mes_tipo, df_passageiros_por_hora

  def _write_delta_table(
      self,
      df,
      schema: str,
      table: str
  ):
    """Escreve um DataFrame como tabela Delta no metastore do Spark.

    Cria o schema caso não exista e grava os dados no formato Delta com
    sobrescrita e merge automático de schema.

    Args:
        df (DataFrame): DataFrame a ser salvo.
        schema (str): Nome do schema (ex: 'trusted_layer').
        table (str): Nome da tabela Delta.

    Returns:
        None

    Raises:
        Exception: Se ocorrer erro ao gravar a tabela.
    """
    try:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{schema}.{table}")
        self.logger.info(f"Dados salvas na tabela: {schema}.{table}")
    except Exception as e:
        self.logger.error(f"Erro ao gravar os dados na tabela: {schema}.{table} \
                            Erro: {e}")
          
  def process_trusted_layer(self):
    """Executa o pipeline completo para gerar a camada Trusted.

    Lê os dados da camada Refined, aplica as transformações analíticas,
    grava os resultados em tabelas Delta e adiciona os metadados descritivos
    definidos em `config_tables`.

    Tabelas geradas:
        - trusted_layer.tb_avg_mes_tipo
        - trusted_layer.tb_passageiro_por_hora

    Returns:
        None
    """
    self.logger.info(f"Lendo dados da camada {self.config['db']['refined']}")
    df = self.spark.read.table(f"{self.config['db']['refined']}.tb_trips_taxis")
    self.logger.info(f"Gerando amostra de indicadores")
    df_media_por_mes_tipo, df_passageiros_por_hora = self._generate_trusted_layer(df=df)

    self.logger.info(f"Escrevendo dados na tabela trusted.tb_avg_mes_tipo")
    self._write_delta_table(df=df_media_por_mes_tipo,schema="trusted_layer",table=f"tb_avg_mes_tipo")
    self.logger.info(f"Escrevendo cometarios e descrição para a tabela trusted_layer.tb_avg_mes_tipo")
    comandos_sql = _gerar_metadados(
        tabela_nome=f"tb_avg_mes_tipo",
        metadata=self.config_tables[f"tb_avg_mes_tipo"],
        schema="trusted_layer"
    )
    for cmd in comandos_sql:
        self.spark.sql(cmd)

    self.logger.info(f"Escrevendo dados na tabela trusted_layer.tb_passageiro_por_hora")
    self._write_delta_table(df=df_passageiros_por_hora,schema="trusted_layer",table=f"tb_passageiro_por_hora")
    self.logger.info(f"Escrevendo cometarios e descrição para a tabela trusted_layer.tb_passageiro_por_hora")
    comandos_sql = _gerar_metadados(
        tabela_nome=f"tb_passageiro_por_hora",
        metadata=self.config_tables[f"tb_passageiro_por_hora"],
        schema="trusted_layer"
    )
    for cmd in comandos_sql:
        self.spark.sql(cmd)