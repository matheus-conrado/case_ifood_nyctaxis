import pyspark.sql.functions as F
from utils import setup_logger, get_config, _gerar_metadados

"""Classe responsável pela transformação dos dados da camada Raw para a camada Refined.

A `TaxiTransformer` realiza o processamento de dados brutos de corridas de táxis
(verdes ou amarelos), aplicando regras de qualidade e enriquecimento, salvando os dados
em formato Delta e adicionando metadados descritivos às tabelas.
"""

class TaxiTransformer:
  """Transformador de dados de táxis para camada Refined."""

  def __init__(self, spark):
    """Inicializa a classe TaxiTransformer.

    Args:
        spark (SparkSession): Sessão Spark ativa.
    """
    self.spark = spark
    self.logger = setup_logger("ifood-case-transform")
    self.config = get_config("config")
    self.config_tables = get_config("config_tables")

  def _generate_refined_layer(self,df):
    """Transforma dados da camada Raw para a camada Refined.

    Realiza renomeações de colunas, conversões de data/hora e extração de hora e minuto
    para a análise temporal. Também filtra registros com dados inválidos
    (por exemplo, corridas com valores ou passageiros nulos ou zerados).

    Args:
        df (DataFrame): DataFrame bruto lido da camada Raw.

    Returns:
        DataFrame: DataFrame transformado e pronto para persistência.

    Raises:
        Exception: Se ocorrer erro durante a transformação dos dados.
    """
    self.logger.info(f"Transformando dados da camada Raw")
    pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "lpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "lpep_dropoff_datetime"
    df = df.withColumnRenamed(pickup_col, "pickup_datetime").withColumnRenamed(dropoff_col, "dropoff_datetime")

    df_trips = df.select(
        F.col("VendorID").alias("id_vendor"),
        F.col("passenger_count").alias("qt_passanger_count"),
        F.col("total_amount").alias("vl_total_amount"),
        F.to_timestamp(F.col("pickup_datetime")).alias("dt_hr_pickup_datetime"),
        F.to_timestamp(F.col("pickup_datetime")).alias("dt_hr_dropoff_datetime"),
        F.hour(F.to_timestamp(F.col("pickup_datetime"))).alias("dt_pickup_hour"),
        F.minute(F.to_timestamp(F.col("pickup_datetime"))).alias("dt_pickup_minute"),
        F.hour(F.to_timestamp(F.col("dropoff_datetime"))).alias("dt_dropoff_hour"),
        F.minute(F.to_timestamp(F.col("dropoff_datetime"))).alias("dt_dropoff_minute"),
        F.col("type").alias("nm_type_service"),
        F.col("dt_partition"),
    ).filter(
        (F.col("passenger_count") > 0) &
        (F.col("total_amount") > 0) &
        (F.col("pickup_datetime").isNotNull()) &
        (F.col("dropoff_datetime").isNotNull())
    )
    return df_trips

  def _write_delta_table(
      self,
      df,
      partitions: list[str],
      schema: str,
      table: str
    ):
    """Escreve um DataFrame como tabela Delta no metastore do Spark.

    Cria o schema se ele não existir, define particionamento dinâmico e
    sobrescreve a tabela Delta com merge de schema habilitado.

    Args:
        df (DataFrame): DataFrame a ser persistido.
        partitions (list[str]): Lista de colunas usadas como particionamento.
        schema (str): Nome do schema (banco de dados no metastore).
        table (str): Nome da tabela Delta a ser criada ou sobrescrita.

    Returns:
        None

    Raises:
        Exception: Se ocorrer erro na escrita dos dados no metastore.
    """
    try:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("partitionOverwriteMode", "dynamic") \
            .partitionBy(partitions) \
            .saveAsTable(f"{schema}.{table}")
        self.logger.info(f"Dados salvas na tabela: {schema}.{table}")
    except Exception as e:
        self.logger.error(f"Erro ao gravar os dados na tabela: {schema}.{table} \
                            Erro: {e}")
          
  def process_refined_layer(self ,types):
    """Executa o pipeline completo de transformação e gravação para múltiplos tipos de táxi.

    Para cada tipo de táxi listado, realiza a leitura da camada Raw, transformação
    para o modelo Refined, escrita da tabela Delta e adição de metadados
    descritivos (comentários e descrições de colunas).

    Args:
        types (list[str]): Lista com os tipos de serviço a serem processados (ex: ['green', 'yellow']).

    Returns:
        None
    """
    for type in types:
        self.logger.info(f"Lendo dados da camada  para o type: {type}")
        df = self.spark.read.parquet(f"{self.config['files']['raw']}/{type}_tripdata_*")
        df_transformed = self._generate_refined_layer(type=type, df=df)
        self.logger.info(f"Escrevendo dados na tabela {self.config['db']['refined']}.tb_trips_taxis")
        self._write_delta_table(df=df_transformed,schema=self.config['db']['refined'],table=f"tb_trips_taxis",partitions=["nm_type_service","dt_partition"])

        self.logger.info(f"Escrevendo cometarios e descrição para a tabela refined.tb_trips_taxis")
        comandos_sql = _gerar_metadados(
            tabela_nome=f"tb_trips_taxis",
            metadata=self.config_tables[f"tb_trips_taxis"],
            schema=self.config['db']['refined']
        )
        for cmd in comandos_sql:
            self.spark.sql(cmd)