from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def adicionar_ingestion_datetime(df: DataFrame, momento: datetime | None = None) -> DataFrame:
    """Adiciona a coluna ingestion_datetime com o instante de ingestão na camada Bronze."""
    instante = momento or datetime.now(UTC)
    return df.withColumn("ingestion_datetime", F.lit(instante).cast("timestamp"))


def cotacao_dolar_para_dataframe(spark: SparkSession, registros: list[dict]) -> DataFrame:
    """Converte a lista de registros retornados pela API PTAX do Banco Central em DataFrame."""
    return spark.createDataFrame(registros)
