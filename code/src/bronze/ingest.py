from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

_SCHEMA_COTACAO = StructType(
    [
        StructField("dataHoraCotacao", StringType()),
        StructField("cotacaoCompra", DoubleType()),
    ]
)


def adicionar_ingestion_datetime(df: DataFrame, momento: datetime | None = None) -> DataFrame:
    """Adiciona a coluna ingestion_datetime com o instante de ingestão na camada Bronze."""
    instante = momento or datetime.now(UTC)
    return df.withColumn("ingestion_datetime", F.lit(instante).cast("timestamp"))


def cotacao_dolar_para_dataframe(spark: SparkSession, registros: list[dict]) -> DataFrame:
    """Converte a lista de registros retornados pela API PTAX do Banco Central em DataFrame."""
    # Schema explícito: com lista vazia (API sem cotação na janela) a inferência falharia.
    return spark.createDataFrame(registros, schema=_SCHEMA_COTACAO)
