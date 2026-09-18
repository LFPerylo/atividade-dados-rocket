from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def preparar_cotacao_diaria(df: DataFrame) -> DataFrame:
    """Extrai a data (sem hora) e renomeia as colunas da resposta da API."""
    return (
        df.withColumn("data_cotacao", F.to_date(F.col("dataHoraCotacao")))
        .withColumnRenamed("cotacaoCompra", "cotacao_dolar")
        .select("data_cotacao", "cotacao_dolar")
        .dropDuplicates(["data_cotacao"])
    )


def preencher_serie_continua(
    df: DataFrame, spark: SparkSession, data_inicio: str, data_fim: str
) -> DataFrame:
    """Garante uma série diária contínua, preenchendo dias sem cotação (Forward Fill)."""
    calendario = spark.sql(
        f"SELECT explode(sequence(to_date('{data_inicio}'), to_date('{data_fim}'), "
        "interval 1 day)) AS data_cotacao"
    )
    unido = calendario.join(df, on="data_cotacao", how="left")
    janela = Window.orderBy("data_cotacao").rowsBetween(Window.unboundedPreceding, 0)
    return unido.withColumn(
        "cotacao_dolar", F.last("cotacao_dolar", ignorenulls=True).over(janela)
    )


def obter_cotacao_mais_recente(df: DataFrame) -> float:
    """Retorna a cotação do dia mais recente disponível na série."""
    linha = df.orderBy(F.col("data_cotacao").desc()).first()
    return float(linha["cotacao_dolar"])


def transformar_cotacao_dolar(
    df: DataFrame, spark: SparkSession, data_inicio: str, data_fim: str
) -> DataFrame:
    """Pipeline completo: origem tb_cotacao_dolar -> silver.tb_cotacao_dolar."""
    df = preparar_cotacao_diaria(df)
    return preencher_serie_continua(df, spark, data_inicio, data_fim)
