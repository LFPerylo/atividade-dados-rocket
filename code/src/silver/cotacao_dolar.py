from datetime import date

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
    """Retorna a cotação do dia mais recente que tenha valor; falha com mensagem clara se
    a série não tiver nenhuma cotação."""
    linha = (
        df.where(F.col("cotacao_dolar").isNotNull())
        .orderBy(F.col("data_cotacao").desc())
        .first()
    )
    if linha is None:
        raise ValueError(
            "Nenhuma cotação do dólar disponível na Bronze; rode Landing_to_Bronze com uma "
            "janela que inclua dias úteis."
        )
    return float(linha["cotacao_dolar"])


def transformar_cotacao_dolar(
    df: DataFrame, spark: SparkSession, data_inicio: str, data_fim: str
) -> DataFrame:
    """Pipeline completo: origem tb_cotacao_dolar -> silver.tb_cotacao_dolar.

    A série começa na cotação mais antiga da Bronze (que acumula as execuções anteriores)
    ou em data_inicio, o que vier antes; assim uma janela sem dia útil ainda encontra a
    última cotação conhecida para o forward fill.
    """
    df = preparar_cotacao_diaria(df)
    primeira_cotacao = df.agg(F.min("data_cotacao")).first()[0]
    if primeira_cotacao is not None:
        data_inicio = min(date.fromisoformat(data_inicio), primeira_cotacao).isoformat()
    return preencher_serie_continua(df, spark, data_inicio, data_fim)
