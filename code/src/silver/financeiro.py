from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_TEXTOS_AUSENTES = {"unknown", "não informado", "nao informado", "n/a", ""}


def higienizar_valor_monetario(df: DataFrame, coluna: str) -> DataFrame:
    """Remove símbolos de moeda/milhar e trata texto de ausência como NULL antes de converter."""
    texto = F.trim(F.col(coluna).cast("string"))
    e_ausente = F.lower(texto).isin(*_TEXTOS_AUSENTES) | texto.isNull()
    numero_limpo = F.regexp_replace(texto, r"[^0-9.\-]", "")
    return df.withColumn(
        coluna,
        F.when(e_ausente | (numero_limpo == ""), None).otherwise(
            numero_limpo.cast("decimal(18,2)")
        ),
    )


def invalidar_valores_nao_positivos(df: DataFrame, colunas: list[str]) -> DataFrame:
    """Valores zerados ou negativos viram NULL (não são orçamento/receita válidos)."""
    for coluna in colunas:
        df = df.withColumn(
            coluna, F.when(F.col(coluna) <= 0, None).otherwise(F.col(coluna))
        )
    return df


def calcular_valores_brl(df: DataFrame, cotacao: float) -> DataFrame:
    """Converte orçamento e receita de USD para BRL usando a cotação informada."""
    return df.withColumn(
        "orcamento_brl", F.round(F.col("orcamento_usd") * F.lit(cotacao), 2)
    ).withColumn("receita_brl", F.round(F.col("receita_usd") * F.lit(cotacao), 2))


def calcular_lucro_e_margem(df: DataFrame) -> DataFrame:
    """Deriva lucro (USD/BRL) e margem percentual, sem dividir por zero nem propagar NULL."""
    df = df.withColumn(
        "lucro_usd", F.col("receita_usd") - F.col("orcamento_usd")
    ).withColumn("lucro_brl", F.col("receita_brl") - F.col("orcamento_brl"))
    margem = F.when(
        F.col("orcamento_usd").isNull()
        | F.col("receita_usd").isNull()
        | (F.col("orcamento_usd") == 0),
        None,
    ).otherwise(F.round((F.col("lucro_usd") / F.col("orcamento_usd")) * 100, 2))
    return df.withColumn("margem_lucro_percentual", margem)


def transformar_financeiro_filmes(df: DataFrame, cotacao: float) -> DataFrame:
    """Pipeline completo: origem tb_movies_financials -> silver.tb_financeiro_filmes."""
    df = (
        df.withColumnRenamed("id", "id_filme")
        .withColumnRenamed("budget", "orcamento_usd")
        .withColumnRenamed("revenue", "receita_usd")
    )
    df = higienizar_valor_monetario(df, "orcamento_usd")
    df = higienizar_valor_monetario(df, "receita_usd")
    df = invalidar_valores_nao_positivos(df, ["orcamento_usd", "receita_usd"])
    df = calcular_valores_brl(df, cotacao)
    df = calcular_lucro_e_margem(df)
    return df.select(
        "id_filme", "orcamento_usd", "receita_usd", "lucro_usd",
        "margem_lucro_percentual", "orcamento_brl", "receita_brl", "lucro_brl",
    )
