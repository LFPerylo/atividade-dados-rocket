from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.deduplicacao import manter_registro_mais_recente_e_completo

# Formatos reais vistos na origem: "97000000", "$ 97000000", "USD 150000000", "34.0M", "250.5K".
# Até 16 dígitos inteiros: decimal(18,2) comporta no máximo 16 antes da vírgula.
_PADRAO_VALOR_MONETARIO = (
    r"^(?:US\$|USD|R\$|\$|€)?\s*"
    r"(\d{1,16}(?:\.\d{1,6})?|\d{1,3}(?:,\d{3}){1,4}(?:\.\d{1,6})?)\s*([KMB])?$"
)
_MULTIPLICADOR = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
_LIMITE_DECIMAL_18_2 = 10**16


def higienizar_valor_monetario(df: DataFrame, coluna: str) -> DataFrame:
    """Converte texto monetário em decimal, sem apagar caracteres arbitrários.

    Aceita prefixo de moeda e sufixo K/M/B ("34.0M" = 34 milhões). Qualquer outra coisa
    ("Unknown", "N/A", "Não Informado", texto vazado) vira NULL: apagar letras juntaria
    dígitos soltos em um valor falso, e apagar o "M" transformaria 34 milhões em 34.
    Valores que não cabem em decimal(18,2) também viram NULL: com ANSI ligado o cast
    lançaria erro e derrubaria o pipeline por causa de um único registro.
    """
    texto = F.upper(F.trim(F.col(coluna).cast("string")))
    numero = F.regexp_replace(F.regexp_extract(texto, _PADRAO_VALOR_MONETARIO, 1), ",", "")
    sufixo = F.regexp_extract(texto, _PADRAO_VALOR_MONETARIO, 2)
    multiplicador = F.create_map(
        *[F.lit(x) for par in _MULTIPLICADOR.items() for x in par]
    )[sufixo]
    valido = texto.rlike(_PADRAO_VALOR_MONETARIO)
    valor = numero.cast("decimal(38,4)") * F.coalesce(
        multiplicador.cast("decimal(38,0)"), F.lit(1).cast("decimal(38,0)")
    )
    cabe = F.when(valor < F.lit(_LIMITE_DECIMAL_18_2), valor)
    return df.withColumn(coluna, F.when(valido, cabe).otherwise(None).cast("decimal(18,2)"))


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
    df = manter_registro_mais_recente_e_completo(df, "id_filme")
    return df.select(
        "id_filme", "orcamento_usd", "receita_usd", "lucro_usd",
        "margem_lucro_percentual", "orcamento_brl", "receita_brl", "lucro_brl",
    )
