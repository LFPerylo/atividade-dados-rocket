from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_PADRAO_DECIMAL = r"^-?\d+(\.\d+)?$"
_PADRAO_INTEIRO = r"^-?\d+$"


def limpar_separador_decimal(df: DataFrame, coluna: str) -> DataFrame:
    """Troca vírgula por ponto quando o valor usa vírgula como separador decimal."""
    valor = F.regexp_replace(F.trim(F.col(coluna).cast("string")), r"[^0-9,.\-]", "")
    valor_corrigido = F.when(
        valor.rlike(r"^-?\d+,\d+$"), F.regexp_replace(valor, ",", ".")
    ).otherwise(valor)
    return df.withColumn(coluna, valor_corrigido)


def converter_texto_para_numero_seguro(
    df: DataFrame, coluna: str, tipo: str = "double"
) -> DataFrame:
    """Converte para número apenas quando o texto é um número válido; senão, NULL."""
    padrao = _PADRAO_DECIMAL if tipo == "double" else _PADRAO_INTEIRO
    valor = F.trim(F.col(coluna).cast("string"))
    return df.withColumn(coluna, F.when(valor.rlike(padrao), valor.cast(tipo)).otherwise(None))


def invalidar_notas_fora_da_escala(
    df: DataFrame, colunas: list[str], minimo: float = 0.0, maximo: float = 10.0
) -> DataFrame:
    """Notas fora de [0, 10] (incluindo erro de escala) viram NULL."""
    for coluna in colunas:
        df = df.withColumn(
            coluna,
            F.when((F.col(coluna) < minimo) | (F.col(coluna) > maximo), None).otherwise(
                F.col(coluna)
            ),
        )
    return df


def invalidar_contagens_negativas(df: DataFrame, colunas: list[str]) -> DataFrame:
    """Contagens/índices negativos viram NULL."""
    for coluna in colunas:
        df = df.withColumn(coluna, F.when(F.col(coluna) < 0, None).otherwise(F.col(coluna)))
    return df


def transformar_metricas_engajamento(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_movies_metrics -> silver.tb_metricas_engajamento."""
    df = (
        df.withColumnRenamed("id", "id_filme")
        .withColumnRenamed("popularity", "popularidade")
        .withColumnRenamed("vote_average", "nota_media_tmdb")
        .withColumnRenamed("vote_count", "qtd_votos_tmdb")
        .withColumnRenamed("averageRating", "nota_media_imdb")
        .withColumnRenamed("numVotes", "qtd_votos_imdb")
    )
    df = limpar_separador_decimal(df, "popularidade")
    df = converter_texto_para_numero_seguro(df, "popularidade", "double")
    df = converter_texto_para_numero_seguro(df, "nota_media_tmdb", "double")
    df = converter_texto_para_numero_seguro(df, "qtd_votos_tmdb", "int")
    df = converter_texto_para_numero_seguro(df, "nota_media_imdb", "double")
    df = converter_texto_para_numero_seguro(df, "qtd_votos_imdb", "int")
    df = invalidar_notas_fora_da_escala(df, ["nota_media_tmdb", "nota_media_imdb"])
    df = invalidar_contagens_negativas(
        df, ["qtd_votos_tmdb", "qtd_votos_imdb", "popularidade"]
    )
    return df.select(
        "id_filme", "popularidade", "nota_media_tmdb", "qtd_votos_tmdb",
        "nota_media_imdb", "qtd_votos_imdb",
    )
