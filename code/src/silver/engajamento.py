from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.deduplicacao import manter_registro_mais_recente_e_completo
from src.common.tipagem import converter_texto_para_numero_seguro


def limpar_separador_decimal(df: DataFrame, coluna: str) -> DataFrame:
    """Troca vírgula decimal por ponto ("154,34" -> "154.34"); demais valores ficam como estão.

    Não remove caracteres: apagar letras/símbolos juntaria os dígitos de texto vazado pelo
    column shift ("2009 ... 2010") em um número falso. O que não for número válido vira NULL
    na conversão segura seguinte.
    """
    valor = F.trim(F.col(coluna).cast("string"))
    valor_corrigido = F.when(
        valor.rlike(r"^-?\d+,\d+$"), F.regexp_replace(valor, ",", ".")
    ).otherwise(valor)
    return df.withColumn(coluna, valor_corrigido)


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
    df = manter_registro_mais_recente_e_completo(df, "id_filme")
    return df.select(
        "id_filme", "popularidade", "nota_media_tmdb", "qtd_votos_tmdb",
        "nota_media_imdb", "qtd_votos_imdb",
    )
