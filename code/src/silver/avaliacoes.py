from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def remover_avaliacoes_duplicadas(df: DataFrame) -> DataFrame:
    """Remove registros onde filme, usuário, nota e comentário são idênticos."""
    return df.dropDuplicates(
        ["id_filme", "nome_usuario", "nota_usuario", "comentario_usuario"]
    )


def invalidar_nota_fora_da_escala(df: DataFrame) -> DataFrame:
    """Notas fora de [0, 10] viram NULL."""
    return df.withColumn(
        "nota_usuario",
        F.when(
            (F.col("nota_usuario") < 0) | (F.col("nota_usuario") > 10), None
        ).otherwise(F.col("nota_usuario")),
    )


def padronizar_comentario_vazio(df: DataFrame) -> DataFrame:
    """Comentários nulos ou só com espaços viram 'Sem comentário'."""
    return df.withColumn(
        "comentario_usuario",
        F.when(
            F.col("comentario_usuario").isNull()
            | (F.trim(F.col("comentario_usuario")) == ""),
            F.lit("Sem comentário"),
        ).otherwise(F.col("comentario_usuario")),
    )


def transformar_avaliacoes_usuarios(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_movies_reviews -> silver.tb_avaliacoes_usuarios."""
    df = (
        df.withColumnRenamed("id", "id_filme")
        .withColumnRenamed("nome", "nome_usuario")
        .withColumnRenamed("nota", "nota_usuario")
        .withColumnRenamed("comentario", "comentario_usuario")
    )
    df = remover_avaliacoes_duplicadas(df)
    df = invalidar_nota_fora_da_escala(df)
    df = padronizar_comentario_vazio(df)
    return df.select("id_filme", "nome_usuario", "nota_usuario", "comentario_usuario")
