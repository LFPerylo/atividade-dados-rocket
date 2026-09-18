from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def padronizar_separador_generos(df: DataFrame, coluna: str = "genres") -> DataFrame:
    """Troca ';' por ',' para que o split funcione com um único separador."""
    return df.withColumn(coluna, F.regexp_replace(F.col(coluna), ";", ","))


def explodir_generos(df: DataFrame, coluna: str = "genres") -> DataFrame:
    """Desmembra a coluna de múltiplos gêneros em uma linha por gênero."""
    generos_array = F.split(F.col(coluna), r"\s*,\s*")
    return df.select("id", F.explode(generos_array).alias("nome_genero")).withColumn(
        "nome_genero", F.trim(F.col("nome_genero"))
    )


def remover_residuos_invalidos(df: DataFrame, coluna: str = "nome_genero") -> DataFrame:
    """Remove valores em branco ou puramente numéricos (resíduos de column shift)."""
    valido = (
        F.col(coluna).isNotNull()
        & (F.col(coluna) != "")
        & (~F.col(coluna).rlike(r"^-?\d+(\.\d+)?$"))
    )
    return df.filter(valido)


def transformar_generos(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_credits_and_tags.genres -> silver.tb_generos."""
    df = padronizar_separador_generos(df)
    df = explodir_generos(df)
    df = remover_residuos_invalidos(df)
    return df.withColumnRenamed("id", "id_filme").dropDuplicates(["id_filme", "nome_genero"])
