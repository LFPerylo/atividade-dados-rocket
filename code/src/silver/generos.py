from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Domínio de gêneros do catálogo TMDB. Na origem, a coluna traz também sinopses, caminhos de
# imagem e números deslocados (column shift); só o que pertence a este conjunto é gênero.
_GENEROS_VALIDOS = [
    "Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family",
    "Fantasy", "History", "Horror", "Music", "Mystery", "Romance", "Science Fiction",
    "TV Movie", "Thriller", "War", "Western",
]


def padronizar_separador_generos(df: DataFrame, coluna: str = "genres") -> DataFrame:
    """Troca ';' e '|' por ',' para que o split funcione com um único separador."""
    return df.withColumn(coluna, F.regexp_replace(F.col(coluna), r"[;|]", ","))


def explodir_generos(df: DataFrame, coluna: str = "genres") -> DataFrame:
    """Desmembra a coluna de múltiplos gêneros em uma linha por gênero."""
    generos_array = F.split(F.col(coluna), r"\s*,\s*")
    return df.select("id", F.explode(generos_array).alias("nome_genero")).withColumn(
        "nome_genero", F.trim(F.col("nome_genero"))
    )


def remover_residuos_invalidos(df: DataFrame, coluna: str = "nome_genero") -> DataFrame:
    """Mantém apenas valores do domínio de gêneros (descarta branco, número, texto, caminho)."""
    return df.filter(F.col(coluna).isin(*_GENEROS_VALIDOS))


def transformar_generos(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_credits_and_tags.genres -> silver.tb_generos."""
    df = padronizar_separador_generos(df)
    df = explodir_generos(df)
    df = remover_residuos_invalidos(df)
    return df.withColumnRenamed("id", "id_filme").dropDuplicates(["id_filme", "nome_genero"])
