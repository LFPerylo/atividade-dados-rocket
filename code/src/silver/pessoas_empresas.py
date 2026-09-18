from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_COLUNA_PARA_TIPO = {
    "cast": "Ator",
    "directors": "Diretor",
    "writers": "Roteirista",
    "production_companies": "Produtora",
}

_VALORES_AUSENTES = {"", "N/A", "n/a", "None"}


def explodir_coluna_entidade(df: DataFrame, coluna: str, tipo_entidade: str) -> DataFrame:
    """Desmembra uma coluna de entidades (cast/directors/writers/production_companies)."""
    valores = F.split(F.regexp_replace(F.col(coluna), ";", ","), r"\s*,\s*")
    return (
        df.select("id", F.explode(valores).alias("nome_entidade"))
        .withColumn("nome_entidade", F.trim(F.col("nome_entidade")))
        .withColumn("tipo_entidade", F.lit(tipo_entidade))
        .filter(
            F.col("nome_entidade").isNotNull()
            & (~F.col("nome_entidade").isin(*_VALORES_AUSENTES))
        )
    )


def padronizar_capitalizacao(df: DataFrame, coluna: str = "nome_entidade") -> DataFrame:
    """Padroniza a capitalização (Title Case) do nome da pessoa/empresa."""
    return df.withColumn(coluna, F.initcap(F.col(coluna)))


def unificar_pessoas_empresas(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_credits_and_tags -> silver.tb_pessoas_empresas."""
    partes = [
        explodir_coluna_entidade(df, coluna, tipo)
        for coluna, tipo in _COLUNA_PARA_TIPO.items()
    ]
    unificado = partes[0]
    for parte in partes[1:]:
        unificado = unificado.unionByName(parte)
    unificado = padronizar_capitalizacao(unificado)
    return unificado.withColumnRenamed("id", "id_filme").dropDuplicates(
        ["id_filme", "nome_entidade", "tipo_entidade"]
    )
