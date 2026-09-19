from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_COLUNA_PARA_TIPO = {
    "cast": "Ator",
    "directors": "Diretor",
    "writers": "Roteirista",
    "production_companies": "Produtora",
}

_VALORES_AUSENTES = {"", "N/A", "n/a", "None"}
_TAMANHO_MAXIMO_NOME = 60  # acima disso é frase/sinopse vazada, não nome de pessoa ou empresa
_PADRAO_NUMERO = r"^-?\d+([.,]\d+)?$"
_PADRAO_CAMINHO_IMAGEM = r"(?i)^/|\.(jpg|jpeg|png)$"


def explodir_coluna_entidade(df: DataFrame, coluna: str, tipo_entidade: str) -> DataFrame:
    """Desmembra uma coluna de entidades (cast/directors/writers/production_companies).

    Limpa resíduos de aspas/barras nas pontas ("\\lance Henriksen", 'Sam Clarke"') e descarta o
    que não é nome: vazio, número, caminho de imagem, frase longa ou um único caractere.
    """
    valores = F.split(F.regexp_replace(F.col(coluna), r"[;|]", ","), r"\s*,\s*")
    nome = F.regexp_replace(F.trim(F.col("nome_entidade")), r'^[\\"\s]+|[\\"\s]+$', "")
    return (
        df.select("id", F.explode(valores).alias("nome_entidade"))
        .withColumn("nome_entidade", nome)
        .withColumn("tipo_entidade", F.lit(tipo_entidade))
        .filter(
            F.col("nome_entidade").isNotNull()
            & (~F.col("nome_entidade").isin(*_VALORES_AUSENTES))
            & (~F.col("nome_entidade").rlike(_PADRAO_NUMERO))
            & (~F.col("nome_entidade").rlike(_PADRAO_CAMINHO_IMAGEM))
            & (F.length("nome_entidade") >= 2)
            & (F.length("nome_entidade") <= _TAMANHO_MAXIMO_NOME)
        )
    )


def padronizar_capitalizacao(df: DataFrame, coluna: str = "nome_entidade") -> DataFrame:
    """Padroniza a capitalização do nome da pessoa/empresa, preservando nomes de caixa mista.

    Só as palavras escritas inteiras em maiúsculas ou em minúsculas viram Title Case; palavras
    já em caixa mista (O'Reilly, DiCaprio, McConaughey) são mantidas. Um initcap direto
    deformaria esses nomes ("O'reilly", "Dicaprio").
    """
    palavras = F.split(F.col(coluna), " ")
    normalizadas = F.transform(
        palavras,
        lambda p: F.when((p == F.upper(p)) | (p == F.lower(p)), F.initcap(p)).otherwise(p),
    )
    return df.withColumn(coluna, F.array_join(normalizadas, " "))


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
