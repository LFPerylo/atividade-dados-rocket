from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_TRADUCAO_STATUS = {
    "RELEASED": "Lançado",
    "POST PRODUCTION": "Pós-Produção",
    "IN PRODUCTION": "Em Produção",
    "PLANNED": "Planejado",
    "RUMORED": "Rumores",
    "CANCELED": "Cancelado",
}

_FORMATOS_DATA = [
    ("yyyy-MM-dd", r"^\d{4}-\d{2}-\d{2}$"),
    ("MM-dd-yyyy", r"^\d{2}-\d{2}-\d{4}$"),
    ("dd/MM/yyyy", r"^\d{2}/\d{2}/\d{4}$"),
]


def renomear_colunas_info_filmes(df: DataFrame) -> DataFrame:
    """Renomeia as colunas de origem (tb_movies_info) para os nomes da camada Silver."""
    return (
        df.withColumnRenamed("id", "id_filme")
        .withColumnRenamed("title", "titulo")
        .withColumnRenamed("original_title", "titulo_original")
        .withColumnRenamed("runtime", "duracao_minutos")
        .withColumnRenamed("original_language", "idioma_original")
        .withColumnRenamed("status", "status_filme")
        .withColumnRenamed("overview", "sinopse")
        .withColumnRenamed("tagline", "frase_divulgacao")
    )


def normalizar_status(df: DataFrame, coluna: str = "status_filme") -> DataFrame:
    """Remove ruídos, hífens sobressalentes e padroniza a caixa antes da tradução."""
    sem_hifen = F.regexp_replace(F.upper(F.trim(F.col(coluna))), r"^-+|-+$", "")
    return df.withColumn(coluna, F.regexp_replace(sem_hifen, r"\s+", " "))


def traduzir_status(df: DataFrame, coluna: str = "status_filme") -> DataFrame:
    """Traduz o status normalizado; termos não mapeados viram 'Não Informado'."""
    mapa = F.create_map([F.lit(x) for par in _TRADUCAO_STATUS.items() for x in par])
    return df.withColumn(coluna, F.coalesce(mapa[F.col(coluna)], F.lit("Não Informado")))


def deduplicar_filmes_mais_recentes(
    df: DataFrame, chave: str = "id_filme", coluna_data: str = "ingestion_datetime"
) -> DataFrame:
    """Mantém, por filme, apenas o registro com o ingestion_datetime mais recente."""
    janela = Window.partitionBy(chave).orderBy(F.col(coluna_data).desc())
    return (
        df.withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def converter_data_lancamento(
    df: DataFrame, coluna_origem: str = "release_date", coluna_destino: str = "data_lancamento"
) -> DataFrame:
    """Testa múltiplos formatos de data; se nenhum funcionar, o valor vira NULL.

    Com ANSI ligado (padrão do Databricks Serverless), converter texto fora do formato
    lança erro em vez de devolver NULL. Por isso cada formato só é tentado quando o texto
    tem a forma esperada (regex), e try_to_timestamp cobre datas impossíveis (31/02/2022).
    """
    texto = F.trim(F.col(coluna_origem))
    tentativas = [
        F.when(
            texto.rlike(padrao),
            F.try_to_timestamp(texto, F.lit(formato)).cast("date"),
        )
        for formato, padrao in _FORMATOS_DATA
    ]
    return df.withColumn(coluna_destino, F.coalesce(*tentativas))


def adicionar_ano_lancamento(
    df: DataFrame, coluna_data: str = "data_lancamento"
) -> DataFrame:
    """Deriva ano_lancamento a partir de data_lancamento."""
    return df.withColumn("ano_lancamento", F.year(F.col(coluna_data)))


def transformar_info_filmes(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_movies_info -> silver.tb_info_filmes."""
    df = renomear_colunas_info_filmes(df)
    df = normalizar_status(df)
    df = traduzir_status(df)
    df = deduplicar_filmes_mais_recentes(df)
    df = converter_data_lancamento(df)
    df = adicionar_ano_lancamento(df)
    return df.select(
        "id_filme", "titulo", "titulo_original", "data_lancamento", "ano_lancamento",
        "duracao_minutos", "idioma_original", "status_filme", "sinopse", "frase_divulgacao",
    )
