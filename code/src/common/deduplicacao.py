from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def manter_registro_mais_recente_e_completo(
    df: DataFrame,
    chave: str,
    coluna_data: str = "ingestion_datetime",
    colunas_completude: list[str] | None = None,
) -> DataFrame:
    """Mantém um único registro por chave, de forma determinística.

    Critérios, em ordem: ingestão mais recente; mais colunas preenchidas (a origem repete o
    mesmo filme várias vezes, às vezes com linhas vazias ou conflitantes); e, por último, um
    hash do conteúdo, para que o resultado não dependa da ordem em que o Spark lê as linhas.
    """
    colunas = colunas_completude or [c for c in df.columns if c not in (chave, coluna_data)]
    preenchidas = sum((F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in colunas), F.lit(0))
    desempate = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in colunas]), 256)
    janela = Window.partitionBy(chave).orderBy(
        F.col(coluna_data).desc(), preenchidas.desc(), desempate.asc()
    )
    return df.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")
