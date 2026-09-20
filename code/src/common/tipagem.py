from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PADRAO_DECIMAL = r"^-?\d+(\.\d+)?$"
PADRAO_INTEIRO = r"^-?\d+$"


def converter_texto_para_numero_seguro(
    df: DataFrame, coluna: str, tipo: str = "double"
) -> DataFrame:
    """Converte para número apenas quando o texto é um número válido; senão, NULL.

    Com ANSI ligado (Databricks Serverless) um cast direto lança erro tanto para texto
    inválido quanto para número grande demais para o tipo (ex.: 99999999999 em INT). O regex
    filtra o formato e try_cast devolve NULL para o que estoura o tipo, sem derrubar o job.
    """
    padrao = PADRAO_DECIMAL if tipo == "double" else PADRAO_INTEIRO
    texto = F.trim(F.col(coluna).cast("string"))
    convertido = F.expr(f"try_cast(trim(cast(`{coluna}` as string)) as {tipo})")
    return df.withColumn(coluna, F.when(texto.rlike(padrao), convertido).otherwise(None))
