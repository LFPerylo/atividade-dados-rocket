from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_PADRAO_DECIMAL = r"^-?\d+(\.\d+)?$"
_PADRAO_INTEIRO = r"^-?\d+$"


def converter_texto_para_numero_seguro(
    df: DataFrame, coluna: str, tipo: str = "double"
) -> DataFrame:
    """Converte para número apenas quando o texto é um número válido; senão, NULL.

    Valida por regex antes do cast: com ANSI ligado (Databricks Serverless) um cast direto
    de texto inválido lançaria erro, e o column shift da origem traz texto nessas colunas.
    """
    padrao = _PADRAO_DECIMAL if tipo == "double" else _PADRAO_INTEIRO
    valor = F.trim(F.col(coluna).cast("string"))
    return df.withColumn(coluna, F.when(valor.rlike(padrao), valor.cast(tipo)).otherwise(None))
