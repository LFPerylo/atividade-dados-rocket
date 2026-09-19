from pyspark.sql import functions as F
from src.common.deduplicacao import manter_registro_mais_recente_e_completo


def _df(spark, linhas):
    return spark.createDataFrame(
        linhas, ["id_filme", "ingestion_datetime", "orcamento", "receita"]
    ).withColumn("ingestion_datetime", F.col("ingestion_datetime").cast("timestamp"))


def test_prefere_o_registro_mais_recente(spark):
    df = _df(spark, [(1, "2026-01-01T00:00:00", 10, 20), (1, "2026-06-01T00:00:00", 30, 40)])

    resultado = manter_registro_mais_recente_e_completo(df, "id_filme")

    linha = resultado.collect()[0]
    assert resultado.count() == 1
    assert (linha["orcamento"], linha["receita"]) == (30, 40)


def test_no_empate_de_data_prefere_o_registro_mais_completo(spark):
    df = _df(
        spark,
        [
            (1, "2026-01-01T00:00:00", None, None),
            (1, "2026-01-01T00:00:00", 10, 20),
            (1, "2026-01-01T00:00:00", 10, None),
        ],
    )

    resultado = manter_registro_mais_recente_e_completo(df, "id_filme")

    linha = resultado.collect()[0]
    assert resultado.count() == 1
    assert (linha["orcamento"], linha["receita"]) == (10, 20)


def test_resultado_e_deterministico_mesmo_com_valores_conflitantes(spark):
    linhas = [(1, "2026-01-01T00:00:00", 10, 20), (1, "2026-01-01T00:00:00", 99, 88)]

    ida = _df(spark, linhas)
    volta = _df(spark, linhas[::-1])
    primeiro = manter_registro_mais_recente_e_completo(ida, "id_filme").collect()
    segundo = manter_registro_mais_recente_e_completo(volta, "id_filme").collect()

    assert primeiro == segundo


def test_mantem_um_registro_por_chave(spark):
    df = _df(
        spark,
        [
            (1, "2026-01-01T00:00:00", 1, 1),
            (2, "2026-01-01T00:00:00", 2, 2),
            (2, "2026-01-01T00:00:00", 3, 3),
        ],
    )

    assert manter_registro_mais_recente_e_completo(df, "id_filme").count() == 2
