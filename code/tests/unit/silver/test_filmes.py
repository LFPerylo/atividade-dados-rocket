from pyspark.sql import functions as F
from src.silver.filmes import (
    converter_data_lancamento,
    deduplicar_filmes_mais_recentes,
    normalizar_status,
    traduzir_status,
    transformar_info_filmes,
)


def test_normalizar_status_remove_ruido_hifen_e_padroniza_caixa(spark):
    df = spark.createDataFrame(
        [(1, "  released-"), (2, "POST PRODUCTION"), (3, "-Planned-")],
        ["id", "status_filme"],
    )

    resultado = normalizar_status(df)

    valores = {row["status_filme"] for row in resultado.collect()}
    assert valores == {"RELEASED", "POST PRODUCTION", "PLANNED"}


def test_traduzir_status_mapeia_termos_conhecidos_e_marca_desconhecidos(spark):
    df = spark.createDataFrame(
        [(1, "RELEASED"), (2, "CANCELED"), (3, "XYZ123"), (4, None)],
        ["id", "status_filme"],
    )

    resultado = traduzir_status(df)

    valores = {row["id"]: row["status_filme"] for row in resultado.collect()}
    assert valores == {
        1: "Lançado",
        2: "Cancelado",
        3: "Não Informado",
        4: "Não Informado",
    }


def test_deduplicar_filmes_mantem_apenas_o_registro_mais_recente(spark):
    df = spark.createDataFrame(
        [
            (1, "2026-01-01T00:00:00"),
            (1, "2026-06-01T00:00:00"),
            (2, "2026-01-01T00:00:00"),
        ],
        ["id_filme", "ingestion_datetime"],
    ).withColumn("ingestion_datetime", F.col("ingestion_datetime").cast("timestamp"))

    resultado = deduplicar_filmes_mais_recentes(df)

    linhas = {row["id_filme"]: row["ingestion_datetime"].isoformat() for row in resultado.collect()}
    assert resultado.count() == 2
    assert linhas[1].startswith("2026-06-01")


def test_converter_data_lancamento_aceita_multiplos_formatos(spark):
    df = spark.createDataFrame(
        [(1, "2016-02-09"), (2, "04-25-2018"), (3, "data-invalida")],
        ["id", "release_date"],
    )

    resultado = converter_data_lancamento(df)

    valores = {row["id"]: row["data_lancamento"] for row in resultado.collect()}
    assert str(valores[1]) == "2016-02-09"
    assert str(valores[2]) == "2018-04-25"
    assert valores[3] is None


def test_transformar_info_filmes_produz_colunas_finais_e_ano_lancamento(spark):
    df = spark.createDataFrame(
        [
            (
                1, "tt1", "Deadpool", "Deadpool", "en", "2016-02-09", 108,
                "Released", "sinopse", "tagline", "2026-01-01T00:00:00",
            )
        ],
        [
            "id", "tconst", "title", "original_title", "original_language",
            "release_date", "runtime", "status", "overview", "tagline",
            "ingestion_datetime",
        ],
    )

    resultado = transformar_info_filmes(df)
    linha = resultado.collect()[0]

    assert set(resultado.columns) == {
        "id_filme", "titulo", "titulo_original", "data_lancamento", "ano_lancamento",
        "duracao_minutos", "idioma_original", "status_filme", "sinopse", "frase_divulgacao",
    }
    assert linha["ano_lancamento"] == 2016
    assert linha["status_filme"] == "Lançado"
