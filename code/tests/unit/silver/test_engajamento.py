from src.silver.engajamento import (
    converter_texto_para_numero_seguro,
    invalidar_contagens_negativas,
    invalidar_notas_fora_da_escala,
    limpar_separador_decimal,
    transformar_metricas_engajamento,
)


def test_limpar_separador_decimal_troca_virgula_por_ponto(spark):
    df = spark.createDataFrame([(1, "154,34"), (2, "72.735")], ["id", "popularidade"])

    resultado = limpar_separador_decimal(df, "popularidade")

    valores = {row["popularidade"] for row in resultado.collect()}
    assert valores == {"154.34", "72.735"}


def test_converter_texto_para_numero_seguro_trata_texto_deslocado_como_null(spark):
    df = spark.createDataFrame(
        [(1, "8.0"), (2, "texto_fora_de_contexto")], ["id", "nota_media_imdb"]
    )

    resultado = converter_texto_para_numero_seguro(df, "nota_media_imdb", "double")

    valores = {row["id"]: row["nota_media_imdb"] for row in resultado.collect()}
    assert valores[1] == 8.0
    assert valores[2] is None


def test_invalidar_notas_fora_da_escala_zero_a_dez(spark):
    df = spark.createDataFrame(
        [(1, 8.5), (2, 76.06), (3, -1.0)], ["id", "nota_media_tmdb"]
    )

    resultado = invalidar_notas_fora_da_escala(df, ["nota_media_tmdb"])

    valores = {row["id"]: row["nota_media_tmdb"] for row in resultado.collect()}
    assert valores[1] == 8.5
    assert valores[2] is None
    assert valores[3] is None


def test_invalidar_contagens_negativas(spark):
    df = spark.createDataFrame([(1, 100), (2, -5)], ["id", "qtd_votos_tmdb"])

    resultado = invalidar_contagens_negativas(df, ["qtd_votos_tmdb"])

    valores = {row["id"]: row["qtd_votos_tmdb"] for row in resultado.collect()}
    assert valores[1] == 100
    assert valores[2] is None


def test_popularidade_com_texto_vazado_vira_null_e_nao_junta_digitos(spark):
    df = spark.createDataFrame(
        [
            (1, "a historia de 2009, 2010 e 2012.1", 7.0, 10, 7.0, 10, "2026-01-01T00:00:00"),
            (2, "89,985", 7.0, 10, 7.0, 10, "2026-01-01T00:00:00"),
            (3, '28"', 7.0, 10, 7.0, 10, "2026-01-01T00:00:00"),
        ],
        [
            "id", "popularity", "vote_average", "vote_count", "averageRating", "numVotes",
            "ingestion_datetime",
        ],
    )

    resultado = transformar_metricas_engajamento(df)
    valores = {row["id_filme"]: row["popularidade"] for row in resultado.collect()}

    assert valores[1] is None
    assert valores[2] == 89.985
    assert valores[3] is None  # aspa sobrando e resto de texto vazado, nao o numero 28


def test_transformar_metricas_engajamento_produz_colunas_finais(spark):
    df = spark.createDataFrame(
        [(1, "154,34", 8.255, 27713, 8.4, 1406782, "2026-01-01T00:00:00")],
        [
            "id", "popularity", "vote_average", "vote_count", "averageRating", "numVotes",
            "ingestion_datetime",
        ],
    )

    resultado = transformar_metricas_engajamento(df)
    linha = resultado.collect()[0]

    assert set(resultado.columns) == {
        "id_filme", "popularidade", "nota_media_tmdb", "qtd_votos_tmdb",
        "nota_media_imdb", "qtd_votos_imdb",
    }
    assert linha["popularidade"] == 154.34


def test_transformar_metricas_engajamento_deduplica_ids_repetidos(spark):
    df = spark.createDataFrame(
        [
            (1, "1.4", "0.0", "0", "abc", "abc", "2026-01-01T00:00:00"),
            (1, "1.4", "7.0", "10", "4.3", "6181", "2026-01-01T00:00:00"),
        ],
        [
            "id", "popularity", "vote_average", "vote_count", "averageRating", "numVotes",
            "ingestion_datetime",
        ],
    )

    resultado = transformar_metricas_engajamento(df)
    linha = resultado.collect()[0]

    assert resultado.count() == 1
    assert linha["qtd_votos_imdb"] == 6181
