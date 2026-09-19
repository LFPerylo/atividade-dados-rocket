from decimal import Decimal

from src.silver.financeiro import (
    calcular_lucro_e_margem,
    calcular_valores_brl,
    higienizar_valor_monetario,
    invalidar_valores_nao_positivos,
    transformar_financeiro_filmes,
)


def test_higienizar_valor_monetario_trata_texto_ausente_como_null(spark):
    df = spark.createDataFrame(
        [(1, "58000000"), (2, "Unknown"), (3, "$1,200,000.50")], ["id", "orcamento_usd"]
    )

    resultado = higienizar_valor_monetario(df, "orcamento_usd")
    valores = {row["id"]: row["orcamento_usd"] for row in resultado.collect()}

    assert valores[1] == Decimal("58000000.00")
    assert valores[2] is None
    assert valores[3] == Decimal("1200000.50")


def test_higienizar_valor_monetario_interpreta_prefixos_e_sufixos_reais_da_origem(spark):
    df = spark.createDataFrame(
        [
            (1, "$ 97000000"),
            (2, "USD 150000000"),
            (3, "34.0M"),
            (4, "250.5K"),
            (5, "N/A"),
            (6, "Não Informado"),
        ],
        ["id", "orcamento_usd"],
    )

    resultado = higienizar_valor_monetario(df, "orcamento_usd")
    valores = {row["id"]: row["orcamento_usd"] for row in resultado.collect()}

    assert valores[1] == Decimal("97000000.00")
    assert valores[2] == Decimal("150000000.00")
    assert valores[3] == Decimal("34000000.00")
    assert valores[4] == Decimal("250500.00")
    assert valores[5] is None
    assert valores[6] is None


def test_higienizar_valor_monetario_nao_junta_digitos_de_texto_vazado(spark):
    df = spark.createDataFrame(
        [(1, "lancado em 2013 e 2017"), (2, "a 9\" fim")], ["id", "orcamento_usd"]
    )

    resultado = higienizar_valor_monetario(df, "orcamento_usd")

    assert all(row["orcamento_usd"] is None for row in resultado.collect())


def test_invalidar_valores_nao_positivos(spark):
    df = spark.createDataFrame([(1, 100.0), (2, 0.0), (3, -50.0)], ["id", "receita_usd"])

    resultado = invalidar_valores_nao_positivos(df, ["receita_usd"])
    valores = {row["id"]: row["receita_usd"] for row in resultado.collect()}

    assert valores[1] == 100.0
    assert valores[2] is None
    assert valores[3] is None


def test_calcular_valores_brl_aplica_cotacao(spark):
    df = spark.createDataFrame(
        [(1, 100.0, 200.0)], ["id", "orcamento_usd", "receita_usd"]
    )

    resultado = calcular_valores_brl(df, cotacao=5.0)
    linha = resultado.collect()[0]

    assert linha["orcamento_brl"] == 500.0
    assert linha["receita_brl"] == 1000.0


def test_calcular_lucro_e_margem_nao_divide_por_zero(spark):
    df = spark.createDataFrame(
        [
            (1, 100.0, 200.0, 500.0, 1000.0),
            (2, None, 300.0, None, 1500.0),
        ],
        ["id", "orcamento_usd", "receita_usd", "orcamento_brl", "receita_brl"],
    )

    resultado = calcular_lucro_e_margem(df)
    valores = {row["id"]: row["margem_lucro_percentual"] for row in resultado.collect()}

    assert valores[1] == 100.0
    assert valores[2] is None


def test_transformar_financeiro_filmes_produz_colunas_finais(spark):
    df = spark.createDataFrame(
        [(1, "100", "200", "2026-01-01T00:00:00")],
        ["id", "budget", "revenue", "ingestion_datetime"],
    )

    resultado = transformar_financeiro_filmes(df, cotacao=5.0)

    assert set(resultado.columns) == {
        "id_filme", "orcamento_usd", "receita_usd", "lucro_usd",
        "margem_lucro_percentual", "orcamento_brl", "receita_brl", "lucro_brl",
    }


def test_transformar_financeiro_filmes_deduplica_ids_repetidos_preferindo_o_mais_completo(spark):
    df = spark.createDataFrame(
        [
            (1, "0", "0", "2026-01-01T00:00:00"),
            (1, "100", "300", "2026-01-01T00:00:00"),
            (2, "N/A", "Unknown", "2026-01-01T00:00:00"),
        ],
        ["id", "budget", "revenue", "ingestion_datetime"],
    )

    resultado = transformar_financeiro_filmes(df, cotacao=5.0)
    valores = {row["id_filme"]: row["receita_usd"] for row in resultado.collect()}

    assert resultado.count() == 2
    assert valores[1] == Decimal("300.00")
    assert valores[2] is None


def test_higienizar_valor_monetario_anula_valor_que_estoura_decimal(spark):
    df = spark.createDataFrame(
        [
            (1, "12345678901234567"),
            (2, "9999999999999999B"),
            (3, "9999999999999999"),
        ],
        ["id", "orcamento_usd"],
    )

    resultado = higienizar_valor_monetario(df, "orcamento_usd")
    valores = {row["id"]: row["orcamento_usd"] for row in resultado.collect()}

    assert valores[1] is None
    assert valores[2] is None
    assert valores[3] == Decimal("9999999999999999.00")
