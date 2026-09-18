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
    df = spark.createDataFrame([(1, "100", "200")], ["id", "budget", "revenue"])

    resultado = transformar_financeiro_filmes(df, cotacao=5.0)

    assert set(resultado.columns) == {
        "id_filme", "orcamento_usd", "receita_usd", "lucro_usd",
        "margem_lucro_percentual", "orcamento_brl", "receita_brl", "lucro_brl",
    }
