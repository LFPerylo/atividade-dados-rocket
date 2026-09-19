import pytest
from pyspark.sql import functions as F
from src.silver.cotacao_dolar import (
    obter_cotacao_mais_recente,
    preencher_serie_continua,
    preparar_cotacao_diaria,
    transformar_cotacao_dolar,
)


def test_preparar_cotacao_diaria_extrai_data_e_renomeia(spark):
    df = spark.createDataFrame(
        [("2026-09-17 13:09:02.5", 5.35)], ["dataHoraCotacao", "cotacaoCompra"]
    )

    resultado = preparar_cotacao_diaria(df)
    linha = resultado.collect()[0]

    assert str(linha["data_cotacao"]) == "2026-09-17"
    assert linha["cotacao_dolar"] == 5.35


def test_preencher_serie_continua_aplica_forward_fill_no_fim_de_semana(spark):
    df = spark.createDataFrame(
        [("2026-09-11", 5.30), ("2026-09-14", 5.40)], ["data_cotacao", "cotacao_dolar"]
    ).withColumn("data_cotacao", F.to_date("data_cotacao"))

    resultado = preencher_serie_continua(df, spark, "2026-09-11", "2026-09-14")
    valores = {str(row["data_cotacao"]): row["cotacao_dolar"] for row in resultado.collect()}

    assert valores["2026-09-12"] == 5.30
    assert valores["2026-09-13"] == 5.30
    assert valores["2026-09-14"] == 5.40


def test_obter_cotacao_mais_recente(spark):
    df = spark.createDataFrame(
        [("2026-09-11", 5.30), ("2026-09-14", 5.40)], ["data_cotacao", "cotacao_dolar"]
    ).withColumn("data_cotacao", F.to_date("data_cotacao"))

    assert obter_cotacao_mais_recente(df) == 5.40


def test_transformar_cotacao_dolar_produz_serie_continua(spark):
    df = spark.createDataFrame(
        [("2026-09-11 10:00:00", 5.30)], ["dataHoraCotacao", "cotacaoCompra"]
    )

    resultado = transformar_cotacao_dolar(df, spark, "2026-09-11", "2026-09-13")

    assert resultado.count() == 3
    assert set(resultado.columns) == {"data_cotacao", "cotacao_dolar"}


def test_obter_cotacao_mais_recente_ignora_dias_sem_valor(spark):
    df = spark.createDataFrame(
        [("2026-09-11", 5.30), ("2026-09-12", None)], ["data_cotacao", "cotacao_dolar"]
    ).withColumn("data_cotacao", F.to_date("data_cotacao"))

    assert obter_cotacao_mais_recente(df) == 5.30


def test_obter_cotacao_mais_recente_sem_nenhuma_cotacao_gera_erro_claro(spark):
    vazio = spark.createDataFrame([], "data_cotacao date, cotacao_dolar double")

    with pytest.raises(ValueError, match="cota"):
        obter_cotacao_mais_recente(vazio)


def test_transformar_cotacao_dolar_usa_historico_quando_a_janela_atual_esta_vazia(spark):
    historico = spark.createDataFrame(
        [("2026-09-05 10:00:00", 5.10)], ["dataHoraCotacao", "cotacaoCompra"]
    )

    serie = transformar_cotacao_dolar(historico, spark, "2026-09-11", "2026-09-13")

    assert obter_cotacao_mais_recente(serie) == 5.10
