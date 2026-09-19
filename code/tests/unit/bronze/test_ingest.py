from datetime import datetime

from src.bronze.ingest import adicionar_ingestion_datetime, cotacao_dolar_para_dataframe


def test_adiciona_coluna_ingestion_datetime_com_timestamp_fixo(spark):
    df = spark.createDataFrame([(1, "Deadpool")], ["id", "title"])
    momento = datetime(2026, 9, 18, 12, 0, 0)

    resultado = adicionar_ingestion_datetime(df, momento=momento)

    linha = resultado.collect()[0]
    assert linha["ingestion_datetime"] == momento


def test_nao_altera_colunas_originais(spark):
    df = spark.createDataFrame([(1, "Deadpool")], ["id", "title"])

    resultado = adicionar_ingestion_datetime(df)

    assert set(resultado.columns) == {"id", "title", "ingestion_datetime"}


def test_cotacao_dolar_para_dataframe_converte_registros_da_api(spark):
    registros = [
        {"dataHoraCotacao": "2026-09-17 13:09:02.5", "cotacaoCompra": 5.35},
        {"dataHoraCotacao": "2026-09-18 13:07:41.2", "cotacaoCompra": 5.40},
    ]

    resultado = cotacao_dolar_para_dataframe(spark, registros)

    linhas = {row["cotacaoCompra"] for row in resultado.collect()}
    assert linhas == {5.35, 5.40}
    assert set(resultado.columns) == {"dataHoraCotacao", "cotacaoCompra"}


def test_cotacao_dolar_para_dataframe_aceita_lista_vazia(spark):
    resultado = cotacao_dolar_para_dataframe(spark, [])

    assert resultado.count() == 0
    assert set(resultado.columns) == {"dataHoraCotacao", "cotacaoCompra"}
