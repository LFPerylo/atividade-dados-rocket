from src.common.tipagem import converter_texto_para_numero_seguro


def test_conversao_segura_para_inteiro_aceita_valor_valido_e_anula_texto(spark):
    df = spark.createDataFrame([("108",), ("texto",), (None,)], ["v"])

    resultado = converter_texto_para_numero_seguro(df, "v", "int")

    assert [row["v"] for row in resultado.collect()] == [108, None, None]


def test_conversao_segura_para_inteiro_anula_valor_que_estoura_o_tipo(spark):
    df = spark.createDataFrame([("99999999999",), ("2147483647",)], ["v"])

    resultado = converter_texto_para_numero_seguro(df, "v", "int")

    assert [row["v"] for row in resultado.collect()] == [None, 2147483647]


def test_conversao_segura_para_double_aceita_decimal(spark):
    df = spark.createDataFrame([("8.4",), ("8,4",)], ["v"])

    resultado = converter_texto_para_numero_seguro(df, "v", "double")

    assert [row["v"] for row in resultado.collect()] == [8.4, None]
