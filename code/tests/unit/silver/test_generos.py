from src.silver.generos import (
    explodir_generos,
    padronizar_separador_generos,
    remover_residuos_invalidos,
    transformar_generos,
)


def test_padronizar_separador_generos_troca_ponto_e_virgula_por_virgula(spark):
    df = spark.createDataFrame([(1, "Action; Adventure")], ["id", "genres"])

    resultado = padronizar_separador_generos(df)

    assert resultado.collect()[0]["genres"] == "Action, Adventure"


def test_explodir_generos_gera_uma_linha_por_genero(spark):
    df = spark.createDataFrame([(1, "Action, Adventure, Comedy")], ["id", "genres"])

    resultado = explodir_generos(df)

    generos = {row["nome_genero"] for row in resultado.collect()}
    assert generos == {"Action", "Adventure", "Comedy"}


def test_remover_residuos_invalidos_descarta_branco_e_numerico(spark):
    df = spark.createDataFrame(
        [(1, "Action"), (1, ""), (1, "123"), (1, None)], ["id_filme", "nome_genero"]
    )

    resultado = remover_residuos_invalidos(df)

    valores = [row["nome_genero"] for row in resultado.collect()]
    assert valores == ["Action"]


def test_transformar_generos_produz_colunas_finais_sem_duplicatas(spark):
    df = spark.createDataFrame([(1, "Action, Action; Comedy")], ["id", "genres"])

    resultado = transformar_generos(df)

    assert set(resultado.columns) == {"id_filme", "nome_genero"}
    assert resultado.count() == 2
