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


def test_padronizar_separador_generos_trata_pipe_como_separador(spark):
    df = spark.createDataFrame([(1, "Comedy|Drama")], ["id", "genres"])

    resultado = transformar_generos(df)

    generos = {row["nome_genero"] for row in resultado.collect()}
    assert generos == {"Comedy", "Drama"}


def test_transformar_generos_descarta_o_que_nao_pertence_ao_dominio_de_generos(spark):
    df = spark.createDataFrame(
        [
            (1, "Drama, Never underestimate a nobody., /oajNi4Su39WAByHI6EONu8G8HYn.jpg"),
            (2, "Science Fiction, TV Movie, 2013"),
        ],
        ["id", "genres"],
    )

    resultado = transformar_generos(df)

    generos = {(row["id_filme"], row["nome_genero"]) for row in resultado.collect()}
    assert generos == {(1, "Drama"), (2, "Science Fiction"), (2, "TV Movie")}


def test_transformar_generos_produz_colunas_finais_sem_duplicatas(spark):
    df = spark.createDataFrame([(1, "Action, Action; Comedy")], ["id", "genres"])

    resultado = transformar_generos(df)

    assert set(resultado.columns) == {"id_filme", "nome_genero"}
    assert resultado.count() == 2
