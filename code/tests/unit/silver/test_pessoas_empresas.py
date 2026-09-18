from src.silver.pessoas_empresas import (
    explodir_coluna_entidade,
    padronizar_capitalizacao,
    unificar_pessoas_empresas,
)


def test_explodir_coluna_entidade_marca_tipo_correto(spark):
    df = spark.createDataFrame(
        [(1, "Ryan Reynolds, Morena Baccarin")], ["id", "cast"]
    )

    resultado = explodir_coluna_entidade(df, "cast", "Ator")

    tipos = {row["tipo_entidade"] for row in resultado.collect()}
    assert tipos == {"Ator"}
    assert resultado.count() == 2


def test_explodir_coluna_entidade_descarta_valor_n_a(spark):
    df = spark.createDataFrame([(1, "N/A")], ["id", "writers"])

    resultado = explodir_coluna_entidade(df, "writers", "Roteirista")

    assert resultado.count() == 0


def test_padronizar_capitalizacao_usa_title_case(spark):
    df = spark.createDataFrame([(1, "RYAN reynolds")], ["id_filme", "nome_entidade"])

    resultado = padronizar_capitalizacao(df)

    assert resultado.collect()[0]["nome_entidade"] == "Ryan Reynolds"


def test_unificar_pessoas_empresas_consolida_os_quatro_tipos(spark):
    df = spark.createDataFrame(
        [
            (
                293660, "Ryan Reynolds", "20th Century Fox",
                "United States", "English", "keywords",
                "Tim Miller", "Rhett Reese", "Ryan Reynolds",
            )
        ],
        [
            "id", "genres", "production_companies", "production_countries",
            "spoken_languages", "keywords", "directors", "writers", "cast",
        ],
    )

    resultado = unificar_pessoas_empresas(df)
    tipos = {row["tipo_entidade"] for row in resultado.collect()}

    assert set(resultado.columns) == {"id_filme", "nome_entidade", "tipo_entidade"}
    assert tipos == {"Ator", "Diretor", "Roteirista", "Produtora"}
