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


def test_explodir_coluna_entidade_limpa_residuos_de_aspas_e_barras(spark):
    df = spark.createDataFrame(
        [(1, '\\lance Henriksen, Sam Clarke"')], ["id", "cast"]
    )

    resultado = explodir_coluna_entidade(df, "cast", "Ator")

    nomes = {row["nome_entidade"] for row in resultado.collect()}
    assert nomes == {"lance Henriksen", "Sam Clarke"}


def test_explodir_coluna_entidade_descarta_numeros_caminhos_e_frases_longas(spark):
    frase = "Until the day a veil falls over the two gods on the run does the existence of poets"
    df = spark.createDataFrame(
        [(1, f"6.1, 766, /abc123.jpg, poster.png, X, {frase}, Ryan Reynolds")],
        ["id", "cast"],
    )

    resultado = explodir_coluna_entidade(df, "cast", "Ator")

    assert [row["nome_entidade"] for row in resultado.collect()] == ["Ryan Reynolds"]


def test_explodir_coluna_entidade_trata_pipe_como_separador(spark):
    df = spark.createDataFrame([(1, "Marvel Studios|Legendary Pictures")], ["id", "cast"])

    resultado = explodir_coluna_entidade(df, "cast", "Produtora")

    nomes = {row["nome_entidade"] for row in resultado.collect()}
    assert nomes == {"Marvel Studios", "Legendary Pictures"}


def test_padronizar_capitalizacao_preserva_nomes_de_caixa_mista(spark):
    df = spark.createDataFrame(
        [
            (1, "Kiefer O'Reilly"),
            (2, "Leonardo DiCaprio"),
            (3, "matthew McConaughey"),
            (4, "RYAN REYNOLDS"),
            (5, "ryan reynolds"),
        ],
        ["id_filme", "nome_entidade"],
    )

    resultado = padronizar_capitalizacao(df)
    nomes = {row["id_filme"]: row["nome_entidade"] for row in resultado.collect()}

    assert nomes[1] == "Kiefer O'Reilly"
    assert nomes[2] == "Leonardo DiCaprio"
    assert nomes[3] == "Matthew McConaughey"
    assert nomes[4] == "Ryan Reynolds"
    assert nomes[5] == "Ryan Reynolds"


def test_padronizar_capitalizacao_usa_title_case(spark):
    df = spark.createDataFrame([(1, "RYAN REYNOLDS")], ["id_filme", "nome_entidade"])

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
