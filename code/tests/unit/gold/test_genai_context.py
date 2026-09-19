from src.gold.genai_context import agregar_pessoas_por_filme, construir_genai_context


def _base(spark):
    dim_movies = spark.createDataFrame(
        [
            (1, "1", "Deadpool", 2016, "sinopse do Deadpool"),
            (2, "2", "Filme Sem Diretor", 2020, None),
        ],
        ["sk_movie_id", "id_filme", "titulo", "ano_lancamento", "sinopse"],
    )
    fact = spark.createDataFrame(
        [(1, 100.0, 200.0), (2, None, None)],
        ["sk_movie_id", "orcamento_usd", "receita_usd"],
    )
    return dim_movies, fact


def test_agregar_pessoas_por_filme_junta_multiplos_atores(spark):
    bridge = spark.createDataFrame([(1, 10), (1, 11)], ["sk_movie_id", "sk_person_id"])
    dim_people = spark.createDataFrame(
        [(10, "Ryan Reynolds", "Ator"), (11, "Morena Baccarin", "Ator")],
        ["sk_person_id", "nome_pessoa", "tipo_pessoa"],
    )

    resultado = agregar_pessoas_por_filme(bridge, dim_people, "Ator")
    linha = resultado.collect()[0]

    assert "Ryan Reynolds" in linha["atores_principais"]
    assert "Morena Baccarin" in linha["atores_principais"]


def test_construir_genai_context_usa_fallback_quando_diretor_e_sinopse_sao_nulos(spark):
    dim_movies, fact = _base(spark)
    atores_por_filme = spark.createDataFrame(
        [(1, "Ryan Reynolds")], ["sk_movie_id", "atores_principais"]
    )
    diretores_por_filme = spark.createDataFrame(
        [(1, "Tim Miller")], ["sk_movie_id", "diretor"]
    )

    resultado = construir_genai_context(dim_movies, fact, atores_por_filme, diretores_por_filme)
    linhas = {row["movie_id"]: row["llm_context_document"] for row in resultado.collect()}

    assert linhas["1"] is not None
    assert "Ryan Reynolds" in linhas["1"]
    # filme 2 nao tem diretor nem sinopse -- o documento nao pode ser NULL (casca de banana)
    assert linhas["2"] is not None
    assert "direção não informada" in linhas["2"]
    assert "sinopse não disponível" in linhas["2"]


def test_agregar_pessoas_por_filme_ordena_de_forma_deterministica(spark):
    bridge = spark.createDataFrame(
        [(1, 10), (1, 11), (1, 12)], ["sk_movie_id", "sk_person_id"]
    )
    dim_people = spark.createDataFrame(
        [(10, "Zoe", "Ator"), (11, "Ana", "Ator"), (12, "Bia", "Ator")],
        ["sk_person_id", "nome_pessoa", "tipo_pessoa"],
    )

    resultado = agregar_pessoas_por_filme(bridge, dim_people, "Ator")

    assert resultado.collect()[0]["atores_principais"] == "Ana, Bia, Zoe"
