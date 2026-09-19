from pyspark.sql import functions as F
from src.gold.star_schema import (
    construir_bridge_movie_company,
    construir_bridge_movie_genre,
    construir_bridge_movie_person,
    construir_dim_companies,
    construir_dim_genres,
    construir_dim_movies,
    construir_dim_people,
    construir_dim_reviews,
    construir_fact_movies_performance,
)


def _df_filmes(spark):
    return spark.createDataFrame(
        [
            (1, "Deadpool", "2016-02-09", 2016, 108, "en", "Lançado", "sinopse"),
            (2, "Filme Inédito", None, None, 90, "en", "Planejado", "outra sinopse"),
        ],
        [
            "id_filme", "titulo", "data_lancamento", "ano_lancamento",
            "duracao_minutos", "idioma_original", "status_filme", "sinopse",
        ],
    ).withColumn("data_lancamento", F.to_date("data_lancamento"))


def test_construir_dim_movies_gera_surrogate_key_unica(spark):
    resultado = construir_dim_movies(_df_filmes(spark))

    skus = [row["sk_movie_id"] for row in resultado.collect()]
    assert len(skus) == len(set(skus))
    assert "id_filme" in resultado.columns


def test_construir_dim_genres_deduplica_generos(spark):
    df = spark.createDataFrame(
        [(1, "Action"), (2, "Action"), (3, "Comedy")], ["id_filme", "nome_genero"]
    )

    resultado = construir_dim_genres(df)

    assert resultado.count() == 2


def test_construir_dim_people_filtra_apenas_pessoas_fisicas(spark):
    df = spark.createDataFrame(
        [
            (1, "Ryan Reynolds", "Ator"),
            (1, "Tim Miller", "Diretor"),
            (1, "20th Century Fox", "Produtora"),
        ],
        ["id_filme", "nome_entidade", "tipo_entidade"],
    )

    resultado = construir_dim_people(df)

    tipos = {row["tipo_pessoa"] for row in resultado.collect()}
    assert tipos == {"Ator", "Diretor"}


def test_construir_dim_companies_filtra_apenas_produtoras(spark):
    df = spark.createDataFrame(
        [(1, "Ryan Reynolds", "Ator"), (1, "20th Century Fox", "Produtora")],
        ["id_filme", "nome_entidade", "tipo_entidade"],
    )

    resultado = construir_dim_companies(df)

    assert resultado.count() == 1
    assert resultado.collect()[0]["nome_produtora"] == "20th Century Fox"


def test_construir_bridge_movie_genre_conecta_sem_duplicar_grao(spark):
    df_filmes = _df_filmes(spark)
    dim_movies = construir_dim_movies(df_filmes)
    df_generos = spark.createDataFrame(
        [(1, "Action"), (1, "Comedy")], ["id_filme", "nome_genero"]
    )
    dim_genres = construir_dim_genres(df_generos)

    resultado = construir_bridge_movie_genre(dim_movies, dim_genres, df_generos)

    assert resultado.count() == 2
    assert set(resultado.columns) == {"sk_movie_id", "sk_genre_id"}


def test_construir_fact_movies_performance_um_registro_por_filme_lancado(spark):
    df_filmes = _df_filmes(spark)
    dim_movies = construir_dim_movies(df_filmes)
    df_financeiro = spark.createDataFrame(
        [(1, 100.0, 200.0, 100.0, 100.0, 500.0, 1000.0, 500.0)],
        [
            "id_filme", "orcamento_usd", "receita_usd", "lucro_usd",
            "margem_lucro_percentual", "orcamento_brl", "receita_brl", "lucro_brl",
        ],
    )
    df_engajamento = spark.createDataFrame(
        [(1, 70.0, 8.0, 100, 8.5, 200)],
        [
            "id_filme", "popularidade", "nota_media_tmdb", "qtd_votos_tmdb",
            "nota_media_imdb", "qtd_votos_imdb",
        ],
    )

    resultado = construir_fact_movies_performance(dim_movies, df_financeiro, df_engajamento)

    assert resultado.count() == 1  # filme 2 ("Planejado") nao entra na fato


def test_construir_dim_reviews_agrega_media_e_contagem(spark):
    df_filmes = _df_filmes(spark)
    dim_movies = construir_dim_movies(df_filmes)
    df_avaliacoes = spark.createDataFrame(
        [(1, "Ana", 8.0, "bom"), (1, "Bia", 6.0, "ok")],
        ["id_filme", "nome_usuario", "nota_usuario", "comentario_usuario"],
    )

    resultado = construir_dim_reviews(dim_movies, df_avaliacoes)
    linha = resultado.collect()[0]

    assert linha["qtd_avaliacoes_usuarios"] == 2
    assert linha["nota_media_usuarios"] == 7.0


def _pessoas_empresas(spark):
    return spark.createDataFrame(
        [
            (1, "Ryan Reynolds", "Ator"),
            (1, "Ryan Reynolds", "Diretor"),
            (1, "Tim Miller", "Diretor"),
            (1, "20th Century Fox", "Produtora"),
            (2, "Ryan Reynolds", "Ator"),
            (99, "Ator Sem Filme", "Ator"),
        ],
        ["id_filme", "nome_entidade", "tipo_entidade"],
    )


def test_construir_bridge_movie_person_separa_homonimos_por_tipo_e_ignora_produtoras(spark):
    dim_movies = construir_dim_movies(_df_filmes(spark))
    pessoas = _pessoas_empresas(spark)
    dim_people = construir_dim_people(pessoas)

    resultado = construir_bridge_movie_person(dim_movies, dim_people, pessoas)

    # filme 1: Reynolds(Ator), Reynolds(Diretor), Miller(Diretor); filme 2: Reynolds(Ator)
    assert resultado.count() == 4
    assert set(resultado.columns) == {"sk_movie_id", "sk_person_id"}
    assert resultado.select("sk_person_id").distinct().count() == 3


def test_construir_bridge_movie_person_ignora_filmes_fora_da_dim_movies(spark):
    dim_movies = construir_dim_movies(_df_filmes(spark))
    pessoas = _pessoas_empresas(spark)
    dim_people = construir_dim_people(pessoas)

    resultado = construir_bridge_movie_person(dim_movies, dim_people, pessoas)

    assert resultado.join(dim_movies, "sk_movie_id").where("id_filme = '99'").count() == 0


def test_construir_bridge_movie_company_conecta_so_produtoras(spark):
    dim_movies = construir_dim_movies(_df_filmes(spark))
    pessoas = _pessoas_empresas(spark)
    dim_companies = construir_dim_companies(pessoas)

    resultado = construir_bridge_movie_company(dim_movies, dim_companies, pessoas)

    assert resultado.count() == 1
    assert set(resultado.columns) == {"sk_movie_id", "sk_company_id"}
