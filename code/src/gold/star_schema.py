from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def construir_dim_movies(df: DataFrame) -> DataFrame:
    """gold.dim_movies: metadados principais e descritivos de cada filme."""
    janela = Window.orderBy("id_filme")
    return df.withColumn("sk_movie_id", F.row_number().over(janela).cast("bigint")).select(
        "sk_movie_id",
        F.col("id_filme").cast("string").alias("id_filme"),
        "titulo", "data_lancamento", "ano_lancamento", "duracao_minutos",
        "idioma_original", "status_filme", "sinopse",
    )


def construir_dim_genres(df: DataFrame) -> DataFrame:
    """gold.dim_genres: catálogo único e deduplicado de gêneros."""
    generos_unicos = df.select("nome_genero").distinct()
    janela = Window.orderBy("nome_genero")
    return generos_unicos.withColumn(
        "sk_genre_id", F.row_number().over(janela).cast("bigint")
    ).select("sk_genre_id", "nome_genero")


def construir_dim_people(df: DataFrame) -> DataFrame:
    """gold.dim_people: pessoas físicas (Ator, Diretor, Roteirista)."""
    pessoas = (
        df.filter(F.col("tipo_entidade").isin("Ator", "Diretor", "Roteirista"))
        .select(
            F.col("nome_entidade").alias("nome_pessoa"),
            F.col("tipo_entidade").alias("tipo_pessoa"),
        )
        .distinct()
    )
    janela = Window.orderBy("nome_pessoa", "tipo_pessoa")
    return pessoas.withColumn(
        "sk_person_id", F.row_number().over(janela).cast("bigint")
    ).select("sk_person_id", "nome_pessoa", "tipo_pessoa")


def construir_dim_companies(df: DataFrame) -> DataFrame:
    """gold.dim_companies: catálogo único de produtoras/estúdios."""
    produtoras = (
        df.filter(F.col("tipo_entidade") == "Produtora")
        .select(F.col("nome_entidade").alias("nome_produtora"))
        .distinct()
    )
    janela = Window.orderBy("nome_produtora")
    return produtoras.withColumn(
        "sk_company_id", F.row_number().over(janela).cast("bigint")
    ).select("sk_company_id", "nome_produtora")


def construir_bridge_movie_genre(
    dim_movies: DataFrame, dim_genres: DataFrame, df_generos: DataFrame
) -> DataFrame:
    """gold.bridge_movie_genre: conecta filmes a gêneros (N:N) sem duplicar o grão da fato."""
    return (
        df_generos.join(dim_movies, on="id_filme", how="inner")
        .join(dim_genres, on="nome_genero", how="inner")
        .select("sk_movie_id", "sk_genre_id")
        .distinct()
    )


def construir_bridge_movie_person(
    dim_movies: DataFrame, dim_people: DataFrame, df_pessoas: DataFrame
) -> DataFrame:
    """gold.bridge_movie_person: conecta filmes a pessoas (Ator/Diretor/Roteirista)."""
    pessoas_filtradas = df_pessoas.filter(
        F.col("tipo_entidade").isin("Ator", "Diretor", "Roteirista")
    )
    return (
        pessoas_filtradas.join(dim_movies, on="id_filme", how="inner")
        .join(
            dim_people,
            (pessoas_filtradas.nome_entidade == dim_people.nome_pessoa)
            & (pessoas_filtradas.tipo_entidade == dim_people.tipo_pessoa),
            how="inner",
        )
        .select("sk_movie_id", "sk_person_id")
        .distinct()
    )


def construir_bridge_movie_company(
    dim_movies: DataFrame, dim_companies: DataFrame, df_pessoas: DataFrame
) -> DataFrame:
    """gold.bridge_movie_company: conecta filmes a produtoras."""
    produtoras = df_pessoas.filter(F.col("tipo_entidade") == "Produtora")
    return (
        produtoras.join(dim_movies, on="id_filme", how="inner")
        .join(
            dim_companies,
            produtoras.nome_entidade == dim_companies.nome_produtora,
            how="inner",
        )
        .select("sk_movie_id", "sk_company_id")
        .distinct()
    )


def construir_fact_movies_performance(
    dim_movies: DataFrame, df_financeiro: DataFrame, df_engajamento: DataFrame
) -> DataFrame:
    """gold.fact_movies_performance: um registro por filme lançado, métricas financeiras
    e de engajamento consolidadas."""
    filmes_lancados = dim_movies.filter(F.col("status_filme") == "Lançado").select(
        "sk_movie_id", "id_filme"
    )
    return (
        filmes_lancados.join(df_financeiro, on="id_filme", how="left")
        .join(df_engajamento, on="id_filme", how="left")
        .select(
            "sk_movie_id",
            F.col("orcamento_usd").cast("decimal(18,2)"),
            F.col("receita_usd").cast("decimal(18,2)"),
            F.col("lucro_usd").cast("decimal(18,2)"),
            F.col("orcamento_brl").cast("decimal(18,2)"),
            F.col("receita_brl").cast("decimal(18,2)"),
            F.col("lucro_brl").cast("decimal(18,2)"),
            F.col("popularidade").cast("double"),
            F.col("nota_media_tmdb").cast("double"),
            F.col("qtd_votos_tmdb").cast("int"),
            F.col("nota_media_imdb").cast("double"),
            F.col("qtd_votos_imdb").cast("int"),
        )
    )


def construir_dim_reviews(dim_movies: DataFrame, df_avaliacoes: DataFrame) -> DataFrame:
    """gold.dim_reviews: avaliações de usuários resumidas por filme."""
    resumo = df_avaliacoes.groupBy("id_filme").agg(
        F.count("*").alias("qtd_avaliacoes_usuarios"),
        F.round(F.avg("nota_usuario"), 2).alias("nota_media_usuarios"),
    )
    janela = Window.orderBy("id_filme")
    return (
        resumo.join(dim_movies.select("sk_movie_id", "id_filme"), on="id_filme", how="inner")
        .withColumn("sk_review_id", F.row_number().over(janela).cast("bigint"))
        .select("sk_review_id", "sk_movie_id", "qtd_avaliacoes_usuarios", "nota_media_usuarios")
    )
