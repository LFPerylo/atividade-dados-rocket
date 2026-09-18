from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def agregar_pessoas_por_filme(
    df_bridge_person: DataFrame, dim_people: DataFrame, tipo: str
) -> DataFrame:
    """Agrega, por filme, os nomes das pessoas de um tipo (Ator/Diretor) em uma string."""
    filtrado = dim_people.filter(F.col("tipo_pessoa") == tipo)
    coluna_saida = "atores_principais" if tipo == "Ator" else "diretor"
    return (
        df_bridge_person.join(filtrado, on="sk_person_id", how="inner")
        .groupBy("sk_movie_id")
        .agg(F.concat_ws(", ", F.collect_set("nome_pessoa")).alias(coluna_saida))
    )


def construir_genai_context(
    dim_movies: DataFrame,
    fact: DataFrame,
    atores_por_filme: DataFrame,
    diretores_por_filme: DataFrame,
) -> DataFrame:
    """gold_genai_movies_context: concatenação segura (com fallback) para o Vector Search.

    concat()/|| retornam NULL para a linha inteira se qualquer campo for NULL -- por isso
    todo campo que pode faltar (receita, orçamento, atores, diretor, sinopse) recebe um
    fallback textual via coalesce() antes da concatenação final.
    """
    base = (
        dim_movies.join(fact, on="sk_movie_id", how="left")
        .join(atores_por_filme, on="sk_movie_id", how="left")
        .join(diretores_por_filme, on="sk_movie_id", how="left")
    )

    titulo = F.coalesce(F.col("titulo"), F.lit("Título desconhecido"))
    ano = F.coalesce(F.col("ano_lancamento").cast("string"), F.lit("ano desconhecido"))
    receita = F.coalesce(
        F.concat(F.lit("US$ "), F.format_number(F.col("receita_usd"), 2)),
        F.lit("valor de bilheteria não informado"),
    )
    orcamento = F.coalesce(
        F.concat(F.lit("US$ "), F.format_number(F.col("orcamento_usd"), 2)),
        F.lit("orçamento não informado"),
    )
    atores = F.coalesce(F.col("atores_principais"), F.lit("elenco não informado"))
    diretor = F.coalesce(F.col("diretor"), F.lit("direção não informada"))
    sinopse = F.coalesce(F.col("sinopse"), F.lit("sinopse não disponível"))

    documento = F.concat(
        F.lit("O filme "), titulo, F.lit(", lançado no ano de "), ano,
        F.lit(", faturou "), receita, F.lit(" e teve um custo de "), orcamento,
        F.lit(". Estrelado por "), atores, F.lit(" e dirigido por "), diretor,
        F.lit(", o filme possui a seguinte sinopse: "), sinopse, F.lit("."),
    )

    return base.select(
        F.col("id_filme").alias("movie_id"),
        F.col("titulo").alias("title"),
        documento.alias("llm_context_document"),
    )
