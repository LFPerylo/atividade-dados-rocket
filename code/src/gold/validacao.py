from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.silver.generos import GENEROS_VALIDOS

_TOLERANCIA_LUCRO = 0.01  # diferença de arredondamento aceita entre lucro e receita - orçamento


@dataclass(frozen=True)
class ResultadoValidacao:
    regra: str
    passou: bool
    detalhe: str


class ValidacaoGoldFalhou(RuntimeError):
    """Uma ou mais regras de qualidade da camada Gold foram reprovadas."""


def _resultado(regra: str, violacoes: int, detalhe_ok: str, detalhe_falha: str):
    passou = violacoes == 0
    return ResultadoValidacao(regra, passou, detalhe_ok if passou else detalhe_falha)


def validar_fato_um_registro_por_filme(fact: DataFrame) -> ResultadoValidacao:
    total = fact.count()
    distintos = fact.select("sk_movie_id").distinct().count()
    return _resultado(
        "fato_um_registro_por_filme",
        total - distintos,
        f"{total} linhas, uma por filme",
        f"{total} linhas para {distintos} filmes distintos (grão duplicado)",
    )


def validar_fato_cobre_filmes_lancados(
    fact: DataFrame, dim_movies: DataFrame
) -> ResultadoValidacao:
    lancados = dim_movies.filter(F.col("status_filme") == "Lançado")
    faltando = lancados.join(fact, "sk_movie_id", "left_anti").count()
    sobrando = fact.join(lancados, "sk_movie_id", "left_anti").count()
    return _resultado(
        "fato_cobre_filmes_lancados",
        faltando + sobrando,
        f"{fact.count()} filmes lançados na fato",
        f"{faltando} lançados fora da fato e {sobrando} filmes na fato que não são lançados",
    )


def validar_contexto_ia_completo(contexto: DataFrame, dim_movies: DataFrame) -> ResultadoValidacao:
    vazios = contexto.filter(
        F.col("llm_context_document").isNull() | (F.trim("llm_context_document") == "")
    ).count()
    diferenca_linhas = abs(contexto.count() - dim_movies.count())
    return _resultado(
        "contexto_ia_completo",
        vazios + diferenca_linhas,
        f"{contexto.count()} documentos, um por filme, nenhum vazio",
        f"{vazios} documentos vazios/nulos e {diferenca_linhas} linhas de diferença "
        "para dim_movies",
    )


def validar_generos_no_dominio(dim_genres: DataFrame) -> ResultadoValidacao:
    fora = dim_genres.filter(~F.col("nome_genero").isin(*GENEROS_VALIDOS)).count()
    return _resultado(
        "generos_no_dominio",
        fora,
        f"{dim_genres.count()} gêneros, todos do domínio",
        f"{fora} gêneros fora do domínio",
    )


def _orfaos(bridge: DataFrame, dimensao: DataFrame, chave: str) -> int:
    return bridge.join(dimensao, chave, "left_anti").count()


def validar_integridade_bridges(
    bridge_genre: DataFrame,
    bridge_person: DataFrame,
    bridge_company: DataFrame,
    dim_movies: DataFrame,
    dim_genres: DataFrame,
    dim_people: DataFrame,
    dim_companies: DataFrame,
) -> ResultadoValidacao:
    orfaos = {
        "genre->movie": _orfaos(bridge_genre, dim_movies, "sk_movie_id"),
        "genre->genre": _orfaos(bridge_genre, dim_genres, "sk_genre_id"),
        "person->movie": _orfaos(bridge_person, dim_movies, "sk_movie_id"),
        "person->person": _orfaos(bridge_person, dim_people, "sk_person_id"),
        "company->movie": _orfaos(bridge_company, dim_movies, "sk_movie_id"),
        "company->company": _orfaos(bridge_company, dim_companies, "sk_company_id"),
    }
    com_problema = {ligacao: n for ligacao, n in orfaos.items() if n}
    return _resultado(
        "integridade_das_bridges",
        sum(orfaos.values()),
        "todas as chaves das bridges existem nas dimensões",
        f"chaves órfãs: {com_problema}",
    )


def _fora_da_escala(df: DataFrame, coluna: str) -> int:
    return df.filter((F.col(coluna) < 0) | (F.col(coluna) > 10)).count()


def validar_notas_no_intervalo(fact: DataFrame, dim_reviews: DataFrame) -> ResultadoValidacao:
    fora = (
        _fora_da_escala(fact, "nota_media_tmdb")
        + _fora_da_escala(fact, "nota_media_imdb")
        + _fora_da_escala(dim_reviews, "nota_media_usuarios")
    )
    return _resultado(
        "notas_entre_0_e_10",
        fora,
        "todas as notas estão entre 0 e 10",
        f"{fora} notas fora do intervalo de 0 a 10",
    )


def validar_financeiro_coerente(fact: DataFrame) -> ResultadoValidacao:
    colunas = ["orcamento_usd", "receita_usd", "orcamento_brl", "receita_brl"]
    negativos = sum(fact.filter(F.col(c) < 0).count() for c in colunas)
    lucro_errado = fact.filter(
        F.col("orcamento_usd").isNotNull()
        & F.col("receita_usd").isNotNull()
        & (
            F.abs(F.col("lucro_usd") - (F.col("receita_usd") - F.col("orcamento_usd")))
            > _TOLERANCIA_LUCRO
        )
    ).count()
    return _resultado(
        "financeiro_coerente",
        negativos + lucro_errado,
        "sem valores negativos e lucro = receita - orçamento",
        f"{negativos} valores negativos e {lucro_errado} lucros incoerentes",
    )


def validar_gold(
    *,
    dim_movies: DataFrame,
    dim_genres: DataFrame,
    dim_people: DataFrame,
    dim_companies: DataFrame,
    dim_reviews: DataFrame,
    fact: DataFrame,
    bridge_genre: DataFrame,
    bridge_person: DataFrame,
    bridge_company: DataFrame,
    contexto: DataFrame,
) -> list[ResultadoValidacao]:
    """Roda todas as regras e devolve o relatório completo (não interrompe na primeira falha)."""
    return [
        validar_fato_um_registro_por_filme(fact),
        validar_fato_cobre_filmes_lancados(fact, dim_movies),
        validar_contexto_ia_completo(contexto, dim_movies),
        validar_generos_no_dominio(dim_genres),
        validar_integridade_bridges(
            bridge_genre, bridge_person, bridge_company,
            dim_movies, dim_genres, dim_people, dim_companies,
        ),
        validar_notas_no_intervalo(fact, dim_reviews),
        validar_financeiro_coerente(fact),
    ]


def levantar_se_houver_falhas(resultados: list[ResultadoValidacao]) -> None:
    """Falha a task do Job listando todas as regras reprovadas."""
    reprovadas = [r for r in resultados if not r.passou]
    if reprovadas:
        linhas = "\n".join(f"- {r.regra}: {r.detalhe}" for r in reprovadas)
        raise ValidacaoGoldFalhou(
            f"{len(reprovadas)} regra(s) de qualidade reprovada(s):\n{linhas}"
        )
