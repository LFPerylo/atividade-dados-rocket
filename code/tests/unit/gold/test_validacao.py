import pytest
from src.gold.validacao import (
    ResultadoValidacao,
    ValidacaoGoldFalhou,
    levantar_se_houver_falhas,
    validar_contexto_ia_completo,
    validar_fato_cobre_filmes_lancados,
    validar_fato_um_registro_por_filme,
    validar_financeiro_coerente,
    validar_generos_no_dominio,
    validar_gold,
    validar_integridade_bridges,
    validar_notas_no_intervalo,
)


def _dim_movies(spark):
    return spark.createDataFrame(
        [(1, "Lançado"), (2, "Lançado"), (3, "Planejado")], "sk_movie_id long, status_filme string"
    )


def test_fato_um_registro_por_filme_passa_e_falha(spark):
    ok = spark.createDataFrame([(1,), (2,)], "sk_movie_id long")
    duplicada = spark.createDataFrame([(1,), (1,)], "sk_movie_id long")

    assert validar_fato_um_registro_por_filme(ok).passou
    resultado = validar_fato_um_registro_por_filme(duplicada)
    assert not resultado.passou
    assert "2 linhas" in resultado.detalhe


def test_fato_cobre_exatamente_os_filmes_lancados(spark):
    dim = _dim_movies(spark)
    ok = spark.createDataFrame([(1,), (2,)], "sk_movie_id long")
    faltando = spark.createDataFrame([(1,)], "sk_movie_id long")
    com_planejado = spark.createDataFrame([(1,), (2,), (3,)], "sk_movie_id long")
    orfa = spark.createDataFrame([(1,), (9,)], "sk_movie_id long")

    assert validar_fato_cobre_filmes_lancados(ok, dim).passou
    assert not validar_fato_cobre_filmes_lancados(faltando, dim).passou
    assert not validar_fato_cobre_filmes_lancados(com_planejado, dim).passou
    assert not validar_fato_cobre_filmes_lancados(orfa, dim).passou


def test_contexto_ia_completo_exige_uma_linha_por_filme_sem_documento_vazio(spark):
    dim = spark.createDataFrame([(1,), (2,)], "sk_movie_id long")
    esquema = "movie_id string, llm_context_document string"
    ok = spark.createDataFrame([("1", "O filme A"), ("2", "O filme B")], esquema)
    nulo = spark.createDataFrame([("1", "O filme A"), ("2", None)], esquema)
    em_branco = spark.createDataFrame([("1", "O filme A"), ("2", "   ")], esquema)
    faltando = spark.createDataFrame([("1", "O filme A")], esquema)

    assert validar_contexto_ia_completo(ok, dim).passou
    assert not validar_contexto_ia_completo(nulo, dim).passou
    assert not validar_contexto_ia_completo(em_branco, dim).passou
    assert not validar_contexto_ia_completo(faltando, dim).passou


def test_generos_no_dominio(spark):
    ok = spark.createDataFrame([("Drama",), ("Science Fiction",)], "nome_genero string")
    ruim = spark.createDataFrame([("Drama",), ("Comedy|Drama",)], "nome_genero string")

    assert validar_generos_no_dominio(ok).passou
    resultado = validar_generos_no_dominio(ruim)
    assert not resultado.passou
    assert "1" in resultado.detalhe


def _dimensoes_para_bridges(spark):
    return (
        spark.createDataFrame([(1,), (2,)], "sk_movie_id long"),
        spark.createDataFrame([(10,)], "sk_genre_id long"),
        spark.createDataFrame([(20,)], "sk_person_id long"),
        spark.createDataFrame([(30,)], "sk_company_id long"),
    )


def test_integridade_das_bridges(spark):
    dim_movies, dim_genres, dim_people, dim_companies = _dimensoes_para_bridges(spark)
    genero = spark.createDataFrame([(1, 10)], "sk_movie_id long, sk_genre_id long")
    pessoa = spark.createDataFrame([(2, 20)], "sk_movie_id long, sk_person_id long")
    empresa = spark.createDataFrame([(1, 30)], "sk_movie_id long, sk_company_id long")
    pessoa_orfa = spark.createDataFrame([(2, 99)], "sk_movie_id long, sk_person_id long")
    filme_orfao = spark.createDataFrame([(77, 30)], "sk_movie_id long, sk_company_id long")

    ok = validar_integridade_bridges(
        genero, pessoa, empresa, dim_movies, dim_genres, dim_people, dim_companies
    )
    assert ok.passou

    ruim_pessoa = validar_integridade_bridges(
        genero, pessoa_orfa, empresa, dim_movies, dim_genres, dim_people, dim_companies
    )
    assert not ruim_pessoa.passou

    ruim_filme = validar_integridade_bridges(
        genero, pessoa, filme_orfao, dim_movies, dim_genres, dim_people, dim_companies
    )
    assert not ruim_filme.passou


def test_notas_no_intervalo_de_zero_a_dez(spark):
    esquema_fato = "nota_media_tmdb double, nota_media_imdb double"
    fato_ok = spark.createDataFrame([(8.5, None), (0.0, 10.0)], esquema_fato)
    fato_ruim = spark.createDataFrame([(8.5, 63.65)], esquema_fato)
    reviews_ok = spark.createDataFrame([(7.0,)], "nota_media_usuarios double")
    reviews_ruim = spark.createDataFrame([(10.5,)], "nota_media_usuarios double")

    assert validar_notas_no_intervalo(fato_ok, reviews_ok).passou
    assert not validar_notas_no_intervalo(fato_ruim, reviews_ok).passou
    assert not validar_notas_no_intervalo(fato_ok, reviews_ruim).passou


def test_financeiro_coerente_sem_negativos_e_com_lucro_correto(spark):
    esquema = (
        "orcamento_usd decimal(18,2), receita_usd decimal(18,2), lucro_usd decimal(18,2), "
        "orcamento_brl decimal(18,2), receita_brl decimal(18,2), lucro_brl decimal(18,2)"
    )
    from decimal import Decimal as D

    ok = spark.createDataFrame(
        [
            (D("100"), D("300"), D("200"), D("500"), D("1500"), D("1000")),
            (None, D("300"), None, None, D("1500"), None),
        ],
        esquema,
    )
    negativo = spark.createDataFrame(
        [(D("-1"), D("300"), D("301"), D("500"), D("1500"), D("1000"))], esquema
    )
    lucro_errado = spark.createDataFrame(
        [(D("100"), D("300"), D("999"), D("500"), D("1500"), D("1000"))], esquema
    )

    assert validar_financeiro_coerente(ok).passou
    assert not validar_financeiro_coerente(negativo).passou
    assert not validar_financeiro_coerente(lucro_errado).passou


def test_levantar_se_houver_falhas_lista_as_regras_reprovadas():
    resultados = [
        ResultadoValidacao("regra_a", True, "ok"),
        ResultadoValidacao("regra_b", False, "3 violações"),
        ResultadoValidacao("regra_c", False, "1 violação"),
    ]

    with pytest.raises(ValidacaoGoldFalhou) as erro:
        levantar_se_houver_falhas(resultados)

    mensagem = str(erro.value)
    assert "regra_b" in mensagem and "regra_c" in mensagem and "regra_a" not in mensagem


def test_levantar_se_houver_falhas_nao_faz_nada_quando_tudo_passa():
    levantar_se_houver_falhas([ResultadoValidacao("regra_a", True, "ok")])


def test_validar_gold_roda_todas_as_regras_sobre_tabelas_consistentes(spark):
    resultados = validar_gold(
        dim_movies=_dim_movies(spark),
        dim_genres=spark.createDataFrame([(10, "Drama")], "sk_genre_id long, nome_genero string"),
        dim_people=spark.createDataFrame([(20,)], "sk_person_id long"),
        dim_companies=spark.createDataFrame([(30,)], "sk_company_id long"),
        dim_reviews=spark.createDataFrame([(7.0,)], "nota_media_usuarios double"),
        fact=spark.createDataFrame(
            [
                (1, None, None, None, None, None, None, 8.0, 7.0),
                (2, None, None, None, None, None, None, None, None),
            ],
            "sk_movie_id long, orcamento_usd decimal(18,2), receita_usd decimal(18,2), "
            "lucro_usd decimal(18,2), orcamento_brl decimal(18,2), receita_brl decimal(18,2), "
            "lucro_brl decimal(18,2), nota_media_tmdb double, nota_media_imdb double",
        ),
        bridge_genre=spark.createDataFrame([(1, 10)], "sk_movie_id long, sk_genre_id long"),
        bridge_person=spark.createDataFrame([(1, 20)], "sk_movie_id long, sk_person_id long"),
        bridge_company=spark.createDataFrame([(2, 30)], "sk_movie_id long, sk_company_id long"),
        contexto=spark.createDataFrame(
            [("1", "a"), ("2", "b"), ("3", "c")], "movie_id string, llm_context_document string"
        ),
    )

    assert len(resultados) == 7
    assert all(r.passou for r in resultados), [r for r in resultados if not r.passou]
