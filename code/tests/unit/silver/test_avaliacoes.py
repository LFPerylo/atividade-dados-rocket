from src.silver.avaliacoes import (
    invalidar_nota_fora_da_escala,
    padronizar_comentario_vazio,
    remover_avaliacoes_duplicadas,
    transformar_avaliacoes_usuarios,
)


def test_remover_avaliacoes_duplicadas_remove_linha_identica(spark):
    df = spark.createDataFrame(
        [
            (1, "Ana", 8.0, "Ótimo"),
            (1, "Ana", 8.0, "Ótimo"),
            (1, "Ana", 8.0, "Diferente"),
        ],
        ["id_filme", "nome_usuario", "nota_usuario", "comentario_usuario"],
    )

    resultado = remover_avaliacoes_duplicadas(df)

    assert resultado.count() == 2


def test_invalidar_nota_fora_da_escala(spark):
    df = spark.createDataFrame([(1, 8.0), (2, 15.0), (3, -3.0)], ["id_filme", "nota_usuario"])

    resultado = invalidar_nota_fora_da_escala(df)

    valores = {row["id_filme"]: row["nota_usuario"] for row in resultado.collect()}
    assert valores[1] == 8.0
    assert valores[2] is None
    assert valores[3] is None


def test_padronizar_comentario_vazio_preenche_texto_padrao(spark):
    df = spark.createDataFrame(
        [(1, "bom filme"), (2, "   "), (3, None)],
        ["id_filme", "comentario_usuario"],
    )

    resultado = padronizar_comentario_vazio(df)

    valores = {row["id_filme"]: row["comentario_usuario"] for row in resultado.collect()}
    assert valores[1] == "bom filme"
    assert valores[2] == "Sem comentário"
    assert valores[3] == "Sem comentário"


def test_transformar_avaliacoes_usuarios_produz_colunas_finais(spark):
    df = spark.createDataFrame(
        [(1, "Ana", 8.0, "")], ["id", "nome", "nota", "comentario"]
    )

    resultado = transformar_avaliacoes_usuarios(df)

    assert set(resultado.columns) == {
        "id_filme", "nome_usuario", "nota_usuario", "comentario_usuario",
    }
