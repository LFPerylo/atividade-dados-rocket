from datetime import date
from unittest.mock import MagicMock, patch

from src.common.api_client import (
    buscar_cotacao_dolar,
    formatar_data_bcb,
    montar_url_cotacao_dolar,
)


def test_formatar_data_bcb_usa_formato_mm_dd_aaaa():
    assert formatar_data_bcb(date(2026, 9, 18)) == "09-18-2026"


def test_montar_url_cotacao_dolar_inclui_datas_formatadas():
    url = montar_url_cotacao_dolar(date(2026, 9, 11), date(2026, 9, 18))

    assert "dataInicial='09-11-2026'" in url
    assert "dataFinalCotacao='09-18-2026'" in url
    assert url.startswith("https://olinda.bcb.gov.br/olinda/servico/PTAX")


@patch("src.common.api_client.requests.get")
def test_buscar_cotacao_dolar_retorna_lista_de_registros(mock_get):
    resposta_mock = MagicMock()
    resposta_mock.json.return_value = {
        "value": [
            {"dataHoraCotacao": "2026-09-17 13:09:02.5", "cotacaoCompra": 5.35},
        ]
    }
    resposta_mock.raise_for_status.return_value = None
    mock_get.return_value = resposta_mock

    registros = buscar_cotacao_dolar(date(2026, 9, 11), date(2026, 9, 18))

    assert registros == [{"dataHoraCotacao": "2026-09-17 13:09:02.5", "cotacaoCompra": 5.35}]
    mock_get.assert_called_once()
