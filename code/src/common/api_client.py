from datetime import date

import requests

_URL_BASE = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
)


def formatar_data_bcb(data: date) -> str:
    """Formata a data no padrão MM-DD-AAAA exigido pela API PTAX do Banco Central."""
    return data.strftime("%m-%d-%Y")


def montar_url_cotacao_dolar(data_inicial: date, data_final: date) -> str:
    """Monta a URL completa da API de cotação do dólar para o período informado."""
    inicio = formatar_data_bcb(data_inicial)
    fim = formatar_data_bcb(data_final)
    return (
        f"{_URL_BASE}?@dataInicial='{inicio}'&@dataFinalCotacao='{fim}'"
        "&$select=dataHoraCotacao,cotacaoCompra&$format=json"
    )


def buscar_cotacao_dolar(data_inicial: date, data_final: date) -> list[dict]:
    """Chama a API PTAX do Banco Central e devolve os registros brutos de cotação."""
    url = montar_url_cotacao_dolar(data_inicial, data_final)
    resposta = requests.get(url, timeout=30)
    resposta.raise_for_status()
    return resposta.json()["value"]
