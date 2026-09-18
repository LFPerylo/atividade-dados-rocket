# CineData Analytics — Pipeline de Dados Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir o pipeline Bronze/Silver/Gold do catálogo de filmes CineData Analytics
(PySpark/Delta no Databricks), com lógica de negócio testável localmente, Star Schema,
tabela de contexto para RAG, orquestração via Job e respostas às 6 perguntas de negócio.

**Architecture:** Funções puras em `code/src/{bronze,silver,gold,common}` (sem I/O, sem
`dbutils`) testadas com `pytest` + Spark local; notebooks finos em `code/notebooks/`
orquestram (leem dados, chamam `src/`, escrevem Delta); sincronização com o Databricks via
Git Folders.

**Tech Stack:** PySpark 3.5, Delta Lake, pytest, ruff, GitHub Actions, Databricks
(Volumes, Git Folders, Workflows).

**Spec:** `docs/superpowers/specs/2026-09-18-cinedata-pipeline-design.md`

## Global Constraints

- Módulos em `code/src/` nunca importam `dbutils` nem fazem I/O — recebem e devolvem
  `DataFrame`. Só os notebooks conhecem caminhos, widgets e `display()`.
- Nomes de função, coluna e variável de negócio em português, espelhando exatamente os
  nomes de coluna definidos em `docs/stream.pdf`.
- Toda função de `src/` tem teste em `code/tests/unit/` escrito **antes** da implementação
  (TDD) e deve passar localmente antes de qualquer execução no Databricks.
- `Inputs/` (CSVs brutos) nunca é commitado no git — vai para `.gitignore`.
- Um commit por tarefa concluída, mensagem em português, seguindo a convenção de
  atribuição já configurada no ambiente (`Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>`).
- `docs/stream.pdf` é a fonte de verdade de qualquer regra de negócio — em caso de dúvida
  durante a implementação, reconferir o PDF antes de assumir um comportamento.

---

### Task 1: Scaffold do projeto

**Files:**
- Create: `requirements.txt`, `.gitignore`, `pytest.ini`, `pyproject.toml`,
  `.pre-commit-config.yaml`, `.github/workflows/ci.yml`, `CLAUDE.md`, `README.md`
- Create: `code/src/__init__.py`, `code/src/bronze/__init__.py`,
  `code/src/silver/__init__.py`, `code/src/gold/__init__.py`, `code/src/common/__init__.py`
- Create: `code/tests/unit/.gitkeep`, `code/notebooks/.gitkeep`

**Interfaces:** Nenhuma — infraestrutura pura, nada é consumido nem produzido para tasks
seguintes além dos arquivos em si.

- [ ] **Step 1: Criar `requirements.txt`**

```
pyspark==3.5.3
delta-spark==3.2.0
pytest==8.3.3
ruff==0.6.9
pre-commit==3.8.0
requests==2.32.3
```

- [ ] **Step 2: Criar `.gitignore`**

```
Inputs/
.venv/
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
.DS_Store
*.egg-info/
spark-warehouse/
derby.log
```

- [ ] **Step 3: Criar `pytest.ini` (permite `from src... import ...` nos testes)**

```ini
[pytest]
pythonpath = code
testpaths = code/tests/unit
```

- [ ] **Step 4: Criar `pyproject.toml` (config do ruff)**

```toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I"]
```

- [ ] **Step 5: Criar `.pre-commit-config.yaml`**

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.6.9
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
```

- [ ] **Step 6: Criar `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements.txt
      - run: ruff check code/src code/tests
      - run: pytest code/tests/unit -v
```

- [ ] **Step 7: Criar os `__init__.py` vazios** em `code/src/`, `code/src/bronze/`,
  `code/src/silver/`, `code/src/gold/`, `code/src/common/` (pacotes Python).

- [ ] **Step 8: Criar `CLAUDE.md`**

```markdown
# CineData Analytics — Pipeline de Dados

Projeto da disciplina de Engenharia de Dados (RocketLab 2026.2). Pipeline Bronze/Silver/Gold
no Databricks para um catálogo de filmes (TMDB/IMDb).

## Arquitetura
- `code/src/`: lógica de negócio em funções Python puras (recebem/devolvem DataFrame Spark,
  sem I/O, sem dbutils). Organizadas por camada: `bronze/`, `silver/`, `gold/`, `common/`.
- `code/notebooks/`: notebooks finos de orquestração (leem dados, chamam `src/`, escrevem
  Delta, exibem `display()`). Sincronizados no Databricks via Git Folders.
- `code/tests/unit/`: testes pytest para cada função de `src/`, rodando com Spark local
  (sem depender do Databricks).

## Comandos
- Rodar testes: `pytest code/tests/unit -v` (a partir da raiz do repo)
- Lint: `ruff check code/src code/tests`
- Instalar dependências: `pip install -r requirements.txt`

## Convenções
- Nomes de função, coluna e variável em português (domínio do negócio é em português,
  conforme `docs/stream.pdf`).
- Cada função de `src/` é testada isoladamente antes de ser usada em um notebook (TDD).
- Um commit por fase concluída do plano, mensagem em português.
- Spec: `docs/superpowers/specs/2026-09-18-cinedata-pipeline-design.md`
- Plano: `docs/superpowers/plans/2026-09-18-cinedata-pipeline-plan.md`

## Fonte de verdade das regras de negócio
`docs/stream.pdf` — mapeamentos de coluna, regras de limpeza e templates estão todos lá.
Ao implementar qualquer transformação, confirme contra o PDF antes de assumir uma regra.
```

- [ ] **Step 9: Atualizar `README.md`** com uma descrição curta do projeto, como rodar os
  testes localmente, e a estrutura de pastas (pode reaproveitar o texto do `CLAUDE.md`,
  adaptado para leitor externo/avaliador).

- [ ] **Step 10: Instalar dependências e validar o scaffold**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest code/tests/unit -v
```

Expected: pytest roda sem erro de coleta (0 testes encontrados, mas sem falha de import).

- [ ] **Step 11: Commit**

```bash
git add requirements.txt .gitignore pytest.ini pyproject.toml .pre-commit-config.yaml \
  .github/workflows/ci.yml CLAUDE.md README.md code/src code/tests code/notebooks
git commit -m "chore: cria scaffold do projeto (estrutura, deps, lint, CI)

Prepara a base de engenharia (ambiente Python, ruff, pytest, GitHub Actions)
antes de qualquer lógica de negócio, para que todo código futuro já nasça
testável e com CI validando cada push.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 2: SparkSession local para testes

**Files:**
- Create: `code/src/common/spark_session.py`
- Create: `code/tests/conftest.py`

**Interfaces:**
- Produces: `build_local_spark_session(app_name: str = "cinedata-tests") -> SparkSession`
  (usada só por `conftest.py`); fixture pytest `spark` (escopo `session`) disponível para
  todos os testes futuros.

- [ ] **Step 1: Criar `code/src/common/spark_session.py`**

```python
from pyspark.sql import SparkSession


def build_local_spark_session(app_name: str = "cinedata-tests") -> SparkSession:
    """Cria uma SparkSession local, usada apenas pelos testes (nunca pelos notebooks)."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
```

- [ ] **Step 2: Criar `code/tests/conftest.py`**

```python
import pytest

from src.common.spark_session import build_local_spark_session


@pytest.fixture(scope="session")
def spark():
    session = build_local_spark_session()
    yield session
    session.stop()
```

- [ ] **Step 3: Validar com um teste descartável**

Rode: `pytest code/tests/unit -v --co` (apenas coleta) para confirmar que não há erro de
import. Depois crie temporariamente `code/tests/unit/test_spark_fixture.py`:

```python
def test_spark_fixture_disponivel(spark):
    df = spark.createDataFrame([(1,)], ["a"])
    assert df.count() == 1
```

Rode: `pytest code/tests/unit -v` — Expected: PASS. Depois **apague** esse arquivo (era só
para validar a fixture; os testes de negócio virão nas próximas tasks).

- [ ] **Step 4: Commit**

```bash
git add code/src/common/spark_session.py code/tests/conftest.py
git commit -m "test: adiciona fixture de SparkSession local para os testes

Permite testar toda a lógica de negócio das próximas fases sem precisar de
um cluster Databricks ligado — feedback em segundos em vez de minutos.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 3: Bronze — `ingest.py`

**Files:**
- Create: `code/src/bronze/ingest.py`
- Test: `code/tests/unit/bronze/test_ingest.py`

**Interfaces:**
- Consumes: fixture `spark` (Task 2).
- Produces: `adicionar_ingestion_datetime(df: DataFrame, momento: datetime | None = None) -> DataFrame`
  e `cotacao_dolar_para_dataframe(spark: SparkSession, registros: list[dict]) -> DataFrame`
  — usadas pelo notebook `Landing_to_Bronze` (Task 5) e por `common/api_client.py` (Task 4).

- [ ] **Step 1: Criar `code/tests/unit/bronze/__init__.py`** (arquivo vazio, pacote de teste).

- [ ] **Step 2: Escrever o teste (falha esperada)**

`code/tests/unit/bronze/test_ingest.py`:

```python
from datetime import datetime

from src.bronze.ingest import adicionar_ingestion_datetime, cotacao_dolar_para_dataframe


def test_adiciona_coluna_ingestion_datetime_com_timestamp_fixo(spark):
    df = spark.createDataFrame([(1, "Deadpool")], ["id", "title"])
    momento = datetime(2026, 9, 18, 12, 0, 0)

    resultado = adicionar_ingestion_datetime(df, momento=momento)

    linha = resultado.collect()[0]
    assert linha["ingestion_datetime"] == momento


def test_nao_altera_colunas_originais(spark):
    df = spark.createDataFrame([(1, "Deadpool")], ["id", "title"])

    resultado = adicionar_ingestion_datetime(df)

    assert set(resultado.columns) == {"id", "title", "ingestion_datetime"}


def test_cotacao_dolar_para_dataframe_converte_registros_da_api(spark):
    registros = [
        {"dataHoraCotacao": "2026-09-17 13:09:02.5", "cotacaoCompra": 5.35},
        {"dataHoraCotacao": "2026-09-18 13:07:41.2", "cotacaoCompra": 5.40},
    ]

    resultado = cotacao_dolar_para_dataframe(spark, registros)

    linhas = {row["cotacaoCompra"] for row in resultado.collect()}
    assert linhas == {5.35, 5.40}
    assert set(resultado.columns) == {"dataHoraCotacao", "cotacaoCompra"}
```

- [ ] **Step 3: Rodar e confirmar falha**

Run: `pytest code/tests/unit/bronze/test_ingest.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.bronze.ingest'`.

- [ ] **Step 4: Implementar `code/src/bronze/ingest.py`**

```python
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def adicionar_ingestion_datetime(df: DataFrame, momento: datetime | None = None) -> DataFrame:
    """Adiciona a coluna ingestion_datetime com o instante de ingestão na camada Bronze."""
    instante = momento or datetime.utcnow()
    return df.withColumn("ingestion_datetime", F.lit(instante).cast("timestamp"))


def cotacao_dolar_para_dataframe(spark: SparkSession, registros: list[dict]) -> DataFrame:
    """Converte a lista de registros retornados pela API PTAX do Banco Central em DataFrame."""
    return spark.createDataFrame(registros)
```

- [ ] **Step 5: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/bronze/test_ingest.py -v`
Expected: 3 passed.

- [ ] **Step 6: Lint**

Run: `ruff check code/src/bronze/ingest.py code/tests/unit/bronze/test_ingest.py`
Expected: sem erros.

- [ ] **Step 7: Commit**

```bash
git add code/src/bronze/ingest.py code/tests/unit/bronze/
git commit -m "feat(bronze): adiciona ingestion_datetime e conversao da cotacao do dolar

A coluna ingestion_datetime rastreia o instante exato em que cada linha
entrou na camada Bronze, exigencia do enunciado para auditoria/append.
cotacao_dolar_para_dataframe isola a conversao da resposta da API do
Banco Central em DataFrame, sem misturar a chamada HTTP com a logica Spark.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 4: `common/api_client.py` — API PTAX do Banco Central

**Files:**
- Create: `code/src/common/api_client.py`
- Test: `code/tests/unit/common/test_api_client.py`

**Interfaces:**
- Produces: `formatar_data_bcb(data: date) -> str`,
  `montar_url_cotacao_dolar(data_inicial: date, data_final: date) -> str`,
  `buscar_cotacao_dolar(data_inicial: date, data_final: date) -> list[dict]` — usada pelo
  notebook `Landing_to_Bronze` (Task 5), que passa o resultado para
  `cotacao_dolar_para_dataframe` (Task 3).

- [ ] **Step 1: Criar `code/tests/unit/common/__init__.py`** (vazio).

- [ ] **Step 2: Escrever o teste (falha esperada)**

`code/tests/unit/common/test_api_client.py`:

```python
from datetime import date
from unittest.mock import patch, MagicMock

from src.common.api_client import (
    formatar_data_bcb,
    montar_url_cotacao_dolar,
    buscar_cotacao_dolar,
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
```

- [ ] **Step 3: Rodar e confirmar falha**

Run: `pytest code/tests/unit/common/test_api_client.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 4: Implementar `code/src/common/api_client.py`**

```python
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
```

- [ ] **Step 5: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/common/test_api_client.py -v`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add code/src/common/api_client.py code/tests/unit/common/
git commit -m "feat(bronze): adiciona cliente da API PTAX de cotacao do dolar

Isola a chamada HTTP e a formatacao de data (MM-DD-AAAA, exigencia da API)
em funcoes puras e testaveis com mock, sem depender de rede nos testes.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 5: Notebook `Landing_to_Bronze.ipynb`

**Files:**
- Create: `code/notebooks/Landing_to_Bronze.ipynb`

**Interfaces:**
- Consumes: `adicionar_ingestion_datetime`, `cotacao_dolar_para_dataframe` (Task 3),
  `buscar_cotacao_dolar` (Task 4).
- Produces: tabelas Delta `bronze.tb_movies_info`, `bronze.tb_movies_financials`,
  `bronze.tb_movies_metrics`, `bronze.tb_credits_and_tags`, `bronze.tb_movies_reviews`,
  `bronze.tb_cotacao_dolar` — consumidas pelo notebook `Bronze_to_Silver` (Task 13).

Este notebook só roda de fato no Databricks (depende de `dbutils`, Volumes e do `spark`
global do cluster), então não há ciclo de teste local aqui — a lógica que ele orquestra já
foi validada nas Tasks 3 e 4. A verificação real acontece na Task 18 (execução no
Databricks).

- [ ] **Step 1: Montar o notebook** com as células abaixo, em ordem (use `nbformat` para
  gerar o `.ipynb`, ou crie manualmente célula a célula):

Célula 1 (Markdown):
```markdown
# Landing to Bronze — CineData Analytics
Ingestão dos 5 CSVs brutos + cotação do dólar (API do Banco Central) na camada Bronze,
sem alteração estrutural/de conteúdo, com `ingestion_datetime` e escrita em modo Append.
```

Célula 2 (código — setup e widgets):
```python
import sys
sys.path.append("/Workspace" + __file__.rsplit("/code/notebooks", 1)[0] + "/code")

from datetime import date, timedelta

from src.bronze.ingest import adicionar_ingestion_datetime, cotacao_dolar_para_dataframe
from src.common.api_client import buscar_cotacao_dolar

dbutils.widgets.text("caminho_volume", "/Volumes/workspace/default/inputs")
dbutils.widgets.text("data_inicio", (date.today() - timedelta(days=7)).isoformat())
dbutils.widgets.text("data_fim", date.today().isoformat())

caminho_volume = dbutils.widgets.get("caminho_volume")
data_inicio = date.fromisoformat(dbutils.widgets.get("data_inicio"))
data_fim = date.fromisoformat(dbutils.widgets.get("data_fim"))
```

Célula 3 (código — cria database):
```python
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
```

Célula 4 (código — mapeamento arquivo → tabela, conforme seção 1.2 do enunciado):
```python
mapeamento_arquivos = {
    "movies_info_TMDB_IMDB.csv": "bronze.tb_movies_info",
    "movies_financials_IMDB_TMDB.csv": "bronze.tb_movies_financials",
    "movies_metrics_IMDB_TMDB.csv": "bronze.tb_movies_metrics",
    "credits_and_tags_IMDB_TMDB.csv": "bronze.tb_credits_and_tags",
    "movies_reviews.csv": "bronze.tb_movies_reviews",
}

for nome_arquivo, tabela_destino in mapeamento_arquivos.items():
    df_bruto = spark.read.csv(
        f"{caminho_volume}/{nome_arquivo}", header=True, inferSchema=True
    )
    df_com_ingestao = adicionar_ingestion_datetime(df_bruto)
    df_com_ingestao.write.format("delta").mode("append").saveAsTable(tabela_destino)
    print(f"{tabela_destino}: {df_com_ingestao.count()} linhas gravadas")
```

Célula 5 (código — ingestão da cotação do dólar):
```python
registros_cotacao = buscar_cotacao_dolar(data_inicio, data_fim)
df_cotacao = cotacao_dolar_para_dataframe(spark, registros_cotacao)
df_cotacao = adicionar_ingestion_datetime(df_cotacao)
df_cotacao.write.format("delta").mode("append").saveAsTable("bronze.tb_cotacao_dolar")

display(df_cotacao)
```

- [ ] **Step 2: Commit**

```bash
git add code/notebooks/Landing_to_Bronze.ipynb
git commit -m "feat(bronze): adiciona notebook Landing_to_Bronze

Notebook fino: le os 5 CSVs e a API do Banco Central, delega toda a logica
de negocio para src/bronze, e apenas orquestra leitura/escrita Delta no
Databricks (dbutils, Volumes, display() ficam isolados aqui).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 6: Silver — `filmes.py`

**Files:**
- Create: `code/src/silver/filmes.py`
- Test: `code/tests/unit/silver/test_filmes.py`

**Interfaces:**
- Produces: `transformar_info_filmes(df: DataFrame) -> DataFrame` (e as funções internas
  `normalizar_status`, `traduzir_status`, `deduplicar_filmes_mais_recentes`,
  `converter_data_lancamento`, `adicionar_ano_lancamento`) — consumida pelo notebook
  `Bronze_to_Silver` (Task 13) e por `gold/star_schema.py::construir_dim_movies` (Task 14).
  Saída: colunas `id_filme, titulo, titulo_original, data_lancamento, ano_lancamento,
  duracao_minutos, idioma_original, status_filme, sinopse, frase_divulgacao`.

- [ ] **Step 1: Criar `code/tests/unit/silver/__init__.py`** (vazio).

- [ ] **Step 2: Escrever o teste (falha esperada)**

`code/tests/unit/silver/test_filmes.py`:

```python
from pyspark.sql import functions as F

from src.silver.filmes import (
    normalizar_status,
    traduzir_status,
    deduplicar_filmes_mais_recentes,
    converter_data_lancamento,
    transformar_info_filmes,
)


def test_normalizar_status_remove_ruido_hifen_e_padroniza_caixa(spark):
    df = spark.createDataFrame(
        [(1, "  released-"), (2, "POST PRODUCTION"), (3, "-Planned-")],
        ["id", "status_filme"],
    )

    resultado = normalizar_status(df)

    valores = {row["status_filme"] for row in resultado.collect()}
    assert valores == {"RELEASED", "POST PRODUCTION", "PLANNED"}


def test_traduzir_status_mapeia_termos_conhecidos_e_marca_desconhecidos(spark):
    df = spark.createDataFrame(
        [(1, "RELEASED"), (2, "CANCELED"), (3, "XYZ123"), (4, None)],
        ["id", "status_filme"],
    )

    resultado = traduzir_status(df)

    valores = {row["id"]: row["status_filme"] for row in resultado.collect()}
    assert valores == {
        1: "Lançado",
        2: "Cancelado",
        3: "Não Informado",
        4: "Não Informado",
    }


def test_deduplicar_filmes_mantem_apenas_o_registro_mais_recente(spark):
    df = spark.createDataFrame(
        [
            (1, "2026-01-01T00:00:00"),
            (1, "2026-06-01T00:00:00"),
            (2, "2026-01-01T00:00:00"),
        ],
        ["id_filme", "ingestion_datetime"],
    ).withColumn("ingestion_datetime", F.col("ingestion_datetime").cast("timestamp"))

    resultado = deduplicar_filmes_mais_recentes(df)

    linhas = {row["id_filme"]: row["ingestion_datetime"].isoformat() for row in resultado.collect()}
    assert resultado.count() == 2
    assert linhas[1].startswith("2026-06-01")


def test_converter_data_lancamento_aceita_multiplos_formatos(spark):
    df = spark.createDataFrame(
        [(1, "2016-02-09"), (2, "04-25-2018"), (3, "data-invalida")],
        ["id", "release_date"],
    )

    resultado = converter_data_lancamento(df)

    valores = {row["id"]: row["data_lancamento"] for row in resultado.collect()}
    assert str(valores[1]) == "2016-02-09"
    assert str(valores[2]) == "2018-04-25"
    assert valores[3] is None


def test_transformar_info_filmes_produz_colunas_finais_e_ano_lancamento(spark):
    df = spark.createDataFrame(
        [
            (
                1, "tt1", "Deadpool", "Deadpool", "en", "2016-02-09", 108,
                "Released", "sinopse", "tagline", "2026-01-01T00:00:00",
            )
        ],
        [
            "id", "tconst", "title", "original_title", "original_language",
            "release_date", "runtime", "status", "overview", "tagline",
            "ingestion_datetime",
        ],
    )

    resultado = transformar_info_filmes(df)
    linha = resultado.collect()[0]

    assert set(resultado.columns) == {
        "id_filme", "titulo", "titulo_original", "data_lancamento", "ano_lancamento",
        "duracao_minutos", "idioma_original", "status_filme", "sinopse", "frase_divulgacao",
    }
    assert linha["ano_lancamento"] == 2016
    assert linha["status_filme"] == "Lançado"
```

- [ ] **Step 3: Rodar e confirmar falha**

Run: `pytest code/tests/unit/silver/test_filmes.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 4: Implementar `code/src/silver/filmes.py`**

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_TRADUCAO_STATUS = {
    "RELEASED": "Lançado",
    "POST PRODUCTION": "Pós-Produção",
    "IN PRODUCTION": "Em Produção",
    "PLANNED": "Planejado",
    "RUMORED": "Rumores",
    "CANCELED": "Cancelado",
}

_FORMATOS_DATA = ["yyyy-MM-dd", "MM-dd-yyyy", "dd/MM/yyyy"]


def renomear_colunas_info_filmes(df: DataFrame) -> DataFrame:
    """Renomeia as colunas de origem (tb_movies_info) para os nomes da camada Silver."""
    return (
        df.withColumnRenamed("id", "id_filme")
        .withColumnRenamed("title", "titulo")
        .withColumnRenamed("original_title", "titulo_original")
        .withColumnRenamed("runtime", "duracao_minutos")
        .withColumnRenamed("original_language", "idioma_original")
        .withColumnRenamed("status", "status_filme")
        .withColumnRenamed("overview", "sinopse")
        .withColumnRenamed("tagline", "frase_divulgacao")
    )


def normalizar_status(df: DataFrame, coluna: str = "status_filme") -> DataFrame:
    """Remove ruídos, hífens sobressalentes e padroniza a caixa antes da tradução."""
    sem_hifen = F.regexp_replace(F.upper(F.trim(F.col(coluna))), r"^-+|-+$", "")
    return df.withColumn(coluna, F.regexp_replace(sem_hifen, r"\s+", " "))


def traduzir_status(df: DataFrame, coluna: str = "status_filme") -> DataFrame:
    """Traduz o status normalizado; termos não mapeados viram 'Não Informado'."""
    mapa = F.create_map([F.lit(x) for par in _TRADUCAO_STATUS.items() for x in par])
    return df.withColumn(coluna, F.coalesce(mapa[F.col(coluna)], F.lit("Não Informado")))


def deduplicar_filmes_mais_recentes(
    df: DataFrame, chave: str = "id_filme", coluna_data: str = "ingestion_datetime"
) -> DataFrame:
    """Mantém, por filme, apenas o registro com o ingestion_datetime mais recente."""
    janela = Window.partitionBy(chave).orderBy(F.col(coluna_data).desc())
    return (
        df.withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def converter_data_lancamento(
    df: DataFrame, coluna_origem: str = "release_date", coluna_destino: str = "data_lancamento"
) -> DataFrame:
    """Testa múltiplos formatos de data; se nenhum funcionar, o valor vira NULL."""
    tentativas = [F.to_date(F.col(coluna_origem), fmt) for fmt in _FORMATOS_DATA]
    return df.withColumn(coluna_destino, F.coalesce(*tentativas))


def adicionar_ano_lancamento(
    df: DataFrame, coluna_data: str = "data_lancamento"
) -> DataFrame:
    """Deriva ano_lancamento a partir de data_lancamento."""
    return df.withColumn("ano_lancamento", F.year(F.col(coluna_data)))


def transformar_info_filmes(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_movies_info -> silver.tb_info_filmes."""
    df = renomear_colunas_info_filmes(df)
    df = normalizar_status(df)
    df = traduzir_status(df)
    df = deduplicar_filmes_mais_recentes(df)
    df = converter_data_lancamento(df)
    df = adicionar_ano_lancamento(df)
    return df.select(
        "id_filme", "titulo", "titulo_original", "data_lancamento", "ano_lancamento",
        "duracao_minutos", "idioma_original", "status_filme", "sinopse", "frase_divulgacao",
    )
```

- [ ] **Step 5: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/silver/test_filmes.py -v`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add code/src/silver/filmes.py code/tests/unit/silver/test_filmes.py \
  code/tests/unit/silver/__init__.py
git commit -m "feat(silver): adiciona transformacao de tb_info_filmes

Normaliza status antes de traduzir (ruido/hifen/caixa), deduplica mantendo
o registro mais recente por ingestion_datetime, e faz parsing robusto de
datas em multiplos formatos, tratando como NULL apenas o que for realmente
impossivel de converter -- conforme a secao 1.3.1 do enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 7: Silver — `engajamento.py`

**Files:**
- Create: `code/src/silver/engajamento.py`
- Test: `code/tests/unit/silver/test_engajamento.py`

**Interfaces:**
- Produces: `transformar_metricas_engajamento(df: DataFrame) -> DataFrame` — colunas
  `id_filme, popularidade, nota_media_tmdb, qtd_votos_tmdb, nota_media_imdb, qtd_votos_imdb`.
  Consumida pelo notebook `Bronze_to_Silver` (Task 13) e por
  `gold/star_schema.py::construir_fact_movies_performance` (Task 14).

- [ ] **Step 1: Escrever o teste (falha esperada)**

`code/tests/unit/silver/test_engajamento.py`:

```python
from src.silver.engajamento import (
    limpar_separador_decimal,
    converter_texto_para_numero_seguro,
    invalidar_notas_fora_da_escala,
    invalidar_contagens_negativas,
    transformar_metricas_engajamento,
)


def test_limpar_separador_decimal_troca_virgula_por_ponto(spark):
    df = spark.createDataFrame([(1, "154,34"), (2, "72.735")], ["id", "popularidade"])

    resultado = limpar_separador_decimal(df, "popularidade")

    valores = {row["popularidade"] for row in resultado.collect()}
    assert valores == {"154.34", "72.735"}


def test_converter_texto_para_numero_seguro_trata_texto_deslocado_como_null(spark):
    df = spark.createDataFrame(
        [(1, "8.0"), (2, "texto_fora_de_contexto")], ["id", "nota_media_imdb"]
    )

    resultado = converter_texto_para_numero_seguro(df, "nota_media_imdb", "double")

    valores = {row["id"]: row["nota_media_imdb"] for row in resultado.collect()}
    assert valores[1] == 8.0
    assert valores[2] is None


def test_invalidar_notas_fora_da_escala_zero_a_dez(spark):
    df = spark.createDataFrame(
        [(1, 8.5), (2, 76.06), (3, -1.0)], ["id", "nota_media_tmdb"]
    )

    resultado = invalidar_notas_fora_da_escala(df, ["nota_media_tmdb"])

    valores = {row["id"]: row["nota_media_tmdb"] for row in resultado.collect()}
    assert valores[1] == 8.5
    assert valores[2] is None
    assert valores[3] is None


def test_invalidar_contagens_negativas(spark):
    df = spark.createDataFrame([(1, 100), (2, -5)], ["id", "qtd_votos_tmdb"])

    resultado = invalidar_contagens_negativas(df, ["qtd_votos_tmdb"])

    valores = {row["id"]: row["qtd_votos_tmdb"] for row in resultado.collect()}
    assert valores[1] == 100
    assert valores[2] is None


def test_transformar_metricas_engajamento_produz_colunas_finais(spark):
    df = spark.createDataFrame(
        [(1, "154,34", 8.255, 27713, 8.4, 1406782)],
        ["id", "popularity", "vote_average", "vote_count", "averageRating", "numVotes"],
    )

    resultado = transformar_metricas_engajamento(df)
    linha = resultado.collect()[0]

    assert set(resultado.columns) == {
        "id_filme", "popularidade", "nota_media_tmdb", "qtd_votos_tmdb",
        "nota_media_imdb", "qtd_votos_imdb",
    }
    assert linha["popularidade"] == 154.34
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `pytest code/tests/unit/silver/test_engajamento.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 3: Implementar `code/src/silver/engajamento.py`**

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_PADRAO_DECIMAL = r"^-?\d+(\.\d+)?$"
_PADRAO_INTEIRO = r"^-?\d+$"


def limpar_separador_decimal(df: DataFrame, coluna: str) -> DataFrame:
    """Troca vírgula por ponto quando o valor usa vírgula como separador decimal."""
    valor = F.regexp_replace(F.trim(F.col(coluna).cast("string")), r"[^0-9,.\-]", "")
    valor_corrigido = F.when(
        valor.rlike(r"^-?\d+,\d+$"), F.regexp_replace(valor, ",", ".")
    ).otherwise(valor)
    return df.withColumn(coluna, valor_corrigido)


def converter_texto_para_numero_seguro(
    df: DataFrame, coluna: str, tipo: str = "double"
) -> DataFrame:
    """Converte para número apenas quando o texto é um número válido; senão, NULL."""
    padrao = _PADRAO_DECIMAL if tipo == "double" else _PADRAO_INTEIRO
    valor = F.trim(F.col(coluna).cast("string"))
    return df.withColumn(coluna, F.when(valor.rlike(padrao), valor.cast(tipo)).otherwise(None))


def invalidar_notas_fora_da_escala(
    df: DataFrame, colunas: list[str], minimo: float = 0.0, maximo: float = 10.0
) -> DataFrame:
    """Notas fora de [0, 10] (incluindo erro de escala) viram NULL."""
    for coluna in colunas:
        df = df.withColumn(
            coluna,
            F.when((F.col(coluna) < minimo) | (F.col(coluna) > maximo), None).otherwise(
                F.col(coluna)
            ),
        )
    return df


def invalidar_contagens_negativas(df: DataFrame, colunas: list[str]) -> DataFrame:
    """Contagens/índices negativos viram NULL."""
    for coluna in colunas:
        df = df.withColumn(coluna, F.when(F.col(coluna) < 0, None).otherwise(F.col(coluna)))
    return df


def transformar_metricas_engajamento(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_movies_metrics -> silver.tb_metricas_engajamento."""
    df = (
        df.withColumnRenamed("id", "id_filme")
        .withColumnRenamed("popularity", "popularidade")
        .withColumnRenamed("vote_average", "nota_media_tmdb")
        .withColumnRenamed("vote_count", "qtd_votos_tmdb")
        .withColumnRenamed("averageRating", "nota_media_imdb")
        .withColumnRenamed("numVotes", "qtd_votos_imdb")
    )
    df = limpar_separador_decimal(df, "popularidade")
    df = converter_texto_para_numero_seguro(df, "popularidade", "double")
    df = converter_texto_para_numero_seguro(df, "nota_media_tmdb", "double")
    df = converter_texto_para_numero_seguro(df, "qtd_votos_tmdb", "int")
    df = converter_texto_para_numero_seguro(df, "nota_media_imdb", "double")
    df = converter_texto_para_numero_seguro(df, "qtd_votos_imdb", "int")
    df = invalidar_notas_fora_da_escala(df, ["nota_media_tmdb", "nota_media_imdb"])
    df = invalidar_contagens_negativas(
        df, ["qtd_votos_tmdb", "qtd_votos_imdb", "popularidade"]
    )
    return df.select(
        "id_filme", "popularidade", "nota_media_tmdb", "qtd_votos_tmdb",
        "nota_media_imdb", "qtd_votos_imdb",
    )
```

- [ ] **Step 4: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/silver/test_engajamento.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add code/src/silver/engajamento.py code/tests/unit/silver/test_engajamento.py
git commit -m "feat(silver): adiciona transformacao de tb_metricas_engajamento

Limpa separador decimal inconsistente, aplica conversao de tipagem segura
para neutralizar o column shift da base bruta (texto em coluna numerica
vira NULL sem quebrar o pipeline), e invalida notas fora de 0-10 e
contagens negativas -- conforme a secao 1.3.3 do enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 8: Silver — `avaliacoes.py`

**Files:**
- Create: `code/src/silver/avaliacoes.py`
- Test: `code/tests/unit/silver/test_avaliacoes.py`

**Interfaces:**
- Produces: `transformar_avaliacoes_usuarios(df: DataFrame) -> DataFrame` — colunas
  `id_filme, nome_usuario, nota_usuario, comentario_usuario`. Consumida pelo notebook
  `Bronze_to_Silver` (Task 13) e por `gold/star_schema.py::construir_dim_reviews` (Task 14).

- [ ] **Step 1: Escrever o teste (falha esperada)**

`code/tests/unit/silver/test_avaliacoes.py`:

```python
from src.silver.avaliacoes import (
    remover_avaliacoes_duplicadas,
    invalidar_nota_fora_da_escala,
    padronizar_comentario_vazio,
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
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `pytest code/tests/unit/silver/test_avaliacoes.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 3: Implementar `code/src/silver/avaliacoes.py`**

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def remover_avaliacoes_duplicadas(df: DataFrame) -> DataFrame:
    """Remove registros onde filme, usuário, nota e comentário são idênticos."""
    return df.dropDuplicates(
        ["id_filme", "nome_usuario", "nota_usuario", "comentario_usuario"]
    )


def invalidar_nota_fora_da_escala(df: DataFrame) -> DataFrame:
    """Notas fora de [0, 10] viram NULL."""
    return df.withColumn(
        "nota_usuario",
        F.when(
            (F.col("nota_usuario") < 0) | (F.col("nota_usuario") > 10), None
        ).otherwise(F.col("nota_usuario")),
    )


def padronizar_comentario_vazio(df: DataFrame) -> DataFrame:
    """Comentários nulos ou só com espaços viram 'Sem comentário'."""
    return df.withColumn(
        "comentario_usuario",
        F.when(
            F.col("comentario_usuario").isNull()
            | (F.trim(F.col("comentario_usuario")) == ""),
            F.lit("Sem comentário"),
        ).otherwise(F.col("comentario_usuario")),
    )


def transformar_avaliacoes_usuarios(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_movies_reviews -> silver.tb_avaliacoes_usuarios."""
    df = (
        df.withColumnRenamed("id", "id_filme")
        .withColumnRenamed("nome", "nome_usuario")
        .withColumnRenamed("nota", "nota_usuario")
        .withColumnRenamed("comentario", "comentario_usuario")
    )
    df = remover_avaliacoes_duplicadas(df)
    df = invalidar_nota_fora_da_escala(df)
    df = padronizar_comentario_vazio(df)
    return df.select("id_filme", "nome_usuario", "nota_usuario", "comentario_usuario")
```

- [ ] **Step 4: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/silver/test_avaliacoes.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add code/src/silver/avaliacoes.py code/tests/unit/silver/test_avaliacoes.py
git commit -m "feat(silver): adiciona transformacao de tb_avaliacoes_usuarios

Remove duplicatas exatas, invalida notas fora de 0-10, e preenche
comentarios vazios/whitespace com texto padronizado, conforme a
secao 1.3.4 do enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 9: Silver — `generos.py`

**Files:**
- Create: `code/src/silver/generos.py`
- Test: `code/tests/unit/silver/test_generos.py`

**Interfaces:**
- Produces: `transformar_generos(df: DataFrame) -> DataFrame` — colunas
  `id_filme, nome_genero`. Consumida pelo notebook `Bronze_to_Silver` (Task 13) e por
  `gold/star_schema.py::construir_dim_genres`/`construir_bridge_movie_genre` (Task 14).

- [ ] **Step 1: Escrever o teste (falha esperada)**

`code/tests/unit/silver/test_generos.py`:

```python
from src.silver.generos import (
    padronizar_separador_generos,
    explodir_generos,
    remover_residuos_invalidos,
    transformar_generos,
)


def test_padronizar_separador_generos_troca_ponto_e_virgula_por_virgula(spark):
    df = spark.createDataFrame([(1, "Action; Adventure")], ["id", "genres"])

    resultado = padronizar_separador_generos(df)

    assert resultado.collect()[0]["genres"] == "Action, Adventure"


def test_explodir_generos_gera_uma_linha_por_genero(spark):
    df = spark.createDataFrame([(1, "Action, Adventure, Comedy")], ["id", "genres"])

    resultado = explodir_generos(df)

    generos = {row["nome_genero"] for row in resultado.collect()}
    assert generos == {"Action", "Adventure", "Comedy"}


def test_remover_residuos_invalidos_descarta_branco_e_numerico(spark):
    df = spark.createDataFrame(
        [(1, "Action"), (1, ""), (1, "123"), (1, None)], ["id_filme", "nome_genero"]
    )

    resultado = remover_residuos_invalidos(df)

    valores = [row["nome_genero"] for row in resultado.collect()]
    assert valores == ["Action"]


def test_transformar_generos_produz_colunas_finais_sem_duplicatas(spark):
    df = spark.createDataFrame(
        [(1, "Action, Action; Comedy")], ["id", "genres"]
    )

    resultado = transformar_generos(df)

    assert set(resultado.columns) == {"id_filme", "nome_genero"}
    assert resultado.count() == 2
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `pytest code/tests/unit/silver/test_generos.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 3: Implementar `code/src/silver/generos.py`**

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def padronizar_separador_generos(df: DataFrame, coluna: str = "genres") -> DataFrame:
    """Troca ';' por ',' para que o split funcione com um único separador."""
    return df.withColumn(coluna, F.regexp_replace(F.col(coluna), ";", ","))


def explodir_generos(df: DataFrame, coluna: str = "genres") -> DataFrame:
    """Desmembra a coluna de múltiplos gêneros em uma linha por gênero."""
    generos_array = F.split(F.col(coluna), r"\s*,\s*")
    return df.select("id", F.explode(generos_array).alias("nome_genero")).withColumn(
        "nome_genero", F.trim(F.col("nome_genero"))
    )


def remover_residuos_invalidos(df: DataFrame, coluna: str = "nome_genero") -> DataFrame:
    """Remove valores em branco ou puramente numéricos (resíduos de column shift)."""
    valido = (
        F.col(coluna).isNotNull()
        & (F.col(coluna) != "")
        & (~F.col(coluna).rlike(r"^-?\d+(\.\d+)?$"))
    )
    return df.filter(valido)


def transformar_generos(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_credits_and_tags.genres -> silver.tb_generos."""
    df = padronizar_separador_generos(df)
    df = explodir_generos(df)
    df = remover_residuos_invalidos(df)
    return df.withColumnRenamed("id", "id_filme").dropDuplicates(["id_filme", "nome_genero"])
```

- [ ] **Step 4: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/silver/test_generos.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add code/src/silver/generos.py code/tests/unit/silver/test_generos.py
git commit -m "feat(silver): adiciona transformacao de tb_generos

Trata a inconsistencia de separadores (virgula vs ponto-e-virgula) antes
do split, explode a coluna genres em uma linha por genero, e remove
residuos em branco ou numericos deixados pelo column shift da origem,
conforme a secao 1.3.5 do enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 10: Silver — `pessoas_empresas.py`

**Files:**
- Create: `code/src/silver/pessoas_empresas.py`
- Test: `code/tests/unit/silver/test_pessoas_empresas.py`

**Interfaces:**
- Produces: `unificar_pessoas_empresas(df: DataFrame) -> DataFrame` — colunas
  `id_filme, nome_entidade, tipo_entidade` (valores `'Ator'|'Diretor'|'Roteirista'|'Produtora'`).
  Consumida pelo notebook `Bronze_to_Silver` (Task 13) e por
  `gold/star_schema.py::construir_dim_people/construir_dim_companies/bridges` (Task 14).

- [ ] **Step 1: Escrever o teste (falha esperada)**

`code/tests/unit/silver/test_pessoas_empresas.py`:

```python
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


def test_padronizar_capitalizacao_usa_title_case(spark):
    df = spark.createDataFrame([(1, "RYAN reynolds")], ["id_filme", "nome_entidade"])

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
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `pytest code/tests/unit/silver/test_pessoas_empresas.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 3: Implementar `code/src/silver/pessoas_empresas.py`**

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_COLUNA_PARA_TIPO = {
    "cast": "Ator",
    "directors": "Diretor",
    "writers": "Roteirista",
    "production_companies": "Produtora",
}

_VALORES_AUSENTES = {"", "N/A", "n/a", "None"}


def explodir_coluna_entidade(df: DataFrame, coluna: str, tipo_entidade: str) -> DataFrame:
    """Desmembra uma coluna de entidades (cast/directors/writers/production_companies)."""
    valores = F.split(F.regexp_replace(F.col(coluna), ";", ","), r"\s*,\s*")
    return (
        df.select("id", F.explode(valores).alias("nome_entidade"))
        .withColumn("nome_entidade", F.trim(F.col("nome_entidade")))
        .withColumn("tipo_entidade", F.lit(tipo_entidade))
        .filter(
            F.col("nome_entidade").isNotNull()
            & (~F.col("nome_entidade").isin(*_VALORES_AUSENTES))
        )
    )


def padronizar_capitalizacao(df: DataFrame, coluna: str = "nome_entidade") -> DataFrame:
    """Padroniza a capitalização (Title Case) do nome da pessoa/empresa."""
    return df.withColumn(coluna, F.initcap(F.col(coluna)))


def unificar_pessoas_empresas(df: DataFrame) -> DataFrame:
    """Pipeline completo: origem tb_credits_and_tags -> silver.tb_pessoas_empresas."""
    partes = [
        explodir_coluna_entidade(df, coluna, tipo)
        for coluna, tipo in _COLUNA_PARA_TIPO.items()
    ]
    unificado = partes[0]
    for parte in partes[1:]:
        unificado = unificado.unionByName(parte)
    unificado = padronizar_capitalizacao(unificado)
    return unificado.withColumnRenamed("id", "id_filme").dropDuplicates(
        ["id_filme", "nome_entidade", "tipo_entidade"]
    )
```

- [ ] **Step 4: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/silver/test_pessoas_empresas.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add code/src/silver/pessoas_empresas.py code/tests/unit/silver/test_pessoas_empresas.py
git commit -m "feat(silver): adiciona transformacao de tb_pessoas_empresas

Unifica cast/directors/writers/production_companies em uma unica dimensao
categorizada por tipo_entidade, padroniza capitalizacao e remove
duplicatas, conforme a secao 1.3.6 do enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 11: Silver — `cotacao_dolar.py`

**Files:**
- Create: `code/src/silver/cotacao_dolar.py`
- Test: `code/tests/unit/silver/test_cotacao_dolar.py`

**Interfaces:**
- Produces: `transformar_cotacao_dolar(df, spark, data_inicio, data_fim) -> DataFrame` e
  `obter_cotacao_mais_recente(df: DataFrame) -> float`. `obter_cotacao_mais_recente` é
  consumida por `silver/financeiro.py` (Task 12) e pelo notebook `Bronze_to_Silver`
  (Task 13).

- [ ] **Step 1: Escrever o teste (falha esperada)**

`code/tests/unit/silver/test_cotacao_dolar.py`:

```python
from pyspark.sql import functions as F

from src.silver.cotacao_dolar import (
    preparar_cotacao_diaria,
    preencher_serie_continua,
    obter_cotacao_mais_recente,
    transformar_cotacao_dolar,
)


def test_preparar_cotacao_diaria_extrai_data_e_renomeia(spark):
    df = spark.createDataFrame(
        [("2026-09-17 13:09:02.5", 5.35)], ["dataHoraCotacao", "cotacaoCompra"]
    )

    resultado = preparar_cotacao_diaria(df)
    linha = resultado.collect()[0]

    assert str(linha["data_cotacao"]) == "2026-09-17"
    assert linha["cotacao_dolar"] == 5.35


def test_preencher_serie_continua_aplica_forward_fill_no_fim_de_semana(spark):
    df = spark.createDataFrame(
        [("2026-09-11", 5.30), ("2026-09-14", 5.40)], ["data_cotacao", "cotacao_dolar"]
    ).withColumn("data_cotacao", F.to_date("data_cotacao"))

    resultado = preencher_serie_continua(df, spark, "2026-09-11", "2026-09-14")
    valores = {str(row["data_cotacao"]): row["cotacao_dolar"] for row in resultado.collect()}

    assert valores["2026-09-12"] == 5.30
    assert valores["2026-09-13"] == 5.30
    assert valores["2026-09-14"] == 5.40


def test_obter_cotacao_mais_recente(spark):
    df = spark.createDataFrame(
        [("2026-09-11", 5.30), ("2026-09-14", 5.40)], ["data_cotacao", "cotacao_dolar"]
    ).withColumn("data_cotacao", F.to_date("data_cotacao"))

    assert obter_cotacao_mais_recente(df) == 5.40


def test_transformar_cotacao_dolar_produz_serie_continua(spark):
    df = spark.createDataFrame(
        [("2026-09-11 10:00:00", 5.30)], ["dataHoraCotacao", "cotacaoCompra"]
    )

    resultado = transformar_cotacao_dolar(df, spark, "2026-09-11", "2026-09-13")

    assert resultado.count() == 3
    assert set(resultado.columns) == {"data_cotacao", "cotacao_dolar"}
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `pytest code/tests/unit/silver/test_cotacao_dolar.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 3: Implementar `code/src/silver/cotacao_dolar.py`**

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def preparar_cotacao_diaria(df: DataFrame) -> DataFrame:
    """Extrai a data (sem hora) e renomeia as colunas da resposta da API."""
    return (
        df.withColumn("data_cotacao", F.to_date(F.col("dataHoraCotacao")))
        .withColumnRenamed("cotacaoCompra", "cotacao_dolar")
        .select("data_cotacao", "cotacao_dolar")
        .dropDuplicates(["data_cotacao"])
    )


def preencher_serie_continua(
    df: DataFrame, spark: SparkSession, data_inicio: str, data_fim: str
) -> DataFrame:
    """Garante uma série diária contínua, preenchendo dias sem cotação (Forward Fill)."""
    calendario = spark.sql(
        f"SELECT explode(sequence(to_date('{data_inicio}'), to_date('{data_fim}'), "
        "interval 1 day)) AS data_cotacao"
    )
    unido = calendario.join(df, on="data_cotacao", how="left")
    janela = Window.orderBy("data_cotacao").rowsBetween(Window.unboundedPreceding, 0)
    return unido.withColumn(
        "cotacao_dolar", F.last("cotacao_dolar", ignorenulls=True).over(janela)
    )


def obter_cotacao_mais_recente(df: DataFrame) -> float:
    """Retorna a cotação do dia mais recente disponível na série."""
    linha = df.orderBy(F.col("data_cotacao").desc()).first()
    return float(linha["cotacao_dolar"])


def transformar_cotacao_dolar(
    df: DataFrame, spark: SparkSession, data_inicio: str, data_fim: str
) -> DataFrame:
    """Pipeline completo: origem tb_cotacao_dolar -> silver.tb_cotacao_dolar."""
    df = preparar_cotacao_diaria(df)
    return preencher_serie_continua(df, spark, data_inicio, data_fim)
```

- [ ] **Step 4: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/silver/test_cotacao_dolar.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add code/src/silver/cotacao_dolar.py code/tests/unit/silver/test_cotacao_dolar.py
git commit -m "feat(silver): adiciona forward-fill da serie de cotacao do dolar

A API do Banco Central nao retorna cotacao em fins de semana/feriados;
preencher_serie_continua gera o calendario completo e aplica forward fill
para que toda data tenha uma cotacao associada, conforme a secao 1.3.7
do enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 12: Silver — `financeiro.py`

**Files:**
- Create: `code/src/silver/financeiro.py`
- Test: `code/tests/unit/silver/test_financeiro.py`

**Interfaces:**
- Consumes: `obter_cotacao_mais_recente` (Task 11, só usado pelo notebook — este módulo
  recebe a cotação já como `float`).
- Produces: `transformar_financeiro_filmes(df: DataFrame, cotacao: float) -> DataFrame` —
  colunas `id_filme, orcamento_usd, receita_usd, lucro_usd, margem_lucro_percentual,
  orcamento_brl, receita_brl, lucro_brl`. Consumida pelo notebook `Bronze_to_Silver`
  (Task 13) e por `gold/star_schema.py::construir_fact_movies_performance` (Task 14).

- [ ] **Step 1: Escrever o teste (falha esperada)**

`code/tests/unit/silver/test_financeiro.py`:

```python
from decimal import Decimal

from src.silver.financeiro import (
    higienizar_valor_monetario,
    invalidar_valores_nao_positivos,
    calcular_valores_brl,
    calcular_lucro_e_margem,
    transformar_financeiro_filmes,
)


def test_higienizar_valor_monetario_trata_texto_ausente_como_null(spark):
    df = spark.createDataFrame(
        [(1, "58000000"), (2, "Unknown"), (3, "$1,200,000.50")], ["id", "orcamento_usd"]
    )

    resultado = higienizar_valor_monetario(df, "orcamento_usd")
    valores = {row["id"]: row["orcamento_usd"] for row in resultado.collect()}

    assert valores[1] == Decimal("58000000.00")
    assert valores[2] is None
    assert valores[3] == Decimal("1200000.50")


def test_invalidar_valores_nao_positivos(spark):
    df = spark.createDataFrame([(1, 100.0), (2, 0.0), (3, -50.0)], ["id", "receita_usd"])

    resultado = invalidar_valores_nao_positivos(df, ["receita_usd"])
    valores = {row["id"]: row["receita_usd"] for row in resultado.collect()}

    assert valores[1] == 100.0
    assert valores[2] is None
    assert valores[3] is None


def test_calcular_valores_brl_aplica_cotacao(spark):
    df = spark.createDataFrame(
        [(1, 100.0, 200.0)], ["id", "orcamento_usd", "receita_usd"]
    )

    resultado = calcular_valores_brl(df, cotacao=5.0)
    linha = resultado.collect()[0]

    assert linha["orcamento_brl"] == 500.0
    assert linha["receita_brl"] == 1000.0


def test_calcular_lucro_e_margem_nao_divide_por_zero(spark):
    df = spark.createDataFrame(
        [
            (1, 100.0, 200.0, 500.0, 1000.0),
            (2, None, 300.0, None, 1500.0),
        ],
        ["id", "orcamento_usd", "receita_usd", "orcamento_brl", "receita_brl"],
    )

    resultado = calcular_lucro_e_margem(df)
    valores = {row["id"]: row["margem_lucro_percentual"] for row in resultado.collect()}

    assert valores[1] == 100.0
    assert valores[2] is None


def test_transformar_financeiro_filmes_produz_colunas_finais(spark):
    df = spark.createDataFrame([(1, "100", "200")], ["id", "budget", "revenue"])

    resultado = transformar_financeiro_filmes(df, cotacao=5.0)

    assert set(resultado.columns) == {
        "id_filme", "orcamento_usd", "receita_usd", "lucro_usd",
        "margem_lucro_percentual", "orcamento_brl", "receita_brl", "lucro_brl",
    }
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `pytest code/tests/unit/silver/test_financeiro.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 3: Implementar `code/src/silver/financeiro.py`**

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_TEXTOS_AUSENTES = {"unknown", "não informado", "nao informado", "n/a", ""}


def higienizar_valor_monetario(df: DataFrame, coluna: str) -> DataFrame:
    """Remove símbolos de moeda/milhar e trata texto de ausência como NULL antes de converter."""
    texto = F.trim(F.col(coluna).cast("string"))
    e_ausente = F.lower(texto).isin(*_TEXTOS_AUSENTES) | texto.isNull()
    numero_limpo = F.regexp_replace(texto, r"[^0-9.\-]", "")
    return df.withColumn(
        coluna,
        F.when(e_ausente | (numero_limpo == ""), None).otherwise(
            numero_limpo.cast("decimal(18,2)")
        ),
    )


def invalidar_valores_nao_positivos(df: DataFrame, colunas: list[str]) -> DataFrame:
    """Valores zerados ou negativos viram NULL (não são orçamento/receita válidos)."""
    for coluna in colunas:
        df = df.withColumn(
            coluna, F.when(F.col(coluna) <= 0, None).otherwise(F.col(coluna))
        )
    return df


def calcular_valores_brl(df: DataFrame, cotacao: float) -> DataFrame:
    """Converte orçamento e receita de USD para BRL usando a cotação informada."""
    return df.withColumn(
        "orcamento_brl", F.round(F.col("orcamento_usd") * F.lit(cotacao), 2)
    ).withColumn("receita_brl", F.round(F.col("receita_usd") * F.lit(cotacao), 2))


def calcular_lucro_e_margem(df: DataFrame) -> DataFrame:
    """Deriva lucro (USD/BRL) e margem percentual, sem dividir por zero nem propagar NULL."""
    df = df.withColumn(
        "lucro_usd", F.col("receita_usd") - F.col("orcamento_usd")
    ).withColumn("lucro_brl", F.col("receita_brl") - F.col("orcamento_brl"))
    margem = F.when(
        F.col("orcamento_usd").isNull()
        | F.col("receita_usd").isNull()
        | (F.col("orcamento_usd") == 0),
        None,
    ).otherwise(F.round((F.col("lucro_usd") / F.col("orcamento_usd")) * 100, 2))
    return df.withColumn("margem_lucro_percentual", margem)


def transformar_financeiro_filmes(df: DataFrame, cotacao: float) -> DataFrame:
    """Pipeline completo: origem tb_movies_financials -> silver.tb_financeiro_filmes."""
    df = (
        df.withColumnRenamed("id", "id_filme")
        .withColumnRenamed("budget", "orcamento_usd")
        .withColumnRenamed("revenue", "receita_usd")
    )
    df = higienizar_valor_monetario(df, "orcamento_usd")
    df = higienizar_valor_monetario(df, "receita_usd")
    df = invalidar_valores_nao_positivos(df, ["orcamento_usd", "receita_usd"])
    df = calcular_valores_brl(df, cotacao)
    df = calcular_lucro_e_margem(df)
    return df.select(
        "id_filme", "orcamento_usd", "receita_usd", "lucro_usd",
        "margem_lucro_percentual", "orcamento_brl", "receita_brl", "lucro_brl",
    )
```

- [ ] **Step 4: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/silver/test_financeiro.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add code/src/silver/financeiro.py code/tests/unit/silver/test_financeiro.py
git commit -m "feat(silver): adiciona transformacao de tb_financeiro_filmes

Higieniza valores monetarios (simbolos, milhar, textos de ausencia),
invalida zeros/negativos, converte para BRL com a cotacao obtida da API
do Banco Central, e deriva lucro/margem sem dividir por zero nem propagar
NULL indevidamente, conforme a secao 1.3.2 do enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 13: Notebook `Bronze_to_Silver.ipynb`

**Files:**
- Create: `code/notebooks/Bronze_to_Silver.ipynb`

**Interfaces:**
- Consumes: `transformar_info_filmes` (6), `transformar_metricas_engajamento` (7),
  `transformar_avaliacoes_usuarios` (8), `transformar_generos` (9),
  `unificar_pessoas_empresas` (10), `transformar_cotacao_dolar`/`obter_cotacao_mais_recente`
  (11), `transformar_financeiro_filmes` (12).
- Produces: as 7 tabelas Delta `silver.tb_*` — consumidas pelo notebook `Silver_to_Gold`
  (Tasks 14–16).

Sem ciclo de teste local (depende do cluster Databricks e das tabelas Bronze já
carregadas); a lógica já foi validada nas Tasks 6–12. Verificação real na Task 18.

- [ ] **Step 1: Montar o notebook** com as células abaixo, em ordem:

Célula 1 (Markdown):
```markdown
# Bronze to Silver — CineData Analytics
Limpeza, padronização, tradução e deduplicação das tabelas Bronze, seguindo as regras de
negócio da seção 1.3 do enunciado. Nenhuma tabela Bronze é alterada.
```

Célula 2 (setup):
```python
import sys
sys.path.append("/Workspace" + __file__.rsplit("/code/notebooks", 1)[0] + "/code")

from src.silver.filmes import transformar_info_filmes
from src.silver.engajamento import transformar_metricas_engajamento
from src.silver.avaliacoes import transformar_avaliacoes_usuarios
from src.silver.generos import transformar_generos
from src.silver.pessoas_empresas import unificar_pessoas_empresas
from src.silver.cotacao_dolar import transformar_cotacao_dolar, obter_cotacao_mais_recente
from src.silver.financeiro import transformar_financeiro_filmes

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
```

Célula 3 (tb_info_filmes):
```python
df_info = spark.table("bronze.tb_movies_info")
df_filmes = transformar_info_filmes(df_info)
df_filmes.write.format("delta").mode("overwrite").saveAsTable("silver.tb_info_filmes")
display(df_filmes)
```

Célula 4 (tb_metricas_engajamento):
```python
df_metrics = spark.table("bronze.tb_movies_metrics")
df_engajamento = transformar_metricas_engajamento(df_metrics)
df_engajamento.write.format("delta").mode("overwrite").saveAsTable("silver.tb_metricas_engajamento")
display(df_engajamento)
```

Célula 5 (tb_avaliacoes_usuarios):
```python
df_reviews = spark.table("bronze.tb_movies_reviews")
df_avaliacoes = transformar_avaliacoes_usuarios(df_reviews)
df_avaliacoes.write.format("delta").mode("overwrite").saveAsTable("silver.tb_avaliacoes_usuarios")
display(df_avaliacoes)
```

Célula 6 (tb_generos e tb_pessoas_empresas, mesma origem):
```python
df_credits = spark.table("bronze.tb_credits_and_tags")

df_generos = transformar_generos(df_credits)
df_generos.write.format("delta").mode("overwrite").saveAsTable("silver.tb_generos")

df_pessoas_empresas = unificar_pessoas_empresas(df_credits)
df_pessoas_empresas.write.format("delta").mode("overwrite").saveAsTable("silver.tb_pessoas_empresas")

display(df_pessoas_empresas)
```

Célula 7 (tb_cotacao_dolar, forward-fill):
```python
df_cotacao_bronze = spark.table("bronze.tb_cotacao_dolar")
data_inicio = dbutils.widgets.get("data_inicio")
data_fim = dbutils.widgets.get("data_fim")

df_cotacao_silver = transformar_cotacao_dolar(df_cotacao_bronze, spark, data_inicio, data_fim)
df_cotacao_silver.write.format("delta").mode("overwrite").saveAsTable("silver.tb_cotacao_dolar")

cotacao_atual = obter_cotacao_mais_recente(df_cotacao_silver)
print(f"Cotação usada para conversão BRL: {cotacao_atual}")
```

Célula 8 (tb_financeiro_filmes, depende da cotação):
```python
df_financials = spark.table("bronze.tb_movies_financials")
df_financeiro = transformar_financeiro_filmes(df_financials, cotacao=cotacao_atual)
df_financeiro.write.format("delta").mode("overwrite").saveAsTable("silver.tb_financeiro_filmes")
display(df_financeiro)
```

- [ ] **Step 2: Commit**

```bash
git add code/notebooks/Bronze_to_Silver.ipynb
git commit -m "feat(silver): adiciona notebook Bronze_to_Silver

Orquestra as 7 transformacoes silver (filmes, financeiro, engajamento,
avaliacoes, generos, pessoas/empresas, cotacao), delegando toda a regra
de negocio para src/silver ja validado por testes unitarios.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 14: Gold — `star_schema.py`

**Files:**
- Create: `code/src/gold/star_schema.py`
- Test: `code/tests/unit/gold/test_star_schema.py`

**Interfaces:**
- Consumes: DataFrames no formato das saídas de `filmes.py`, `financeiro.py`,
  `engajamento.py`, `avaliacoes.py`, `generos.py`, `pessoas_empresas.py` (Tasks 6–12).
- Produces: `construir_dim_movies`, `construir_dim_genres`, `construir_dim_people`,
  `construir_dim_companies`, `construir_dim_reviews`, `construir_fact_movies_performance`,
  `construir_bridge_movie_genre`, `construir_bridge_movie_person`,
  `construir_bridge_movie_company` — todas `(DataFrame, ...) -> DataFrame`. Consumidas
  pelo notebook `Silver_to_Gold` (Task 16) e por `gold/genai_context.py` (Task 15).

- [ ] **Step 1: Criar `code/tests/unit/gold/__init__.py`** (vazio).

- [ ] **Step 2: Escrever o teste (falha esperada)**

`code/tests/unit/gold/test_star_schema.py`:

```python
from pyspark.sql import functions as F

from src.gold.star_schema import (
    construir_dim_movies,
    construir_dim_genres,
    construir_dim_people,
    construir_dim_companies,
    construir_bridge_movie_genre,
    construir_bridge_movie_person,
    construir_fact_movies_performance,
    construir_dim_reviews,
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
        ["id_filme", "popularidade", "nota_media_tmdb", "qtd_votos_tmdb", "nota_media_imdb", "qtd_votos_imdb"],
    )

    resultado = construir_fact_movies_performance(dim_movies, df_financeiro, df_engajamento)

    assert resultado.count() == 1  # filme 2 ("Planejado") não entra na fato


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
```

- [ ] **Step 3: Rodar e confirmar falha**

Run: `pytest code/tests/unit/gold/test_star_schema.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 4: Implementar `code/src/gold/star_schema.py`**

```python
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
```

- [ ] **Step 5: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/gold/test_star_schema.py -v`
Expected: 7 passed.

- [ ] **Step 6: Commit**

```bash
git add code/src/gold/star_schema.py code/tests/unit/gold/
git commit -m "feat(gold): adiciona modelagem dimensional (Star Schema)

Constroi dim_movies, dim_genres, dim_people, dim_companies, dim_reviews,
fact_movies_performance e as tres bridge tables com surrogate keys via
row_number(), garantindo que a fato nao duplique grao mesmo com filmes
tendo multiplos generos/atores/produtoras, conforme a Entrega 1 da
Parte 2 do enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 15: Gold — `genai_context.py`

**Files:**
- Create: `code/src/gold/genai_context.py`
- Test: `code/tests/unit/gold/test_genai_context.py`

**Interfaces:**
- Consumes: saídas de `construir_dim_movies`, `construir_fact_movies_performance`,
  `construir_bridge_movie_person`, `construir_dim_people` (Task 14).
- Produces: `agregar_pessoas_por_filme(...) -> DataFrame`,
  `construir_genai_context(dim_movies, fact, atores_por_filme, diretores_por_filme) -> DataFrame`
  — colunas `movie_id, title, llm_context_document`. Consumida pelo notebook
  `Silver_to_Gold` (Task 16).

- [ ] **Step 1: Escrever o teste (falha esperada)**

`code/tests/unit/gold/test_genai_context.py`:

```python
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
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `pytest code/tests/unit/gold/test_genai_context.py -v`
Expected: FAIL — módulo não existe.

- [ ] **Step 3: Implementar `code/src/gold/genai_context.py`**

```python
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
```

- [ ] **Step 4: Rodar e confirmar sucesso**

Run: `pytest code/tests/unit/gold/test_genai_context.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add code/src/gold/genai_context.py code/tests/unit/gold/test_genai_context.py
git commit -m "feat(gold): adiciona tabela de contexto para o RAG do time de IA

Usa coalesce() com fallback textual em cada campo que pode vir nulo
(receita, orcamento, atores, diretor, sinopse) antes da concatenacao
final, evitando a 'casca de banana' descrita no enunciado -- onde um
unico campo nulo faz o documento inteiro (e o filme) desaparecer da
tabela de contexto silenciosamente.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 16: Notebook `Silver_to_Gold.ipynb` (Star Schema + GenAI context + Analytics)

**Files:**
- Create: `code/notebooks/Silver_to_Gold.ipynb`

**Interfaces:**
- Consumes: todas as funções de `gold/star_schema.py` (Task 14) e `gold/genai_context.py`
  (Task 15).
- Produces: tabelas Delta `gold.dim_movies`, `gold.dim_genres`, `gold.dim_people`,
  `gold.dim_companies`, `gold.dim_reviews`, `gold.fact_movies_performance`,
  `gold.bridge_movie_genre`, `gold.bridge_movie_person`, `gold.bridge_movie_company`,
  `gold.gold_genai_movies_context` — consumidas na Task 18 (execução) e pelas 6 perguntas
  de negócio (mesma notebook).

- [ ] **Step 1: Montar o notebook** com as células abaixo, em ordem:

Célula 1 (Markdown):
```markdown
# Silver to Gold — CineData Analytics
Modelagem dimensional (Star Schema), tabela de contexto para o RAG do time de IA, e
respostas às 6 perguntas de negócio (Desafio de Analytics, seção 4 do enunciado).
```

Célula 2 (setup):
```python
import sys
sys.path.append("/Workspace" + __file__.rsplit("/code/notebooks", 1)[0] + "/code")

from src.gold.star_schema import (
    construir_dim_movies, construir_dim_genres, construir_dim_people,
    construir_dim_companies, construir_dim_reviews, construir_fact_movies_performance,
    construir_bridge_movie_genre, construir_bridge_movie_person, construir_bridge_movie_company,
)
from src.gold.genai_context import agregar_pessoas_por_filme, construir_genai_context

spark.sql("CREATE DATABASE IF NOT EXISTS gold")
```

Célula 3 (dimensões base):
```python
df_filmes = spark.table("silver.tb_info_filmes")
df_generos = spark.table("silver.tb_generos")
df_pessoas_empresas = spark.table("silver.tb_pessoas_empresas")
df_financeiro = spark.table("silver.tb_financeiro_filmes")
df_engajamento = spark.table("silver.tb_metricas_engajamento")
df_avaliacoes = spark.table("silver.tb_avaliacoes_usuarios")

dim_movies = construir_dim_movies(df_filmes)
dim_genres = construir_dim_genres(df_generos)
dim_people = construir_dim_people(df_pessoas_empresas)
dim_companies = construir_dim_companies(df_pessoas_empresas)

dim_movies.write.format("delta").mode("overwrite").saveAsTable("gold.dim_movies")
dim_genres.write.format("delta").mode("overwrite").saveAsTable("gold.dim_genres")
dim_people.write.format("delta").mode("overwrite").saveAsTable("gold.dim_people")
dim_companies.write.format("delta").mode("overwrite").saveAsTable("gold.dim_companies")
```

Célula 4 (fato + bridges + dim_reviews):
```python
fact = construir_fact_movies_performance(dim_movies, df_financeiro, df_engajamento)
bridge_genre = construir_bridge_movie_genre(dim_movies, dim_genres, df_generos)
bridge_person = construir_bridge_movie_person(dim_movies, dim_people, df_pessoas_empresas)
bridge_company = construir_bridge_movie_company(dim_movies, dim_companies, df_pessoas_empresas)
dim_reviews = construir_dim_reviews(dim_movies, df_avaliacoes)

fact.write.format("delta").mode("overwrite").saveAsTable("gold.fact_movies_performance")
bridge_genre.write.format("delta").mode("overwrite").saveAsTable("gold.bridge_movie_genre")
bridge_person.write.format("delta").mode("overwrite").saveAsTable("gold.bridge_movie_person")
bridge_company.write.format("delta").mode("overwrite").saveAsTable("gold.bridge_movie_company")
dim_reviews.write.format("delta").mode("overwrite").saveAsTable("gold.dim_reviews")

display(fact)
```

Célula 5 (gold_genai_movies_context):
```python
atores_por_filme = agregar_pessoas_por_filme(bridge_person, dim_people, "Ator")
diretores_por_filme = agregar_pessoas_por_filme(bridge_person, dim_people, "Diretor")

contexto_genai = construir_genai_context(dim_movies, fact, atores_por_filme, diretores_por_filme)
contexto_genai.write.format("delta").mode("overwrite").saveAsTable("gold.gold_genai_movies_context")

display(contexto_genai.limit(5))
```

Célula 6 (Markdown — perguntas de negócio):
```markdown
## Desafio de Analytics
```

Célula 7 (pergunta 1):
```python
# 1. Receita total (em R$) de todos os filmes da base
display(fact.agg(F.sum("receita_brl").alias("receita_total_brl")))
```

Célula 8 (pergunta 2):
```python
# 2. Os 5 filmes com maior popularidade (título + popularidade)
display(
    fact.join(dim_movies, "sk_movie_id")
    .select("titulo", "popularidade")
    .orderBy(F.col("popularidade").desc())
    .limit(5)
)
```

Célula 9 (pergunta 3):
```python
# 3. Quantidade de filmes por gênero, do maior para o menor volume
display(
    bridge_genre.join(dim_genres, "sk_genre_id")
    .groupBy("nome_genero")
    .agg(F.count("*").alias("qtd_filmes"))
    .orderBy(F.col("qtd_filmes").desc())
)
```

Célula 10 (pergunta 4):
```python
from pyspark.sql.window import Window

# 4. Os 10 filmes de maior receita: título, receita (US$ e R$) e posição no ranking
janela_receita = Window.orderBy(F.col("receita_usd").desc())
display(
    fact.join(dim_movies, "sk_movie_id")
    .select("titulo", "receita_usd", "receita_brl")
    .withColumn("posicao_ranking", F.rank().over(janela_receita))
    .orderBy("posicao_ranking")
    .limit(10)
)
```

Célula 11 (pergunta 5):
```python
# 5. Ator com mais participações nos filmes lançados nos últimos 2 anos
data_limite = dim_movies.agg(F.max("data_lancamento")).first()[0]
data_corte_2_anos = F.add_months(F.lit(data_limite), -24)

filmes_recentes = dim_movies.filter(
    (F.col("data_lancamento") <= F.lit(data_limite)) & (F.col("data_lancamento") >= data_corte_2_anos)
)
display(
    bridge_person.join(filmes_recentes, "sk_movie_id")
    .join(dim_people.filter(F.col("tipo_pessoa") == "Ator"), "sk_person_id")
    .groupBy("nome_pessoa")
    .agg(F.count("*").alias("qtd_participacoes"))
    .orderBy(F.col("qtd_participacoes").desc())
    .limit(1)
)
```

Célula 12 (pergunta 6):
```python
# 6. Produtora com maior lucro nos últimos 5 anos
data_corte_5_anos = F.add_months(F.lit(data_limite), -60)
filmes_ultimos_5_anos = dim_movies.filter(
    (F.col("data_lancamento") <= F.lit(data_limite)) & (F.col("data_lancamento") >= data_corte_5_anos)
)
display(
    bridge_company.join(filmes_ultimos_5_anos, "sk_movie_id")
    .join(fact, "sk_movie_id")
    .join(dim_companies, "sk_company_id")
    .groupBy("nome_produtora")
    .agg(F.sum("lucro_usd").alias("lucro_total_usd"))
    .orderBy(F.col("lucro_total_usd").desc())
    .limit(1)
)
```

- [ ] **Step 2: Commit**

```bash
git add code/notebooks/Silver_to_Gold.ipynb
git commit -m "feat(gold): adiciona notebook Silver_to_Gold com Star Schema e Analytics

Orquestra a construcao do Star Schema, a tabela gold_genai_movies_context,
e responde as 6 perguntas de negocio do Desafio de Analytics (secao 4 do
enunciado) usando display() sobre a camada Gold.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 17: Configuração do workspace Databricks (guiado)

**Files:** nenhum arquivo de código — configuração de infraestrutura na UI do Databricks.

Tarefa procedural, feita junto com o usuário (que nunca usou a plataforma). Cada passo
termina com uma verificação visual antes de avançar para o próximo.

- [ ] **Step 1: Criar um cluster.** No Databricks, ir em "Compute" → "Create compute" →
  cluster single-node, runtime LTS mais recente com suporte a Unity Catalog/Delta (padrão
  já vem assim). Verificação: status do cluster fica "Running" (bolinha verde).

- [ ] **Step 2: Criar um Volume e subir os 5 CSVs.** Em "Catalog" → escolher/criar um
  catalog e schema → "Create" → "Volume" (ex.: `workspace.default.inputs`) → usar o botão
  de upload para subir os 5 arquivos de `Inputs/`. Verificação: os 5 arquivos aparecem
  listados dentro do Volume, com o caminho completo (`/Volumes/workspace/default/inputs/...`)
  visível — esse é o valor que vai no widget `caminho_volume` do notebook `Landing_to_Bronze`.

- [ ] **Step 3: Conectar o repositório via Git Folder.** "Workspace" → "Git Folders" (ou
  ícone de pasta com o logo do Git) → "Add Repo" → colar
  `https://github.com/LFPerylo/atividade-dados-rocket` → "Create". Verificação: a árvore de
  pastas do repositório (incluindo `code/src/` e `code/notebooks/`) aparece no Workspace.

- [ ] **Step 4: Anexar o cluster e abrir o primeiro notebook.** Abrir
  `code/notebooks/Landing_to_Bronze.ipynb` dentro do Git Folder, selecionar o cluster
  criado no Step 1 no canto superior direito. Verificação: notebook abre sem erro e mostra
  "Attached" ao lado do nome do cluster.

Sem commit nesta task (não há mudança de código — é configuração de ambiente).

---

### Task 18: Execução dos 3 notebooks no Databricks

**Files:** nenhum arquivo de código.

- [ ] **Step 1: Rodar `Landing_to_Bronze.ipynb`.** "Run All". Ajustar o widget
  `caminho_volume` para o caminho real do Step 2 da Task 17 antes de rodar. Verificação:
  cada célula mostra a contagem de linhas gravadas; `spark.sql("SHOW TABLES IN bronze")`
  ao final lista as 6 tabelas Bronze.

- [ ] **Step 2: Rodar `Bronze_to_Silver.ipynb`.** "Run All". Verificação: cada `display()`
  mostra dados limpos (status traduzido, datas parseadas, sem símbolos de moeda);
  `spark.sql("SHOW TABLES IN silver")` lista as 7 tabelas Silver.

- [ ] **Step 3: Rodar `Silver_to_Gold.ipynb`.** "Run All". Verificação:
  `spark.sql("SHOW TABLES IN gold")` lista as 9 dimensões/fato/bridges +
  `gold_genai_movies_context`; as 6 perguntas de negócio mostram resultado não vazio.

- [ ] **Step 4: Conferência cruzada com os testes locais.** Se algum resultado no
  Databricks parecer errado (ex.: contagem zerada, coluna nula em excesso), voltar para o
  teste local correspondente (Tasks 6–15), adicionar um caso de teste que reproduza o
  problema visto nos dados reais, corrigir o `src/`, rodar `pytest` local, e só então dar
  "Pull" no Git Folder e rodar de novo no Databricks. Isso fecha o ciclo de feedback rápido
  que motivou a Abordagem B do design.

Sem commit fixo aqui — cada correção feita neste ciclo gera seu próprio commit (mensagem
descrevendo o bug real encontrado nos dados), seguindo a mesma convenção das tasks
anteriores.

---

### Task 19: Orquestração — Databricks Workflow (Job)

**Files:**
- Create: `job.yaml` (exportado do Databricks, não escrito à mão)

- [ ] **Step 1: Criar o Job.** "Workflows" → "Jobs" → "Create Job". Nomear como
  `cinedata_pipeline`.

- [ ] **Step 2: Criar a task `to_Bronze`.** Type: Notebook. Path:
  `code/notebooks/Landing_to_Bronze.ipynb` (dentro do Git Folder). Cluster: o mesmo
  criado na Task 17 (ou um Job Cluster novo). Sem dependências.

- [ ] **Step 3: Criar a task `to_Silver`.** Type: Notebook, path
  `code/notebooks/Bronze_to_Silver.ipynb`. Em "Depends on", selecionar `to_Bronze`.

- [ ] **Step 4: Criar a task `to_Gold`.** Type: Notebook, path
  `code/notebooks/Silver_to_Gold.ipynb`. Em "Depends on", selecionar `to_Silver`.
  Verificação: o diagrama de tarefas mostra `to_Bronze → to_Silver → to_Gold` em sequência
  linear.

- [ ] **Step 5: Configurar o agendamento.** "Schedule" → "Add trigger" → tipo "Scheduled",
  definir uma periodicidade (ex.: diária, às 06:00), simulando uma rotina real de
  atualização de dados em produção.

- [ ] **Step 6: Rodar o Job manualmente ("Run now") e confirmar sucesso.** Verificação:
  as 3 tasks ficam verdes na ordem correta, sem erro.

- [ ] **Step 7: Exportar o YAML do Job.** No Job criado, menu "⋮" (kebab) → "Export" →
  "Export as YAML" (ou "View JSON"/"Job as code", dependendo da versão da UI — procurar a
  opção de exportação). Salvar o conteúdo como `job.yaml` na raiz do repositório.

- [ ] **Step 8: Capturar o print da execução de sucesso.** Na aba "Runs" do Job, abrir a
  execução bem-sucedida (que mostra o diagrama com as 3 tasks e as setas de dependência em
  verde) e salvar a captura de tela como `docs/evidencias/job_execucao_sucesso.png`.

- [ ] **Step 9: Commit**

```bash
git add job.yaml docs/evidencias/job_execucao_sucesso.png
git commit -m "feat(devops): adiciona orquestracao via Databricks Workflow

Job cinedata_pipeline com 3 tasks (to_Bronze -> to_Silver -> to_Gold) com
dependencia explicita entre elas e agendamento diario, simulando uma
rotina real de atualizacao de dados em producao, conforme a secao 3 do
enunciado.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 20: Revisão final e checklist de entrega

**Files:** nenhum arquivo novo obrigatório — apenas ajustes finais identificados na
revisão.

- [ ] **Step 1: Rodar a suíte completa localmente.**

Run: `pytest code/tests/unit -v` e `ruff check code/src code/tests`
Expected: todos os testes passam, lint sem erro.

- [ ] **Step 2: Confirmar que o CI do GitHub Actions está verde** no último push (aba
  "Actions" do repositório).

- [ ] **Step 3: Invocar a skill `code-review`** sobre o diff completo do branch (ou
  `git diff main` se tudo já estiver commitado direto em `main`), pedindo revisão de
  correção, simplificação e aderência às regras de negócio do `docs/stream.pdf`. Aplicar
  as correções que fizerem sentido, cada uma com seu próprio commit em português.

- [ ] **Step 4: Conferir o checklist de formato de entrega (seção 5 do enunciado):**
  - [ ] `code/notebooks/Landing_to_Bronze.ipynb`, `Bronze_to_Silver.ipynb`,
    `Silver_to_Gold.ipynb` presentes e com comentários explicando as regras de negócio
    aplicadas (revisar se os comentários já escritos nas células são suficientes, ou
    complementar).
  - [ ] `job.yaml` na raiz do repositório.
  - [ ] Print da execução de sucesso do Job em `docs/evidencias/`.
  - [ ] Repositório público no GitHub (`https://github.com/LFPerylo/atividade-dados-rocket`)
    — confirmar visibilidade em "Settings" → "General" → "Danger Zone".

- [ ] **Step 5: Atualizar o `README.md`** com um resumo final do projeto, badge de CI,
  como rodar os testes, e um link para o spec/plano em `docs/superpowers/`.

- [ ] **Step 6: Push final**

```bash
git push origin main
```

Sem necessidade de commit adicional se o Step 3 já gerou os commits de correção — este
step é só o push final para garantir que tudo está sincronizado com o GitHub antes do
prazo (segunda-feira, 21/09/2026, às 18:00).
