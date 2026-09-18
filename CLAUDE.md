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
- **Pré-requisito local:** Python 3.12 + JDK 17 (PySpark 3.5.x não roda com Python 3.14
  nem com JDK muito recente — ver README para o setup do `.venv`).

## Convenções
- Nomes de função, coluna e variável em português (domínio do negócio é em português,
  conforme `docs/stream.pdf`).
- Cada função de `src/` é testada isoladamente antes de ser usada em um notebook (TDD).
- Um commit por fase concluída do plano, mensagem em português, sem linha de coautoria de IA.
- Spec: `docs/superpowers/specs/2026-09-18-cinedata-pipeline-design.md`
- Plano: `docs/superpowers/plans/2026-09-18-cinedata-pipeline-plan.md`

## Fonte de verdade das regras de negócio
`docs/stream.pdf` — mapeamentos de coluna, regras de limpeza e templates estão todos lá.
Ao implementar qualquer transformação, confirme contra o PDF antes de assumir uma regra.
