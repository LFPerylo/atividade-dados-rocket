# CineData Analytics — Pipeline de Dados

Projeto da disciplina de Engenharia de Dados (RocketLab 2026.2). Pipeline Bronze/Silver/Gold
no Databricks para um catálogo de filmes (TMDB/IMDb).

## Arquitetura
- `code/src/`: lógica de negócio em funções Python puras (recebem/devolvem DataFrame Spark,
  sem I/O, sem dbutils). Organizadas por camada: `bronze/`, `silver/`, `gold/`, `common/`.
  `gold/validacao.py` é o quality gate rodado pelo notebook `Validate_Gold` (task do Job).
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

## Armadilhas já encontradas (não repetir)
- Databricks Serverless roda com **ANSI ligado**: cast direto e `to_date` com formato errado lançam
  erro. Use regex + `try_cast`/`try_to_timestamp`; a SparkSession de teste também liga ANSI.
- Nunca limpar número apagando caracteres: junta dígitos de texto vazado. Fazer parse estrito.
- Não ler os CSVs com `multiLine=True` (aspas quebradas fazem o Spark engolir linhas).
- Os CSVs repetem o mesmo `id` várias vezes: deduplicar com
  `common/deduplicacao.manter_registro_mais_recente_e_completo` antes da fato.
- Após `Pull` no Git folder, reiniciar o Python do notebook (módulo antigo fica em memória).
