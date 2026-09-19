# CineData Analytics — Pipeline de Dados

Pipeline de dados end-to-end (Databricks | PySpark | SQL) para o catálogo de filmes da
CineData Analytics, seguindo a Arquitetura Medalhão (Bronze, Silver, Gold). Projeto da
disciplina de Engenharia de Dados — RocketLab 2026.2.

## Arquitetura

A lógica de negócio (limpeza, tradução, deduplicação, cálculo de métricas) vive em
funções Python puras em `code/src/`, organizadas por camada (`bronze/`, `silver/`,
`gold/`, `common/`) — sem I/O, sem `dbutils`, testáveis isoladamente. Os notebooks em
`code/notebooks/` são finos: apenas orquestram leitura, chamada às funções de `src/` e
escrita em Delta.

```
code/
├── src/        # lógica de negócio testável
├── notebooks/  # Landing_to_Bronze, Bronze_to_Silver, Silver_to_Gold
└── tests/unit/ # testes pytest (Spark local, sem depender do Databricks)
```

## Rodando os testes localmente

Pré-requisitos: **Python 3.12** e **JDK 17** (o PySpark 3.5.x não é compatível com
versões muito novas de Python/Java — em macOS com Homebrew: `brew install python@3.12
openjdk@17`).

```bash
python3.12 -m venv .venv
source .venv/bin/activate

# aponta o venv para o JDK 17 (só precisa rodar uma vez após criar o venv)
cat >> .venv/bin/activate <<'EOF'
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
EOF
source .venv/bin/activate

pip install -r requirements.txt
pytest code/tests/unit -v
```

## Documentação

- Enunciado da atividade: [docs/stream.pdf](docs/stream.pdf)
- Spec de design: [docs/superpowers/specs/2026-09-18-cinedata-pipeline-design.md](docs/superpowers/specs/2026-09-18-cinedata-pipeline-design.md)
- Plano de implementação: [docs/superpowers/plans/2026-09-18-cinedata-pipeline-plan.md](docs/superpowers/plans/2026-09-18-cinedata-pipeline-plan.md)

## Qualidade dos dados de origem e decisões tomadas

Os 5 CSVs são intencionalmente sujos. Estes são os problemas medidos nos dados reais e a
decisão tomada em cada um (regras completas no enunciado, `docs/stream.pdf`):

| Problema encontrado | Decisão |
|---|---|
| Aspas quebradas e texto de outras bases dentro das colunas numéricas (`movies_metrics` tem linhas com até 24 campos) | Bronze lê uma linha física por registro, sem `multiLine` (com ele o Spark perdia ~4 mil linhas). A Silver converte só o que é número válido; o resto vira `NULL`. |
| `popularity` com vírgula decimal (`89,985`) e texto vazado | Só troca vírgula por ponto; nunca apaga caracteres (apagar juntava dígitos de texto e criava números falsos). |
| `budget` em formatos `97000000`, `$ 97000000`, `USD 150000000`, `34.0M`, `250.5K`; `revenue` com `Unknown`/`Não Informado`/negativos | Parse estrito de prefixo de moeda e sufixo K/M/B; ausência, zero e negativo viram `NULL`. |
| ~86% dos orçamentos e ~88% das receitas são `0` na origem | Tratados como ausentes, conforme o enunciado; por isso há muitos `NULL` na fato. |
| Mesmo filme repetido até 34 vezes (info, financials e metrics), às vezes com valores conflitantes | Mantém o registro mais recente; no empate, o mais completo; por fim um hash do conteúdo (resultado determinístico). Garante um registro por filme na fato. |
| Datas em `yyyy-MM-dd`, `MM-dd-yyyy` e `dd/MM/yyyy`; filme "Lançado" com data em 2029 | Cada formato só é tentado quando o texto casa com o padrão. As perguntas de 2 e 5 anos usam como limite o último lançamento já ocorrido. |

### Limitação conhecida

Quatro linhas de `movies_metrics` estão deslocadas por inteiro (o ano de lançamento caiu na
coluna `popularity` e há texto em `averageRating`/`numVotes`). Seguindo o enunciado, só as
células com texto viram `NULL`; a popularidade (`2018`, `2019`, `2020`) é numérica e permanece,
por isso esses filmes aparecem no top 5 de popularidade (pergunta 2). Anular a linha inteira
seria uma regra além do que o enunciado pede, com risco de descartar dados válidos.
