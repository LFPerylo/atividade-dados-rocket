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
