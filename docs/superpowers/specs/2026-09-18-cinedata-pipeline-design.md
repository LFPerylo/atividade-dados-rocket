# CineData Analytics — Pipeline de Dados End-to-End (Design)

Data: 2026-09-18
Autor: Sessão colaborativa (usuário + Claude)
Prazo de entrega: segunda-feira, 21/09/2026, às 18:00

## 1. Contexto e objetivo

Atividade da disciplina de Engenharia de Dados (RocketLab 2026.2 / Visagio). O objetivo é
construir um pipeline de dados completo no Databricks (PySpark/SQL/Delta Lake) para o
catálogo de filmes CineData Analytics, seguindo a Arquitetura Medalhão (Bronze, Silver,
Gold), incluindo:

- Ingestão de 5 CSVs "sujos" + 1 API externa (cotação do dólar, Banco Central) na camada
  Bronze.
- Limpeza, padronização, deduplicação e enriquecimento na camada Silver (7 tabelas).
- Modelagem dimensional (Star Schema: 1 fato, 5 dimensões, 3 bridges) e uma tabela de
  contexto para RAG (`gold_genai_movies_context`) na camada Gold.
- Orquestração via Databricks Workflow (Job) com 3 tasks e dependências + agendamento.
- Respostas a 6 perguntas de negócio via SQL/PySpark sobre a camada Gold.

O documento fonte completo do enunciado está em `docs/stream.pdf`. Este spec não repete
todas as regras de negócio detalhadas ali (mapeamentos de coluna, templates de tradução,
etc.) — elas são a fonte de verdade e serão referenciadas durante a implementação de cada
módulo.

Requisito adicional do usuário: além da entrega funcional, o projeto deve demonstrar boas
práticas de Engenharia de Software aplicadas a dados (clean code, Clean Architecture,
testes automatizados, DevOps/CI) e servir como material de aprendizado — cada decisão deve
ser explicada durante a implementação, feita em etapas pequenas e testadas, com um commit
em português ao final de cada fase concluída.

## 2. Decisão arquitetural

**Abordagem escolhida: módulos Python testáveis + notebooks finos de orquestração**
(avaliada contra "tudo dentro do notebook" e "framework DLT/dbt" — descartadas por,
respectivamente, não serem testáveis/revisáveis e fugirem do escopo pedido no prazo
disponível).

Princípio: a lógica de transformação (regras de negócio de limpeza, tradução, cálculo,
deduplicação) vive em funções Python puras em `code/src/`, que recebem e devolvem
DataFrames Spark, sem dependência de `dbutils`, caminhos fixos ou I/O. Os notebooks
(`Landing_to_Bronze.ipynb`, `Bronze_to_Silver.ipynb`, `Silver_to_Gold.ipynb`) são finos:
leem dados, chamam as funções de `src/`, escrevem Delta, e exibem `display()` para as
perguntas de negócio.

Isso permite testar toda a regra de negócio localmente com `pytest` + Spark local, sem
depender de um cluster Databricks ligado a cada iteração — crítico dado o prazo de 3 dias
e o fato de o usuário ainda estar aprendendo a navegar na plataforma Databricks.

Integração com Databricks: via **Git Folders** (Repos), sincronizando este repositório
GitHub diretamente no workspace. Os notebooks importam `src/` como pacote Python normal
(`from src.silver.filmes import limpar_status`), evitando `%run` ou código colado
manualmente.

## 3. Estrutura de pastas

```
atividade-dados-rocket/
├── .github/workflows/ci.yml
├── code/
│   ├── src/
│   │   ├── bronze/ingest.py
│   │   ├── silver/
│   │   │   ├── filmes.py
│   │   │   ├── financeiro.py
│   │   │   ├── engajamento.py
│   │   │   ├── avaliacoes.py
│   │   │   ├── generos.py
│   │   │   ├── pessoas_empresas.py
│   │   │   └── cotacao_dolar.py
│   │   ├── gold/
│   │   │   ├── star_schema.py
│   │   │   └── genai_context.py
│   │   └── common/
│   │       ├── api_client.py
│   │       └── spark_session.py
│   ├── notebooks/
│   │   ├── Landing_to_Bronze.ipynb
│   │   ├── Bronze_to_Silver.ipynb
│   │   └── Silver_to_Gold.ipynb
│   └── tests/
│       ├── unit/
│       └── conftest.py
├── Inputs/                (os 5 CSVs fornecidos pelo usuário)
├── job.yaml                (exportado do Databricks Workflow)
├── docs/
│   ├── stream.pdf
│   └── superpowers/specs/
├── requirements.txt
├── .pre-commit-config.yaml
├── CLAUDE.md
└── README.md
```

Regra de dependência: `src/bronze|silver|gold` nunca importa nada de `notebooks/` nem
conhece Databricks; a direção de conhecimento é sempre notebook → src, nunca o contrário.

## 4. Ambiente local e ferramentas

- Python `venv` + `pip`, dependências em `requirements.txt`.
- `pyspark` + `delta-spark` rodando em modo local (`local[*]`) para desenvolvimento e
  testes, sem depender do Databricks.
- `ruff` para lint + formatação.
- `pre-commit` rodando `ruff` antes de cada commit.
- `pytest` para os testes unitários das funções de `src/`.

## 5. Estratégia de testes (TDD)

Fluxo por função: escrever teste com DataFrame de entrada reproduzindo o problema real do
enunciado → rodar (falha) → implementar o mínimo → rodar (passa) → confirmar que testes
anteriores continuam passando.

Casos mínimos de teste por módulo (não exaustivo — a fonte de verdade é `docs/stream.pdf`,
seção 1.3 e Parte 2):

- **`bronze/ingest.py`**: leitura de CSV sem alteração estrutural; coluna
  `ingestion_datetime` preenchida com timestamp da execução; modo append.
- **`common/api_client.py`**: parsing da resposta da API PTAX do Banco Central; formatação
  de datas `MM-DD-AAAA`.
- **`silver/filmes.py`**: normalização de status com ruído/hífen/caixa mista → tradução
  correta para português; status não mapeável → `"Não Informado"`; deduplicação mantendo
  o registro mais recente por `ingestion_datetime`; parsing de datas multi-formato,
  inválido → NULL; coluna derivada `ano_lancamento`.
- **`silver/financeiro.py`**: textos de ausência (`"Unknown"`, etc.) → NULL antes da
  conversão; remoção de símbolos de moeda/milhar; valores zerados/negativos → NULL;
  conversão para BRL usando a cotação; lucro e margem sem dividir por zero nem propagar
  NULL indevidamente.
- **`silver/engajamento.py`**: popularidade com separador decimal inconsistente → limpeza
  antes da conversão; column shift (texto em coluna numérica) → NULL sem interromper o
  pipeline; notas fora de 0–10 (incluindo erro de escala) → NULL; contagens/popularidade
  negativas → NULL.
- **`silver/avaliacoes.py`**: deduplicação exata (filme+usuário+nota+comentário);
  nota fora de 0–10 → NULL; comentário vazio/whitespace → `"Sem comentário"`.
- **`silver/generos.py`**: split tolerante a `,` e `;`; explode; remoção de resíduos em
  branco, texto descritivo e valores numéricos deslocados.
- **`silver/pessoas_empresas.py`**: unificação de `cast/directors/writers/production_companies`
  com `tipo_entidade` correto; padronização de capitalização; deduplicação.
- **`silver/cotacao_dolar.py`**: forward-fill garantindo série temporal contínua
  (sem gaps em fins de semana/feriados).
- **`gold/star_schema.py`**: geração de surrogate keys; fato sem duplicar grão (um
  registro por filme) mesmo após os joins com as dimensões periféricas via bridge.
- **`gold/genai_context.py`**: concatenação segura — overview, diretor ou receita nulos
  não podem fazer a linha inteira desaparecer (uso de `coalesce`/fallback textual antes da
  concatenação); agregação de múltiplos atores em uma única string.

## 6. CI (GitHub Actions)

`.github/workflows/ci.yml`, disparado em `push` e `pull_request`:
1. Setup Python + instala `requirements.txt`.
2. `ruff check .`
3. `pytest code/tests/unit`

Build quebra se lint ou testes falharem. Badge de status no `README.md`.

## 7. Integração com Databricks

- Workspace → Git Folders → conectar a `https://github.com/LFPerylo/atividade-dados-rocket`.
- Cluster: single-node, runtime com Delta Lake nativo (padrão do Databricks).
- Upload dos 5 CSVs como Volume (Unity Catalog) na pasta `Inputs`.
- Fluxo de trabalho contínuo: editar/testar localmente → commit/push → "Pull" no Git
  Folder do Databricks → executar notebook atualizado.
- Usuário nunca usou a plataforma Databricks — cada etapa de UI (criação de cluster,
  volume, Git Folder, Job/Workflow, agendamento, exportação de YAML) será guiada
  passo a passo durante a Fase 5/6 de execução.

## 8. Plano faseado

Cada fase: implementação pequena → testes passando localmente → explicação do "porquê" →
checkpoint de aprovação do usuário → **commit em português** ao final da fase (mensagem
descrevendo o que foi entregue, seguindo a convenção de atribuição do ambiente).

0. Scaffold do projeto (estrutura de pastas, `requirements.txt`, `pre-commit`, `ci.yml`,
   `CLAUDE.md`, `.gitignore`, `README.md`).
1. Bronze: `src/bronze/ingest.py` + `src/common/api_client.py` com testes →
   `Landing_to_Bronze.ipynb`.
2. Silver: um módulo por vez (filmes → financeiro → engajamento → avaliações → gêneros →
   pessoas/empresas → cotação dólar), teste antes do código, cada um revisado com o
   usuário → `Bronze_to_Silver.ipynb`.
3. Gold — Star Schema: dimensões, fato, bridges, surrogate keys, com testes.
4. Gold — GenAI context: concatenação segura com testes de NULL → completa
   `Silver_to_Gold.ipynb`.
5. Execução real no Databricks (ensino guiado): Volume, Git Folder, cluster, rodar os 3
   notebooks.
6. Orquestração: Databricks Workflow com 3 tasks e dependências + agendamento; exportar
   `job.yaml`; capturar print da execução.
7. Analytics: as 6 perguntas de negócio.
8. Revisão final com a skill `code-review`, conferência contra o checklist de entrega do
   enunciado (seção 5 do PDF), push final.

## 9. Riscos e pendências abertas

- **CSVs de entrada**: o usuário fornecerá os 5 arquivos antes do início da Fase 0/1 —
  bloqueante para testes com dados reais (mas não bloqueia o scaffold nem os testes com
  DataFrames sintéticos que reproduzem os problemas descritos no enunciado).
- **Prazo apertado (3 dias)**: o plano faseado prioriza o caminho crítico (Bronze → Silver
  → Gold → orquestração → analytics); ajustes de escopo, se necessários, serão sinalizados
  ao usuário assim que identificados, não decididos unilateralmente.
- **Aprendizado da plataforma Databricks**: as fases 5 e 6 dependem de o usuário estar
  disponível para navegar na UI junto comigo (guiado por texto/print), já que não tenho
  acesso direto ao workspace Databricks do usuário.

## 10. Convenção de commits

Um commit por fase concluída, mensagem em português, descrevendo o que foi entregue e por
quê (não apenas "o quê"). Sem linha de coautoria/atribuição a ferramentas de IA — a pedido
explícito do usuário, o que sobrepõe a convenção padrão do ambiente.
