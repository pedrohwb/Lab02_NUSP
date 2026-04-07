# Lab02 — Transformação de Dados com DBT

> **Identificação:** Lab02_NUSP  
> **Disciplina:** Laboratório de Engenharia de Dados
> **Aluno:** Pedro Henrique Wege Barbosa   
> **Base de referência:** [Lab01_PART2_NUSP](https://github.com/pedrohwb/Lab01_PART2_NUSP)  
> **Dataset:** [Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii)

---

## Visão Geral

Este laboratório implementa a camada de transformação **Silver → Gold** usando **DBT (Data Build Tool)**, integrando um pipeline completo de engenharia de dados:

```
Excel (raw)
    │
    ▼  Python (01_ingest_raw.py)
data/raw/
    │
    ▼  Python (02_silver_processing.py)
data/silver/*.parquet
    │
    ▼  Python (03_load_silver_pg.py)
PostgreSQL: schema silver
    │
    ▼  DBT (dbt run)
PostgreSQL: schema gold
    │
    ▼  Streamlit Dashboard
http://localhost:8501
```

## Estrutura do Repositório

```
Lab02_NUSP/
├── .env.example                  # Template de variáveis de ambiente
├── .gitignore
├── Dockerfile                    # Imagem Python (pipeline raw→silver)
├── Dockerfile.dbt                # Imagem DBT (silver→gold)
├── Dockerfile.dashboard          # Imagem Streamlit (dashboard)
├── docker-compose.yml            # Orquestração completa
├── requirements.txt              # Dependências Python (sem GX)
├── README.md
│
├── src/                          # Pipeline Python
│   ├── config.py                 # Configurações de caminhos e banco
│   ├── 01_ingest_raw.py          # Valida e registra metadados do Excel
│   ├── 02_silver_processing.py   # Limpeza → parquet
│   └── 03_load_silver_pg.py      # Carrega parquet → PostgreSQL silver schema
│
├── data/
│   ├── raw/                      # Excel original (não versionado)
│   └── silver/                   # Parquet + relatórios (não versionados)
│
├── docs/
│   ├── plots/                    # Gráficos gerados pelo pipeline
│   └── screenshots/              # Prints para documentação
│
└── dbt_retail/                   # Projeto DBT
    ├── dbt_project.yml
    ├── profiles.yml
    ├── macros/
    │   ├── generate_schema_name.sql   # Macro técnica: schemas limpos
    │   └── get_period_label.sql       # Macro de negócio: YYYY-MM
    ├── models/
    │   ├── staging/
    │   │   ├── sources.yml            # Source apontando para silver schema
    │   │   ├── stg_sales.sql          # View de staging
    │   │   └── stg_sales.yml          # Testes genéricos na staging
    │   └── marts/
    │       ├── dim_product.sql
    │       ├── dim_customer.sql
    │       ├── dim_country.sql
    │       ├── dim_date.sql
    │       ├── fct_sales.sql
    │       └── marts.yml              # Testes genéricos (not_null, unique, FK)
    └── tests/
        ├── assert_no_negative_revenue.sql      # Teste singular
        └── assert_canceled_invoice_flag.sql    # Teste singular
```

---

## Pré-requisitos

- **Docker Desktop** (recomendado) **ou** Python 3.11 + PostgreSQL local
- Arquivo `online_retail_ii.xlsx` colocado em `data/raw/`
  - Download: [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii)

---

## Execução com Docker (recomendado)

### 1. Clone e configure

```bash
git clone https://github.com/pedrohwb/Lab02_NUSP.git
cd Lab02_NUSP

cp .env.example .env
# edite .env se necessário (valores padrão funcionam)
```

### 2. Coloque o dataset

```
data/raw/online_retail_ii.xlsx
```

### 3. Suba o ambiente

```bash
# Pipeline completo: postgres + ingestion + dbt + dashboard
docker compose up --build

# Apenas o banco e o dashboard (se já rodou o pipeline antes):
docker compose up postgres dashboard
```

### 4. Acesse o Dashboard

```
http://localhost:8501
```

### Ordem de execução dos serviços

```
postgres (healthcheck)
    └── ingestion  → raw → silver → carrega silver schema
            └── dbt  → silver → gold (via DBT models)
                └── dashboard  → lê do gold schema
```

### Pipeline executado com sucesso

![Docker Compose Up](docs/screenshots/docker_compose_up.png)

---

## Execução Local (sem Docker)

### 1. Instale as dependências Python

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure o banco e variáveis de ambiente

```bash
cp .env.example .env
# ajuste POSTGRES_HOST, POSTGRES_PORT, etc.
```

### 3. Execute o pipeline Python

```bash
export PYTHONPATH=src

python src/01_ingest_raw.py
python src/02_silver_processing.py
python src/03_load_silver_pg.py
```

### 4. Instale e execute o DBT

```bash
pip install dbt-postgres==1.9.4

cd dbt_retail
export DBT_PROFILES_DIR=$(pwd)

dbt deps
dbt run
dbt test
```

### 5. Rode o dashboard

```bash
cd ..
pip install streamlit plotly
streamlit run dashboard/app.py
```

---

## DBT em Detalhe

### Configuração de Schemas

O projeto usa o macro `generate_schema_name` para que os schemas sejam exatamente os configurados:

- `staging/stg_sales` → view em **`staging`**
- `marts/dim_*` e `fct_sales` → tabelas em **`gold`**

### Macro de Negócio: `get_period_label`

```sql
{{ get_period_label('invoice_year', 'invoice_month') }}
-- → invoice_year::text || '-' || LPAD(invoice_month::text, 2, '0')
-- → Ex: '2011-03'
```

Utilizado em `stg_sales.sql` e replicado em `fct_sales.sql` para criar a coluna `period`.

### Models DBT

| Model | Tipo | Schema | Descrição |
|---|---|---|---|
| `stg_sales` | view | `staging` | Staging da camada silver |
| `dim_product` | table | `gold` | Dimensão de produtos |
| `dim_customer` | table | `gold` | Dimensão de clientes |
| `dim_country` | table | `gold` | Dimensão de países |
| `dim_date` | table | `gold` | Dimensão de datas |
| `fct_sales` | table | `gold` | Fato de vendas (star schema) |

### Lineage DBT

```
silver.online_retail_ii (source)
        │
        ▼
    stg_sales (view)
        │
    ┌───┼───────────────────────┐
    ▼   ▼       ▼       ▼       ▼
dim_product  dim_customer  dim_country  dim_date
    └───┴───────────────────────┘
                    │
                    ▼
                fct_sales (tabela Gold)
```

#### Lineage gerado pelo DBT Docs

![Lineage DBT](docs/screenshots/dbt_lineage_graph.png)

### Testes DBT

#### Testes Genéricos (YAML)

Definidos em `stg_sales.yml` e `marts.yml`:

| Tipo | Modelos | Colunas |
|---|---|---|
| `not_null` | todos | todas as colunas chave |
| `unique` | `dim_*`, `stg_sales` | colunas PK |
| `relationships` | `fct_sales` | `product_id`, `country_id`, `date_id` |

#### Testes Singulares (SQL)

| Arquivo | Regra de negócio |
|---|---|
| `assert_no_negative_revenue.sql` | Vendas não canceladas devem ter `gross_revenue > 0` |
| `assert_canceled_invoice_flag.sql` | Notas com prefixo 'C' devem ter `is_canceled = TRUE` |

#### Executar apenas os testes

```bash
cd dbt_retail
dbt test
# ou apenas um modelo:
dbt test --select fct_sales
```

---

## Documentação Automática do DBT

```bash
cd dbt_retail
dbt docs generate
dbt docs serve --port 8080
# Acesse: http://localhost:8080
```

A documentação inclui:
- Descrições de todos os modelos, colunas e sources
- **Grafo de lineage interativo** (DAG)
- Resultados dos testes
- Catálogo completo de metadados

### Tela inicial da documentação

![DBT Docs — Visão Geral](docs/screenshots/dbt_docs_overview.png)

### Resultados dos testes

![DBT Test Results](docs/screenshots/dbt_test_results.png)

---

## Dashboard BI (Streamlit)

O dashboard lê diretamente do schema `gold` no PostgreSQL e apresenta **6 visualizações**:

| # | Visualização | Tipo |
|---|---|---|
| 1 | Receita Bruta Mensal | Linha |
| 2 | Top 10 Países por Receita | Barras |
| 3 | Top 10 Produtos por Quantidade | Barras horizontais |
| 4 | Cancelamentos vs Concluídos | Pizza (Donut) |
| 5 | Pedidos Únicos por Mês | Área |
| 6 | Top 10 Clientes por Receita | Barras |

**Acesso:** `http://localhost:8501`

### Dashboard em funcionamento

![Dashboard Streamlit1](docs/screenshots/dashboard_overview_2.png)
![Dashboard Streamlit2](docs/screenshots/dashboard_overview_3.png)
![Dashboard Streamlit3](docs/screenshots/dashboard_overview_6.png)
![Dashboard Streamlit4](docs/screenshots/dashboard_overview.png)
![Dashboard Streamlit5](docs/screenshots/dashboard_overview_4.png)
![Dashboard Streamlit6](docs/screenshots/dashboard_overview_5.png)

### Ferramentas BI Alternativas

Além do Streamlit, o schema `gold` pode ser conectado a:

#### Apache Superset

```bash
docker run -d -p 8088:8088 \
  -e "SUPERSET_SECRET_KEY=lab02secret" \
  apache/superset:latest

# Configure a conexão:
# SQLAlchemy URI: postgresql+psycopg2://postgres:postgres@host.docker.internal:5433/lab02_retail
# Schema: gold
```

#### Metabase

```bash
docker run -d -p 3000:3000 metabase/metabase

# Acesse http://localhost:3000
# Add database → PostgreSQL
# Host: host.docker.internal | Port: 5433
# Database: lab02_retail | Schema: gold
```

#### Grafana

```bash
docker run -d -p 3001:3000 grafana/grafana

# Acesse http://localhost:3001 (admin/admin)
# Add data source → PostgreSQL
# Host: host.docker.internal:5433
# Database: lab02_retail | Schema: gold
```

---

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | Host do PostgreSQL |
| `POSTGRES_PORT` | `5432` | Porta interna (5433 local) |
| `POSTGRES_DB` | `lab02_retail` | Nome do banco |
| `POSTGRES_USER` | `postgres` | Usuário |
| `POSTGRES_PASSWORD` | `postgres` | Senha |

---

## Comandos DBT de Referência

```bash
cd dbt_retail
export DBT_PROFILES_DIR=$(pwd)

# Instalar dependências
dbt deps

# Executar todos os models
dbt run

# Executar apenas staging
dbt run --select staging

# Executar apenas marts
dbt run --select marts

# Rodar todos os testes
dbt test

# Rodar testes de um model específico
dbt test --select fct_sales

# Gerar e servir documentação
dbt docs generate
dbt docs serve --port 8080

# Limpar artefatos
dbt clean
```

---

## Sobre o Dataset

**Online Retail II** — Transações de uma loja de varejo online do Reino Unido (2009–2011).

- Fonte: [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
- ~1 milhão de linhas após processamento
- Colunas originais: Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country
