# 🎧 ETL Spotify – Pipeline de Engenharia de Dados (Raw → Silver → Gold)

## 📌 Visão Geral

Este projeto implementa um pipeline completo de engenharia de dados utilizando a API do Spotify, seguindo o padrão moderno de camadas Raw, Silver e Gold, com persistência em PostgreSQL e foco em boas práticas de modelagem, versionamento e escalabilidade.
O objetivo do projeto é demonstrar, na prática, competências essenciais de um Engenheiro de Dados Júnior, indo além da simples extração de dados e abordando:

- Arquitetura de dados em camadas;
- Padronização e qualidade de dados;
- Modelagem dimensional (Star Schema);
- Integração com APIs REST;
- Persistência em Data Warehouse relacional;
- Organização de projeto e pipelines reexecutáveis.

---
## 🏗️ Arquitetura do Pipeline

```
Spotify API
     │
     ▼
Extract
     │
     ▼
RAW (JSON)
     │
     ▼
Transform
     │
     ▼
SILVER (CSV estruturado)
     │
     ▼
Transform
     │
     ▼
GOLD (Dimensões + Fato)
     │
     ▼
Load
     │
     ▼
PostgreSQL (schemas silver e gold)
```
## 🧠 Tecnologias Utilizadas

- Python 3
- Spotify Web API
- Pandas
- PostgreSQL
- psycopg2
- dotenv
- Arquitetura em camadas (Raw / Silver / Gold)
- Modelagem Dimensional (Star Schema)

## 🧱 Estrutura do Projeto

```
etl_spotify/
│
├── data/
│   ├── raw/            # Dados brutos (JSON)
│   ├── silver/         # Dados tratados e normalizados
│   └── gold/           # Dados analíticos (dimensões e fatos)
│
├── src/
│   ├── extract/
│   │   └── spotify/
│   │       └── user_recently_played.py
│   │
│   ├── load/
│   │   ├── raw/
│   │   │   └── raw_loader.py
│   │   └── db/
│   │       ├── load_silver_to_db.py
│   │       └── load_gold_to_db.py
│   │
│   ├── transform/
│   │   ├── silver/
│   │   │   └── silver_recently_played.py
│   │   └── gold/
│   │       └── gold_recently_played.py
│   │
│   └── pipeline.py
│
├── create_tables.py
├── .env
├── requirements.txt
└── README.md

```

---

## 🔐 Etapa 1 - Extract (Spotify API)

- A autenticação utiliza o Authorization Code Flow, padrão adotado por APIs modernas.
- Consumo do endpoint:
```
  GET /v1/me/player/recently-played
```

- Coleta dos dados de músicas recentemente tocadas
- Resposta armazenada sem tratamento na camada Raw

### 📌 Por que Raw?
A camada Raw preserva os dados originais, permitindo:
- Reprocessamentos
- Auditoria
- Comparação de versões
- Correção de regras sem nova extração

## 🗃️ Etapa 2 — Raw Layer

- Armazenamento dos dados em JSON
- Organização por timestamp de carga
- Nenhuma regra de negócio aplicada

Exemplo:
```
data/raw/recently_played_2025-01-15T10-30-00.json
```

## 🥈 Etapa 3 — Silver Layer (Tratamento e Padronização)

Nesta etapa os dados passam por limpeza, normalização e padronização:

### Transformações aplicadas:

- Flatten de estruturas aninhadas
- Seleção de colunas relevantes
- Conversão de tipos (timestamp, boolean, int)
- Padronização de nomes
- Inclusão de load_date

📄 Output:
```
data/silver/recently_played.csv

```
### 📌 Objetivo da Silver
Criar uma base:
- Confiável
- Estruturada
- Pronta para consumo analítico

## 🥇 Etapa 4 — Gold Layer (Modelagem Dimensional)

A camada Gold foi construída seguindo modelagem dimensional (Star Schema).

### 📐 Modelagem
### Dimensões

- dim_artist
- dim_album
- dim_track

### Fato

- fact_recently_played

### Benefícios:

- Melhor performance analítica
- Clareza semântica
- Facilidade de integração com BI
 - Escalabilidade

### 📄 Outputs em CSV:
```
data/gold/dim_artist.csv
data/gold/dim_album.csv
data/gold/dim_track.csv
data/gold/fact_recently_played.csv
```
## 🛢️ Etapa 5 — Load no PostgreSQL
### Schemas criados:

- silver
- gold

### Estratégias utilizadas:

- CREATE SCHEMA IF NOT EXISTS
- CREATE TABLE IF NOT EXISTS

### Chaves primárias

- Foreign Keys
- ON CONFLICT DO NOTHING para evitar duplicidade

Exemplo:
```
INSERT INTO silver.recently_played (...)
ON CONFLICT (played_at, track_id) DO NOTHING;
```

## 🚀 Execução do Pipeline
```
python -m src.pipeline
```


O pipeline executa automaticamente:

- Extract + Raw
- Transform Silver
- Transform Gold
- Load Silver → PostgreSQL
- Load Gold → PostgreSQL

## 🧪 Boas Práticas Aplicadas

- Separação clara de responsabilidades
- Código modular e reutilizável
- Uso de variáveis de ambiente
- Versionamento pronto para produção
- Estrutura escalável para novos endpoints
- Pipeline reexecutável (idempotência)

## ⚙️ Configuração do Ambiente

### Arquivo `.env`

Na raiz do projeto:

```
SPOTIFY_CLIENT_ID=seu_client_id
SPOTIFY_CLIENT_SECRET=seu_client_secret
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8000/callback
SPOTIFY_SCOPE=user-top-read
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=spotify_dw
POSTGRES_USER=spotify
POSTGRES_PASSWORD=spotify
```

O `REDIRECT_URI` deve ser idêntico ao configurado no Spotify Developer Dashboard.

---

## 🧪 Ambiente Virtual

Criar o ambiente virtual:

```
python -m venv .venv
```

Ativar:

Linux / Mac:
```
source .venv/bin/activate
```

Windows:
```
.venv\Scripts\activate
```

---

## 📦 Instalação das Dependências

```
pip install flask requests python-dotenv
```

---

## ▶️ Execução da Autenticação

A partir da pasta `src/auth`:

```
python app.py
```

Acesse:

```
http://127.0.0.1:8000
```

Após a autenticação, o arquivo `token.json` será criado.

---

## 📥 Extração de Dados

Endpoint utilizado:

```
GET /v1/me/top/artists
```

Implementação:

```
src/extract/user_top_artists.py
```

O retorno da API é um JSON contendo, entre outros campos:

- Nome do artista  
- Popularidade  
- Número de seguidores  
- Gêneros musicais  

---

## 🎯 Competências Demonstradas

- ✔ Engenharia de Dados
- ✔ Arquitetura em camadas
- ✔ Consumo de APIs REST
- ✔ Modelagem Dimensional
- ✔ SQL e PostgreSQL
- ✔ Python para ETL
✔ Organização de projetos de dados
✔ Pensamento analítico e escalável

## 📈 Próximos Passos (Roadmap)

- Incrementar carga incremental (watermark)
- Adicionar testes de qualidade de dados
- Orquestração com Airflow
- Deploy em cloud (AWS / GCP / Azure)
- Camada de métricas para BI (Power BI)

## 👨‍💻 Sobre o Autor

### Jackson Nascimento - Engenheiro de Dados em formação | BI | Analytics


Projeto desenvolvido com foco em aprendizado real de engenharia de dados, indo além de tutoriais e demonstrando capacidade de estruturar pipelines próximos ao cenário profissional.

#### 🔗 LinkedIn: https://www.linkedin.com/in/jackson10/
