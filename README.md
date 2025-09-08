# Pipeline de Dados BanVic

## Visão Geral
Pipeline de dados desenvolvida para o Banco Vitória (BanVic) utilizando Apache Airflow para extrair dados de fontes SQL e CSV, processá-los e carregá-los em um Data Warehouse PostgreSQL para análises de crédito.

## Arquitetura
- **Orquestração**: Apache Airflow 2.8.1
- **Data Warehouse**: PostgreSQL 16
- **Fontes de Dados**: PostgreSQL (dados transacionais) + CSV (transações)
- **Processamento**: Pipeline ELT com camadas staging e mart

## Pré-requisitos
- Docker e Docker Compose instalados
- Portas livres: 8080 (Airflow), 55432 (PostgreSQL DW)
## Instalação

### 1. Configurar Data Warehouse
```bash
# Clone ou extraia os arquivos do projeto
cd banvic-pipeline

# Subir PostgreSQL Data Warehouse
docker-compose up -d

### 2. Configurar Airflow
```bash
cd airflow-docker

# Copie a DAG para o diretório do Airflow
cp ../dags/banvic_pipeline.py dags/

# Copie o CSV para o diretório include do Airflow
cp ../include/transacoes.csv include/

# Subir Airflow
docker-compose up -d
```

### 3. Configurar Conexões no Airflow
Acesse http://localhost:8080 e configure:

**Conexão 1: banvic_postgres (fonte)**
- Connection Id: `banvic_postgres`
- Connection Type: `Postgres`
- Host: `host.docker.internal`
- Database: `banvic`
- Login: `data_engineer`
- Password: `v3rysecur&pas5w0rd`
- Port: `55432`

**Conexão 2: dw_postgres (destino)**
- Connection Id: `dw_postgres`
- Connection Type: `Postgres`
- Host: `host.docker.internal`
- Database: `banvic`
- Login: `data_engineer`
- Password: `v3rysecur&pas5w0rd`
- Port: `55432`

<img width="1920" height="1080" alt="airflow1" src="https://github.com/user-attachments/assets/37a27ff8-f35f-4d1a-b78d-60369c1a1ead" />
<img width="1920" height="1080" alt="airflow2" src="https://github.com/user-attachments/assets/9bebec81-037a-4550-846a-c8e295187fad" />

## Execução

### 1. Execução Automática
A pipeline está configurada para rodar diariamente às 04:35.



### 1. Arquivos CSV Extraídos
```bash
# Acessar container do Airflow
docker exec -it airflow-docker-airflow-worker-1 bash

### 2. Dados no Data Warehouse
```bash
# Conectar ao PostgreSQL
docker exec -it banvic-pipeline-db-1 psql -U data_engineer -d banvic 

# Verificar schemas e tabelas
\dn
\dt staging.*
\dt mart.*

# Contar registros
SELECT COUNT(*) FROM mart.sql_agencias;
SELECT COUNT(*) FROM mart.sql_clientes;
SELECT COUNT(*) FROM mart.csv_transacoes;
```

## Estrutura dos Dados

### Schemas
- **staging**: Dados brutos extraídos das fontes
- **mart**: Dados processados prontos para análise

### Tabelas Principais
- `sql_agencias`: Informações das agências
- `sql_clientes`: Dados dos clientes
- `sql_propostas_credito`: Propostas de crédito
- `csv_transacoes`: Transações financeiras

## Características Técnicas

### Idempotência
A pipeline é idempotente - múltiplas execuções na mesma data produzem o mesmo resultado usando TRUNCATE + INSERT.

### Paralelização
As extrações SQL e CSV executam em paralelo.

### Tratamento de Erros
- Dependências entre tasks garantem ordem correta
