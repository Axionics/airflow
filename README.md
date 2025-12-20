# Airflow - Grid Axionics

Orquestração de pipelines de dados usando Apache Airflow, incluindo execução de modelos dbt em produção.

## Estrutura do Projeto

```
airflow/
├── dags/                  # DAGs do Airflow
│   ├── dbt_run_staging.py      # Executa modelos staging (tag:stg) - 5min
│   └── dbt_run_int_mart.py     # Executa modelos int+mart (tag:int,mart) - 5min
├── logs/                  # Logs do Airflow
├── plugins/              # Plugins customizados
├── config/               # Configurações do Airflow
├── docker-compose.yml    # Configuração dos serviços
└── .env                  # Variáveis de ambiente (não versionado)
```

## DAGs de dbt Disponíveis

### 1. `dbt_run_staging`
- **Frequência**: A cada 5 minutos
- **Descrição**: Executa modelos dbt com tag "stg" (camada staging)
- **Características**:
  - Captura automaticamente novos modelos com tag "stg"
  - Sem retries (retries=0) em caso de falha
  - Logs detalhados para debug (--debug)
- **Comando**: `dbt run --select tag:stg`

### 2. `dbt_run_int_mart`
- **Frequência**: A cada 5 minutos
- **Descrição**: Executa modelos dbt com tags "int" e "mart" (camadas intermediate e marts)
- **Características**:
  - Captura automaticamente novos modelos com tags "int" ou "mart"
  - Sem retries (retries=0) em caso de falha
  - Logs detalhados para debug (--debug)
  - Ordem de execução: intermediate → marts
- **Comandos**:
  1. `dbt run --select tag:int`
  2. `dbt run --select tag:mart`

## Configuração

### 1. Variáveis do Airflow (Admin > Variables)

As credenciais do banco de dados são armazenadas como Airflow Variables.
Configure pela interface web do Airflow:

1. Acesse: **Admin > Variables**
2. Clique em **+** para adicionar nova variável
3. Adicione as seguintes variáveis:

| Key | Value | Description |
|-----|-------|-------------|
| `dbt_db_host_prod` | `seu_ip_ou_host` | Host do banco de produção |
| `dbt_db_port_prod` | `5432` | Porta do banco (padrão: 5432) |
| `dbt_db_name_prod` | `powerplants` | Nome do banco de dados |
| `dbt_db_user_prod` | `seu_usuario` | Usuário do banco |
| `dbt_db_password_prod` | `sua_senha` | Senha do banco ⚠️ **marcar como secret** |

**IMPORTANTE**: Ao adicionar `dbt_db_password_prod`, marque a checkbox "Secret" para ocultar a senha nos logs.

### 2. Projeto dbt

O projeto dbt está montado em `/opt/dbt` dentro do container do Airflow através do volume.

**Configuração por Ambiente:**

- **Local (Desenvolvimento)**:
  - Defina `DBT_PROJECT_PATH=../axionics-dbt` no arquivo `.env`
  - O volume será: `../axionics-dbt:/opt/dbt`

- **Produção (VPS)**:
  - **NÃO defina** `DBT_PROJECT_PATH` no arquivo `.env` (ou comente a linha)
  - O volume usará o default: `/opt/dbt:/opt/dbt`
  - Certifique-se que o projeto dbt está em `/opt/dbt` na VPS

O perfil dbt (`profiles.yml`) está em `/opt/dbt/profiles`.

### 3. Tags do dbt

Os modelos são organizados por tags no `dbt_project.yml`:

- **Tag `stg`**: Modelos da camada staging (todos em `models/staging/`)
- **Tag `int`**: Modelos da camada intermediate (todos em `models/intermediate/`)
- **Tag `mart`**: Modelos da camada marts (todos em `models/marts/`)

**Novos modelos são detectados automaticamente** pelas DAGs se estiverem nos diretórios corretos.

## Deploy

### Desenvolvimento Local

```bash
# Subir os serviços
docker-compose up -d

# Ver logs
docker-compose logs -f

# Parar os serviços
docker-compose down
```

### Produção (VPS Hostinger)

O deploy em produção é automático via GitHub Actions:

1. Faça commit das mudanças:
```bash
git add .
git commit -m "feat: adicionar DAGs de orquestração dbt"
git push origin main
```

2. Aguarde ~2 minutos para o CI/CD aplicar as mudanças na VPS

3. As novas DAGs aparecerão no Airflow em até 30 segundos (configuração do `dag_dir_list_interval`)

## Adicionar Novos Pacotes Python

Para adicionar novos pacotes Python ao Airflow, edite a linha `_PIP_ADDITIONAL_REQUIREMENTS` na seção `x-airflow-common` do `docker-compose.yml`:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "apache-airflow-providers-postgres==6.5.0 dbt-core==1.9.1 dbt-postgres==1.9.1 seu-novo-pacote==versao"
```

## Monitoramento

### Acessar Interface Web

- **URL**: `http://seu-servidor:8080`
- **Usuário**: Definido em `.env` (`AIRFLOW_USER`)
- **Senha**: Definida em `.env` (`AIRFLOW_PASSWORD`)

### Verificar Status das DAGs

1. Acesse a interface web
2. Vá em "DAGs"
3. Procure por "dbt_run_*"
4. Clique na DAG para ver histórico de execuções

### Ver Logs de Execução

1. Clique na DAG
2. Clique em um run específico
3. Clique na task desejada
4. Clique em "Log" para ver a saída completa

## Troubleshooting

### DAG não aparece no Airflow

1. Verifique se o arquivo está na pasta `dags/`
2. Aguarde até 30 segundos (intervalo de scan)
3. Verifique logs: `docker-compose logs airflow-scheduler`

### Erro de conexão com banco de dados

1. Verifique as Airflow Variables em **Admin > Variables**
2. Confirme que `dbt_db_password_prod` está marcada como "Secret"
3. Teste manualmente executando a DAG `dbt_run_staging`
4. Verifique os logs detalhados (modo debug está ativado)

### Erro ao instalar pacotes dbt

1. Verifique se `dbt-core` e `dbt-postgres` estão em `_PIP_ADDITIONAL_REQUIREMENTS`
2. Reconstrua a imagem: `docker-compose build`
3. Reinicie os serviços: `docker-compose up -d`

## Estrutura de Rede

O Airflow está na rede `stack_net` (externa), permitindo comunicação com outros serviços da stack (banco de dados, etc.).

## Links Úteis

- [Documentação do Airflow](https://airflow.apache.org/docs/)
- [Documentação do dbt](https://docs.getdbt.com/)
- [Projeto dbt Axionics](../axionics-dbt/)
