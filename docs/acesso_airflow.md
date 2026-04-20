# Acesso Airflow — Grid Co

## Onde roda

Airflow roda em **Docker** na EC2 `gridco-ppo-airflow` (`32.192.226.169`, `i-0f10e7513d122efbc`).

**Containers ativos:**

| Container | Função |
|-----------|--------|
| `airflow-airflow-apiserver-1` | API + Web UI |
| `airflow-airflow-scheduler-1` | Scheduler (rodar comandos CLI aqui) |
| `airflow-airflow-dag-processor-1` | Processamento de DAGs |
| `airflow-airflow-triggerer-1` | Triggerer |
| `airflow-postgres-1` | Metadata database |

**UI:** `https://airflow.axionics.cloud`
**API:** `http://32.192.226.169:8080/api/v2/` (porta não exposta externamente — usar via SSH)

---

## Credenciais do banco de metadados do Airflow

```
Host    : airflow-postgres-1 (container local, não exposto externamente)
User    : airfl0w_4ddmi1n
Password: T5zjL0Jj2H3R!I28
DB      : airflow
```

---

## Como acessar o servidor

Sem a PEM `gridco-ppo-key`, usar **EC2 Instance Connect** (ver [acesso_aws.md](../aws/acesso_aws.md)).

```bash
# Gerar chave temporária (se ainda não existir)
ssh-keygen -t rsa -b 2048 -f /tmp/temp_gridco_key -N "" -q

# Função helper — reenvia a chave e executa um comando no servidor Airflow
airflow_ssh() {
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  aws ec2-instance-connect send-ssh-public-key \
    --region us-east-1 \
    --instance-id i-0f10e7513d122efbc \
    --instance-os-user ubuntu \
    --ssh-public-key file:///tmp/temp_gridco_key.pub > /dev/null 2>&1
  ssh -i /tmp/temp_gridco_key -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@32.192.226.169 "$1"
}
```

> A chave expira em 60s — sempre reenviar antes de cada conexão.

---

## Operações comuns via CLI (docker exec no scheduler)

> **Atenção:** o comando `airflow tasks clear` pode falhar se algum DAG tiver import quebrado.  
> Nesse caso, usar SQL direto no postgres (ver seção abaixo).

```bash
# Listar DAGs
airflow_ssh "docker exec airflow-airflow-scheduler-1 airflow dags list"

# Ver runs de um DAG
airflow_ssh "docker exec airflow-airflow-scheduler-1 airflow dags list-runs -d <dag_id> --output table"

# Limpar tasks de um DAG (reprocessa)
airflow_ssh "docker exec airflow-airflow-scheduler-1 airflow tasks clear <dag_id> \
  --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD> --yes"

# Pausar / despausar DAG
airflow_ssh "docker exec airflow-airflow-scheduler-1 airflow dags pause <dag_id>"
airflow_ssh "docker exec airflow-airflow-scheduler-1 airflow dags unpause <dag_id>"

# Trigger manual de um DAG
airflow_ssh "docker exec airflow-airflow-scheduler-1 airflow dags trigger <dag_id>"
```

---

## Operações via SQL no banco de metadados

Para quando o CLI falha (ex: import error no DAG).

```bash
# Abrir psql no container postgres
airflow_ssh "docker exec -it airflow-postgres-1 psql -U airfl0w_4ddmi1n -d airflow"

# Ou executar query diretamente
airflow_ssh "docker exec airflow-postgres-1 psql -U airfl0w_4ddmi1n -d airflow -c \"<SQL>\""
```

### Consultas úteis

```sql
-- Ver runs recentes de um DAG por estado
SELECT dag_id, run_id, state, logical_date
FROM dag_run
WHERE dag_id = '<dag_id>'
ORDER BY logical_date DESC
LIMIT 20;

-- Contar runs por estado
SELECT state, COUNT(*)
FROM dag_run
WHERE dag_id = '<dag_id>'
GROUP BY state;

-- Ver task instances de um run específico
SELECT task_id, state, start_date, end_date
FROM task_instance
WHERE dag_id = '<dag_id>'
  AND run_id = '<run_id>'
ORDER BY start_date;
```

### Limpar fila acumulada (queued + failed)

```sql
BEGIN;

-- 1. Remover task instances das runs a deletar
DELETE FROM task_instance
WHERE dag_id = '<dag_id>'
  AND run_id IN (
      SELECT run_id FROM dag_run
      WHERE dag_id = '<dag_id>'
        AND state IN ('queued', 'failed')
  );

-- 2. Remover as dag_runs
DELETE FROM dag_run
WHERE dag_id = '<dag_id>'
  AND state IN ('queued', 'failed');

COMMIT;

-- Confirmar
SELECT state, COUNT(*) FROM dag_run WHERE dag_id = '<dag_id>' GROUP BY state;
```

### Marcar run como success (sem reprocessar)

```sql
UPDATE dag_run
SET state = 'success'
WHERE dag_id = '<dag_id>'
  AND run_id = '<run_id>';

UPDATE task_instance
SET state = 'success'
WHERE dag_id = '<dag_id>'
  AND run_id = '<run_id>';
```

---

## DAGs existentes

| DAG | Schedule | Função |
|-----|----------|--------|
| `dbt_run_staging_cosmos` | `*/5 * * * *` | Roda modelos staging (stg_*) |
| `dbt_run_int_mart_cosmos` | triggered | Roda modelos intermediate e mart |
| `airflow_log_cleanup` | domingo 03h UTC | Apaga logs de DAGs com mais de 30 dias |

---

## Deploy / CI-CD

O repo `Axionics/airflow` tem deploy automático via **GitHub Actions + self-hosted runner** na EC2.

Push em `main` com mudança em `dags/`, `plugins/`, `scripts/`, `requirements.txt`, `Dockerfile` ou `docker-compose.yml` dispara o deploy automaticamente.

- Trigger manual: `github.com/Axionics/airflow/actions` → **Deploy Airflow** → **Run workflow**
- Logs do runner: `sudo journalctl -u github-runner -f` (na EC2)
- Status do runner: `sudo systemctl status github-runner`

> Airflow detecta novos DAGs automaticamente — não é necessário restart para mudanças apenas em `dags/`.

---

## Observações

- O SSM (Systems Manager) **não está instalado** na instância — não usar `aws ssm send-command`.
- A porta `8080` (Airflow web) **não está aberta** no security group — API só acessível dentro do servidor.
- O DAG `dbt_run_int_mart_cosmos` tem um import `discord_alerts` que pode causar `ModuleNotFoundError` no CLI — usar SQL nesses casos.
