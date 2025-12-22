# Setup Astronomer Cosmos - Instruções

## 1. Instalar dependências na VPS

Após fazer push deste código, rode na VPS:

```bash
cd /opt/airflow
docker compose down
docker compose build
docker compose up -d
```

Isso vai instalar o `astronomer-cosmos[dbt-postgres]==1.7.5`.

## 2. Criar Airflow Connection

O Cosmos usa Airflow Connections em vez de Airflow Variables para credenciais.

### Opção A: Via Interface Web (RECOMENDADO)

1. Acesse o Airflow UI: `http://seu-servidor:8080`
2. Vá em **Admin → Connections**
3. Clique em **+** para adicionar nova connection
4. Preencha os campos:

```
Connection Id: postgres_prod
Connection Type: Postgres
Host: <valor de dbt_db_host_prod>
Schema: powerplants
Login: <valor de dbt_db_user_prod>
Password: <valor de dbt_db_password_prod>
Port: 5432
```

5. Clique em **Save**

### Opção B: Via CLI na VPS

```bash
docker compose exec airflow-scheduler bash

airflow connections add 'postgres_prod' \
    --conn-type 'postgres' \
    --conn-host '44.199.229.137' \
    --conn-schema 'powerplants' \
    --conn-login 'pggr1dc0_us3rdb' \
    --conn-password 'SUA_SENHA_AQUI' \
    --conn-port 5432

exit
```

## 3. Verificar se as DAGs aparecem

Aguarde ~30 segundos para o Airflow detectar as novas DAGs:
- `dbt_run_staging_cosmos`
- `dbt_run_int_mart_cosmos`

## 4. Testar execução

1. Na UI do Airflow, clique em `dbt_run_staging_cosmos`
2. Clique em **Trigger DAG**
3. Veja na Graph View: cada modelo dbt aparece como uma "caixinha" individual!

## 5. Desativar DAGs antigas (opcional)

Depois que confirmar que as novas DAGs funcionam, você pode desativar as antigas:
- `dbt_run_staging` (versão antiga)
- `dbt_run_int_mart` (versão antiga)

Ou deletar os arquivos:
```bash
cd /opt/airflow
rm dags/dbt_run_staging.py
rm dags/dbt_run_int_mart.py
```

## Benefícios da nova estrutura

✅ Cada modelo = 1 task (fácil monitoramento)
✅ Dependências automáticas entre modelos
✅ Task Groups para organização visual
✅ Logs individuais por modelo
✅ Grid view mostra todas as tasks em cascata
✅ Retry granular (só o modelo que falhou)

## Troubleshooting

**Erro: "Connection 'postgres_prod' not found"**
- Verifique se criou a connection no passo 2

**Erro: "dbt executable not found"**
- Verifique se dbt está instalado: `docker compose exec airflow-scheduler dbt --version`

**DAG não aparece**
- Verifique logs: `docker compose logs airflow-scheduler | grep cosmos`
- Aguarde 30 segundos para scan de DAGs
