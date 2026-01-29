"""
DAG para execução dos modelos STAGING (tag: stg) do dbt usando Astronomer Cosmos.

Esta DAG usa Cosmos para criar automaticamente uma task para cada modelo dbt,
proporcionando melhor visibilidade e monitoramento no Airflow.

Configurações:
- Frequência: A cada 5 minutos
- Cada modelo dbt = Uma task individual (caixinha no Airflow)
- Dependências automáticas entre modelos
- Task Group para organização visual

Variáveis necessárias no Airflow (Admin > Variables):
- dbt_db_host_prod: Host do banco de dados de produção
- dbt_db_port_prod: Porta do banco (padrão: 5432)
- dbt_db_name_prod: Nome do banco de dados
- dbt_db_user_prod: Usuário do banco
- dbt_db_password_prod: Senha do banco (marcar como secret)
"""

from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior

from discord_alerts import notify_discord_on_failure

# Configurações padrão
default_args = {
    'owner': 'axionics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Diretórios
DBT_PROJECT_DIR = '/opt/dbt'
DBT_EXECUTABLE_PATH = '/home/airflow/.local/bin/dbt'


@dag(
    dag_id='dbt_run_staging_cosmos',
    default_args=default_args,
    description='Executa modelos dbt com tag "stg" usando Astronomer Cosmos',
    schedule='*/5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'staging', 'stg', 'cosmos'],
    max_active_runs=1,
    on_failure_callback=notify_discord_on_failure,
)
def dbt_staging_dag():
    """
    DAG que executa modelos dbt de staging usando Cosmos.
    Cada modelo dbt aparece como uma task individual no Airflow
    """

    # Profile config - credenciais do banco de dados
    profile_config = ProfileConfig(
        profile_name='Axionics_dbt',
        target_name='prod',
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id='postgres_prod',
            profile_args={
                'schema': 'dbt',  # Schema onde os modelos dbt serão criados
            },
        ),
    )

    # TODO: REMOVER - Task temporária para testar alerta Discord
    force_fail = BashOperator(
        task_id='force_fail_test',
        bash_command='echo "Forçando erro para testar alerta Discord" && exit 1',
    )

    # Cosmos DbtTaskGroup - cria automaticamente uma task por modelo
    dbt_staging_tg = DbtTaskGroup(
        group_id='dbt_staging_models',
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            select=['tag:stg'],  # Seleciona apenas modelos com tag 'stg'
            exclude=['config.materialized:view'],  # Views não precisam ser recriadas a cada run
            test_behavior=TestBehavior.NONE,  # Não rodar testes automaticamente
            dbt_deps=False,  # Rodar dbt deps para garantir dependências
        ),
        operator_args={
            'install_deps': False,  # Não instalar dependências a cada run
        },
    )

    # Task para triggar a DAG int_mart após sucesso dos modelos staging
    trigger_int_mart = TriggerDagRunOperator(
        task_id='trigger_int_mart_dag',
        trigger_dag_id='dbt_run_int_mart_cosmos',
        wait_for_completion=False,
    )

    # Definir ordem: force_fail -> staging models -> trigger int_mart
    force_fail >> dbt_staging_tg >> trigger_int_mart


# Instanciar a DAG
dbt_staging_cosmos = dbt_staging_dag()
