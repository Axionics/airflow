"""
DAG para execução dos modelos INTERMEDIATE e MART (tags: int, mart) usando Cosmos.

Esta DAG usa Cosmos para criar automaticamente uma task para cada modelo dbt,
com dependências automáticas entre as camadas intermediate e marts

Configurações:
- Frequência: A cada 5 minutos
- Cada modelo dbt = Uma task individual (caixinha no Airflow)
- Dependências automáticas: intermediate -> marts
- Task Groups para organização visual por camada

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

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior

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
    dag_id='dbt_run_int_mart_cosmos',
    default_args=default_args,
    description='Executa modelos dbt com tags "int" e "mart" usando Cosmos',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'intermediate', 'marts', 'cosmos'],
    max_active_runs=1,
)
def dbt_int_mart_dag():
    """
    DAG que executa modelos dbt intermediate e marts usando Cosmos.
    Cada modelo dbt aparece como uma task individual no Airflow.
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

    # Task Group 1: Modelos Intermediate
    dbt_intermediate_tg = DbtTaskGroup(
        group_id='dbt_intermediate_models',
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            select=['tag:int'],
            test_behavior=TestBehavior.NONE,
            dbt_deps=False,
        ),
        operator_args={
            'install_deps': False,    
        },
    )

    # Task Group 2: Modelos Marts
    dbt_marts_tg = DbtTaskGroup(
        group_id='dbt_mart_models',
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            select=['tag:mart'],
            test_behavior=TestBehavior.NONE,
            dbt_deps=False,
        ),
        operator_args={
            'install_deps': False,
            
        },
    )

    # Definir ordem: intermediate -> marts
    dbt_intermediate_tg >> dbt_marts_tg


# Instanciar a DAG
dbt_int_mart_cosmos = dbt_int_mart_dag()
