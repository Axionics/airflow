"""
DAG para execução dos modelos STAGING (tag: stg) do dbt.

Esta DAG executa todos os modelos dbt que possuem a tag "stg",
capturando automaticamente novos modelos quando forem criados.

Configurações:
- Frequência: A cada 5 minutos
- Retries: 0 (não tenta novamente em caso de falha)
- Logs detalhados para debug

Variáveis necessárias no Airflow (Admin > Variables):
- dbt_db_host_prod: Host do banco de dados de produção
- dbt_db_port_prod: Porta do banco (padrão: 5432)
- dbt_db_name_prod: Nome do banco de dados
- dbt_db_user_prod: Usuário do banco
- dbt_db_password_prod: Senha do banco (marcar como secret)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import logging

# Configurações padrão da DAG
default_args = {
    'owner': 'axionics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Não tenta novamente em caso de falha
    'retry_delay': timedelta(minutes=1),
}

# Diretórios
DBT_PROJECT_DIR = '/opt/dbt'
DBT_PROFILES_DIR = '/opt/dbt/profiles'

def get_dbt_env_vars():
    """
    Retorna as variáveis de ambiente do dbt a partir das Airflow Variables.
    """
    try:
        env_vars = {
            'DB_HOST_PROD': Variable.get('dbt_db_host_prod'),
            'DB_PORT_PROD': Variable.get('dbt_db_port_prod', default_var='5432'),
            'DBT_DB_NAME_PROD': Variable.get('dbt_db_name_prod'),
            'DBT_DB_USER_PROD': Variable.get('dbt_db_user_prod'),
            'DBT_DB_PASSWORD_PROD': Variable.get('dbt_db_password_prod'),
        }
        logging.info(f"Variáveis de ambiente carregadas: {list(env_vars.keys())}")
        return env_vars
    except Exception as e:
        logging.error(f"Erro ao carregar variáveis do Airflow: {e}")
        raise


with DAG(
    dag_id='dbt_run_staging',
    default_args=default_args,
    description='Executa modelos dbt com tag "stg" (staging layer)',
    schedule_interval='*/5 * * * *',  # A cada 5 minutos
    start_date=days_ago(1),
    catchup=False,
    tags=['dbt', 'staging', 'stg'],
    max_active_runs=1,  # Apenas uma execução por vez
) as dag:

    # Task: Executar modelos staging
    run_staging_models = BashOperator(
        task_id='run_staging_models',
        bash_command=f'''
            set -x  # Ativar modo debug (mostra comandos executados)
            echo "========================================="
            echo "Iniciando execução dos modelos STAGING"
            echo "========================================="
            echo "Diretório: {DBT_PROJECT_DIR}"
            echo "Perfil: {DBT_PROFILES_DIR}"
            echo "Target: prod"
            echo "Tag: stg"
            echo "========================================="

            cd {DBT_PROJECT_DIR}

            # Executar modelos com tag "stg"
            dbt run --select tag:stg --profiles-dir {DBT_PROFILES_DIR} --target prod --debug

            echo "========================================="
            echo "Execução dos modelos STAGING concluída"
            echo "========================================="
        ''',
        env=get_dbt_env_vars(),
    )

    run_staging_models
