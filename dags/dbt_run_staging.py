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
from airflow.decorators import task
from airflow.models import Variable
import os

# Configurações padrão da DAG
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
DBT_PROFILES_DIR = '/opt/dbt/profiles'


with DAG(
    dag_id='dbt_run_staging',
    default_args=default_args,
    description='Executa modelos dbt com tag "stg" (staging layer)',
    schedule='*/5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'staging', 'stg'],
    max_active_runs=1,
) as dag:

    @task
    def get_dbt_env_vars():
        """
        Retorna as variáveis de ambiente do dbt como dict.
        """
        return {
            'DB_HOST_PROD': Variable.get('dbt_db_host_prod'),
            'DB_PORT_PROD': Variable.get('dbt_db_port_prod', default_var='5432'),
            'DBT_DB_NAME_PROD': Variable.get('dbt_db_name_prod'),
            'DBT_DB_USER_PROD': Variable.get('dbt_db_user_prod'),
            'DBT_DB_PASSWORD_PROD': Variable.get('dbt_db_password_prod'),
        }

    @task
    def list_staging_models(env_vars: dict):
        """
        Lista os modelos dbt que possuem a tag 'stg'.
        Retorna uma lista de dicts para dynamic task mapping.
        """
        import subprocess
        import logging

        # Exportar variáveis de ambiente
        env = os.environ.copy()
        env.update(env_vars)

        try:
            cmd = f"cd {DBT_PROJECT_DIR} && dbt ls --select tag:stg --profiles-dir {DBT_PROFILES_DIR} --target prod --output name"
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
                env=env
            )

            models = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            logging.info(f"Modelos encontrados com tag 'stg': {models}")

            # Retornar lista de dicts para o expand()
            return [{"model_name": model, "env_vars": env_vars} for model in models]

        except subprocess.CalledProcessError as e:
            logging.error(f"Erro ao listar modelos dbt: {e.stderr}")
            raise
        except Exception as e:
            logging.error(f"Erro inesperado ao listar modelos: {e}")
            raise

    # Task 1: Pegar variáveis de ambiente
    env_vars = get_dbt_env_vars()

    # Task 2: Listar modelos staging
    models_list = list_staging_models(env_vars)

    @task
    def run_staging_model(model_name: str, env_vars: dict):
        """
        Executa um modelo dbt específico.
        """
        import subprocess
        import logging

        # Exportar variáveis de ambiente
        env = os.environ.copy()
        env.update(env_vars)

        logging.info(f"========================================")
        logging.info(f"Executando modelo STAGING: {model_name}")
        logging.info(f"Diretório: {DBT_PROJECT_DIR}")
        logging.info(f"Target: prod")
        logging.info(f"========================================")

        try:
            cmd = f"cd {DBT_PROJECT_DIR} && dbt run --select {model_name} --profiles-dir {DBT_PROFILES_DIR} --target prod --debug"
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
                env=env
            )

            logging.info(result.stdout)
            logging.info(f"✓ Modelo {model_name} executado com sucesso")
            return {"model": model_name, "status": "success"}

        except subprocess.CalledProcessError as e:
            logging.error(f"Erro ao executar modelo {model_name}: {e.stderr}")
            logging.error(e.stdout)
            raise

    # Task 3: Executar cada modelo individualmente (dynamic task mapping)
    run_model = run_staging_model.expand_kwargs(models_list)

    # Definir ordem de execução
    env_vars >> models_list >> run_model
