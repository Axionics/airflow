"""
DAG para execução dos modelos INTERMEDIATE e MART (tags: int, mart) do dbt.

Esta DAG executa todos os modelos dbt que possuem as tags "int" ou "mart",
capturando automaticamente novos modelos quando forem criados.

Ordem de execução:
1. Modelos INTERMEDIATE (tag: int)
2. Modelos MART (tag: mart)

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
    dag_id='dbt_run_int_mart',
    default_args=default_args,
    description='Executa modelos dbt com tags "int" e "mart" (intermediate + marts)',
    schedule='*/5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'intermediate', 'marts'],
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
    def list_models_by_tag(env_vars: dict, tag: str):
        """
        Lista os modelos dbt que possuem uma tag específica.
        Retorna uma lista de dicts para dynamic task mapping.
        """
        import subprocess
        import logging

        # Exportar variáveis de ambiente
        env = os.environ.copy()
        env.update(env_vars)

        try:
            cmd = f"cd {DBT_PROJECT_DIR} && dbt ls --select tag:{tag} --profiles-dir {DBT_PROFILES_DIR} --target prod --output name"
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
                env=env
            )

            models = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            logging.info(f"Modelos encontrados com tag '{tag}': {models}")

            # Retornar lista de dicts para o expand()
            return [{"model_name": model, "env_vars": env_vars} for model in models]

        except subprocess.CalledProcessError as e:
            logging.error(f"Erro ao listar modelos dbt com tag '{tag}': {e.stderr}")
            raise
        except Exception as e:
            logging.error(f"Erro inesperado ao listar modelos: {e}")
            raise

    # Task 1: Pegar variáveis de ambiente
    env_vars = get_dbt_env_vars()

    # Task 2: Listar modelos intermediate
    int_models_list = list_models_by_tag(env_vars, tag='int')

    # Task 3: Executar modelos intermediate
    run_int_model = BashOperator.partial(
        task_id='run_intermediate_model',
        bash_command=f"""
            echo "========================================="
            echo "Executando modelo INTERMEDIATE: {{{{ params.model_name }}}}"
            echo "Diretório: {DBT_PROJECT_DIR}"
            echo "Target: prod"
            echo "========================================="

            cd {DBT_PROJECT_DIR}

            dbt run --select {{{{ params.model_name }}}} \\
                --profiles-dir {DBT_PROFILES_DIR} \\
                --target prod \\
                --debug

            EXIT_CODE=$?

            if [ $EXIT_CODE -eq 0 ]; then
                echo "✓ Modelo {{{{ params.model_name }}}} executado com sucesso"
            else
                echo "✗ Erro ao executar modelo {{{{ params.model_name }}}}"
                exit $EXIT_CODE
            fi

            echo "========================================="
        """,
    ).expand_kwargs(int_models_list)

    # Task 4: Listar modelos mart
    mart_models_list = list_models_by_tag(env_vars, tag='mart')

    # Task 5: Executar modelos mart
    run_mart_model = BashOperator.partial(
        task_id='run_mart_model',
        bash_command=f"""
            echo "========================================="
            echo "Executando modelo MART: {{{{ params.model_name }}}}"
            echo "Diretório: {DBT_PROJECT_DIR}"
            echo "Target: prod"
            echo "========================================="

            cd {DBT_PROJECT_DIR}

            dbt run --select {{{{ params.model_name }}}} \\
                --profiles-dir {DBT_PROFILES_DIR} \\
                --target prod \\
                --debug

            EXIT_CODE=$?

            if [ $EXIT_CODE -eq 0 ]; then
                echo "✓ Modelo {{{{ params.model_name }}}} executado com sucesso"
            else
                echo "✗ Erro ao executar modelo {{{{ params.model_name }}}}"
                exit $EXIT_CODE
            fi

            echo "========================================="
        """,
    ).expand_kwargs(mart_models_list)

    # Definir ordem de execução: env_vars -> int -> mart
    env_vars >> int_models_list >> run_int_model >> mart_models_list >> run_mart_model
