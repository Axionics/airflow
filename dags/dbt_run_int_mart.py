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
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging
import subprocess

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


def get_dbt_models_with_tag(tag: str):
    """
    Lista os modelos dbt que possuem uma tag específica usando dbt ls.
    Retorna uma lista de nomes de modelos.
    """
    try:
        cmd = f"cd {DBT_PROJECT_DIR} && dbt ls --select tag:{tag} --profiles-dir {DBT_PROFILES_DIR} --target prod --output name"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        models = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
        logging.info(f"Modelos encontrados com tag '{tag}': {models}")
        return models
    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao listar modelos dbt: {e.stderr}")
        return []
    except Exception as e:
        logging.error(f"Erro inesperado ao listar modelos: {e}")
        return []


with DAG(
    dag_id='dbt_run_int_mart',
    default_args=default_args,
    description='Executa modelos dbt com tags "int" e "mart" (intermediate + marts) - um modelo por task',
    schedule='*/5 * * * *',  # A cada 5 minutos
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'intermediate', 'marts'],
    max_active_runs=1,  # Apenas uma execução por vez
) as dag:

    # Task: Listar modelos intermediate
    list_int_models = PythonOperator(
        task_id='list_intermediate_models',
        python_callable=get_dbt_models_with_tag,
        op_kwargs={'tag': 'int'},
    )

    # Task dinâmica: Executar cada modelo intermediate individualmente
    run_int_model = BashOperator.partial(
        task_id='run_int_model',
        bash_command="""
            echo "========================================="
            echo "Executando modelo INTERMEDIATE: {{ params.model_name }}"
            echo "Diretório: """ + DBT_PROJECT_DIR + """"
            echo "Target: prod"
            echo "========================================="

            cd """ + DBT_PROJECT_DIR + """

            dbt run --select {{ params.model_name }} --profiles-dir """ + DBT_PROFILES_DIR + """ --target prod

            EXIT_CODE=$?

            if [ $EXIT_CODE -eq 0 ]; then
                echo "✓ Modelo {{ params.model_name }} executado com sucesso"
            else
                echo "✗ Erro ao executar modelo {{ params.model_name }}"
                exit $EXIT_CODE
            fi

            echo "========================================="
        """,
        env=get_dbt_env_vars(),
    ).expand(
        params=[{"model_name": model} for model in get_dbt_models_with_tag('int')]
    )

    # Task: Listar modelos mart
    list_mart_models = PythonOperator(
        task_id='list_mart_models',
        python_callable=get_dbt_models_with_tag,
        op_kwargs={'tag': 'mart'},
    )

    # Task dinâmica: Executar cada modelo mart individualmente
    run_mart_model = BashOperator.partial(
        task_id='run_mart_model',
        bash_command="""
            echo "========================================="
            echo "Executando modelo MART: {{ params.model_name }}"
            echo "Diretório: """ + DBT_PROJECT_DIR + """"
            echo "Target: prod"
            echo "========================================="

            cd """ + DBT_PROJECT_DIR + """

            dbt run --select {{ params.model_name }} --profiles-dir """ + DBT_PROFILES_DIR + """ --target prod

            EXIT_CODE=$?

            if [ $EXIT_CODE -eq 0 ]; then
                echo "✓ Modelo {{ params.model_name }} executado com sucesso"
            else
                echo "✗ Erro ao executar modelo {{ params.model_name }}"
                exit $EXIT_CODE
            fi

            echo "========================================="
        """,
        env=get_dbt_env_vars(),
    ).expand(
        params=[{"model_name": model} for model in get_dbt_models_with_tag('mart')]
    )

    # Dependências: list_int -> run_int -> list_mart -> run_mart
    list_int_models >> run_int_model >> list_mart_models >> run_mart_model
