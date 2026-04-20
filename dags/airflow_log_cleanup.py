from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from discord_alerts import notify_discord_on_failure

RETENTION_DAYS = 30

default_args = {
    'owner': 'axionics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': notify_discord_on_failure,
}

# Remove run_id=*__<DATE>T... folders older than RETENTION_DAYS.
# dag_processor logs are plain files — cleaned by mtime.
CLEANUP_CMD = f"""
set -e
CUTOFF=$(date -d '{RETENTION_DAYS} days ago' '+%Y-%m-%d')
echo "Removendo logs com data < $CUTOFF"

REMOVED=$(find /opt/airflow/logs \
    -mindepth 2 -maxdepth 2 -type d -name 'run_id=*' \
    | awk -v c="$CUTOFF" -F'__' '{{split($2,d,"T"); if(d[1]<c) print}}' \
    | tee /dev/stderr \
    | xargs -r -P4 rm -rf && echo ok)

find /opt/airflow/logs/dag_processor -type f -mtime +{RETENTION_DAYS} -delete 2>/dev/null || true
find /opt/airflow/logs -mindepth 1 -type d -empty -delete 2>/dev/null || true

df -h /opt/airflow/logs 2>/dev/null || df -h /
echo "Limpeza concluída"
"""


@dag(
    dag_id='airflow_log_cleanup',
    default_args=default_args,
    description=f'Remove logs do Airflow com mais de {RETENTION_DAYS} dias',
    schedule='0 3 * * 0',  # domingo 03h00 UTC
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=['maintenance', 'cleanup', 'logs'],
    max_active_runs=1,
    on_failure_callback=notify_discord_on_failure,
)
def airflow_log_cleanup():
    BashOperator(
        task_id='cleanup_old_logs',
        bash_command=CLEANUP_CMD,
        execution_timeout=timedelta(minutes=30),
    )


airflow_log_cleanup()
