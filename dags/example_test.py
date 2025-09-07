"""
Example DAG demonstrating the integration between Airflow, Airbyte and dbt
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_integration',
    default_args=default_args,
    description='A DAG demonstrating integration between Airbyte and dbt',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 9, 5),
    catchup=False,
    tags=['example', 'integration'],
)

# This is a simplified example. In a real scenario, you would need to implement
# proper API calls to Airbyte and handle authentication
def trigger_airbyte_sync():
    # Replace with actual Airbyte API endpoints and connection IDs
    print("Triggering Airbyte sync")
    # Example API call (not functional without actual endpoints and auth)
    # response = requests.post(
    #     "http://airbyte-server:8001/api/v1/connections/sync",
    #     headers={"Content-Type": "application/json"},
    #     data=json.dumps({"connectionId": "your-connection-id"})
    # )
    # return response.json()
    return "Airbyte sync triggered"

trigger_airbyte = PythonOperator(
    task_id='trigger_airbyte_sync',
    python_callable=trigger_airbyte_sync,
    dag=dag,
)

run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='cd /usr/app/dbt && dbt run',
    dag=dag,
)

# Define task dependencies
trigger_airbyte >> run_dbt

