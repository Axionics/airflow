"""
Callbacks de alerta para Discord via webhook.

Uso:
    Adicionar como on_failure_callback no decorator @dag:

    from discord_alerts import notify_discord_on_failure

    @dag(
        ...,
        on_failure_callback=notify_discord_on_failure,
    )

Requer a Airflow Variable 'discord_webhook_url' configurada em Admin > Variables.
"""

import json
import logging
from urllib.request import Request, urlopen
from urllib.error import URLError

from airflow.models import Variable

logger = logging.getLogger(__name__)


def notify_discord_on_failure(context):
    """Envia alerta ao Discord quando uma DAG falha."""
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    exec_date = context.get('logical_date') or context.get('execution_date')
    execution_date = exec_date.strftime('%Y-%m-%d %H:%M:%S') if exec_date else 'N/A'

    webhook_url = Variable.get('discord_webhook_url')

    message = {
        "content": (
            f"**DAG Falhou**\n"
            f"- **DAG:** `{dag_id}`\n"
            f"- **Task:** `{task_id}`\n"
            f"- **Execução:** {execution_date}"
        )
    }

    req = Request(
        webhook_url,
        data=json.dumps(message).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )

    try:
        urlopen(req)
        logger.info("Alerta Discord enviado para DAG %s", dag_id)
    except URLError as e:
        logger.error("Falha ao enviar alerta Discord: %s", e)
