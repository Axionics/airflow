from datetime import datetime, timedelta
import json
import subprocess
from urllib.request import Request, urlopen
from urllib.error import URLError

import boto3

from airflow.decorators import dag, task
from airflow.models import Variable

from discord_alerts import notify_discord_on_failure

EC2_NAMES = {
    'i-0f10e7513d122efbc': 'gridco-ppo-airflow',
    'i-0255f1a44c57ac0d0': 'gridco-ppo-db-grafana',
    'i-0722d0a0d4fd865eb': 'Servidor_gridco',
}

default_args = {
    'owner': 'axionics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_discord_on_failure,
}


def _send_discord(message: str) -> None:
    webhook_url = Variable.get('discord_webhook_url').strip()
    req = Request(
        webhook_url,
        data=json.dumps({'content': message}).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'User-Agent': 'Airflow-Discord-Alert/1.0',
        },
        method='POST',
    )
    try:
        urlopen(req)
    except URLError as e:
        raise RuntimeError(f'Falha ao enviar Discord: {e}')


def _disk_emoji(pct: int) -> str:
    if pct >= 85:
        return '🚨'
    if pct >= 70:
        return '⚠️'
    return '✅'


@dag(
    dag_id='disk_space_monitor',
    default_args=default_args,
    description='Monitora espaço em disco dos volumes EBS e alerta no Discord a cada 3h',
    schedule='0 */3 * * *',
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=['monitoring', 'infra', 'disk'],
    max_active_runs=1,
    on_failure_callback=notify_discord_on_failure,
)
def disk_space_monitor():

    @task
    def check_and_alert():
        # --- Disco local (airflow EC2) via volume montado ---
        df = subprocess.run(['df', '-h', '/opt/airflow'], capture_output=True, text=True)
        parts = df.stdout.strip().splitlines()[-1].split()
        local_size, local_used, local_avail = parts[1], parts[2], parts[3]
        local_pct = int(parts[4].replace('%', ''))

        # --- Volumes EBS via AWS API ---
        ec2 = boto3.client(
            'ec2',
            region_name='us-east-1',
            aws_access_key_id=Variable.get('aws_access_key_id'),
            aws_secret_access_key=Variable.get('aws_secret_access_key'),
        )

        volumes = ec2.describe_volumes()['Volumes']
        statuses = {
            s['InstanceId']: s['InstanceState']['Name']
            for s in ec2.describe_instance_status(
                InstanceIds=list(EC2_NAMES.keys()),
                IncludeAllInstances=True,
            )['InstanceStatuses']
        }

        # --- Monta mensagem ---
        local_emoji = _disk_emoji(local_pct)
        lines = [
            '🖥️ **Monitoramento de Disco — Grid Co**',
            f'📅 {datetime.utcnow().strftime("%Y-%m-%d %H:%M")} UTC',
            '',
            '**gridco-ppo-airflow** (32.192.226.169)',
            f'└ {local_emoji} `{local_used}` / `{local_size}` — **{local_pct}%** usado  |  livre: `{local_avail}`',
            '',
            '📦 **Volumes EBS**',
        ]

        for vol in sorted(volumes, key=lambda v: (
            EC2_NAMES.get(v['Attachments'][0]['InstanceId'], 'z') if v['Attachments'] else 'z'
        )):
            if not vol['Attachments']:
                continue
            inst_id = vol['Attachments'][0]['InstanceId']
            inst_name = EC2_NAMES.get(inst_id, inst_id)
            device = vol['Attachments'][0]['Device']
            size_gb = vol['Size']
            state = statuses.get(inst_id, 'unknown')
            state_icon = '🟢' if state == 'running' else '🔴'
            lines.append(
                f'└ {state_icon} `{inst_name}` {device} — **{size_gb} GB** ({vol["VolumeType"]})'
            )

        _send_discord('\n'.join(lines))

    check_and_alert()


disk_space_monitor()
