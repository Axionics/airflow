#!/usr/bin/env bash
# Limpeza de logs do Airflow no host — roda via cron como safety net.
# Instalar: crontab -e  →  0 4 * * 0 /opt/airflow/scripts/cleanup_logs.sh >> /var/log/airflow_cleanup.log 2>&1
set -euo pipefail

LOGS_DIR="${AIRFLOW_LOGS_DIR:-/opt/airflow/logs}"
RETENTION_DAYS="${LOG_RETENTION_DAYS:-30}"
CUTOFF=$(date -d "${RETENTION_DAYS} days ago" '+%Y-%m-%d')

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Iniciando limpeza — cutoff: ${CUTOFF} (retenção: ${RETENTION_DAYS} dias)"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Disco antes: $(df -h "${LOGS_DIR}" 2>/dev/null | tail -1 || df -h / | tail -1)"

# Remove pastas de run antigas (run_id=<tipo>__<DATA>T...)
find "${LOGS_DIR}" \
    -mindepth 2 -maxdepth 2 -type d -name 'run_id=*' \
    | awk -v c="${CUTOFF}" -F'__' '{split($2,d,"T"); if(d[1]<c) print}' \
    | xargs -r -P4 rm -rf

# Remove logs do dag_processor por mtime
find "${LOGS_DIR}/dag_processor" -type f -mtime +"${RETENTION_DAYS}" -delete 2>/dev/null || true

# Remove diretórios vazios
find "${LOGS_DIR}" -mindepth 1 -type d -empty -delete 2>/dev/null || true

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Disco depois: $(df -h "${LOGS_DIR}" 2>/dev/null | tail -1 || df -h / | tail -1)"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Limpeza concluída"
