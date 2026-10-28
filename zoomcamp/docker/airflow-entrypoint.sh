#!/usr/bin/env bash
set -euo pipefail

# Stale PID files on the airflow_data volume can block the webserver from binding :8080
# after a container stop/start (dead PID still recorded).
clean_stale_airflow_pids() {
  local base=/opt/airflow
  local name pid
  for name in airflow-webserver.pid airflow-webserver-monitor.pid airflow-scheduler.pid; do
    local f="${base}/${name}"
    [[ -f "$f" ]] || continue
    pid=$(tr -d '[:space:]' <"$f" 2>/dev/null || true)
    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$f"
    fi
  done
}

cd /opt/transitpulse/zoomcamp
uv sync --frozen

admin_user="${_AIRFLOW_WWW_USER_USERNAME:-admin}"
admin_password="${_AIRFLOW_WWW_USER_PASSWORD:-admin}"

airflow db migrate

if airflow users list 2>/dev/null | grep -q " admin "; then
  airflow users reset-password --username "$admin_user" --password "$admin_password"
else
  airflow users create \
    --username "$admin_user" \
    --password "$admin_password" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
fi

clean_stale_airflow_pids
exec airflow standalone
