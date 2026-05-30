"""
reference_market_event_daily.py

DAG lập lịch lấy lịch sự kiện thị trường (market events calendar) và ghi MinIO.

Script: ingestion/reference/vnstock_event_ingestion.py
  - Gọi Reference().events.market() (một lần, toàn thị trường).
  - Ghi: raw/reference/event/event.parquet
  - --append: merge với file cũ, dedup theo các key có trong DataFrame.

Lịch chạy: 00:20 UTC = 07:20 ICT hàng ngày.

Yêu cầu (xem docker-compose.yml):
  - ./ingestion/reference  mount tại /opt/airflow/scripts/reference_ingestion
  - ./vtit_gx              mount tại /opt/airflow/packages/vtit_gx (PYTHONPATH=/opt/airflow/packages)
  - ./.env                 mount tại /opt/airflow/.env
  - vnstock-venv volume    mount tại /opt/vnstock-venv (tạo bởi: docker compose run --rm vnstock-setup)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PYTHON = "/opt/vnstock-venv/bin/python"
SCRIPT_DIR = "/opt/airflow/scripts/reference_ingestion"
ENV_FILE = "/opt/airflow/.env"

_SOURCE_ENV = (
    f"set -a && source {ENV_FILE} && set +a && "
    "export HOME=/opt/vnstock-home && "
    "export MINIO_ENDPOINT=http://minio:9100 && "
    "export MPLCONFIGDIR=/tmp/mplconfig-airflow"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}

with DAG(
    dag_id="reference_market_event_daily",
    description="Daily market events calendar → raw/reference/event/event.parquet",
    schedule_interval="20 0 * * *",  # 00:20 UTC = 07:20 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["reference", "event", "daily"],
) as dag:

    fetch_market_events = BashOperator(
        task_id="fetch_market_events",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_event_ingestion.py "
            "--append"
        ),
        execution_timeout=timedelta(minutes=30),
    )
