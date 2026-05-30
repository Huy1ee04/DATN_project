"""
reference_equity_daily.py

DAG lập lịch lấy danh sách cổ phiếu HOSE/STOCK và ghi snapshot lên MinIO.

Script: ingestion/reference/vnstock_equity_ingestion.py
  - Gọi Reference().equity.list_by_exchange(), lọc HOSE + STOCK.
  - Ghi đè snapshot: raw/reference/equity/equity.parquet (không truyền --append).
  - File này là nguồn symbol cho market_equity_summary_daily và các pipeline khác.

Lịch chạy: 00:00 UTC = 07:00 ICT hàng ngày — chạy sớm nhất trong nhóm reference.

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
    dag_id="reference_equity_daily",
    description="Daily HOSE/STOCK equity list snapshot → raw/reference/equity/equity.parquet",
    schedule_interval="0 0 * * *",  # 00:00 UTC = 07:00 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["reference", "equity", "daily"],
) as dag:

    fetch_equity_list = BashOperator(
        task_id="fetch_equity_list",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_equity_ingestion.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )
