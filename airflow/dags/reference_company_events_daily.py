"""
reference_company_events_daily.py

DAG lập lịch lấy dữ liệu corporate events cho tất cả mã HOSE mỗi ngày.

Script: reference_ingestion/vnstock_company_events_ingestion.py
  - Gọi Reference().company(symbol).events() cho từng mã.
  - Ghi kết quả vào: raw/reference/company/events/events.parquet
  - Dùng --append: deduplicate theo (symbol, event_name, notify_date).

Lịch chạy: 01:00 UTC = 08:00 ICT hàng ngày (trước phiên giao dịch).

Yêu cầu (xem docker-compose.yml):
  - ./reference_ingestion  mount tại /opt/airflow/scripts/reference_ingestion
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
    "export MINIO_ENDPOINT=http://minio:9000 && "
    "export MPLCONFIGDIR=/tmp/mplconfig-airflow"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}

with DAG(
    dag_id="reference_company_events_daily",
    description="Daily company events ingestion (HOSE) → raw/reference/company/events/events.parquet",
    schedule_interval="0 1 * * *",  # 01:00 UTC = 08:00 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["reference", "company", "events", "daily"],
) as dag:

    fetch_company_events = BashOperator(
        task_id="fetch_company_events",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_company_events_ingestion.py "
            "--append"
        ),
        # ~700 mã × 0.5s delay/mã + retry waits
        execution_timeout=timedelta(hours=4),
    )
