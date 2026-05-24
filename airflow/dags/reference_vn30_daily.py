"""
reference_vn30_daily.py

DAG lập lịch lấy thành phần chỉ số VN30 và ghi snapshot lên MinIO.

Script: reference_ingestion/vnstock_vn30_ingestion.py
  - Gọi Reference().index.members("VN30").
  - Ghi đè snapshot: raw/reference/equity/vn30.parquet (không truyền --append).
  - Mỗi lần chạy thay thế toàn bộ file; atomic write qua .tmp trong script.

Lịch chạy: 00:30 UTC = 07:30 ICT hàng ngày — chạy sớm trước các DAG company
(events 01 UTC, news 02 UTC, info 03 UTC).

Trigger tay trên Airflow UI (tuỳ chọn chỉ số khác):
  DAG → ▶ Trigger → “Configuration JSON”:

    {"index_code": "VN30"}

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
    dag_id="reference_vn30_daily",
    description="Daily VN30 index members snapshot → raw/reference/equity/vn30.parquet",
    schedule_interval="30 0 * * *",  # 00:30 UTC = 07:30 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["reference", "equity", "vn30", "daily"],
    params={"index_code": "VN30"},
) as dag:

    _INDEX_CODE = "{{ dag_run.conf.get('index_code') or 'VN30' }}"

    fetch_vn30_members = BashOperator(
        task_id="fetch_vn30_members",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_vn30_ingestion.py "
            f"--index {_INDEX_CODE}"
        ),
        execution_timeout=timedelta(minutes=30),
    )
