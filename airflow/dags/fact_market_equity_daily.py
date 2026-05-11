"""
fact_market_equity_daily.py

DAG transform dữ liệu raw intraday equity lên transformed daily theo từng ngày.

Lịch chạy:
  18:00 ICT (11:00 UTC) các ngày thứ 2 – 6.

Ngày xử lý (run_date):
  - Scheduled run : dùng {{ next_ds }}.
  - Manual trigger: truyền conf {"run_date": "YYYY-MM-DD"}.

Ví dụ trigger tay:
  {"run_date": "2026-04-16"}

Yêu cầu (xem docker-compose.yml):
  - ./transformed       mount tại /opt/airflow/scripts/transformed
  - ./.env              mount tại /opt/airflow/.env
  - vnstock-venv volume mount tại /opt/vnstock-venv (tạo bởi: docker compose run --rm vnstock-setup)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PYTHON = "/opt/vnstock-venv/bin/python"
SCRIPT_DIR = "/opt/airflow/scripts/transformed"
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
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="fact_market_equity_daily",
    description="Daily transform raw market equity intraday -> transformed fact market equity daily",
    schedule_interval="0 11 * * 1-5",  # 11:00 UTC = 18:00 ICT, thứ 2 – thứ 6
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["transformed", "fact", "market", "equity", "daily"],
    params={"run_date": ""},
) as dag:

    _RUN_DATE = "{{ dag_run.conf.get('run_date') or next_ds }}"

    transform_equity_daily = BashOperator(
        task_id="transform_equity_daily",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/fact_market_equity_daily.py "
            f"--run-date {_RUN_DATE} "
            "--overwrite"
        ),
        execution_timeout=timedelta(hours=2),
    )
