"""
market_equity_summary_daily.py

DAG lập lịch lấy Market().equity(symbol).summary() cho toàn bộ mã trong equity.parquet.

Script: market_ingestion/vnstock_equity_summary_ingestion.py
  - Đọc symbol từ raw/reference/equity/equity.parquet.
  - Ghi: raw/market/equity/summary.parquet
  - --append: merge + dedup theo symbol (row mới ưu tiên).

Lịch chạy: 01:00 UTC = 08:00 ICT hàng ngày — sau reference_equity_daily (00:00 UTC)
để đảm bảo equity.parquet đã được cập nhật.

Yêu cầu (xem docker-compose.yml):
  - ./market_ingestion  mount tại /opt/airflow/scripts/market_ingestion
  - ./reference_ingestion (equity.parquet) qua MinIO, không mount trực tiếp
  - ./vtit_gx           mount tại /opt/airflow/packages/vtit_gx (PYTHONPATH=/opt/airflow/packages)
  - ./.env              mount tại /opt/airflow/.env
  - vnstock-venv volume mount tại /opt/vnstock-venv (tạo bởi: docker compose run --rm vnstock-setup)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PYTHON = "/opt/vnstock-venv/bin/python"
SCRIPT_DIR = "/opt/airflow/scripts/market_ingestion"
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
    dag_id="market_equity_summary_daily",
    description="Daily equity summary() per symbol → raw/market/equity/summary.parquet",
    schedule_interval="0 1 * * *",  # 01:00 UTC = 08:00 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["market", "equity", "summary", "daily"],
) as dag:

    fetch_equity_summary = BashOperator(
        task_id="fetch_equity_summary",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_equity_summary_ingestion.py "
            "--append"
        ),
        # ~400 mã × delay + retry
        execution_timeout=timedelta(hours=3),
    )
