"""
market_ohlc_intraday_daily.py

DAG lập lịch lấy dữ liệu OHLCV intraday hàng ngày sau giờ đóng cửa phiên giao dịch.

Lịch chạy:
  16:00 ICT (09:00 UTC) các ngày thứ 2 – 6 (ngày giao dịch).

Ngày xử lý (run_date):
  - Scheduled run : tự động dùng {{ next_ds }} = ngày của phiên hôm đó.
  - Manual trigger: truyền conf {"run_date": "YYYY-MM-DD"} để chỉ định ngày cụ thể.

  Ví dụ trigger tay cho ngày 15/04/2026:
    Airflow UI → Trigger DAG w/ config → {"run_date": "2026-04-15"}

Task:
  fetch_equity_ohlc_intraday  — mã cơ sở HOSE (Reference API)
  fetch_index_ohlc_intraday   — chỉ số: VNINDEX, VN30, HNXINDEX, HNX30
  Hai task chạy song song, không phụ thuộc nhau.

Yêu cầu (xem docker-compose.yml):
  - ./market_ingestion  mount tại /opt/airflow/scripts/market_ingestion
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

# Sau source .env: ghi đè MinIO cho mạng Docker (localhost trong container ≠ host).
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
    dag_id="market_ohlc_intraday_daily",
    description="Daily intraday OHLCV ingestion (equity + index) — chạy sau đóng cửa phiên",
    schedule_interval="0 9 * * 1-5",  # 09:00 UTC = 16:00 ICT, thứ 2 – thứ 6
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["market", "ohlcv", "intraday", "daily"],
    params={"run_date": ""},
) as dag:

    _RUN_DATE = "{{ dag_run.conf.get('run_date') or next_ds }}"

    fetch_equity = BashOperator(
        task_id="fetch_equity_ohlc_intraday",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_equity_ohlc_intraday_ingestion.py "
            f"--start {_RUN_DATE} "
            f"--end {_RUN_DATE} "
            "--append"
        ),
        execution_timeout=timedelta(hours=3),
    )

    fetch_index = BashOperator(
        task_id="fetch_index_ohlc_intraday",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_index_ohlc_intraday_ingestion.py "
            f"--start {_RUN_DATE} "
            f"--end {_RUN_DATE} "
            "--append"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # Hai task chạy song song (không có dependency)
    [fetch_equity, fetch_index]
