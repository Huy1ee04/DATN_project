"""
reference_company_info_daily.py

DAG lập lịch lấy thông tin tổng quan công ty (company overview) cho toàn bộ mã HOSE STOCK
qua Reference().company(symbol).info() và ghi snapshot lên MinIO.

Script: reference_ingestion/vnstock_company_info_ingestion.py
  - Lấy danh sách mã từ ref.equity.list_by_exchange() (mặc định HOSE + STOCK).
  - Ghi đè snapshot: raw/reference/company/info/info.parquet (không truyền --append).
  - Mỗi lần chạy thay thế toàn bộ file; atomic write qua .tmp trong script.

Lịch chạy: 03:00 UTC = 10:00 ICT hàng ngày — lệch sau DAG events (01 UTC) và news (02 UTC)
để giảm tranh chấp rate limit VCI.

Optional (giống market_ohlc_intraday_daily): có thể bổ sung sau conf / params
nếu script hỗ trợ lọc theo ngày; hiện tại luôn full refresh. (xem docker-compose.yml):
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
    dag_id="reference_company_info_daily",
    description="Daily company info snapshot (HOSE STOCK) → raw/reference/company/info/info.parquet",
    schedule_interval="0 3 * * *",  # 03:00 UTC = 10:00 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["reference", "company", "info", "daily"],
) as dag:

    fetch_company_info = BashOperator(
        task_id="fetch_company_info",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_company_info_ingestion.py "
            "--exchange HOSE "
            "--instrument-type STOCK"
        ),
        # ~700 mã × delay + retry VCI; tương đương events/news
        execution_timeout=timedelta(hours=4),
    )
