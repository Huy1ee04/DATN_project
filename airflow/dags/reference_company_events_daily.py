"""
reference_company_events_daily.py

DAG lập lịch lấy dữ liệu corporate events cho tất cả mã HOSE mỗi ngày.

Script: reference_ingestion/vnstock_company_events_ingestion.py
  - Gọi Reference().company(symbol).events() cho từng mã.
  - Ghi vào file gốc: raw/reference/company/events/events.parquet
  - --append: read → concat → dedup (symbol, id) → ghi đè an toàn, giữ toàn bộ dữ liệu cũ.
  - --date: chỉ lưu events có public_date >= ngày đó (YYYY-MM-DD).
    - Chạy theo lịch: mặc định dùng logical date của DAG (ds).
    - Trigger tay trên UI: truyền JSON conf {"min_public_date": "2026-05-01"} (vd. từ đầu tháng 5/2026).
      Không truyền hoặc để rỗng → vẫn dùng ds.

Lịch chạy: 01:00 UTC = 08:00 ICT hàng ngày (trước phiên giao dịch).

Trigger trên Airflow UI (ví dụ):
  DAG → ▶ Trigger → tab “Configuration JSON”:

    {"min_public_date": "2026-05-01"}

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
    params={"min_public_date": ""},
) as dag:

    # scheduled: ds | manual w/ conf: min_public_date (YYYY-MM-DD), giống pattern market_ohlc_intraday_daily
    _EVENT_DATE = "{{ dag_run.conf.get('min_public_date') or ds }}"

    fetch_company_events = BashOperator(
        task_id="fetch_company_events",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_company_events_ingestion.py "
            f"--append --date {_EVENT_DATE}"
        ),
        # ~700 mã × 0.5s delay/mã + retry waits
        execution_timeout=timedelta(hours=4),
    )
