"""
reference_company_news_daily.py

DAG lập lịch lấy tin tức công ty cho tất cả mã HOSE mỗi ngày.

Script: reference_ingestion/vnstock_company_news_ingestion.py
  - Gọi Reference().company(symbol).news() cho từng mã.
  - Ghi vào file gốc: raw/reference/company/news/news.parquet
  - --append: read → concat → dedup (symbol, id) → ghi đè an toàn, giữ toàn bộ dữ liệu cũ.
  - --date: chỉ lưu news có public_date >= ngày đó (YYYY-MM-DD).
    - Chạy theo lịch: mặc định dùng logical date của DAG (ds).
    - Trigger tay trên UI: JSON conf {"min_public_date": "2026-05-01"}.
      Không truyền → dùng ds (cùng key với DAG events).

Lịch chạy: 02:00 UTC = 09:00 ICT hàng ngày (giờ mở cửa phiên).
  Lệch 1 giờ so với events DAG để tránh tranh chấp API rate limit.

Trigger trên Airflow UI (ví dụ):
  DAG → ▶ Trigger → “Configuration JSON”:

    {"min_public_date": "2026-05-01"}

Yêu cầu (xem docker-compose.yml):
  - ./reference_ingestion  mount tại /opt/airflow/scripts/reference_ingestion
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
    dag_id="reference_company_news_daily",
    description="Daily company news ingestion (HOSE) → raw/reference/company/news/news.parquet",
    schedule_interval="0 2 * * *",  # 02:00 UTC = 09:00 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["reference", "company", "news", "daily"],
    params={"min_public_date": ""},
) as dag:

    _NEWS_DATE = "{{ dag_run.conf.get('min_public_date') or ds }}"

    fetch_company_news = BashOperator(
        task_id="fetch_company_news",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {SCRIPT_DIR}/vnstock_company_news_ingestion.py "
            f"--append --date {_NEWS_DATE}"
        ),
        # ~700 mã × 0.5s delay + retry waits
        execution_timeout=timedelta(hours=4),
    )
