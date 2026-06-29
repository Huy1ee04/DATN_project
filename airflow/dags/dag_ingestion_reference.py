"""
dag_ingestion_reference.py

DAG tổng hợp: Ingest toàn bộ reference data (danh sách mã, events, news, info, index members).

Lịch chạy:
  13:00 UTC = 20:00 ICT hàng ngày (7 ngày/tuần).

Flow:
  fetch_equity_list ──→ [fetch_index_members,
                         fetch_market_events,
                         fetch_company_events,
                         fetch_company_news,
                         fetch_company_info]   (song song)

Lý do equity_list chạy trước:
  - Nhiều script downstream (events, news, info) đọc danh sách mã từ
    raw/reference/equity/equity.parquet — phải có sẵn.

Ngày xử lý:
  - Scheduled: ds (logical date).
  - Manual trigger: conf {"min_public_date": "YYYY-MM-DD"} cho events/news.

Yêu cầu (xem docker-compose.yml):
  - ./ingestion/market     mount tại /opt/airflow/scripts/market_ingestion
  - ./ingestion/reference  mount tại /opt/airflow/scripts/reference_ingestion
  - ./.env                 mount tại /opt/airflow/.env
  - vnstock-venv volume    mount tại /opt/vnstock-venv
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from utils.slack_alert import slack_on_failure, slack_on_success

PYTHON = "/opt/vnstock-venv/bin/python"
REF_DIR = "/opt/airflow/scripts/reference_ingestion"
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
    "on_failure_callback": slack_on_failure,
}

with DAG(
    dag_id="dag_ingestion_reference",
    description="Daily reference data ingestion: equity list, index members, events, news, company info",
    schedule_interval="0 13 * * *",  # 13:00 UTC = 20:00 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "reference", "daily", "orchestration"],
    params={"min_public_date": ""},
) as dag:

    _EVENT_NEWS_DATE = "{{ dag_run.conf.get('min_public_date') or ds }}"

    # ── Step 1: Equity list (phải chạy trước — downstream đọc file này) ──
    fetch_equity_list = BashOperator(
        task_id="fetch_equity_list",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {REF_DIR}/vnstock_equity_ingestion.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ── Step 2: Các reference khác (song song) ────────────────────────────
    fetch_index_members = BashOperator(
        task_id="fetch_index_members",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {REF_DIR}/vnstock_index_members_ingestion.py "
            "--indices 'VN30,VN100'"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    fetch_market_events = BashOperator(
        task_id="fetch_market_events",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {REF_DIR}/vnstock_event_ingestion.py "
            "--append"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    fetch_company_events = BashOperator(
        task_id="fetch_company_events",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {REF_DIR}/vnstock_company_events_ingestion.py "
            f"--append --date {_EVENT_NEWS_DATE}"
        ),
        execution_timeout=timedelta(hours=4),
    )

    fetch_company_news = BashOperator(
        task_id="fetch_company_news",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {REF_DIR}/vnstock_company_news_ingestion.py "
            f"--append --date {_EVENT_NEWS_DATE}"
        ),
        execution_timeout=timedelta(hours=4),
    )

    fetch_company_info = BashOperator(
        task_id="fetch_company_info",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {REF_DIR}/vnstock_company_info_ingestion.py "
            "--exchange HOSE "
            "--instrument-type STOCK"
        ),
        execution_timeout=timedelta(hours=4),
    )

    fetch_sector_info = BashOperator(
        task_id="fetch_sector_info",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {REF_DIR}/sector_info_ingestion.py"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    # equity_list phải xong trước (downstream đọc danh sách mã từ file này)
    fetch_equity_list >> [
        fetch_index_members,
        fetch_market_events,
        fetch_company_events,
        fetch_company_news,
        fetch_company_info,
        fetch_sector_info,
    ]

    # ── Slack notification khi DAG hoàn thành ─────────────────────────
    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        on_success_callback=slack_on_success,
    )
    [
        fetch_index_members,
        fetch_market_events,
        fetch_company_events,
        fetch_company_news,
        fetch_company_info,
        fetch_sector_info,
    ] >> pipeline_done
