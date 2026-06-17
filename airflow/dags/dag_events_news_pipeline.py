"""
dag_events_news_pipeline.py

DAG xử lý Events + News: Transform → Master → ClickHouse.
Ingestion đã được thực hiện bởi dag_ingestion_reference (20:00 ICT).
Dimensions đã sẵn sàng bởi dag_dimension_pipeline (21:30 ICT).

Lịch chạy:
  15:30 UTC = 22:30 ICT, T2-T6, hàng ngày — SAU reference ingestion (~20:30-23:00 ICT)
  và dimension pipeline (~21:15-22:00 ICT) hoàn tất.

Flow:
  ┌── EVENTS ──────────────────────────────────────────────────────────┐
  │  events_s1 → events_s2 → events_master                             │
  └──────────────────────────────────────────────────────────────────────┘
  ┌── NEWS ────────────────────────────────────────────────────────────┐
  │  news_s1 → news_s2 → news_master                                   │
  └──────────────────────────────────────────────────────────────────────┘
                              ↓ (song song)
  ┌── CLICKHOUSE LOAD ──────────────────────────────────────────────────┐
  │  [ch_fact_events, ch_fact_news]  (song song)                         │
  └──────────────────────────────────────────────────────────────────────┘

Note:
  - dim_stock_master + dim_date_master đã sẵn trên MinIO (21:30 ICT)
  - fact_master_events.py / fact_master_news.py đọc dim masters từ MinIO
    để resolve FK stock_key + date_key

Yêu cầu (docker-compose.yml):
  - ./transformed               mount tại /opt/airflow/scripts/transformed
  - ./master                    mount tại /opt/airflow/scripts/master
  - ./clickhouse_student/loader mount tại /opt/airflow/scripts/ch_loader
  - ./.env                      mount tại /opt/airflow/.env
  - vnstock-venv                mount tại /opt/vnstock-venv
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from utils.slack_alert import slack_on_failure, slack_on_success

PYTHON = "/opt/vnstock-venv/bin/python"
TRANSFORMED_DIR = "/opt/airflow/scripts/transformed"
MASTER_DIR = "/opt/airflow/scripts/master"
LOADER_DIR = "/opt/airflow/scripts/ch_loader"
ENV_FILE = "/opt/airflow/.env"

_SOURCE_ENV = (
    f"set -a && source {ENV_FILE} && set +a && "
    "export HOME=/opt/vnstock-home && "
    "export MINIO_ENDPOINT=http://minio:9100 && "
    "export MPLCONFIGDIR=/tmp/mplconfig-airflow"
)

_SOURCE_ENV_CH = (
    f"{_SOURCE_ENV} && "
    "export CLICKHOUSE_HOST=clickhouse-01 && "
    "export CLICKHOUSE_HTTP_PORT=8123"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "on_failure_callback": slack_on_failure,
}

with DAG(
    dag_id="dag_events_news_pipeline",
    description=(
        "Events + News: Transform (S1→S2) → Master (FK) → ClickHouse. "
        "Ingestion by dag_ingestion_reference. Dims by dag_dimension_pipeline."
    ),
    schedule_interval="30 15 * * 1-5",  # 15:30 UTC = 22:30 ICT, T2-T6, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pipeline", "events", "news", "transform", "clickhouse", "daily"],
) as dag:

    # ═══════════════════════════════════════════════════════════════════════
    # FACT — Events: S1 → S2 → Master
    # ═══════════════════════════════════════════════════════════════════════

    events_s1 = BashOperator(
        task_id="events_s1",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/fact_stock_events.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    events_s2 = BashOperator(
        task_id="events_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/fact_stock_events.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    events_master = BashOperator(
        task_id="events_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_events.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    events_s1 >> events_s2 >> events_master

    # ═══════════════════════════════════════════════════════════════════════
    # FACT — News: S1 → S2 → Master
    # ═══════════════════════════════════════════════════════════════════════

    news_s1 = BashOperator(
        task_id="news_s1",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/fact_stock_news.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    news_s2 = BashOperator(
        task_id="news_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/fact_stock_news.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    news_master = BashOperator(
        task_id="news_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_news.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    news_s1 >> news_s2 >> news_master

    # ═══════════════════════════════════════════════════════════════════════
    # CLICKHOUSE LOAD — Events + News facts
    # ═══════════════════════════════════════════════════════════════════════

    all_masters_ready = EmptyOperator(task_id="all_masters_ready")
    [events_master, news_master] >> all_masters_ready

    def _ch_task(task_id: str, script: str) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=f"{_SOURCE_ENV_CH} && {PYTHON} {LOADER_DIR}/{script}",
            execution_timeout=timedelta(minutes=30),
        )

    ch_events = _ch_task("ch_load_fact_events", "load_fact_stock_events.py")
    ch_news = _ch_task("ch_load_fact_news", "load_fact_stock_news.py")

    all_masters_ready >> [ch_events, ch_news]

    # ── Slack notification khi DAG hoàn thành ─────────────────────────
    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        on_success_callback=slack_on_success,
    )
    [ch_events, ch_news] >> pipeline_done
