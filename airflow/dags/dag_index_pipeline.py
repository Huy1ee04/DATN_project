"""
dag_index_pipeline.py

DAG end-to-end cho chỉ số index: Ingestion → Transform → Signals → Master → ClickHouse.
Dimensions (dim_index) được xử lý bởi dag_dimension_pipeline (07:45 ICT).

Lịch chạy:
  15:30 UTC = 22:30 ICT, T2-T6 — sau đóng cửa phiên (~15:00 ICT).

Flow:
  ┌── INGESTION ────────────────────────────────────────────────────────┐
  │  fetch_index_ohlc_intraday                                          │
  └──────────────────────────────────────────────────────────────────────┘
                              ↓
  ┌── FACT TRANSFORM ───────────────────────────────────────────────────┐
  │  S1 → S2_indicators → Master_FK                                     │
  │       └─→ index_signals: S2_signals → Master_FK                     │
  └──────────────────────────────────────────────────────────────────────┘
                              ↓
  ┌── CLICKHOUSE LOAD ──────────────────────────────────────────────────┐
  │  [ch_fact_index, ch_fact_index_signals]  (song song)                 │
  └──────────────────────────────────────────────────────────────────────┘

Note:
  - dim_index master đã có sẵn trên MinIO (xử lý bởi dag_dimension_pipeline lúc 07:45 ICT)
  - fact_master_index.py đọc dim_index master từ MinIO để resolve FK index_key

Ngày xử lý (run_date):
  - Scheduled: {{ next_ds }} = ngày phiên vừa kết thúc.
  - Manual trigger: conf {"run_date": "YYYY-MM-DD"}.

Yêu cầu (docker-compose.yml):
  - ./ingestion/market               mount tại /opt/airflow/scripts/market_ingestion
  - ./transformed                    mount tại /opt/airflow/scripts/transformed
  - ./master                         mount tại /opt/airflow/scripts/master
  - ./clickhouse_student/loader      mount tại /opt/airflow/scripts/ch_loader
  - ./.env                           mount tại /opt/airflow/.env
  - vnstock-venv volume              mount tại /opt/vnstock-venv
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from utils.slack_alert import slack_on_failure, slack_on_success

PYTHON = "/opt/vnstock-venv/bin/python"
MKT_DIR = "/opt/airflow/scripts/market_ingestion"
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
    dag_id="dag_index_pipeline",
    description=(
        "Index end-to-end: Ingestion (OHLCV) → Transform (S1→S2→signals) "
        "→ Master (FK) → ClickHouse. Dims provided by dag_dimension_pipeline."
    ),
    schedule_interval="30 15 * * 1-5",  # 15:30 UTC = 22:30 ICT, T2-T6
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pipeline", "index", "ingestion", "transform", "clickhouse", "daily"],
    params={"run_date": ""},
) as dag:

    _RUN_DATE = "{{ dag_run.conf.get('run_date') or next_ds }}"

    # ═══════════════════════════════════════════════════════════════════════
    # INGESTION — Index OHLCV
    # ═══════════════════════════════════════════════════════════════════════

    fetch_index_ohlc = BashOperator(
        task_id="fetch_index_ohlc",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MKT_DIR}/vnstock_index_ohlc_intraday_ingestion.py "
            f"--start {_RUN_DATE} "
            f"--end {_RUN_DATE} "
            "--append"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ═══════════════════════════════════════════════════════════════════════
    # FACT — Index: S1 → S2 indicators → Master
    # ═══════════════════════════════════════════════════════════════════════

    index_s1 = BashOperator(
        task_id="index_s1",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/fact_market_index.py "
            f"--run-date {_RUN_DATE} "
            "--mode append"
        ),
        execution_timeout=timedelta(hours=1),
    )

    index_s2_indicators = BashOperator(
        task_id="index_s2_indicators",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/fact_market_index.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    index_master = BashOperator(
        task_id="index_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_index.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # Ingestion → S1 → S2 → Master
    fetch_index_ohlc >> index_s1 >> index_s2_indicators >> index_master

    # ═══════════════════════════════════════════════════════════════════════
    # FACT — Index Signals: S2 → Master
    # Cần: index S2 indicators (chỉ số kỹ thuật đã tính xong)
    # ═══════════════════════════════════════════════════════════════════════

    index_signals_s2 = BashOperator(
        task_id="index_signals_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/fact_index_signals.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    index_signals_master = BashOperator(
        task_id="index_signals_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_index_signals.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    index_s2_indicators >> index_signals_s2 >> index_signals_master

    # ═══════════════════════════════════════════════════════════════════════
    # CLICKHOUSE LOAD — Index facts
    # ═══════════════════════════════════════════════════════════════════════

    all_masters_ready = EmptyOperator(task_id="all_masters_ready")
    [index_master, index_signals_master] >> all_masters_ready

    def _ch_task(task_id: str, script: str) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=f"{_SOURCE_ENV_CH} && {PYTHON} {LOADER_DIR}/{script}",
            execution_timeout=timedelta(minutes=30),
        )

    ch_fact_index = _ch_task("ch_load_fact_index", "load_fact_market_index.py")
    ch_fact_index_signals = _ch_task("ch_load_fact_index_signals", "load_fact_index_signals.py")

    all_masters_ready >> [ch_fact_index, ch_fact_index_signals]

    # ── Slack notification khi DAG hoàn thành ─────────────────────────
    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        on_success_callback=slack_on_success,
    )
    [ch_fact_index, ch_fact_index_signals] >> pipeline_done
