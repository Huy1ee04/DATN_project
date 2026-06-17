"""
dag_index_backfill.py

DAG backfill hàng tuần: re-ingest + re-transform 7 ngày gần nhất cho index.
Mục đích: bắt dữ liệu bị thiếu do nguồn chưa cung cấp kịp.

Lịch chạy:
  03:30 UTC Chủ Nhật = 10:30 ICT Chủ Nhật (weekly).
  Lệch 30 phút so với equity backfill để tránh tranh chấp tài nguyên.

Flow:
  ┌── BACKFILL 7 NGÀY (tuần tự, tránh rate limit API) ────────────────┐
  │  Ngày D-7: ingest_index → s1_index                                 │
  │  Ngày D-6: ingest_index → s1_index                                 │
  │  ...                                                                │
  │  Ngày D-1: ingest_index → s1_index                                 │
  └──────────────────────────────────────────────────────────────────────┘
                              ↓ (tất cả xong)
  ┌── REBUILD (chạy 1 lần) ────────────────────────────────────────────┐
  │  Stage 2 indicators (full rebuild) → Master FK                      │
  │       └─→ Index signals S2 → Master FK                              │
  └──────────────────────────────────────────────────────────────────────┘
                              ↓
  ┌── CLICKHOUSE LOAD (chạy 1 lần) ────────────────────────────────────┐
  │  [ch_fact_index, ch_fact_index_signals]                              │
  └──────────────────────────────────────────────────────────────────────┘

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

BACKFILL_DAYS = 7

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
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
    "on_failure_callback": slack_on_failure,
}

with DAG(
    dag_id="dag_index_backfill",
    description=(
        f"Weekly backfill: re-ingest + re-transform last {BACKFILL_DAYS} days for index. "
        "Catches missing data from delayed sources."
    ),
    schedule_interval="30 3 * * 0",  # 03:30 UTC Chủ Nhật = 10:30 ICT
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["backfill", "index", "weekly"],
    max_active_runs=1,
) as dag:

    # ═══════════════════════════════════════════════════════════════════════
    # BACKFILL: Re-ingest + Stage 1 cho 7 ngày (tuần tự)
    # ═══════════════════════════════════════════════════════════════════════

    prev_day_done = EmptyOperator(task_id="backfill_start")
    all_s1_done = EmptyOperator(task_id="all_s1_done")

    for i in range(BACKFILL_DAYS, 0, -1):
        _DATE = f"{{{{ macros.ds_add(ds, -{i}) }}}}"
        suffix = f"d_minus_{i}"

        ingest_index = BashOperator(
            task_id=f"ingest_index_{suffix}",
            bash_command=(
                f"{_SOURCE_ENV} && "
                f"{PYTHON} {MKT_DIR}/vnstock_index_ohlc_intraday_ingestion.py "
                f"--start {_DATE} --end {_DATE} --append"
            ),
            execution_timeout=timedelta(hours=1),
        )

        s1_index = BashOperator(
            task_id=f"s1_index_{suffix}",
            bash_command=(
                f"{_SOURCE_ENV} && "
                f"{PYTHON} {TRANSFORMED_DIR}/stage_1/fact_market_index.py "
                f"--run-date {_DATE} --mode append"
            ),
            execution_timeout=timedelta(hours=1),
        )

        day_done = EmptyOperator(task_id=f"day_done_{suffix}")

        prev_day_done >> ingest_index >> s1_index >> day_done
        prev_day_done = day_done

    prev_day_done >> all_s1_done

    # ═══════════════════════════════════════════════════════════════════════
    # REBUILD: Stage 2 + Master (1 lần)
    # ═══════════════════════════════════════════════════════════════════════

    index_s2 = BashOperator(
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
            f"{PYTHON} {MASTER_DIR}/fact_master_index.py --overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

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
            f"{PYTHON} {MASTER_DIR}/fact_master_index_signals.py --overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    all_s1_done >> index_s2 >> index_master
    index_s2 >> index_signals_s2 >> index_signals_master

    # ═══════════════════════════════════════════════════════════════════════
    # CLICKHOUSE LOAD (1 lần)
    # ═══════════════════════════════════════════════════════════════════════

    all_masters_ready = EmptyOperator(task_id="all_masters_ready")
    [index_master, index_signals_master] >> all_masters_ready

    def _ch_task(task_id: str, script: str) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=f"{_SOURCE_ENV_CH} && {PYTHON} {LOADER_DIR}/{script}",
            execution_timeout=timedelta(minutes=30),
        )

    ch_index = _ch_task("ch_load_fact_index", "load_fact_market_index.py")
    ch_index_signals = _ch_task("ch_load_fact_index_signals", "load_fact_index_signals.py")

    all_masters_ready >> [ch_index, ch_index_signals]

    # ── Slack notification khi DAG hoàn thành ─────────────────────────
    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        on_success_callback=slack_on_success,
    )
    [ch_index, ch_index_signals] >> pipeline_done
