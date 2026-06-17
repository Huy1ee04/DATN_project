"""
dag_dimension_pipeline.py

DAG xử lý toàn bộ Dimensions + Bridge: Transform → Master → ClickHouse.
Là DAG trung tâm cung cấp surrogate keys cho tất cả fact pipelines.

Lịch chạy:
  14:30 UTC = 21:30 ICT, hàng ngày (7/7) — SAU dag_ingestion_reference (20:30 ICT).

Flow:
  ┌── DIMENSIONS (song song) ──────────────────────────────────────────┐
  │  dim_stock:      S1 → S2 → Master                                  │
  │  dim_index:      S1 → S2 → Master                                  │
  │  dim_date_event: S1 → S2 → Master                                  │
  └─────────────┬──────────────────────────────────────────────────────┘
                ↓
  ┌── BRIDGE (cần dim_stock + dim_index masters) ──────────────────────┐
  │  bridge_stock_index: S1 → S2_SCD2 → Master                         │
  └─────────────┬──────────────────────────────────────────────────────┘
                ↓
  ┌── CLICKHOUSE LOAD ─────────────────────────────────────────────────┐
  │  [ch_dim_stock, ch_dim_index, ch_dim_date] → ch_bridge              │
  └────────────────────────────────────────────────────────────────────┘

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
    dag_id="dag_dimension_pipeline",
    description=(
        "All dimensions + bridge: Transform (S1→S2) → Master → ClickHouse. "
        "Provides surrogate keys for all fact pipelines."
    ),
    schedule_interval="30 14 * * *",  # 14:30 UTC = 21:30 ICT, hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pipeline", "dimension", "bridge", "master", "clickhouse", "daily"],
) as dag:

    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION — dim_stock (S1 → S2 → Master)
    # ═══════════════════════════════════════════════════════════════════════

    dim_stock_s1 = BashOperator(
        task_id="dim_stock_s1",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/dim_stock_info.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    dim_stock_s2 = BashOperator(
        task_id="dim_stock_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/dim_stock_info.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    dim_stock_master = BashOperator(
        task_id="dim_stock_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/dim_master_stock.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    dim_stock_s1 >> dim_stock_s2 >> dim_stock_master

    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION — dim_index (S1 → S2 → Master)
    # ═══════════════════════════════════════════════════════════════════════

    dim_index_s1 = BashOperator(
        task_id="dim_index_s1",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/dim_index_info.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    dim_index_s2 = BashOperator(
        task_id="dim_index_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/dim_index_info.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    dim_index_master = BashOperator(
        task_id="dim_index_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/dim_master_index.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    dim_index_s1 >> dim_index_s2 >> dim_index_master

    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION — dim_date_event (S1 → S2 → Master)
    # ═══════════════════════════════════════════════════════════════════════

    dim_date_s1 = BashOperator(
        task_id="dim_date_s1",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/dim_date_event.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    dim_date_s2 = BashOperator(
        task_id="dim_date_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/dim_date_event.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    dim_date_master = BashOperator(
        task_id="dim_date_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/dim_master_event.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    dim_date_s1 >> dim_date_s2 >> dim_date_master

    # ═══════════════════════════════════════════════════════════════════════
    # BRIDGE — bridge_stock_index (S1 → S2 SCD2 → Master FK)
    # Cần: dim_stock_master + dim_index_master (surrogate keys)
    # ═══════════════════════════════════════════════════════════════════════

    bridge_s1 = BashOperator(
        task_id="bridge_s1",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/bridge_stock_index.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    bridge_s2 = BashOperator(
        task_id="bridge_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/bridge_stock_index.py "
            "--date '{{ ds }}'"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    bridge_master = BashOperator(
        task_id="bridge_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/bridge_master_stock_index.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # Bridge cần dim_stock + dim_index masters (surrogate keys)
    [dim_stock_master, dim_index_master] >> bridge_s1 >> bridge_s2 >> bridge_master

    # ═══════════════════════════════════════════════════════════════════════
    # CLICKHOUSE LOAD — Dimensions + Bridge
    # ═══════════════════════════════════════════════════════════════════════

    all_masters_ready = EmptyOperator(task_id="all_masters_ready")
    [dim_stock_master, dim_index_master, dim_date_master, bridge_master] >> all_masters_ready

    def _ch_task(task_id: str, script: str) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=f"{_SOURCE_ENV_CH} && {PYTHON} {LOADER_DIR}/{script}",
            execution_timeout=timedelta(minutes=30),
        )

    ch_dim_stock = _ch_task("ch_load_dim_stock", "load_dim_stock.py")
    ch_dim_index = _ch_task("ch_load_dim_index", "load_dim_index.py")
    ch_dim_date = _ch_task("ch_load_dim_date_event", "load_dim_date_event.py")
    ch_bridge = _ch_task("ch_load_bridge", "load_bridge_stock_index.py")

    # dims song song → bridge
    all_masters_ready >> [ch_dim_stock, ch_dim_index, ch_dim_date]
    [ch_dim_stock, ch_dim_index, ch_dim_date] >> ch_bridge

    # ── Slack notification khi DAG hoàn thành ─────────────────────────
    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        on_success_callback=slack_on_success,
    )
    ch_bridge >> pipeline_done
