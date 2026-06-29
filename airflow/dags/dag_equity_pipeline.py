"""
dag_equity_pipeline.py

DAG end-to-end cho cổ phiếu: Ingestion → Transform → Signals → Master → ClickHouse.
Dimensions (dim_stock) được xử lý bởi dag_dimension_pipeline (07:45 ICT).

Lịch chạy:
  15:30 UTC = 22:30 ICT, T2-T6 — sau đóng cửa phiên (~15:00 ICT).

Flow:
  ┌── INGESTION ────────────────────────────────────────────────────────┐
  │  [fetch_equity_ohlc, fetch_equity_summary]  (song song)             │
  └──────────────────────────────────────────────────────────────────────┘
                              ↓
  ┌── FACT TRANSFORM ───────────────────────────────────────────┐
  │  S1_OHLCV + S1_Summary → S2_indicators → Master_FK                 │
  │       └─→ stock_signals: S2_signals → Master_FK                     │
  │       └─→ sector_agg:   Master_FK → fact_master_sector              │
  └──────────────────────────────────────────────────────────────┘────────┘
                              ↓
  ┌── CLICKHOUSE LOAD ──────────────────────────────────────────┐
  │  [ch_fact_equity, ch_fact_stock_signals, ch_fact_sector]              │
  └──────────────────────────────────────────────────────────────┘────────┘

Note:
  - dim_stock master đã có sẵn trên MinIO (xử lý bởi dag_dimension_pipeline lúc 07:45 ICT)
  - fact_master_equity.py và fact_master_stock_signals.py đọc dim_stock master từ MinIO
    để resolve FK stock_key → không cần chạy lại dim trong DAG này

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
    dag_id="dag_equity_pipeline",
    description=(
        "Equity end-to-end: Ingestion (OHLCV+Summary) → Transform (S1→S2→signals) "
        "→ Master (FK) → ClickHouse. Dims provided by dag_dimension_pipeline."
    ),
    schedule_interval="30 15 * * 1-5",  # 15:30 UTC = 22:30 ICT, T2-T6
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pipeline", "equity", "ingestion", "transform", "clickhouse", "daily"],
    params={"run_date": ""},
) as dag:

    _RUN_DATE = "{{ dag_run.conf.get('run_date') or next_ds }}"

    # ═══════════════════════════════════════════════════════════════════════
    # INGESTION — Equity OHLCV + Summary (song song)
    # ═══════════════════════════════════════════════════════════════════════

    fetch_equity_ohlc = BashOperator(
        task_id="fetch_equity_ohlc",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MKT_DIR}/vnstock_equity_ohlc_intraday_ingestion.py "
            f"--start {_RUN_DATE} "
            f"--end {_RUN_DATE} "
            "--append"
        ),
        execution_timeout=timedelta(hours=3),
    )

    fetch_equity_summary = BashOperator(
        task_id="fetch_equity_summary",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MKT_DIR}/vnstock_equity_summary_ingestion.py "
            f"--date {_RUN_DATE} --append"
        ),
        execution_timeout=timedelta(hours=3),
    )

    ingestion_done = EmptyOperator(task_id="ingestion_done")
    [fetch_equity_ohlc, fetch_equity_summary] >> ingestion_done

    # ═══════════════════════════════════════════════════════════════════════
    # FACT — Equity: S1 OHLCV + S1 Summary → S2 indicators → Master
    # ═══════════════════════════════════════════════════════════════════════

    equity_s1_ohlcv = BashOperator(
        task_id="equity_s1_ohlcv",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/fact_market_equity.py "
            f"--run-date {_RUN_DATE} "
            "--mode append"
        ),
        execution_timeout=timedelta(hours=2),
    )

    equity_s1_summary = BashOperator(
        task_id="equity_s1_summary",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_1/fact_equity_summary.py "
            f"--run-date {_RUN_DATE} "
            "--mode append"
        ),
        execution_timeout=timedelta(hours=2),
    )

    equity_s2_indicators = BashOperator(
        task_id="equity_s2_indicators",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/fact_market_equity.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    equity_master = BashOperator(
        task_id="equity_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_equity.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # Ingestion xong → S1 song song → S2 → Master
    ingestion_done >> [equity_s1_ohlcv, equity_s1_summary]
    [equity_s1_ohlcv, equity_s1_summary] >> equity_s2_indicators >> equity_master

    # ═══════════════════════════════════════════════════════════════════════
    # FACT — Stock Signals: S2 → Master
    # Cần: equity S2 (chỉ số kỹ thuật đã tính xong)
    # dim_stock S2 đã sẵn trên MinIO (07:45 ICT bởi dag_dimension_pipeline)
    # ═══════════════════════════════════════════════════════════════════════

    stock_signals_s2 = BashOperator(
        task_id="stock_signals_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/fact_stock_signals.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    stock_signals_master = BashOperator(
        task_id="stock_signals_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_stock_signals.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    equity_s2_indicators >> stock_signals_s2 >> stock_signals_master

    # ═══════════════════════════════════════════════════════════════════════
    # FACT — Sector Aggregate: equity_master → fact_master_sector
    # Cần: equity_master (dữ liệu equity đã có FK)
    # dim_stock + dim_sector masters đã sẵn trên MinIO (từ dag_dimension_pipeline)
    # ═══════════════════════════════════════════════════════════════════════

    sector_master = BashOperator(
        task_id="sector_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_sector.py "
            "--overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    equity_master >> sector_master

    # ═══════════════════════════════════════════════════════════════════════
    # CLICKHOUSE LOAD — Equity facts
    # ═══════════════════════════════════════════════════════════════════════

    all_masters_ready = EmptyOperator(task_id="all_masters_ready")
    [equity_master, stock_signals_master, sector_master] >> all_masters_ready

    def _ch_task(task_id: str, script: str) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=f"{_SOURCE_ENV_CH} && {PYTHON} {LOADER_DIR}/{script}",
            execution_timeout=timedelta(minutes=30),
        )

    ch_fact_equity = _ch_task("ch_load_fact_equity", "load_fact_market_equity.py")
    ch_fact_signals = _ch_task("ch_load_fact_stock_signals", "load_fact_stock_signals.py")
    ch_fact_sector = _ch_task("ch_load_fact_sector", "load_fact_market_sector.py")

    all_masters_ready >> [ch_fact_equity, ch_fact_signals, ch_fact_sector]

    # ── Slack notification khi DAG hoàn thành ─────────────────────────
    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        on_success_callback=slack_on_success,
    )
    [ch_fact_equity, ch_fact_signals, ch_fact_sector] >> pipeline_done
