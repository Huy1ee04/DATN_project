"""
dag_equity_backfill.py

DAG backfill hàng tuần: re-ingest + re-transform 7 ngày gần nhất cho equity.
Mục đích: bắt dữ liệu bị thiếu do nguồn chưa cung cấp kịp.

Lịch chạy:
  03:00 UTC Chủ Nhật = 10:00 ICT Chủ Nhật (weekly).
  Chạy vào CN vì thị trường đóng cửa → không ảnh hưởng pipeline daily.

Flow:
  ┌── BACKFILL 7 NGÀY (tuần tự, tránh rate limit API) ────────────────┐
  │  Ngày D-7: [ingest_ohlc, ingest_summary] → [s1_ohlcv, s1_summary] │
  │  Ngày D-6: [ingest_ohlc, ingest_summary] → [s1_ohlcv, s1_summary] │
  │  ...                                                                │
  │  Ngày D-1: [ingest_ohlc, ingest_summary] → [s1_ohlcv, s1_summary] │
  └──────────────────────────────────────────────────────────────────────┘
                              ↓ (tất cả xong)
  ┌── REBUILD (chạy 1 lần) ────────────────────────────────────────────┐
  │  Stage 2 indicators (full rebuild) → Master FK                      │
  │       └─→ Stock signals S2 → Master FK                              │
  └──────────────────────────────────────────────────────────────────────┘
                              ↓
  ┌── CLICKHOUSE LOAD (chạy 1 lần) ────────────────────────────────────┐
  │  [ch_fact_equity, ch_fact_stock_signals]                             │
  └──────────────────────────────────────────────────────────────────────┘

Cơ chế an toàn:
  - Stage 1 dùng --mode append + dedup → symbol+date trùng sẽ bị loại
  - Stage 2 full rebuild → indicators tính lại chính xác với data đầy đủ
  - Master --overwrite → ghi đè file master
  - ClickHouse TRUNCATE + INSERT → đồng bộ hoàn toàn

Trigger thủ công:
  Không cần config — DAG tự tính 7 ngày gần nhất từ logical date.

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

# Số ngày backfill (7 ngày = ~5 trading days)
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
    dag_id="dag_equity_backfill",
    description=(
        f"Weekly backfill: re-ingest + re-transform last {BACKFILL_DAYS} days for equity. "
        "Catches missing symbols from delayed data sources."
    ),
    schedule_interval="0 3 * * 0",  # 03:00 UTC Chủ Nhật = 10:00 ICT
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["backfill", "equity", "weekly"],
    max_active_runs=1,
) as dag:

    # ═══════════════════════════════════════════════════════════════════════
    # BACKFILL: Re-ingest + Stage 1 cho 7 ngày gần nhất (tuần tự)
    # Tuần tự để tránh rate limit API vnstock
    # ═══════════════════════════════════════════════════════════════════════

    prev_day_done = EmptyOperator(task_id="backfill_start")
    all_s1_done = EmptyOperator(task_id="all_s1_done")

    for i in range(BACKFILL_DAYS, 0, -1):
        # Jinja template: tính ngày D-i từ logical date
        _DATE = f"{{{{ macros.ds_add(ds, -{i}) }}}}"
        suffix = f"d_minus_{i}"

        # ── Ingestion: re-fetch từ API ────────────────────────────────
        ingest_ohlc = BashOperator(
            task_id=f"ingest_ohlc_{suffix}",
            bash_command=(
                f"{_SOURCE_ENV} && "
                f"{PYTHON} {MKT_DIR}/vnstock_equity_ohlc_intraday_ingestion.py "
                f"--start {_DATE} --end {_DATE} --append"
            ),
            execution_timeout=timedelta(hours=3),
        )

        ingest_summary = BashOperator(
            task_id=f"ingest_summary_{suffix}",
            bash_command=(
                f"{_SOURCE_ENV} && "
                f"{PYTHON} {MKT_DIR}/vnstock_equity_summary_ingestion.py "
                f"--date {_DATE} --append"
            ),
            execution_timeout=timedelta(hours=3),
        )

        # ── Stage 1: re-transform cho ngày đó ────────────────────────
        s1_ohlcv = BashOperator(
            task_id=f"s1_ohlcv_{suffix}",
            bash_command=(
                f"{_SOURCE_ENV} && "
                f"{PYTHON} {TRANSFORMED_DIR}/stage_1/fact_market_equity.py "
                f"--run-date {_DATE} --mode append"
            ),
            execution_timeout=timedelta(hours=2),
        )

        s1_summary = BashOperator(
            task_id=f"s1_summary_{suffix}",
            bash_command=(
                f"{_SOURCE_ENV} && "
                f"{PYTHON} {TRANSFORMED_DIR}/stage_1/fact_equity_summary.py "
                f"--run-date {_DATE} --mode append"
            ),
            execution_timeout=timedelta(hours=2),
        )

        ingest_done = EmptyOperator(task_id=f"ingest_done_{suffix}")
        day_done = EmptyOperator(task_id=f"day_done_{suffix}")

        # Dependency trong 1 ngày: ingest song song → checkpoint → s1 song song → done
        prev_day_done >> [ingest_ohlc, ingest_summary] >> ingest_done
        ingest_done >> [s1_ohlcv, s1_summary] >> day_done

        # Ngày tiếp theo chờ ngày trước xong (tuần tự giữa các ngày)
        prev_day_done = day_done

    prev_day_done >> all_s1_done

    # ═══════════════════════════════════════════════════════════════════════
    # REBUILD: Stage 2 + Master (chạy 1 lần sau khi tất cả S1 xong)
    # ═══════════════════════════════════════════════════════════════════════

    equity_s2 = BashOperator(
        task_id="equity_s2_indicators",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/fact_market_equity.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    equity_master = BashOperator(
        task_id="equity_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_equity.py --overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    signals_s2 = BashOperator(
        task_id="stock_signals_s2",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {TRANSFORMED_DIR}/stage_2/fact_stock_signals.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    signals_master = BashOperator(
        task_id="stock_signals_master",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {MASTER_DIR}/fact_master_stock_signals.py --overwrite"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    all_s1_done >> equity_s2 >> equity_master
    equity_s2 >> signals_s2 >> signals_master

    # ═══════════════════════════════════════════════════════════════════════
    # CLICKHOUSE LOAD (chạy 1 lần sau khi master xong)
    # ═══════════════════════════════════════════════════════════════════════

    all_masters_ready = EmptyOperator(task_id="all_masters_ready")
    [equity_master, signals_master] >> all_masters_ready

    def _ch_task(task_id: str, script: str) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=f"{_SOURCE_ENV_CH} && {PYTHON} {LOADER_DIR}/{script}",
            execution_timeout=timedelta(minutes=30),
        )

    ch_equity = _ch_task("ch_load_fact_equity", "load_fact_market_equity.py")
    ch_signals = _ch_task("ch_load_fact_stock_signals", "load_fact_stock_signals.py")

    all_masters_ready >> [ch_equity, ch_signals]

    # ── Slack notification khi DAG hoàn thành ─────────────────────────
    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        on_success_callback=slack_on_success,
    )
    [ch_equity, ch_signals] >> pipeline_done
