"""
dag_vwap_archiver.py

DAG quản lý vòng đời dữ liệu thời gian thực:
  1. Archive dữ liệu ClickHouse (vwap.*) theo ngày → Parquet trên MinIO
  2. Purge dữ liệu cũ hơn 30 ngày trên ClickHouse

Lịch chạy:
  16:00 UTC = 23:00 ICT, T2-T6 — sau khi pipeline equity đã hoàn thành (22:30 ICT).

Flow:
  archive_yesterday → purge_old_data → pipeline_done (Slack)

Dữ liệu xử lý:
  - trades_raw   : tick giao dịch
  - ohlc_raw     : nến OHLCV 1 phút
  - alerts       : cảnh báo v1
  - alerts_v2    : cảnh báo multi-signal

Cấu trúc MinIO output:
  realtime/{table}/date=YYYY-MM-DD/data.parquet

Ngày archive (archive_date):
  - Scheduled: {{ next_ds }} = ngày phiên vừa kết thúc.
  - Manual trigger: conf {"archive_date": "YYYY-MM-DD"}.

Yêu cầu mount (docker-compose.yml):
  - ./vwap_system/backup  tại /opt/airflow/scripts/vwap_backup (cần thêm volume)
  - Hoặc sử dụng TRANSFORMED_DIR đã mount sẵn (script nằm cùng project)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from utils.slack_alert import slack_on_failure, slack_on_success

PYTHON = "/opt/vnstock-venv/bin/python"
ENV_FILE = "/opt/airflow/.env"
BACKUP_SCRIPT = "/opt/airflow/scripts/vwap_backup/vwap_realtime_archiver.py"

# Cần export biến môi trường cho ClickHouse (trong Docker dùng tên service)
_SOURCE_ENV = (
    f"set -a && source {ENV_FILE} && set +a && "
    "export MINIO_ENDPOINT=http://minio:9100 && "
    "export CLICKHOUSE_HOST=clickhouse-01 && "
    "export CLICKHOUSE_HTTP_PORT=8123 && "
    "export CLICKHOUSE_DB=vwap"
)

RETENTION_DAYS = 30

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": slack_on_failure,
}

with DAG(
    dag_id="dag_vwap_archiver",
    description=(
        "Quản lý vòng đời dữ liệu realtime: "
        "archive ClickHouse → MinIO (Parquet) + purge dữ liệu cũ hơn 30 ngày."
    ),
    schedule_interval="0 16 * * 1-5",  # 16:00 UTC = 23:00 ICT, T2-T6
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["vwap", "archiver", "realtime", "maintenance", "daily"],
    params={"archive_date": "", "retention_days": RETENTION_DAYS},
) as dag:

    _ARCHIVE_DATE = "{{ dag_run.conf.get('archive_date') or next_ds }}"
    _RETENTION = "{{ dag_run.conf.get('retention_days', " + str(RETENTION_DAYS) + ") }}"

    # ═══════════════════════════════════════════════════════════════════════
    # Task 1: Archive dữ liệu ngày chỉ định → MinIO
    # ═══════════════════════════════════════════════════════════════════════

    archive_yesterday = BashOperator(
        task_id="archive_yesterday",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {BACKUP_SCRIPT} "
            f"--action archive "
            f"--date {_ARCHIVE_DATE}"
        ),
    )

    # ═══════════════════════════════════════════════════════════════════════
    # Task 2: Purge dữ liệu cũ hơn N ngày trên ClickHouse
    # ═══════════════════════════════════════════════════════════════════════

    purge_old_data = BashOperator(
        task_id="purge_old_data",
        bash_command=(
            f"{_SOURCE_ENV} && "
            f"{PYTHON} {BACKUP_SCRIPT} "
            f"--action purge "
            f"--retention-days {_RETENTION}"
        ),
    )

    # ═══════════════════════════════════════════════════════════════════════
    # Task 3: Checkpoint — gửi Slack thông báo hoàn thành
    # ═══════════════════════════════════════════════════════════════════════

    pipeline_done = EmptyOperator(
        task_id="pipeline_done",
        on_success_callback=slack_on_success,
    )

    # ── Flow ─────────────────────────────────────────────────────────────
    archive_yesterday >> purge_old_data >> pipeline_done
