"""
MinIO Exporter — Daily Parquet backup

ClickHouse trades_raw + alerts → MinIO (S3-compatible)
Chạy: uv run minio_exporter.py [YYYY-MM-DD]
"""

import io
import logging
import os
import sys
from datetime import datetime, timezone, timedelta

import pandas as pd
import clickhouse_connect
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger('minio_exporter')
ICT = timezone(timedelta(hours=7))


def get_env(key: str, default: str = '') -> str:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
    return os.getenv(key, default)


def export(export_date: str | None = None) -> None:
    date_str = export_date or datetime.now(ICT).strftime('%Y-%m-%d')
    logger.info(f"Exporting data for: {date_str}")

    # Connections
    ch = clickhouse_connect.get_client(
        host=get_env('CLICKHOUSE_HOST', 'localhost'),
        port=int(get_env('CLICKHOUSE_HTTP_PORT', '8123')),
        username=get_env('CLICKHOUSE_USER', 'default'),
        password=get_env('CLICKHOUSE_PASSWORD', 'default'),
        database=get_env('CLICKHOUSE_DB', 'vwap'),
    )
    mc = Minio(
        endpoint=get_env('MINIO_ENDPOINT', 'localhost:9100'),
        access_key=get_env('MINIO_ACCESS_KEY', 'minio_access_key'),
        secret_key=get_env('MINIO_SECRET_KEY', 'minio_secret_key'),
        secure=False,
    )
    bucket = get_env('MINIO_BUCKET', 'stock-data')

    # Tạo bucket nếu chưa có
    if not mc.bucket_exists(bucket):
        mc.make_bucket(bucket)
        logger.info(f"Created bucket: s3://{bucket}")

    # ── Export trades_raw ──────────────────────────────────────
    rows = ch.query(
        f"SELECT received_at, symbol, price, quantity, total_volume, board_id, market_id "
        f"FROM trades_raw WHERE toDate(received_at) = '{date_str}' "
        f"ORDER BY symbol, received_at"
    ).result_rows

    if not rows:
        logger.warning(f"No trades data for {date_str}")
    else:
        df = pd.DataFrame(rows, columns=[
            'received_at', 'symbol', 'price', 'quantity',
            'total_volume', 'board_id', 'market_id',
        ])
        _upload_parquet(mc, bucket, f'trades/date={date_str}/data.parquet', df)
        logger.info(f"Trades exported: {len(df):,} rows")

    # ── Export alerts ──────────────────────────────────────────
    alert_rows = ch.query(
        f"SELECT alert_time, symbol, alert_type, price, vwap, deviation_pct "
        f"FROM alerts WHERE toDate(alert_time) = '{date_str}' "
        f"ORDER BY alert_time"
    ).result_rows

    if not alert_rows:
        logger.info(f"No alerts for {date_str}")
    else:
        df_a = pd.DataFrame(alert_rows, columns=[
            'alert_time', 'symbol', 'alert_type', 'price',
            'vwap', 'deviation_pct',
        ])
        _upload_parquet(mc, bucket, f'alerts/date={date_str}/data.parquet', df_a)
        logger.info(f"Alerts exported: {len(df_a)} records")


def _upload_parquet(mc: Minio, bucket: str, object_name: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, engine='pyarrow', index=False, compression='snappy')
    size = buf.tell()
    buf.seek(0)
    mc.put_object(bucket, object_name, buf, size,
                  content_type='application/octet-stream')
    logger.info(f"✅ Uploaded s3://{bucket}/{object_name} ({size / 1024:.1f} KB)")


if __name__ == '__main__':
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    export(date_arg)
