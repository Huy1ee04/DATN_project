#!/usr/bin/env python3
"""
vwap_realtime_archiver.py

Quản lý vòng đời dữ liệu thời gian thực:
  1. Archive: Export dữ liệu ClickHouse (vwap.*) theo ngày → Parquet trên MinIO
  2. Purge:   Xóa dữ liệu cũ hơn N ngày trên ClickHouse (chỉ xóa khi đã archive)

Bảng xử lý:
  - trades_raw   (partition by toDate(received_at))
  - ohlc_raw     (partition by toDate(candle_time))
  - alerts       (partition by toDate(alert_time))   — không có partition key, dùng ALTER DELETE
  - alerts_v2    (không partition)                    — dùng ALTER DELETE

Cấu trúc MinIO:
  realtime/{table}/date=YYYY-MM-DD/data.parquet

Usage:
  python vwap_realtime_archiver.py --action archive --date 2024-06-01
  python vwap_realtime_archiver.py --action purge   --retention-days 30
  python vwap_realtime_archiver.py --action full     --date 2024-06-01 --retention-days 30
"""

import io
import os
import sys
import logging
import argparse
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("vwap_realtime_archiver")
ICT = timezone(timedelta(hours=7))

# ── Env ──────────────────────────────────────────────────────────────────────

_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in [
    os.path.join(_script_dir, "..", "..", ".env"),
    os.path.join(_script_dir, "..", ".env"),
    os.path.join(_script_dir, ".env"),
]:
    if os.path.isfile(_env_path):
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=_env_path)
        break

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "vwap")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

# Prefix trên MinIO cho dữ liệu realtime
MINIO_PREFIX = "realtime"

# ── Bảng cần xử lý ──────────────────────────────────────────────────────────

# Mỗi entry: (table_name, date_column, has_partition)
# has_partition = True  → dùng DROP PARTITION (nhanh, instant)
# has_partition = False → dùng ALTER TABLE DELETE (mutation, chậm hơn nhưng an toàn)
TABLES = [
    ("trades_raw",  "received_at",  True),
    ("ohlc_raw",    "candle_time",  True),
    ("alerts",      "alert_time",   False),
    ("alerts_v2",   "alert_time",   False),
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_ch() -> clickhouse_connect.driver.Client:
    """Tạo ClickHouse client."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def _build_fs() -> s3fs.S3FileSystem:
    """Tạo S3FileSystem cho MinIO."""
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def _s3_path(table: str, date_str: str) -> str:
    """Đường dẫn S3 cho file archive."""
    return f"{MINIO_BUCKET}/{MINIO_PREFIX}/{table}/date={date_str}/data.parquet"


def _archive_exists(fs: s3fs.S3FileSystem, table: str, date_str: str) -> bool:
    """Kiểm tra file archive đã tồn tại trên MinIO."""
    return fs.exists(_s3_path(table, date_str))


# ── Archive ──────────────────────────────────────────────────────────────────

def archive_table(
    ch: clickhouse_connect.driver.Client,
    fs: s3fs.S3FileSystem,
    table: str,
    date_col: str,
    date_str: str,
    overwrite: bool = False,
) -> int:
    """
    Export dữ liệu 1 bảng theo 1 ngày → Parquet trên MinIO.
    Returns: số rows đã export.
    """
    s3_dest = _s3_path(table, date_str)

    # Kiểm tra đã archive chưa
    if not overwrite and fs.exists(s3_dest):
        logger.info(f"  [{table}] Đã archive: s3://{s3_dest} — bỏ qua.")
        return -1  # -1 = đã tồn tại

    # Query dữ liệu từ ClickHouse
    query = (
        f"SELECT * FROM {table} "
        f"WHERE toDate({date_col}) = '{date_str}' "
        f"ORDER BY {date_col}"
    )
    logger.info(f"  [{table}] Query: {query}")

    result = ch.query(query)
    if not result.result_rows:
        logger.info(f"  [{table}] Không có dữ liệu cho {date_str}.")
        return 0

    # Chuyển sang Polars DataFrame
    df = pl.DataFrame(
        {
            col_name: [row[i] for row in result.result_rows]
            for i, col_name in enumerate(result.column_names)
        }
    )

    # Ghi Parquet lên MinIO
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)

    with fs.open(s3_dest, "wb") as f:
        f.write(buf.read())

    size_kb = (fs.size(s3_dest) or 0) / 1024
    logger.info(
        f"  [{table}] ✅ Archived {df.shape[0]:,} rows → s3://{s3_dest} ({size_kb:.1f} KB)"
    )
    return df.shape[0]


def archive(date_str: str, overwrite: bool = False) -> dict:
    """Archive tất cả bảng cho 1 ngày."""
    logger.info(f"{'='*70}")
    logger.info(f"ARCHIVE: {date_str}")
    logger.info(f"{'='*70}")

    ch = _build_ch()
    fs = _build_fs()

    results = {}
    for table, date_col, _ in TABLES:
        try:
            rows = archive_table(ch, fs, table, date_col, date_str, overwrite)
            results[table] = rows
        except Exception as e:
            logger.error(f"  [{table}] ❌ Lỗi archive: {e}")
            results[table] = -2  # -2 = lỗi

    ch.close()

    # Tổng kết
    logger.info(f"\n{'─'*50}")
    logger.info("Kết quả archive:")
    for table, rows in results.items():
        if rows == -1:
            status = "đã tồn tại (bỏ qua)"
        elif rows == -2:
            status = "LỖI"
        elif rows == 0:
            status = "không có dữ liệu"
        else:
            status = f"{rows:,} rows"
        logger.info(f"  {table:20s} → {status}")
    logger.info(f"{'─'*50}")

    return results


# ── Purge ────────────────────────────────────────────────────────────────────

def purge_table(
    ch: clickhouse_connect.driver.Client,
    fs: s3fs.S3FileSystem,
    table: str,
    date_col: str,
    has_partition: bool,
    date_str: str,
) -> bool:
    """
    Xóa dữ liệu 1 bảng cho 1 ngày trên ClickHouse.
    Chỉ xóa nếu đã archive trên MinIO (safety check).
    Returns: True nếu đã xóa.
    """
    # Safety check: phải có archive trên MinIO
    if not _archive_exists(fs, table, date_str):
        logger.warning(
            f"  [{table}] ⚠️  Archive chưa tồn tại cho {date_str} — KHÔNG xóa."
        )
        return False

    # Đếm rows trước khi xóa
    count_query = (
        f"SELECT count() FROM {table} WHERE toDate({date_col}) = '{date_str}'"
    )
    count = ch.query(count_query).result_rows[0][0]

    if count == 0:
        logger.info(f"  [{table}] Không có dữ liệu cho {date_str} trên ClickHouse.")
        return True

    # Xóa dữ liệu
    if has_partition:
        # DROP PARTITION — instant, hiệu quả cho bảng có partition by toDate()
        drop_stmt = f"ALTER TABLE {table} DROP PARTITION '{date_str}'"
        logger.info(f"  [{table}] DROP PARTITION '{date_str}' ({count:,} rows)")
    else:
        # ALTER DELETE — mutation, dùng cho bảng không có partition phù hợp
        drop_stmt = (
            f"ALTER TABLE {table} DELETE WHERE toDate({date_col}) = '{date_str}'"
        )
        logger.info(f"  [{table}] ALTER DELETE toDate({date_col}) = '{date_str}' ({count:,} rows)")

    ch.command(drop_stmt)
    logger.info(f"  [{table}] ✅ Đã xóa dữ liệu ngày {date_str}.")
    return True


def purge(retention_days: int = 30) -> dict:
    """Purge dữ liệu cũ hơn retention_days trên tất cả bảng."""
    cutoff_date = datetime.now(ICT).date() - timedelta(days=retention_days)

    logger.info(f"{'='*70}")
    logger.info(f"PURGE: retention={retention_days} ngày, cutoff={cutoff_date}")
    logger.info(f"{'='*70}")

    ch = _build_ch()
    fs = _build_fs()

    results = {}
    for table, date_col, has_partition in TABLES:
        # Tìm các ngày có dữ liệu cũ hơn cutoff
        dates_query = (
            f"SELECT DISTINCT toDate({date_col}) AS d FROM {table} "
            f"WHERE toDate({date_col}) < '{cutoff_date}' "
            f"ORDER BY d"
        )
        try:
            date_rows = ch.query(dates_query).result_rows
        except Exception as e:
            logger.error(f"  [{table}] ❌ Lỗi query ngày: {e}")
            results[table] = {"error": str(e)}
            continue

        if not date_rows:
            logger.info(f"  [{table}] Không có dữ liệu cũ hơn {cutoff_date}.")
            results[table] = {"purged_days": 0}
            continue

        dates = [row[0].strftime("%Y-%m-%d") if hasattr(row[0], 'strftime') else str(row[0]) for row in date_rows]
        logger.info(f"  [{table}] Tìm thấy {len(dates)} ngày cần purge: {dates[0]} → {dates[-1]}")

        purged = 0
        skipped = 0
        for d in dates:
            try:
                ok = purge_table(ch, fs, table, date_col, has_partition, d)
                if ok:
                    purged += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"  [{table}] ❌ Lỗi purge ngày {d}: {e}")
                skipped += 1

        results[table] = {"purged_days": purged, "skipped_days": skipped}

    ch.close()

    # Tổng kết
    logger.info(f"\n{'─'*50}")
    logger.info("Kết quả purge:")
    for table, info in results.items():
        if "error" in info:
            logger.info(f"  {table:20s} → LỖI: {info['error']}")
        else:
            logger.info(
                f"  {table:20s} → {info['purged_days']} ngày đã xóa, "
                f"{info.get('skipped_days', 0)} ngày bỏ qua"
            )
    logger.info(f"{'─'*50}")

    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="VWAP Realtime Data Archiver — Archive ClickHouse → MinIO + Purge old data"
    )
    p.add_argument(
        "--action",
        choices=["archive", "purge", "full"],
        required=True,
        help="archive: export ngày chỉ định. purge: xóa dữ liệu cũ. full: archive + purge.",
    )
    p.add_argument(
        "--date",
        default=None,
        help="Ngày cần archive (YYYY-MM-DD). Mặc định: hôm qua.",
    )
    p.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Số ngày giữ lại trên ClickHouse (mặc định: 30).",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Ghi đè file archive nếu đã tồn tại.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Mặc định archive ngày hôm qua
    if args.date:
        date_str = args.date
    else:
        date_str = (datetime.now(ICT).date() - timedelta(days=1)).strftime("%Y-%m-%d")

    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S ICT")
    separator = "=" * 70
    logger.info(
        f"\n{separator}\n"
        f"VWAP Realtime Archiver\n"
        f"{separator}\n"
        f"Action         : {args.action}\n"
        f"Date           : {date_str}\n"
        f"Retention      : {args.retention_days} ngày\n"
        f"Overwrite      : {args.overwrite}\n"
        f"ClickHouse     : {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}\n"
        f"MinIO          : {MINIO_ENDPOINT}/{MINIO_BUCKET}/{MINIO_PREFIX}/\n"
        f"Run at         : {run_at}\n"
        f"{separator}"
    )

    if args.action == "archive":
        archive(date_str, overwrite=args.overwrite)

    elif args.action == "purge":
        purge(retention_days=args.retention_days)

    elif args.action == "full":
        logger.info("Mode: FULL (archive → purge)")
        archive(date_str, overwrite=args.overwrite)
        purge(retention_days=args.retention_days)

    logger.info(f"\n{separator}\nVWAP Realtime Archiver complete!\n{separator}")


if __name__ == "__main__":
    main()
