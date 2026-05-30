#!/usr/bin/env python3
"""
dim_date_event_transform.py

Transform: left join date dimension với market events.

Input (MinIO):
  raw/date_2024_2026.parquet          → bảng ngày (date dimension)
  raw/reference/event/event.parquet   → market events

Join: left join on key "date"

Output (MinIO):
  transformed/dimension/dim_date_event.parquet
  (không giữ ingested_at từ raw;
   cột week → cal_week; event_type → is_holiday (1 nếu event_type có giá trị, 0 nếu không);
   is_holiday + is_weekend → is_day_off; event_name cuối tuần → 'Cuối tuần';
   không giữ cột duration)
"""

import io
import os
import logging
import argparse
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

_script_dir = os.path.dirname(os.path.abspath(__file__))

# ── Load .env ────────────────────────────────────────────────────────────────
for _env_path in [
    os.path.join(_script_dir, ".env"),
    os.path.join(_script_dir, "..", ".env"),
]:
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("dim_date_event_transform")
ICT = timezone(timedelta(hours=7))

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET   = os.getenv("MINIO_BUCKET", "stock-data")

SRC_DATE_PATH  = "raw/date_2024_2026.parquet"
SRC_EVENT_PATH = "raw/reference/event/event.parquet"
DST_PREFIX     = "transformed/dimension"
DST_FILENAME   = "dim_date_event.parquet"

JOIN_KEY = "date"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    """Đọc parquet từ MinIO vào Polars DataFrame."""
    logger.info(f"Reading s3://{s3_path} ...")
    with fs.open(s3_path, "rb") as f:
        raw = f.read()
    df = pl.read_parquet(io.BytesIO(raw))
    logger.info(f"  → {df.shape[0]:,} rows × {df.shape[1]} cols | schema: {df.schema}")
    return df


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    overwrite: bool = False,
) -> None:
    if fs.exists(s3_path) and not overwrite:
        logger.info(f"Exists, skipping: s3://{s3_path}  (dùng --overwrite để ghi đè)")
        return
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"Saved s3://{s3_path} ({size_kb:.1f} KB, {df.shape[0]:,} rows)")


# ── Transform logic ──────────────────────────────────────────────────────────

def transform(
    df_date: pl.DataFrame,
    df_event: pl.DataFrame,
) -> pl.DataFrame:
    """
    Left join df_date với df_event trên key "date".

    - df_date  : bảng ngày (date dimension), mỗi ngày 1 row
    - df_event : bảng sự kiện thị trường, có thể nhiều event/ngày

    Sau join: bỏ cột ingested_at từ nguồn (nếu có); đổi week → cal_week; thay event_type
    bằng is_holiday (1 khi event_type khác null và khác chuỗi rỗng sau trim, 0 ngược lại);
    gộp is_holiday và is_weekend thành is_day_off; event_name ngày cuối tuần ghi 'Cuối tuần';
    bỏ duration.
    """
    # Đảm bảo cột join cùng kiểu dữ liệu (Date)
    # Xử lý nhiều trường hợp kiểu dữ liệu của cột date trong parquet nguồn:
    #   - Int64/Int32  : Unix timestamp in milliseconds (e.g. 1704067200000 = 2024-01-01)
    #   - Datetime     : cast trực tiếp sang Date
    #   - String/Utf8  : parse từ chuỗi "YYYY-MM-DD"
    #   - Date         : giữ nguyên
    def to_date_col(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
        if col_name not in df.columns:
            return df
        dtype = df[col_name].dtype
        if dtype == pl.Date:
            return df
        if dtype in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
            # Unix timestamp milliseconds → Datetime(ms) → Date
            return df.with_columns(
                pl.from_epoch(pl.col(col_name), time_unit="ms").cast(pl.Date)
            )
        if dtype in (pl.Datetime, pl.Datetime("ms"), pl.Datetime("us"), pl.Datetime("ns")):
            return df.with_columns(pl.col(col_name).cast(pl.Date))
        if dtype in (pl.Utf8, pl.String):
            return df.with_columns(pl.col(col_name).str.to_date("%Y-%m-%d"))
        # fallback
        return df.with_columns(pl.col(col_name).cast(pl.Date))

    def drop_ingested_columns(df: pl.DataFrame) -> pl.DataFrame:
        drop = [
            c
            for c in df.columns
            if c == "ingested_at" or c.startswith("ingested_at_")
        ]
        return df.drop(drop) if drop else df

    df_date = drop_ingested_columns(to_date_col(df_date, JOIN_KEY))
    df_event = drop_ingested_columns(to_date_col(df_event, JOIN_KEY))

    logger.info(f"Left joining on key='{JOIN_KEY}' ...")
    df_joined = df_date.join(df_event, on=JOIN_KEY, how="left")
    df_joined = drop_ingested_columns(df_joined)

    if "week" in df_joined.columns:
        df_joined = df_joined.rename({"week": "cal_week"})

    if "event_type" in df_joined.columns:
        et = pl.col("event_type")
        df_joined = df_joined.with_columns(
            pl.when(et.is_not_null())
            .then(et.cast(pl.Utf8, strict=False).str.strip_chars() != "")
            .otherwise(False)
            .cast(pl.Int8)
            .alias("is_holiday")
        ).drop("event_type")
    else:
        df_joined = df_joined.with_columns(pl.lit(0).cast(pl.Int8).alias("is_holiday"))

    if "is_weekend" not in df_joined.columns:
        df_joined = df_joined.with_columns(
            (pl.col(JOIN_KEY).dt.weekday() >= 6).cast(pl.Int8).alias("is_weekend")
        )
    else:
        df_joined = df_joined.with_columns(pl.col("is_weekend").cast(pl.Int8))

    weekend = pl.col("is_weekend") == 1
    if "event_name" in df_joined.columns:
        df_joined = df_joined.with_columns(
            pl.when(weekend)
            .then(pl.lit("Cuối tuần"))
            .otherwise(pl.col("event_name"))
            .alias("event_name")
        )
    else:
        df_joined = df_joined.with_columns(
            pl.when(weekend).then(pl.lit("Cuối tuần")).otherwise(None).alias("event_name")
        )

    df_joined = df_joined.with_columns(
        ((pl.col("is_holiday") == 1) | weekend)
        .cast(pl.Int8)
        .alias("is_day_off")
    ).drop("is_holiday", "is_weekend")

    if "duration" in df_joined.columns:
        df_joined = df_joined.drop("duration")

    logger.info(
        f"Join result: {df_joined.shape[0]:,} rows × {df_joined.shape[1]} cols"
    )
    return df_joined


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Transform: left join date dimension với market events."
    )
    p.add_argument("--bucket",    default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true",    help="Ghi đè file output nếu đã tồn tại")
    return p.parse_args()


def log_run_info(args: argparse.Namespace, bucket: str) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nTransform: dim_date LEFT JOIN event → dim_date_event\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source 1       : s3://%s/%s\n"
        "Source 2       : s3://%s/%s\n"
        "Destination    : s3://%s/%s/%s\n"
        "Join key       : %s\n"
        "Overwrite      : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        bucket,
        bucket, SRC_DATE_PATH,
        bucket, SRC_EVENT_PATH,
        bucket, DST_PREFIX, DST_FILENAME,
        JOIN_KEY,
        args.overwrite,
        run_at,
        separator,
    )


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    log_run_info(args, bucket)

    fs = _build_fs()

    df_date  = read_parquet(fs, f"{bucket}/{SRC_DATE_PATH}")
    df_event = read_parquet(fs, f"{bucket}/{SRC_EVENT_PATH}")

    df_result = transform(df_date, df_event)

    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"
    write_parquet(df_result, fs, dst_path, overwrite=args.overwrite)

    separator = "=" * 80
    logger.info("\n%s\ndim_date_event transform complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
