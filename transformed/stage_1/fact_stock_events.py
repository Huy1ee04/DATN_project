#!/usr/bin/env python3
"""
fact_stock_events.py

Transform: đọc toàn bộ partition events, chỉ giữ các trường cần thiết.

Source (MinIO):
  raw/reference/company/events/  → ticker, event_name_vi, event_title_vi,
                                     event_code, public_date

Output (MinIO):
  transformed/stage_1/fact/fact_stock_events.parquet
"""

import io
import os
import logging
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional

import polars as pl
import s3fs
from dotenv import load_dotenv

from vtit_gx.polars import (
    gx_check_columns_not_null,
    gx_check_table_row_count_between,
)

# ── Load .env ────────────────────────────────────────────────────────────────
_script_dir = os.path.dirname(os.path.abspath(__file__))
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
logger = logging.getLogger("fact_stock_events_transform")
ICT = timezone(timedelta(hours=7))

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET   = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PREFIX   = "raw/reference/company/events"
DST_PREFIX   = "transformed/stage_1/fact"
DST_FILENAME = "fact_stock_events.parquet"

SELECT_COLS = [
    "ticker",
    "event_name_vi",
    "event_title_vi",
    "event_code",
    "public_date",
]
OUTPUT_COLS = [
    "symbol",
    "event_name_vi",
    "event_title_vi",
    "event_code",
    "public_date",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_partition(
    fs: s3fs.S3FileSystem,
    bucket: str,
    prefix: str,
    select_cols: Optional[list[str]] = None,
) -> pl.DataFrame:
    """Đọc tất cả file .parquet trong prefix (partition), gộp thành 1 DataFrame."""
    pattern      = f"{bucket}/{prefix}/**/*.parquet"
    flat_pattern = f"{bucket}/{prefix}/*.parquet"
    paths = list(dict.fromkeys(fs.glob(pattern) + fs.glob(flat_pattern)))

    if not paths:
        logger.warning(f"No parquet files found at s3://{bucket}/{prefix}")
        return pl.DataFrame()

    logger.info(f"Reading {len(paths)} file(s) from s3://{bucket}/{prefix} ...")
    frames: list[pl.DataFrame] = []
    for path in paths:
        try:
            with fs.open(path, "rb") as f:
                raw = f.read()
            df = pl.read_parquet(io.BytesIO(raw))
            df = df.drop([c for c in ("ingested_at",) if c in df.columns])
            if select_cols:
                available = [c for c in select_cols if c in df.columns]
                missing   = [c for c in select_cols if c not in df.columns]
                if missing:
                    logger.warning(f"  {path}: missing columns {missing}, skipped")
                df = df.select(available)
            frames.append(df)
            logger.info(f"  ✓ {path} → {df.shape[0]:,} rows × {df.shape[1]} cols")
        except Exception as e:
            logger.error(f"  ✗ Cannot read {path}: {e}")

    if not frames:
        return pl.DataFrame()

    combined = pl.concat(frames, how="diagonal").unique()
    logger.info(f"  → Combined: {combined.shape[0]:,} rows × {combined.shape[1]} cols")
    return combined


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    overwrite: bool = True,
) -> None:
    if fs.exists(s3_path) and not overwrite:
        logger.info(f"Exists, skipping: s3://{s3_path}")
        return
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"Saved s3://{s3_path} ({size_kb:.1f} KB, {df.shape[0]:,} rows)")


# ── Transform ─────────────────────────────────────────────────────────────────

def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Rename ticker → symbol, cast public_date về Date, chỉ giữ output columns."""
    missing_cols = sorted(set(SELECT_COLS) - set(df.columns))
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df.rename({"ticker": "symbol"})

    dtype = df["public_date"].dtype
    if dtype == pl.Date:
        pass
    elif dtype in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
        df = df.with_columns(pl.from_epoch(pl.col("public_date"), time_unit="ms").cast(pl.Date))
    elif dtype in (pl.Datetime, pl.Datetime("ms"), pl.Datetime("us"), pl.Datetime("ns")):
        df = df.with_columns(pl.col("public_date").cast(pl.Date))
    elif dtype in (pl.Utf8, pl.String):
        df = df.with_columns(
            pl.col("public_date")
            .cast(pl.Utf8, strict=False)
            .str.slice(0, 10)
            .str.to_date(format="%Y-%m-%d", strict=False)
        )

    return (
        df.select(OUTPUT_COLS)
        .drop_nulls(["symbol"])
        .with_columns(pl.col("symbol").cast(pl.Utf8).str.strip_chars().str.to_uppercase())
        .filter(pl.col("symbol") != "")
        .unique()
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Transform: events → fact_stock_events."
    )
    p.add_argument("--bucket",    default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument(
        "--skip-existing",
        action="store_true",
        help="Bỏ qua nếu output đã tồn tại. Mặc định là ghi đè.",
    )
    return p.parse_args()


def log_run_info(args: argparse.Namespace, bucket: str) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nTransform: events → fact_stock_events\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s/%s/\n"
        "Columns        : %s\n"
        "Destination    : s3://%s/%s/%s\n"
        "Mode           : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        bucket,
        bucket, SRC_PREFIX,
        SELECT_COLS,
        bucket, DST_PREFIX, DST_FILENAME,
        "skip existing" if args.skip_existing else "overwrite",
        run_at,
        separator,
    )


def main() -> None:
    args   = parse_args()
    bucket = args.bucket
    log_run_info(args, bucket)

    fs = _build_fs()

    df = read_partition(fs, bucket, SRC_PREFIX, select_cols=SELECT_COLS)
    if df.is_empty():
        logger.error("Source is empty — aborting.")
        return

    df_result = transform(df)

    # ── GX Gate: Basic quality check after cleaning ───────────────────
    logger.info("Running GX validation (Stage 1: BK not-null + Volume)...")
    gx_check_columns_not_null(df_result, {"columns": ["symbol", "event_code"]})
    gx_check_table_row_count_between(df_result, {"min_value": 1})
    logger.info("GX validation passed ✓")

    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"
    write_parquet(df_result, fs, dst_path, overwrite=not args.skip_existing)

    separator = "=" * 80
    logger.info("\n%s\nfact_stock_events transform complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
