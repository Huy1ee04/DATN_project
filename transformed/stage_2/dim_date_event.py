#!/usr/bin/env python3
"""
dim_date_event (stage_2) — conforming layer.

Đọc dữ liệu đã cleanse từ stage_1, conform tên cột cho đồng nhất
với master/ClickHouse, validate rồi ghi sang stage_2.

Source (MinIO):
  transformed/stage_1/dimension/dim_date_event.parquet

Destination (MinIO):
  transformed/stage_2/dimension/dim_date_event.parquet

Logic:
  - Rename date → trade_date (tránh trùng hàm DATE trong ClickHouse).
  - Select & sort đúng schema target.
  - Validate schema + GX.
"""

import io
import os
import logging
import argparse
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

from vtit_gx.polars import (
    gx_check_column_not_null,
    gx_check_column_values_unique,
    gx_check_table_row_count_between,
)

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
logger = logging.getLogger("dim_date_event_stage2")
ICT = timezone(timedelta(hours=7))

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET   = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PATH     = "transformed/stage_1/dimension/dim_date_event.parquet"
DST_PREFIX   = "transformed/stage_2/dimension"
DST_FILENAME = "dim_date_event.parquet"

# Cột cần có từ stage_1 (trước khi conform)
REQUIRED_STAGE1_COLS = {"date", "cal_year", "cal_week", "cal_month", "cal_quarter",
                        "event_name", "is_day_off"}

# Schema target sau conform (stage_2 / master / ClickHouse)
OUTPUT_COLS = [
    "trade_date",
    "cal_year",
    "cal_quarter",
    "cal_month",
    "cal_week",
    "event_name",
    "is_day_off",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    logger.info(f"Reading s3://{s3_path} ...")
    with fs.open(s3_path, "rb") as f:
        df = pl.read_parquet(io.BytesIO(f.read()))
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


# ── Validate ─────────────────────────────────────────────────────────────────

def validate_schema(df: pl.DataFrame) -> None:
    """Kiểm tra schema stage_1 có đủ cột cần thiết."""
    actual = set(df.columns)
    missing = REQUIRED_STAGE1_COLS - actual
    if missing:
        raise ValueError(
            f"stage_1 dim_date_event thiếu cột: {sorted(missing)}. "
            f"Cột thực tế: {sorted(actual)}"
        )
    logger.info("Schema validation passed ✓")


def conform(df: pl.DataFrame) -> pl.DataFrame:
    """
    Conform stage_1 → stage_2:
      - Rename date → trade_date
      - Select & sort đúng thứ tự OUTPUT_COLS
    """
    df = df.rename({"date": "trade_date"})
    df = df.select(OUTPUT_COLS).sort("trade_date")
    logger.info(f"Conformed: {df.shape[0]:,} rows × {df.shape[1]} cols")
    return df


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Stage 2: passthrough dim_date_event từ stage_1 → stage_2 "
            "(validate schema, giữ nguyên dữ liệu)."
        )
    )
    p.add_argument("--bucket",    default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true",    help="Ghi đè file output nếu đã tồn tại")
    return p.parse_args()


def log_run_info(args: argparse.Namespace, src: str, dst: str) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nStage 2: dim_date_event (passthrough + validate)\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s\n"
        "Destination    : s3://%s\n"
        "Overwrite      : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.bucket,
        src, dst,
        args.overwrite,
        run_at,
        separator,
    )


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    src = f"{bucket}/{SRC_PATH}"
    dst = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"
    log_run_info(args, src, dst)

    fs = _build_fs()

    if not fs.exists(src):
        logger.error(f"Source not found: s3://{src}")
        return

    df = read_parquet(fs, src)
    if df.is_empty():
        logger.error("Source is empty — aborting.")
        return

    # Validate schema từ stage_1
    validate_schema(df)

    # Conform: rename date → trade_date, select target cols
    df = conform(df)

    # ── GX Gate: Business Logic ─────────────────────────────────────────
    logger.info("Running GX validation (Stage 2: Unique PK + Not-null)...")
    gx_check_column_not_null(df, {"column": "trade_date"})
    gx_check_column_values_unique(df, {"column": "trade_date"})
    gx_check_table_row_count_between(df, {"min_value": 1})
    logger.info("GX validation passed ✓")

    # Ghi sang stage_2
    write_parquet(df, fs, dst, overwrite=args.overwrite)

    separator = "=" * 80
    logger.info("\n%s\ndim_date_event stage_2 complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
