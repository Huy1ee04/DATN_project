#!/usr/bin/env python3
"""
dim_master_index.py

Đưa dữ liệu từ layer transformed (stage_2) lên master (dimension).
Pure SCD Type 1 theo Kimball: overwrite toàn bộ dimension mỗi lần chạy.

Source (MinIO):
  transformed/stage_2/dimension/dim_index_info.parquet

Destination (MinIO):
  master/dimension/dim_master_index.parquet

Logic publish:
  - Đọc dữ liệu đã conform từ stage_2.
  - Surrogate key: `index_key` = `index_id` (đã là meaningless Int64 từ source).
  - SCD Type 1: ghi đè toàn bộ master mỗi lần chạy (full refresh).
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
logger = logging.getLogger("dim_master_index_publish")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PATH = "transformed/stage_2/dimension/dim_index_info.parquet"
DST_PREFIX = "master/dimension"
DST_FILENAME = "dim_master_index.parquet"

# Surrogate key (dùng index_id vì đã là meaningless integer)
SURROGATE_KEY = "index_key"
NATURAL_KEY = "index_id"


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
    logger.info(f"  → {df.shape[0]:,} rows × {df.shape[1]} cols")
    return df


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"Saved s3://{s3_path} ({size_kb:.1f} KB, {df.shape[0]:,} rows)")


# ── Transform: surrogate key ────────────────────────────────────────────────

def add_surrogate_key(df: pl.DataFrame) -> pl.DataFrame:
    """
    Surrogate key cho dim_index: dùng index_id trực tiếp (đã là meaningless integer).
    Đặt index_key làm cột đầu tiên.
    """
    if NATURAL_KEY not in df.columns:
        raise ValueError(f"Cột '{NATURAL_KEY}' không tồn tại — không thể tạo surrogate key.")

    df = df.with_columns(
        pl.col(NATURAL_KEY).cast(pl.Int64).alias(SURROGATE_KEY)
    )

    # Đưa surrogate key lên đầu, bỏ cột natural key gốc (tránh duplicate)
    col_order = [SURROGATE_KEY] + [c for c in df.columns if c not in (SURROGATE_KEY, NATURAL_KEY)]
    df = df.select(col_order)

    logger.info(f"Added surrogate key `{SURROGATE_KEY}` (Int64, = {NATURAL_KEY}).")
    return df


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Publish dim_index_info (stage_2) → master/dimension/dim_master_index (SCD Type 1)"
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true", help="Ghi đè file master nếu đã tồn tại")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    src = f"{bucket}/{SRC_PATH}"
    dst = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nPublish: dim_index_info (stage_2) → master/dimension/dim_master_index\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s\n"
        "Destination    : s3://%s\n"
        "Run at         : %s\n"
        "SCD Type       : Type 1 (full refresh)\n"
        "Surrogate key  : %s (Int64, = %s)\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.bucket,
        src, dst,
        run_at,
        SURROGATE_KEY, NATURAL_KEY,
        separator,
    )

    fs = _build_fs()
    if not fs.exists(src):
        logger.error(f"Source not found: s3://{src}")
        return

    df = read_parquet(fs, src)
    if df.is_empty():
        logger.error("Source is empty — aborting.")
        return

    # 1. Thêm surrogate key
    df = add_surrogate_key(df)

    # 2. Sort theo surrogate key
    df = df.sort(SURROGATE_KEY)

    logger.info(f"Final schema: {df.schema}")
    logger.info(f"Sample:\n{df.head(5)}")

    # ── GX Gate: Star Schema Integrity ─────────────────────────────
    logger.info("Running GX validation (Master: SK unique + not null)...")
    gx_check_column_not_null(df, {"column": "index_key"})
    gx_check_column_values_unique(df, {"column": "index_key"})
    gx_check_table_row_count_between(df, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_parquet(df, fs, dst)  # SCD1: luôn full refresh

    logger.info("\n%s\ndim_master_index publish complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
