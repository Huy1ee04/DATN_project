#!/usr/bin/env python3
"""
dim_sector_info.py (stage_1)

Transform: cleanse sector info từ raw lên transformed/stage_1.

Source (MinIO):
  raw/reference/equity/sector_info.parquet

Output (MinIO):
  transformed/stage_1/dimension/dim_sector_info.parquet

Logic:
  - Ép kiểu dữ liệu (sector_id → Int32, sector → String).
  - Trim whitespace, loại bỏ null/trùng lặp.
  - Validate schema + GX checks.
"""

import argparse
import io
import logging
import os
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

from vtit_gx.polars import (
    gx_check_columns_not_null,
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dim_sector_info_stage1")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PATH = "raw/reference/equity/sector_info.parquet"
DST_PREFIX = "transformed/stage_1/dimension"
DST_FILENAME = "dim_sector_info.parquet"


def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    logger.info("Reading s3://%s ...", s3_path)
    with fs.open(s3_path, "rb") as f:
        df = pl.read_parquet(io.BytesIO(f.read()))
    logger.info("  → %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    return df


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    overwrite: bool = False,
) -> None:
    if fs.exists(s3_path) and not overwrite:
        logger.info("Exists, skipping: s3://%s  (dùng --overwrite để ghi đè)", s3_path)
        return

    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="snappy")
    buffer.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buffer.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info("Saved s3://%s (%.1f KB, %s rows)", s3_path, size_kb, f"{df.shape[0]:,}")


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Chuẩn hóa dữ liệu sector info:
    - Ép kiểu dữ liệu (sector_id → Int32, sector → String)
    - Trim whitespace
    - Loại bỏ null và trùng lặp theo sector_id
    """
    required = {"sector_id", "sector"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"sector info source missing columns: {missing}")

    # Loại bỏ cột ingested_at nếu có (từ ingestion layer)
    drop_cols = [c for c in df.columns if c in ("ingested_at",)]

    return (
        df.drop(drop_cols)
        .select([
            pl.col("sector_id").cast(pl.Int32, strict=False).alias("sector_id"),
            pl.col("sector").cast(pl.Utf8).str.strip_chars().alias("sector"),
        ])
        .drop_nulls(["sector_id", "sector"])
        .unique(subset=["sector_id"])
        .sort("sector_id")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage 1: Transform raw sector_info → dim_sector_info."
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    parser.add_argument("--overwrite", action="store_true", help="Ghi đè output nếu đã tồn tại")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fs = _build_fs()
    src = f"{args.bucket}/{SRC_PATH}"
    dst = f"{args.bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nStage 1: sector_info → dim_sector_info\n%s\n"
        "MinIO Endpoint : %s\n"
        "Source         : s3://%s\n"
        "Destination    : s3://%s\n"
        "Overwrite      : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        src, dst,
        args.overwrite, run_at, separator,
    )

    if not fs.exists(src):
        logger.error("Source not found: s3://%s", src)
        return

    df_raw = read_parquet(fs, src)
    if df_raw.is_empty():
        logger.error("Source is empty — aborting.")
        return

    df_clean = transform(df_raw)

    logger.info("Final: %s rows × %s cols", f"{df_clean.shape[0]:,}", df_clean.shape[1])
    logger.info("Schema: %s", df_clean.schema)
    logger.info("Data:\n%s", df_clean)

    # ── GX Gate ──────────────────────────────────────────────────────────
    logger.info("Running GX validation (Stage 1: PK not-null + Unique + Volume)...")
    gx_check_columns_not_null(df_clean, {"columns": ["sector_id", "sector"]})
    gx_check_column_values_unique(df_clean, {"column": "sector_id"})
    gx_check_table_row_count_between(df_clean, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_parquet(df_clean, fs, dst, overwrite=args.overwrite)
    logger.info("\n%s\ndim_sector_info stage_1 transform complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
