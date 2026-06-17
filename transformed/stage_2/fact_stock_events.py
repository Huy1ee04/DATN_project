#!/usr/bin/env python3
"""
fact_stock_events.py (stage_2) — Validate + Passthrough.

Đọc fact_stock_events từ stage_1, validate schema, ghi nguyên xi sang stage_2.

Source (MinIO):
  transformed/stage_1/fact/fact_stock_events.parquet

Destination (MinIO):
  transformed/stage_2/fact/fact_stock_events.parquet
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
    gx_check_columns_not_null,
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
logger = logging.getLogger("fact_stock_events_stage2")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PATH = "transformed/stage_1/fact/fact_stock_events.parquet"
DST_PREFIX = "transformed/stage_2/fact"
DST_FILENAME = "fact_stock_events.parquet"

REQUIRED_COLS = {"symbol", "public_date", "event_name_vi", "event_title_vi", "event_code"}


def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs, s3_path):
    logger.info("Reading s3://%s ...", s3_path)
    with fs.open(s3_path, "rb") as f:
        df = pl.read_parquet(io.BytesIO(f.read()))
    logger.info("  → %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    return df


def write_parquet(df, fs, s3_path, overwrite=True):
    if fs.exists(s3_path) and not overwrite:
        logger.info("Exists, skipping: s3://%s", s3_path)
        return
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info("Saved s3://%s (%.1f KB, %s rows)", s3_path, size_kb, f"{df.shape[0]:,}")


def validate(df):
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Stage_1 thiếu cột: {sorted(missing)}")
    n_before = df.shape[0]
    df = df.drop_nulls(["symbol", "public_date"])
    n_after = df.shape[0]
    if n_before != n_after:
        logger.warning("Dropped %d rows with null symbol/public_date", n_before - n_after)
    logger.info("Schema validation passed ✓ (%s rows)", f"{n_after:,}")
    return df


def parse_args():
    p = argparse.ArgumentParser(description="Stage 2: validate fact_stock_events.")
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    src = f"{args.bucket}/{SRC_PATH}"
    dst = f"{args.bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    logger.info("\n%s\nStage 2: validate fact_stock_events\n%s\nSource: s3://%s\nDest:   s3://%s\n%s",
                separator, separator, src, dst, separator)

    fs = _build_fs()
    if not fs.exists(src):
        logger.error("Source not found: s3://%s", src)
        return

    df = read_parquet(fs, src)
    if df.is_empty():
        logger.error("Source is empty — aborting.")
        return

    df = validate(df)

    # ── GX Gate: Business Logic ─────────────────────────────────────────
    logger.info("Running GX validation (Stage 2: Completeness + Volume)...")
    gx_check_columns_not_null(df, {"columns": ["symbol", "public_date", "event_code"]})
    gx_check_table_row_count_between(df, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_parquet(df, fs, dst, overwrite=args.overwrite)
    logger.info("\n%s\nfact_stock_events stage_2 complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
